package ratelimiter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Delayed independent backend: the receipt oracle and actual unique deductions
// are separate. All counters and timing samples have a fixed bound.
type capacityBackend struct {
	mu                               sync.Mutex
	models                           map[string]*redisReference
	delay                            time.Duration
	down                             atomic.Bool
	fast                             atomic.Bool
	active, peak, ops, times, probes atomic.Int64
	operationNS, operationMax        atomic.Int64
}

func atomicPeak(p *atomic.Int64, n int64) {
	for old := p.Load(); n > old; old = p.Load() {
		if p.CompareAndSwap(old, n) {
			return
		}
	}
}
func (b *capacityBackend) operation(ctx context.Context) (func(), error) {
	start := time.Now()
	n := b.active.Add(1)
	atomicPeak(&b.peak, n)
	b.ops.Add(1)
	finish := func() {
		d := time.Since(start).Nanoseconds()
		b.operationNS.Add(d)
		atomicPeak(&b.operationMax, d)
		b.active.Add(-1)
	}
	delay := b.delay
	if b.fast.Load() {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		finish()
		return nil, &backendError{class: errorTransient, cause: ctx.Err()}
	case <-timer.C:
	}
	if b.down.Load() {
		finish()
		return nil, transientFailure()
	}
	return finish, nil
}
func (b *capacityBackend) Time(ctx context.Context, key string) (time.Time, error) {
	b.times.Add(1)
	end, err := b.operation(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer end()
	return time.Now(), nil
}
func (b *capacityBackend) Probe(ctx context.Context) error {
	b.probes.Add(1)
	end, err := b.operation(ctx)
	if err == nil {
		end()
	}
	return err
}
func (b *capacityBackend) Close() error { return nil }
func (b *capacityBackend) Execute(ctx context.Context, key string, c command) (reply, error) {
	end, err := b.operation(ctx)
	if err != nil {
		return reply{}, err
	}
	defer end()
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.models[key]
	if m == nil {
		m = &redisReference{rate: c.RateU, burst: c.BurstU}
		b.models[key] = m
	}
	return m.execute(c, time.Now().UnixMilli()), nil
}
func (b *capacityBackend) applied() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for _, m := range b.models {
		for _, v := range m.applied {
			n += v
		}
	}
	return n
}

func TestLoadCapacitySmoke(t *testing.T) {
	t.Run("saturated_fair_drain", TestSchedulerBoundedQueueAndFairDrain)
	// Deterministic barriers in the scheduler test cover a >1024-entry queue.
	// This end-to-end run checks receipt conservation through outage/recovery.
	r := runCapacitySmoke(t, 100, time.Millisecond, 100*time.Millisecond)
	if r.peak > 4 || r.queuePeak > 1024 || r.admitted == 0 || r.applied != r.admitted {
		t.Fatalf("resource/accounting gate: %+v", r)
	}
}

type capacityResult struct {
	keys                                                         int
	delay, initialization, traffic, recovery                     time.Duration
	offered, admitted, applied, ops, times, probes, peak         int64
	reasons                                                      map[string]int64
	queuePeak                                                    int
	queueSampleSpan, scanOverdueMax, operationMean, operationMax time.Duration
}

func (r capacityResult) String() string {
	return fmt.Sprintf("keys=%d delay=%s init=%s traffic=%s offered=%d admitted=%d reasons=%v applied=%d ops=%d TIME=%d PING=%d peak=%d queue_peak=%d queue_sample_span=%s scan_overdue_max=%s operation_mean=%s operation_max=%s recovery=%s", r.keys, r.delay, r.initialization, r.traffic, r.offered, r.admitted, r.reasons, r.applied, r.ops, r.times, r.probes, r.peak, r.queuePeak, r.queueSampleSpan, r.scanOverdueMax, r.operationMean, r.operationMax, r.recovery)
}

func runCapacitySmoke(t testing.TB, keys int, delay, traffic time.Duration) capacityResult {
	t.Helper()
	cfg, err := normalizeConfig(Config{GlobalRate: 10000, GlobalBurst: 10000, LocalRate: 100, LocalBurst: 20, MaxOutstanding: 20, SyncThreshold: 10, SyncInterval: 100 * time.Millisecond, MaxSyncAge: time.Second, MaxKeys: keys, Redis: RedisConfig{Address: "unused:6379", Timeout: 200 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend := &capacityBackend{models: make(map[string]*redisReference, keys), delay: delay}
	l := newLimiterCore(cfg.core(), ctx, realClock{}, backend, nil)
	l.jitter = func(d time.Duration) time.Duration { return d / 2 }
	l.start()
	defer func() { l.cancelWorkers(); <-l.workersDone }()
	buckets := make([]*bucket, keys)
	names := make([]string, keys)
	start := time.Now()
	for i := range names {
		names[i] = fmt.Sprint(i)
		if ok, reason, _ := l.Allow(names[i]); ok || reason != ReasonSyncPending {
			t.Fatalf("cold initialization: %v %s", ok, reason)
		}
		buckets[i], _ = l.store.acquire(names[i], time.Now())
	}
	defer func() {
		for _, b := range buckets {
			l.store.release(b)
		}
	}()
	wait := func(f func() bool) {
		deadline := time.Now().Add(15 * time.Second)
		for !f() {
			if time.Now().After(deadline) {
				t.Fatal("capacity drain deadline")
			}
			time.Sleep(time.Millisecond)
		}
	}
	wait(func() bool {
		for _, b := range buckets {
			b.mu.Lock()
			ready := b.initialized
			b.mu.Unlock()
			if !ready {
				return false
			}
		}
		return true
	})
	r := capacityResult{keys: keys, delay: delay, initialization: time.Since(start), traffic: traffic, reasons: map[string]int64{}}
	// Sample state rather than adding production instrumentation. The queue
	// span is a coarse 1ms proxy, not exact enqueue-to-worker latency: brief
	// dequeue/requeue cycles between samples may merge. Scan overdue is sampled
	// only while healthy, due, neither busy nor queued. Storage is one time/key.
	sampleStop := make(chan struct{})
	sampleDone := make(chan struct{})
	queuedSince := make([]time.Time, keys)
	go func() {
		defer close(sampleDone)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-sampleStop:
				return
			case now := <-tick.C:
				r.queuePeak = max(r.queuePeak, len(l.work))
				if l.control.Load().circuit != circuitHealthy {
					continue
				}
				for i, b := range buckets {
					b.mu.Lock()
					queued, busy, due := b.queued, b.busy, b.nextSyncAt
					b.mu.Unlock()
					if queued {
						if queuedSince[i].IsZero() {
							queuedSince[i] = now
						}
					} else if !queuedSince[i].IsZero() {
						r.queueSampleSpan = max(r.queueSampleSpan, now.Sub(queuedSince[i])-2*time.Millisecond)
						queuedSince[i] = time.Time{}
					}
					if !queued && !busy && !due.IsZero() && now.After(due) {
						r.scanOverdueMax = max(r.scanOverdueMax, now.Sub(due))
					}
				}
			}
		}
	}()
	start = time.Now()
	tick := time.NewTicker(20 * time.Millisecond)
	for time.Since(start) < traffic {
		<-tick.C
		for _, name := range names {
			r.offered++
			ok, reason, _ := l.Allow(name)
			if ok {
				r.admitted++
			} else {
				r.reasons[reason]++
			}
		}
	}
	tick.Stop()
	if l.control.Load().circuit != circuitHealthy {
		t.Fatal("load alone fabricated outage")
	}
	// Bounded outage, then stop offered traffic and require every admitted receipt
	// to reach the independent backend exactly once with no outstanding ledger.
	backend.down.Store(true)
	wait(func() bool {
		return l.control.Load().circuit == circuitOpen || l.control.Load().circuit == circuitProbing
	})
	time.Sleep(100 * time.Millisecond)
	backend.down.Store(false)
	start = time.Now()
	wait(func() bool {
		if l.control.Load().circuit != circuitHealthy {
			return false
		}
		for _, b := range buckets {
			b.mu.Lock()
			done := b.ledger.outstanding() == 0 && b.authorizedGeneration == l.control.Load().generation
			b.mu.Unlock()
			if !done {
				return false
			}
		}
		return true
	})
	r.recovery = time.Since(start)
	close(sampleStop)
	<-sampleDone
	l.cancelWorkers()
	<-l.workersDone
	r.applied = backend.applied()
	r.ops = backend.ops.Load()
	r.times = backend.times.Load()
	r.probes = backend.probes.Load()
	r.peak = backend.peak.Load()
	r.operationMean = time.Duration(backend.operationNS.Load() / max(int64(1), r.ops))
	r.operationMax = time.Duration(backend.operationMax.Load())
	if r.peak > 4 || r.queuePeak > 1024 || r.applied != r.admitted {
		t.Fatalf("capacity invariant: %s", r)
	}
	if keys <= 100 && float64(r.reasons[ReasonSyncPending]+r.reasons[ReasonRedisUnavailable])/float64(r.offered) >= 0.001 {
		t.Fatalf("steady-state capacity gate: %s", r)
	}
	t.Log(r.String())
	return r
}

// capacityHistogram uses 64 power-of-two nanosecond buckets. Quantiles are
// upper bounds (at most 2x a positive observation); zero is included in the 1ns bin.
// Each traffic goroutine owns one histogram, merged only after the join barrier.
type capacityHistogram [64]uint64

func (h *capacityHistogram) add(d time.Duration) { h[bits.Len64(uint64(max(time.Duration(0), d)))]++ }
func (h *capacityHistogram) quantile(percent uint64) time.Duration {
	var total, seen uint64
	for _, n := range h {
		total += n
	}
	if total == 0 {
		return 0
	}
	target := (total*percent + 99) / 100
	for i, n := range h {
		seen += n
		if seen >= target {
			if i >= 63 {
				return time.Duration(math.MaxInt64)
			}
			return time.Duration(uint64(1) << i)
		}
	}
	return 0
}
func TestCapacityHistogram(t *testing.T) {
	var h capacityHistogram
	for _, n := range []time.Duration{0, 1, 2, 3, 4, 100} {
		h.add(n)
	}
	if h.quantile(50) != 4 || h.quantile(95) != 128 || h.quantile(99) != 128 {
		t.Fatal(h)
	}
}

type measuredBackend struct {
	backend
	ops, times, probes, active, peak, elapsed atomic.Int64
	store                                     *bucketStore // Set before workers start; buckets remain retained by the harness.
	timingMu                                  sync.Mutex
	operationTiming, scanTiming               capacityHistogram
	operationSamples, scanSamples             uint64
	scanInitial, scanEarly                    uint64
	nextDeadlines                             map[string]time.Time
}

func (b *measuredBackend) begin() func() {
	start := time.Now()
	b.ops.Add(1)
	atomicPeak(&b.peak, b.active.Add(1))
	return func() {
		elapsed := time.Since(start)
		b.elapsed.Add(elapsed.Nanoseconds())
		b.active.Add(-1)
		b.timingMu.Lock()
		b.operationTiming.add(elapsed)
		b.operationSamples++
		b.timingMu.Unlock()
	}
}
func (b *measuredBackend) Execute(ctx context.Context, key string, c command) (reply, error) {
	// prepareAttempt has already advanced nextSyncAt. Retain that deadline for
	// the NEXT operation, including when this attempt was threshold-triggered.
	// The busy flag stabilizes both timestamps during this adapter call.
	if b.store != nil {
		bucket, _ := b.store.acquire(key, time.Now())
		if bucket != nil {
			bucket.mu.Lock()
			due, queued := bucket.nextSyncAt, bucket.queuedAt
			bucket.mu.Unlock()
			b.store.release(bucket)
			b.recordScanTiming(key, due, queued)
		}
	}
	defer b.begin()()
	return b.backend.Execute(ctx, key, c)
}

// This map is bounded by the harness's retained key population. It describes
// healthy periodic scheduling; outages/cancellation may skip adapter execution.
func (b *measuredBackend) recordScanTiming(key string, next, queued time.Time) {
	b.timingMu.Lock()
	defer b.timingMu.Unlock()
	if b.nextDeadlines == nil {
		b.nextDeadlines = make(map[string]time.Time)
	}
	previous, known := b.nextDeadlines[key]
	b.nextDeadlines[key] = next
	if !known || previous.IsZero() {
		b.scanInitial++
	} else if queued.Before(previous) {
		b.scanEarly++
	} else {
		b.scanTiming.add(queued.Sub(previous))
		b.scanSamples++
	}
}
func TestCapacityScanTimingUsesPreviousDeadline(t *testing.T) {
	b := &measuredBackend{}
	at := func(n int64) time.Time { return time.Unix(0, n) }
	b.recordScanTiming("key", at(100), at(0))   // Initial operation has no preceding deadline.
	b.recordScanTiming("key", at(150), at(50))  // Early work replaces the deadline.
	b.recordScanTiming("key", at(260), at(160)) // 10ns after that replacement deadline.
	if b.scanSamples != 1 || b.scanInitial != 1 || b.scanEarly != 1 || b.scanTiming.quantile(99) != 16 {
		t.Fatalf("scan samples=%d initial=%d early=%d p99=%s", b.scanSamples, b.scanInitial, b.scanEarly, b.scanTiming.quantile(99))
	}
}
func (b *measuredBackend) Time(ctx context.Context, key string) (time.Time, error) {
	b.times.Add(1)
	defer b.begin()()
	return b.backend.Time(ctx, key)
}
func (b *measuredBackend) Probe(ctx context.Context) error {
	b.probes.Add(1)
	defer b.begin()()
	return b.backend.Probe(ctx)
}

type loadPoint struct {
	keys, goroutines              int
	hot                           bool
	delay                         time.Duration
	duration                      time.Duration
	outage, closeDuring, fallback bool
	real                          backend
}
type loadMeasurement struct {
	Keys, Goroutines                                   int
	Hot                                                bool
	Backend                                            string
	DelayNS, WarmupNS, TrafficNS                       int64
	Offered, Normal, Fallback                          int64
	Reasons, Modes                                     map[string]int64
	P50NS, P95NS, P99NS                                int64
	TotalOperations, TotalTimes                        int64
	Operations, Times, Probes, PeakWorkers             int64
	OperationMeanNS                                    int64
	BackendOpsPerSecond, OpsPerNormal, WorkerOccupancy float64
	QueuePeak                                          int
	QueueP95NS, SyncP95NS, ScanOverdueMaxNS            int64
	ScanP99NS, QueueP99NS, OperationP99NS              int64
	ScanSamples, QueueSamples, OperationSamples        uint64
	ScanInitial, ScanEarly                             uint64
	AllocBytes, AllocObjects                           uint64
	CloseNS                                            int64
	RecoveryNS                                         []int64
}

func metricQuantile(m *PrometheusMetrics, name string, percent uint64) int64 {
	value, _ := metricTiming(m, name, percent)
	return value
}
func metricTiming(m *PrometheusMetrics, name string, percent uint64) (int64, uint64) {
	for _, h := range m.histograms {
		if h.name != name {
			continue
		}
		bins := make([]uint64, len(h.bins))
		var count, seen uint64
		for i := range bins {
			bins[i] = h.bins[i].Load()
			count += bins[i]
		}
		if count == 0 {
			return 0, 0
		}
		for i, n := range bins {
			seen += n
			if seen >= (count*percent+99)/100 {
				if i == len(durationBuckets) {
					return -1, count
				}
				return int64(durationBuckets[i] * float64(time.Second)), count
			}
		}
	}
	return 0, 0
}
func capacityConfig(keys int) normalizedConfig {
	cfg, err := normalizeConfig(Config{GlobalRate: 10000, GlobalBurst: 10000, LocalRate: 100, LocalBurst: 20, MaxOutstanding: 20, SyncThreshold: 10, SyncInterval: 100 * time.Millisecond, MaxSyncAge: time.Second, MaxKeys: keys, Redis: RedisConfig{Address: "unused:6379", Timeout: 200 * time.Millisecond}})
	if err != nil {
		panic(err)
	}
	return cfg
}
func runLoadPoint(t testing.TB, p loadPoint) loadMeasurement {
	t.Helper()
	cfg := capacityConfig(p.keys)
	if p.fallback {
		cfg.FailurePolicy = LocalOnly
		cfg.FallbackRate = 20
		cfg.FallbackBurst = 5
	}
	simulated := &capacityBackend{models: make(map[string]*redisReference, p.keys), delay: p.delay}
	var source backend = simulated
	if p.real != nil {
		source = p.real
	}
	measured := &measuredBackend{backend: source}
	metrics := NewPrometheusMetrics()
	l := newLimiterCore(cfg.core(), context.Background(), realClock{}, measured, metrics)
	measured.store = l.store
	l.jitter = func(d time.Duration) time.Duration { return d / 2 }
	l.start()
	t.Cleanup(func() { l.cancelWorkers(); <-l.workersDone })
	names := make([]string, p.keys)
	buckets := make([]*bucket, p.keys)
	for i := range names {
		names[i] = fmt.Sprint(i)
		if ok, reason, _ := l.Allow(names[i]); ok || reason != ReasonSyncPending {
			t.Fatalf("cold: %v %s", ok, reason)
		}
		buckets[i], _ = l.store.acquire(names[i], time.Now())
	}
	defer func() {
		for _, b := range buckets {
			l.store.release(b)
		}
	}()
	r := loadMeasurement{Keys: p.keys, Goroutines: p.goroutines, Hot: p.hot, Backend: "simulated", DelayNS: int64(p.delay), Reasons: map[string]int64{}, Modes: map[string]int64{}}
	if p.real != nil {
		r.Backend = "redis"
	}
	// A fixed, measured warmup bounds even intentionally overloaded cold starts.
	// Cold outcomes above are exactly Keys sync_pending, separate from traffic.
	warm := time.Now()
	time.Sleep(time.Second)
	r.WarmupNS = int64(time.Since(warm))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	trafficStart := time.Now()
	opStart, timeStart, probeStart, elapsedStart := measured.ops.Load(), measured.times.Load(), measured.probes.Load(), measured.elapsed.Load()
	type localResult struct {
		reasons, modes   map[string]int64
		normal, fallback int64
		hist             capacityHistogram
	}
	locals := make([]localResult, p.goroutines)
	var capViolation atomic.Bool
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case now := <-tick.C:
				r.QueuePeak = max(r.QueuePeak, len(l.work))
				for _, b := range buckets {
					b.mu.Lock()
					if b.ledger.outstanding() < 0 || b.ledger.outstanding() > int64(cfg.MaxOutstanding) {
						capViolation.Store(true)
					}
					if !b.queued && !b.busy && !b.nextSyncAt.IsZero() && now.After(b.nextSyncAt) {
						r.ScanOverdueMaxNS = max(r.ScanOverdueMaxNS, int64(now.Sub(b.nextSyncAt)))
					}
					b.mu.Unlock()
				}
			}
		}
	}()
	outageDone := make(chan struct{})
	go func() {
		defer close(outageDone)
		if !p.outage {
			return
		}
		for cycle := 0; cycle < 2; cycle++ {
			time.Sleep(150 * time.Millisecond)
			simulated.down.Store(true)
			deadline := time.Now().Add(5 * time.Second)
			for l.control.Load().circuit == circuitHealthy {
				if time.Now().After(deadline) {
					return
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond)
			simulated.down.Store(false)
			restored := time.Now()
			for !loadReconciled(l, buckets) {
				if time.Since(restored) > 5*time.Second {
					return
				}
				time.Sleep(time.Millisecond)
			}
			r.RecoveryNS = append(r.RecoveryNS, int64(time.Since(restored)))
		}
	}()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range locals {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			result := &locals[g]
			result.reasons = map[string]int64{}
			result.modes = map[string]int64{}
			<-start
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			for round := 0; round < int(p.duration/(20*time.Millisecond)); round++ {
				<-tick.C
				slots := max(p.keys, p.goroutines)
				for i := g; i < slots; i += p.goroutines {
					key := i % p.keys
					if p.hot {
						key = (i + round*slots) / 10 % p.keys
						if (i+round*slots)%10 != 0 {
							key = 0
						}
					}
					begin := time.Now()
					ok, reason, info := l.Allow(names[key])
					result.hist.add(time.Since(begin))
					result.reasons[reason]++
					result.modes[info.Mode]++
					if ok {
						if info.Mode == ModeLocalFallback {
							result.fallback++
						} else {
							result.normal++
						}
					}
				}
			}
		}(g)
	}
	close(start)
	closeResults := make(chan error, 8)
	if p.closeDuring {
		go func() {
			time.Sleep(p.duration / 2)
			for i := 0; i < 8; i++ {
				go func() { closeResults <- l.Close(context.Background()) }()
			}
		}()
	}
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(p.duration + 10*time.Second):
		t.Fatal("load progress watchdog: admission goroutines stalled")
	}
	r.TrafficNS = int64(time.Since(trafficStart))
	r.TotalOperations = measured.ops.Load()
	r.TotalTimes = measured.times.Load()
	r.Operations = measured.ops.Load() - opStart
	r.Times = measured.times.Load() - timeStart
	r.Probes = measured.probes.Load() - probeStart
	elapsed := measured.elapsed.Load() - elapsedStart
	r.OperationMeanNS = elapsed / max(int64(1), r.Operations)
	r.BackendOpsPerSecond = float64(r.Operations) / time.Duration(r.TrafficNS).Seconds()
	r.WorkerOccupancy = float64(elapsed) / float64(r.TrafficNS)
	runtime.ReadMemStats(&after)
	r.AllocBytes = after.TotalAlloc - before.TotalAlloc
	r.AllocObjects = after.Mallocs - before.Mallocs
	close(stopSampler)
	<-samplerDone
	<-outageDone
	var hist capacityHistogram
	for _, v := range locals {
		r.Normal += v.normal
		r.Fallback += v.fallback
		for k, n := range v.reasons {
			r.Reasons[k] += n
			r.Offered += n
		}
		for k, n := range v.modes {
			r.Modes[k] += n
		}
		for i, n := range v.hist {
			hist[i] += n
		}
	}
	r.P50NS = int64(hist.quantile(50))
	r.P95NS = int64(hist.quantile(95))
	r.P99NS = int64(hist.quantile(99))
	r.PeakWorkers = measured.peak.Load()
	r.OpsPerNormal = float64(r.Operations) / float64(max(int64(1), r.Normal))
	r.QueueP95NS = metricQuantile(metrics, MetricQueueDelay, 95)
	r.SyncP95NS = metricQuantile(metrics, MetricSyncDuration, 95)
	r.QueueP99NS, r.QueueSamples = metricTiming(metrics, MetricQueueDelay, 99)
	measured.timingMu.Lock()
	r.ScanP99NS = int64(measured.scanTiming.quantile(99))
	r.ScanSamples = measured.scanSamples
	r.ScanInitial, r.ScanEarly = measured.scanInitial, measured.scanEarly
	r.OperationP99NS = int64(measured.operationTiming.quantile(99))
	r.OperationSamples = measured.operationSamples
	measured.timingMu.Unlock()
	if p.outage && len(r.RecoveryNS) != 2 {
		t.Fatal("outage recovery watchdog")
	}
	if !p.outage && !p.closeDuring && l.control.Load().circuit != circuitHealthy {
		t.Fatal("healthy load fabricated outage")
	}
	// Reduce simulator delay only AFTER the measured traffic window. Fast cleanup
	// is not a recovery-performance result. Close joins before flushing; its return
	// is the accounting snapshot barrier, including concurrent Close callers.
	simulated.fast.Store(true)
	closed := time.Now()
	if p.closeDuring {
		for i := 0; i < 8; i++ {
			if err := <-closeResults; err != nil {
				t.Fatal(err)
			}
		}
	} else if err := l.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.CloseNS = int64(time.Since(closed))
	for _, b := range buckets {
		b.mu.Lock()
		n := b.ledger.outstanding()
		b.mu.Unlock()
		if n != 0 {
			t.Fatalf("undrained ledger %d", n)
		}
	}
	if p.real == nil && simulated.applied() != r.Normal {
		t.Fatalf("conservation applied=%d normal=%d", simulated.applied(), r.Normal)
	}
	if capViolation.Load() || r.PeakWorkers > 4 || r.QueuePeak > 1024 || r.Normal == 0 {
		t.Fatalf("resource/progress gate: %+v", r)
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("CAPACITY " + string(data))
	return r
}

func TestLoadConcurrent(t *testing.T) {
	for _, p := range []loadPoint{{keys: 100, goroutines: 16, duration: 400 * time.Millisecond}, {keys: 10000, goroutines: 16, duration: 400 * time.Millisecond}, {keys: 1, goroutines: 16, delay: time.Millisecond, duration: 2 * time.Second, outage: true}, {keys: 1, goroutines: 16, delay: time.Millisecond, duration: 2 * time.Second, outage: true, fallback: true}, {keys: 100, goroutines: 16, duration: 400 * time.Millisecond, closeDuring: true}} {
		t.Run(fmt.Sprintf("keys%d_outage%v_close%v_fallback%v", p.keys, p.outage, p.closeDuring, p.fallback), func(t *testing.T) { runLoadPoint(t, p) })
	}
}
func TestLoadCapacityMatrix(t *testing.T) {
	if os.Getenv("RUN_CAPACITY") != "1" {
		t.Skip("set RUN_CAPACITY=1 for the bounded 48-cell measurement matrix")
	}
	for _, keys := range []int{1, 100, 1000, 10000} {
		for _, hot := range []bool{false, true} {
			for _, g := range []int{1, 16} {
				for _, delay := range []time.Duration{200 * time.Microsecond, time.Millisecond, 10 * time.Millisecond} {
					t.Run(fmt.Sprintf("K%d_hot%v_G%d_D%s", keys, hot, g, delay), func(t *testing.T) {
						runLoadPoint(t, loadPoint{keys: keys, goroutines: g, hot: hot, delay: delay, duration: time.Second})
					})
				}
			}
		}
	}
}
func TestLoadSupportedPoint(t *testing.T) {
	if os.Getenv("RUN_CAPACITY") != "1" {
		t.Skip("set RUN_CAPACITY=1 for sustained supported workload measurement")
	}
	r := runLoadPoint(t, loadPoint{keys: 100, goroutines: 16, delay: time.Millisecond, duration: 5 * time.Second})
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"ScanP99NS", "ScanSamples", "QueueP99NS", "QueueSamples", "OperationP99NS", "OperationSamples"} {
		value, ok := fields[field].(float64)
		if !ok || value <= 0 {
			t.Errorf("missing nonempty component timing %s: %v", field, fields[field])
		}
	}
	if r.ScanSamples+r.ScanInitial+r.ScanEarly > r.OperationSamples+uint64(r.PeakWorkers) || r.ScanInitial != uint64(r.Keys) {
		t.Fatalf("inconsistent component sample counts: %+v", r)
	}
	if float64(r.Reasons[ReasonSyncPending]+r.Reasons[ReasonRedisUnavailable]+r.Reasons[ReasonCapacityExhausted]+r.Fallback)/float64(r.Offered) >= .001 {
		t.Fatalf("capacity/freshness target missed: %+v", r)
	}
}
func TestLoadRealRedis(t *testing.T) {
	if os.Getenv("RUN_CAPACITY") != "1" {
		t.Skip("set RUN_CAPACITY=1 for real Redis capacity baseline")
	}
	client, prefix := redisFixture(t)
	cfg := capacityConfig(100)
	cfg.Redis.Address = ""
	cfg.Redis.Client = client
	cfg.Redis.Prefix = prefix
	source, err := newRedisBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for i := 0; i < 100; i++ {
			client.Del(ctx, redisKey(prefix, fmt.Sprint(i)))
		}
	}()
	runLoadPoint(t, loadPoint{keys: 100, goroutines: 16, duration: 5 * time.Second, real: source})
}

func TestCapacityMemory(t *testing.T) {
	if os.Getenv("RUN_CAPACITY") != "1" {
		t.Skip("set RUN_CAPACITY=1 for heap measurements")
	}
	for _, state := range []string{"clean", "unresolved", "rejected"} {
		for _, size := range []int{16, 1024} {
			t.Run(fmt.Sprintf("%s_key%d", state, size), func(t *testing.T) {
				runtime.GC()
				var before, base, after runtime.MemStats
				runtime.ReadMemStats(&before)
				cfg := capacityConfig(10000)
				l := newLimiterCore(cfg.core(), context.Background(), realClock{}, nil, nil)
				runtime.GC()
				runtime.ReadMemStats(&base)
				for i := 0; i < 10000; i++ {
					key := fmt.Sprintf("%0*d", size, i)
					b, _ := l.store.acquire(key, time.Now())
					if state == "unresolved" {
						b.ledger.Pending = 10
						b.ledger.Flight = &batch{Sequence: 1, Delta: 10}
					}
					if state == "rejected" {
						b.globalRejected = true
					}
					l.store.release(b)
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				t.Logf("HEAP state=%s key_bytes=%d keys=10000 empty_limiter_bytes=%d retained_bytes=%d bytes_per_key=%.2f allocated_bytes=%d", state, size, int64(base.HeapAlloc)-int64(before.HeapAlloc), int64(after.HeapAlloc)-int64(base.HeapAlloc), float64(int64(after.HeapAlloc)-int64(base.HeapAlloc))/10000, after.TotalAlloc-base.TotalAlloc)
				if directory := os.Getenv("CAPACITY_HEAP_DIR"); directory != "" {
					file, err := os.Create(filepath.Join(directory, fmt.Sprintf("%s-%d.pprof", state, size)))
					if err != nil {
						t.Fatal(err)
					}
					if err = pprof.WriteHeapProfile(file); err != nil {
						t.Fatal(err)
					}
					if err = file.Close(); err != nil {
						t.Fatal(err)
					}
				}
				runtime.KeepAlive(l)
			})
		}
	}
}
func TestCapacityRedisMemory(t *testing.T) {
	if os.Getenv("RUN_CAPACITY") != "1" {
		t.Skip("set RUN_CAPACITY=1 for Redis memory measurements")
	}
	client, key := redisFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := initCommand(t, client)
	var first int64
	for i := 1; i <= 256; i++ {
		c.Session = fmt.Sprintf("%032x", i)
		r := evalProtocol(t, client, key, redisLua, c)
		if r.Outcome != outcomeOK {
			t.Fatal(r)
		}
		if i == 1 || i == 32 || i == 256 {
			n, err := client.MemoryUsage(ctx, key, 0).Result()
			if err != nil {
				t.Fatal(err)
			}
			if i == 1 {
				first = n
			}
			encoding, err := client.ObjectEncoding(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("REDIS_MEMORY sessions=%d bytes=%d extra_over_one=%d encoding=%s", i, n, n-first, encoding)
		}
	}
}

func loadReconciled(l *Limiter, buckets []*bucket) bool {
	s := l.control.Load()
	if s.circuit != circuitHealthy {
		return false
	}
	for _, b := range buckets {
		b.mu.Lock()
		ready := b.initialized && !b.fallback && b.authorizedGeneration == s.generation
		b.mu.Unlock()
		if !ready {
			return false
		}
	}
	return true
}
