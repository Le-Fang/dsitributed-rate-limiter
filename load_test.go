package ratelimiter

import (
	"context"
	"fmt"
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
	timer := time.NewTimer(b.delay)
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
