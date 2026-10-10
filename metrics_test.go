package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func scrapeMetrics(m *PrometheusMetrics) string {
	r := httptest.NewRecorder()
	m.ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	return r.Body.String()
}
func TestMetricsBoundedAndHistogram(t *testing.T) {
	m := NewPrometheusMetrics()
	before := scrapeMetrics(m)
	for i := 0; i < 10000; i++ {
		m.AddCounter("rate_limit_allowed_total", Labels{Tier: "local", Reason: fmt.Sprint(i)}, 1)
		m.SetGauge(fmt.Sprint(i), 1)
	}
	if after := scrapeMetrics(m); after != before {
		t.Fatal("unknown labels/names changed bounded collector")
	}
	m.AddCounter("rate_limit_allowed_total", Labels{Tier: TierLocal, Reason: ReasonAllowed}, 2)
	m.Observe("rate_limit_sync_duration_seconds", .0005)
	m.Observe("rate_limit_sync_duration_seconds", 6)
	s := scrapeMetrics(m)
	for _, want := range []string{`rate_limit_allowed_total{tier="local",reason="allowed"} 2`, `rate_limit_sync_duration_seconds_bucket{le="0.0001"} 0`, `rate_limit_sync_duration_seconds_bucket{le="0.0005"} 1`, `rate_limit_sync_duration_seconds_bucket{le="+Inf"} 2`, `rate_limit_sync_duration_seconds_count 2`, `rate_limit_sync_duration_seconds_sum 6.0005`} {
		if !strings.Contains(s, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
}
func TestMetricsConcurrentScrapes(t *testing.T) {
	m := NewPrometheusMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.AddCounter("rate_limit_allowed_total", Labels{Tier: TierLocal, Reason: ReasonAllowed}, 1)
				m.Observe("rate_limit_sync_duration_seconds", .01)
				scrapeMetrics(m)
			}
		}()
	}
	wg.Wait()
	if !strings.Contains(scrapeMetrics(m), `rate_limit_allowed_total{tier="local",reason="allowed"} 800`) {
		t.Fatal("lost decisions")
	}
}

type panicMetrics struct{}

func (panicMetrics) AddCounter(string, Labels, uint64) { panic("broken exporter") }
func (panicMetrics) SetGauge(string, float64)          { panic("broken exporter") }
func (panicMetrics) Observe(string, float64)           { panic("broken exporter") }
func TestMetricsCallbackPanicIsolation(t *testing.T) {
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = panicMetrics{}
	l, _, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.sampleMetrics()
	l.observeDuration(MetricSyncDuration, .1)
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonAllowed {
		t.Fatal(ok, r)
	}
}

func metricValue(t *testing.T, m *PrometheusMetrics, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrapeMetrics(m), "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, name+" "), 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("missing metric %s", name)
	return 0
}
func TestMetricsLazyFallbackDurationsAndTransitions(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	c.Advance(200 * time.Millisecond)
	l.recordFailure(transientFailure())
	l.recordFailure(transientFailure())
	c.Advance(300 * time.Millisecond)
	l.sampleMetrics()
	if got := metricValue(t, m, "rate_limit_fallback_keys"); got != 1 {
		t.Fatal("lazy keys", got)
	}
	if got := metricValue(t, m, "rate_limit_fallback_key_seconds_total"); math.Abs(got-.3) > 1e-9 {
		t.Fatal("lazy seconds", got)
	}
	if got := metricValue(t, m, "rate_limit_circuit_transitions_total"); got != 1 {
		t.Fatal("duplicate circuit transitions", got)
	}
	if !l.beginProbe() {
		t.Fatal("probe")
	}
	l.finishProbe(nil)
	c.Advance(200 * time.Millisecond)
	a, ok := l.prepareAttempt(b, c.Now())
	if !ok {
		t.Fatal("prepare")
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 20e6, SessionTTLMS: 86400000})
	l.sampleMetrics()
	l.sampleMetrics()
	for name, want := range map[string]float64{"rate_limit_fallback_keys": 0, "rate_limit_fallback_key_seconds_total": .5, "rate_limit_fallback_transitions_total": 2, "rate_limit_circuit_transitions_total": 3} {
		if got := metricValue(t, m, name); math.Abs(got-want) > 1e-9 {
			t.Errorf("%s=%v want %v", name, got, want)
		}
	}
	c.Advance(2 * time.Second)
	l.sampleMetrics()
	if got := metricValue(t, m, "rate_limit_fallback_key_seconds_total"); math.Abs(got-1.5) > 1e-9 {
		t.Fatal("freshness duration", got)
	}
	if got := metricValue(t, m, "rate_limit_freshness_expiries_total"); got != 1 {
		t.Fatal("expiry count", got)
	}
	l.recordFailure(context.Canceled)
	c.Advance(5 * time.Second)
	l.sampleMetrics()
	if got := metricValue(t, m, "rate_limit_fallback_key_seconds_total"); math.Abs(got-1.5) > 1e-9 {
		t.Fatal("closed duration", got)
	}
	if got := metricValue(t, m, "rate_limit_fallback_keys"); got != 0 {
		t.Fatal("closed keys", got)
	}
}
func TestMetricsOutstandingAgeAfterPartialACK(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.Allow("k")
	a, _ := l.prepareAttempt(b, c.Now())
	c.Advance(100 * time.Millisecond)
	l.Allow("k")
	c.Advance(100 * time.Millisecond)
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 20e6, SessionTTLMS: 86400000})
	l.sampleMetrics()
	if got := metricValue(t, m, "rate_limit_normal_outstanding_tokens"); got != 1 {
		t.Fatal(got)
	}
	if got := metricValue(t, m, "rate_limit_normal_oldest_pending_seconds"); math.Abs(got-.1) > 1e-9 {
		t.Fatal(got)
	}
}
func TestMetricsHandlerOwnership(t *testing.T) {
	for _, custom := range []bool{false, true} {
		cfg := Config{GlobalRate: 100, GlobalBurst: 32, Redis: RedisConfig{Address: "127.0.0.1:1"}}
		if custom {
			cfg.Metrics = panicMetrics{}
		}
		l, err := NewLimiter(cfg)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRecorder()
		l.MetricsHandler().ServeHTTP(r, httptest.NewRequest("GET", "/", nil))
		want := 200
		if custom {
			want = 404
		}
		if r.Code != want {
			t.Fatal(r.Code, want)
		}
		if err := l.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMetricsRepeatedClosedSamplingAndMissedEpochs(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	c.Advance(100 * time.Millisecond)
	l.recordFailure(transientFailure())
	l.beginProbe()
	l.finishProbe(nil)
	c.Advance(100 * time.Millisecond)
	l.recordFailure(transientFailure())
	l.beginProbe()
	l.finishProbe(nil)
	c.Advance(100 * time.Millisecond)
	l.recordFailure(context.Canceled)
	l.sampleMetrics()
	l.sampleMetrics()
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackSeconds); math.Abs(got-.2) > 1e-9 {
		t.Fatal(got)
	}
	if got := metricValue(t, m, MetricFallbackTransitions); got != 2 {
		t.Fatal("repeated closed sampling created transitions", got)
	}
}
func TestMetricsSchedulerQueueAndEviction(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	cfg.IdleTTL = time.Millisecond
	l, c, _ := newTestLimiter(t, cfg)
	l.Allow("k")
	l.Allow("k")
	l.sampleMetrics()
	if got := metricValue(t, m, MetricNotificationsCoalesced); got != 1 {
		t.Fatal("coalesced", got)
	}
	if got := metricValue(t, m, MetricQueueDepth); got != 1 {
		t.Fatal("depth", got)
	}
	l.drainWork()
	l.recordFailure(transientFailure())
	c.Advance(2 * time.Second)
	l.scanDue(c.Now())
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackSeconds); got != 2 {
		t.Fatal("eviction lost duration", got)
	}
	if got := metricValue(t, m, MetricFallbackKeys); got != 0 {
		t.Fatal("evicted key counted", got)
	}
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackSeconds); got != 2 {
		t.Fatal("counter regressed", got)
	}
}

func TestMetricsFreshNegativeReplyDoesNotStartFallback(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	c.Advance(200 * time.Millisecond)
	a, _ := l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 0, SessionTTLMS: 86400000})
	c.Advance(100 * time.Millisecond)
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackSeconds); got != 0 {
		t.Fatal("fresh negative balance activated fallback", got)
	}
}

type lockCheckingMetrics struct {
	l     *Limiter
	t     *testing.T
	calls atomic.Int64
}

func (m *lockCheckingMetrics) check() {
	if m.l == nil {
		return
	}
	m.calls.Add(1)
	l := m.l
	if !l.observations.transitionMu.TryLock() {
		m.t.Error("callback under observation transition lock")
	} else {
		l.observations.transitionMu.Unlock()
	}
	if !l.circuitMu.TryLock() {
		m.t.Error("callback under circuit lock")
	} else {
		l.circuitMu.Unlock()
	}
	if !l.enqueueMu.TryLock() {
		m.t.Error("callback under enqueue lock")
	} else {
		l.enqueueMu.Unlock()
	}
	if !l.observations.sampleMu.TryLock() {
		m.t.Error("callback under sample lock")
	} else {
		l.observations.sampleMu.Unlock()
	}
	for i := range l.store.shards {
		sh := &l.store.shards[i]
		if !sh.mu.TryLock() {
			m.t.Error("callback under shard lock")
			continue
		}
		for _, b := range sh.keys {
			if !b.mu.TryLock() {
				m.t.Error("callback under bucket lock")
			} else {
				b.mu.Unlock()
			}
		}
		sh.mu.Unlock()
	}
}
func (m *lockCheckingMetrics) AddCounter(string, Labels, uint64) { m.check() }
func (m *lockCheckingMetrics) SetGauge(string, float64)          { m.check() }
func (m *lockCheckingMetrics) Observe(string, float64)           { m.check() }
func TestMetricsCallbacksOutsideLocks(t *testing.T) {
	m := &lockCheckingMetrics{t: t}
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	m.l = l
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.Allow("k")
	l.Allow("k")
	l.notify(b)
	l.notify(b)
	c.Advance(time.Second)
	l.recordFailure(transientFailure())
	l.Allow("k")
	l.beginProbe()
	l.finishProbe(nil)
	a, _ := l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: a.batch.Sequence, BalanceU: 20e6, SessionTTLMS: 86400000})
	l.sampleMetrics()
	if m.calls.Load() == 0 {
		t.Fatal("no callbacks")
	}
}
func TestMetricsSchedulerOccupancyDurationAndSaturation(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = m
	cfg.MaxKeys = 2000
	l, c, backend := newTestLimiter(t, cfg)
	for i := 0; i < 1025; i++ {
		l.Allow(fmt.Sprint(i))
	}
	l.sampleMetrics()
	if got := metricValue(t, m, MetricNotificationsDropped); got != 1 {
		t.Fatal("dropped", got)
	}
	if got := metricValue(t, m, MetricQueueDepth); got != 1024 {
		t.Fatal("queue", got)
	}
	l.drainWork()
	b := testBucket(t, l, "0")
	a, _ := l.prepareAttempt(b, c.Now())
	done := make(chan struct{})
	go func() { l.runAttempt(context.Background(), b, a); close(done) }()
	deadline := time.Now().Add(time.Second)
	for len(backend.queued()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("backend never called")
		}
		time.Sleep(time.Millisecond)
	}
	l.sampleMetrics()
	if got := metricValue(t, m, MetricWorkers); got != 1 {
		t.Fatal("busy", got)
	}
	c.Advance(50 * time.Millisecond)
	backend.deliver(backend.queued()[0], reply{Outcome: outcomeOK, Slot: 0, BalanceU: 20e6, SessionTTLMS: 86400000}, nil)
	<-done
	if got := metricValue(t, m, MetricWorkers); got != 0 {
		t.Fatal("idle", got)
	}
	if got := metricValue(t, m, MetricSyncDuration+"_sum"); got != .05 {
		t.Fatal("duration", got)
	}
}

func TestMetricsTenThousandApplicationKeys(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = m
	cfg.MaxKeys = 10000
	l, _, _ := newTestLimiter(t, cfg)
	before := strings.Count(scrapeMetrics(m), "\n")
	for i := 0; i < 10000; i++ {
		l.Allow(fmt.Sprintf("private-application-key-%d", i))
	}
	l.sampleMetrics()
	s := scrapeMetrics(m)
	if strings.Count(s, "\n") != before || strings.Contains(s, "private-application-key") {
		t.Fatal("application keys grew exposition cardinality")
	}
	if !strings.Contains(s, `rate_limit_rejected_total{tier="local",reason="sync_pending"} 10000`) {
		t.Fatal("decisions not counted exactly once")
	}
}
func TestMetricsClosedBeforeFreshnessDoesNotExpire(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	c.Advance(100 * time.Millisecond)
	l.recordFailure(context.Canceled)
	c.Advance(2 * time.Second)
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFreshnessExpiries); got != 0 {
		t.Fatal("post-close expiry", got)
	}
}
func TestMetricsWorkerQueueDelay(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = m
	l, c, backend := newTestLimiter(t, cfg)
	l.Allow("k")
	c.Advance(100 * time.Millisecond)
	l.start()
	deadline := time.Now().Add(time.Second)
	for len(backend.queued()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("worker did not run")
		}
		time.Sleep(time.Millisecond)
	}
	if got := metricValue(t, m, MetricQueueDelay+"_sum"); got != .1 {
		t.Fatal("queue wait", got)
	}
	if err := l.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type metricsClosingContext struct {
	context.Context
	calls atomic.Int64
}

func (c *metricsClosingContext) Err() error {
	if c.calls.Add(1) >= 3 {
		return context.Canceled
	}
	return nil
}
func TestMetricsCancellationAfterGlobalDecisionCounted(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	a, _ := l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 0, SessionTTLMS: 86400000})
	l.parent = &metricsClosingContext{Context: context.Background()}
	ok, reason, info := l.Allow("k")
	if ok || reason != ReasonClosed {
		t.Fatal(ok, reason)
	}
	name := fmt.Sprintf("%s{tier=%q,reason=%q}", MetricRejected, info.Tier, reason)
	if got := metricValue(t, m, name); got != 1 {
		t.Fatal("lost cancellation decision", got)
	}
}

func TestMetricsFinalFlushErrorsAreObserved(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(FailClosed)
	cfg.Metrics = m
	l, c, backend := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.Allow("k")
	done := make(chan error, 1)
	go func() { done <- l.flushBucket(context.Background(), b) }()
	deadline := time.Now().Add(time.Second)
	for len(backend.queued()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("flush did not start")
		}
		time.Sleep(time.Millisecond)
	}
	c.Advance(time.Millisecond)
	backend.deliver(backend.queued()[0], reply{}, &backendError{class: errorPermanent, cause: errors.New("secret")})
	if <-done == nil {
		t.Fatal("missing error")
	}
	if got := metricValue(t, m, MetricRedisErrors); got != 1 {
		t.Fatal("unobserved final error", got)
	}
}

// Cold keys which never execute Allow during an outage do not latch fallback.
// Their pre-freshness exposure ends at recovery, independently of scrape timing.
func TestMetricsColdOutagesIndependentOfSampling(t *testing.T) {
	for _, sampleDuring := range []bool{false, true} {
		t.Run(fmt.Sprint(sampleDuring), func(t *testing.T) {
			m := NewPrometheusMetrics()
			cfg := fallbackConfig(LocalOnly)
			cfg.Metrics = m
			l, c, _ := newTestLimiter(t, cfg)
			l.Allow("cold")
			c.Advance(100 * time.Millisecond)
			l.recordFailure(transientFailure())
			c.Advance(100 * time.Millisecond)
			if sampleDuring {
				l.sampleMetrics()
			}
			c.Advance(100 * time.Millisecond)
			if !l.beginProbe() {
				t.Fatal("probe")
			}
			l.finishProbe(nil)
			c.Advance(100 * time.Millisecond)
			l.sampleMetrics()
			for name, want := range map[string]float64{MetricFallbackSeconds: .2, MetricFallbackKeys: 0, MetricFallbackTransitions: 2} {
				if got := metricValue(t, m, name); math.Abs(got-want) > 1e-9 {
					t.Errorf("first cycle %s=%v want %v", name, got, want)
				}
			}
			for i := 0; i < 2; i++ {
				c.Advance(50 * time.Millisecond)
				l.recordFailure(transientFailure())
				if sampleDuring {
					l.sampleMetrics()
				}
				c.Advance(50 * time.Millisecond)
				l.beginProbe()
				l.finishProbe(nil)
			}
			c.Advance(100 * time.Millisecond)
			l.sampleMetrics()
			l.sampleMetrics()
			for name, want := range map[string]float64{MetricFallbackSeconds: .3, MetricFallbackKeys: 0, MetricFallbackTransitions: 6} {
				if got := metricValue(t, m, name); math.Abs(got-want) > 1e-9 {
					t.Errorf("repeated cycles %s=%v want %v", name, got, want)
				}
			}
			if ok, r, _ := l.Allow("cold"); ok || r != ReasonSyncPending {
				t.Fatal("sampling latched cold fallback", ok, r)
			}
		})
	}
}
func TestMetricsColdOutageCutoffCrossing(t *testing.T) {
	for _, sampleDuring := range []bool{false, true} {
		t.Run(fmt.Sprint(sampleDuring), func(t *testing.T) {
			m := NewPrometheusMetrics()
			cfg := fallbackConfig(LocalOnly)
			cfg.Metrics = m
			l, c, _ := newTestLimiter(t, cfg)
			l.Allow("cold")
			c.Advance(200 * time.Millisecond)
			l.recordFailure(transientFailure())
			c.Advance(200 * time.Millisecond)
			l.beginProbe()
			l.finishProbe(nil)
			c.Advance(400 * time.Millisecond)
			l.recordFailure(transientFailure())
			if sampleDuring {
				l.sampleMetrics()
			}
			c.Advance(400 * time.Millisecond)
			l.beginProbe()
			l.finishProbe(nil)
			c.Advance(300 * time.Millisecond)
			l.sampleMetrics()
			for name, want := range map[string]float64{MetricFallbackSeconds: .9, MetricFallbackKeys: 1, MetricFallbackTransitions: 3} {
				if got := metricValue(t, m, name); math.Abs(got-want) > 1e-9 {
					t.Errorf("cutoff %s=%v want %v", name, got, want)
				}
			}
			c.Advance(100 * time.Millisecond)
			l.recordFailure(transientFailure())
			c.Advance(100 * time.Millisecond)
			l.beginProbe()
			l.finishProbe(nil)
			c.Advance(300 * time.Millisecond)
			l.sampleMetrics()
			if got := metricValue(t, m, MetricFallbackSeconds); math.Abs(got-1.4) > 1e-9 {
				t.Fatal("post-cutoff cycles double counted", got)
			}
			if got := metricValue(t, m, MetricFallbackTransitions); got != 3 {
				t.Fatal("post-cutoff recovery exited fallback", got)
			}
			if err := l.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := metricValue(t, m, MetricFallbackTransitions); got != 4 {
				t.Fatal("closure did not end continuous fallback", got)
			}
		})
	}
}

func TestMetricsColdReinitializationDoesNotReplayExposure(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	c.Advance(2 * time.Second)
	l.sampleMetrics()
	a, ok := l.prepareAttempt(b, c.Now())
	if !ok {
		t.Fatal("prepare")
	}
	l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
	c.Advance(time.Second)
	l.sampleMetrics()
	for name, want := range map[string]float64{MetricFallbackSeconds: 2, MetricFallbackKeys: 1, MetricFallbackTransitions: 1} {
		if got := metricValue(t, m, name); got != want {
			t.Errorf("reinitialized %s=%v want %v", name, got, want)
		}
	}
	initializeTestBucket(t, l, b, 20e6)
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackTransitions); got != 2 {
		t.Fatal("successful reinit did not exit once", got)
	}
}
func TestMetricsColdActualAdmissionLatchesFallback(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	l.Allow("cold")
	c.Advance(100 * time.Millisecond)
	l.recordFailure(transientFailure())
	c.Advance(100 * time.Millisecond)
	if ok, r, _ := l.Allow("cold"); !ok || r != ReasonLocalFallback {
		t.Fatal(ok, r)
	}
	c.Advance(100 * time.Millisecond)
	l.beginProbe()
	l.finishProbe(nil)
	c.Advance(200 * time.Millisecond)
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackSeconds); math.Abs(got-.4) > 1e-9 {
		t.Fatal("latched fallback ended at circuit recovery", got)
	}
	if got := metricValue(t, m, MetricFallbackKeys); got != 1 {
		t.Fatal("lost actual fallback", got)
	}
}

// A held bucket blocks the post-publication sweep. Recovery/closure and births
// must queue behind that sweep rather than replacing its sole preceding interval.
func TestMetricsColdTransitionOrdering(t *testing.T) {
	for _, closeLimiter := range []bool{false, true} {
		t.Run(fmt.Sprint(closeLimiter), func(t *testing.T) {
			m := NewPrometheusMetrics()
			cfg := fallbackConfig(LocalOnly)
			cfg.Metrics = m
			l, c, _ := newTestLimiter(t, cfg)
			l.Allow("cold")
			b := testBucket(t, l, "cold")
			otherKey := "other"
			for shardIndex(otherKey) == shardIndex("cold") {
				otherKey += "x"
			}
			other := testBucket(t, l, otherKey)
			other.terminalReason = ReasonAccountingError
			other.terminalAt = c.Now()
			l.sampleMetrics()
			b.mu.Lock()
			c.Advance(100 * time.Millisecond)
			opened := make(chan struct{})
			go func() { l.recordFailure(transientFailure()); close(opened) }()
			eventually(t, func() bool { return l.control.Load().circuit == circuitOpen })
			c.Advance(100 * time.Millisecond)
			started := make(chan struct{})
			finished := make(chan struct{})
			go func() {
				close(started)
				if closeLimiter {
					_ = l.Close(context.Background())
				} else {
					if l.beginProbe() {
						l.finishProbe(nil)
					}
				}
				close(finished)
			}()
			<-started
			// Existing-key lookup is independent of the observation transition gate.
			existing, ok := l.store.acquire(otherKey, c.Now())
			if !ok {
				t.Fatal("existing key unavailable")
			}
			l.store.release(existing)
			b.mu.Unlock()
			<-opened
			<-finished
			c.Advance(200 * time.Millisecond)
			l.sampleMetrics()
			for name, want := range map[string]float64{MetricFallbackSeconds: .1, MetricFallbackKeys: 0, MetricFallbackTransitions: 2} {
				if got := metricValue(t, m, name); math.Abs(got-want) > 1e-9 {
					t.Errorf("ordered %s=%v want %v", name, got, want)
				}
			}
		})
	}
}

func TestMetricsColdBirthBaselineExcludesEarlierOutages(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	l.recordFailure(transientFailure())
	c.Advance(200 * time.Millisecond)
	l.beginProbe()
	l.finishProbe(nil)
	// A miss must take its birth timestamp/baseline after the transition gate,
	// rather than carrying a timestamp sampled before a delayed creation.
	l.observations.transitionMu.Lock()
	started := make(chan struct{})
	born := make(chan *bucket, 1)
	go func() { at := c.Now(); close(started); b, _ := l.store.acquire("new", at); born <- b }()
	<-started
	c.Advance(300 * time.Millisecond)
	l.observations.transitionMu.Unlock()
	b := <-born
	defer l.store.release(b)
	if !b.created.Equal(c.Now()) {
		t.Fatal("creation retained a pre-gate timestamp")
	}
	c.Advance(100 * time.Millisecond)
	l.recordFailure(transientFailure())
	c.Advance(200 * time.Millisecond)
	l.beginProbe()
	l.finishProbe(nil)
	c.Advance(100 * time.Millisecond)
	l.sampleMetrics()
	if got := metricValue(t, m, MetricFallbackSeconds); math.Abs(got-.2) > 1e-9 {
		t.Fatal("birth charged an earlier outage", got)
	}
	if got := metricValue(t, m, MetricFallbackTransitions); got != 2 {
		t.Fatal("birth charged earlier transitions", got)
	}
}

func TestAuditQuotaRejectionIsNotFallback(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "audit-key")
	initializeTestBucket(t, l, b, 20e6)
	c.Advance(200 * time.Millisecond)
	a, ok := l.prepareAttempt(b, c.Now())
	if !ok {
		t.Fatal("no attempt")
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 0, SessionTTLMS: 86400000})
	c.Advance(100 * time.Millisecond)
	allowed, reason, info := l.Allow("audit-key")
	l.sampleMetrics()
	t.Logf("allowed=%v reason=%s mode=%s circuit=%v keys=%v seconds=%v transitions=%v", allowed, reason, info.Mode, l.control.Load().circuit, metricValue(t, m, MetricFallbackKeys), metricValue(t, m, MetricFallbackSeconds), metricValue(t, m, MetricFallbackTransitions))
	if allowed || reason != ReasonGlobalLimit || info.Mode != ModeBlocked {
		t.Fatal("unexpected admission")
	}
	if metricValue(t, m, MetricFallbackKeys) != 0 || metricValue(t, m, MetricFallbackSeconds) != 0 || metricValue(t, m, MetricFallbackTransitions) != 0 {
		t.Fatal("globally rejected key was reported in fallback")
	}
}

type auditCapturingEvents struct{ events []logEvent }

func (e *auditCapturingEvents) offer(v logEvent) { e.events = append(e.events, v) }
func (e *auditCapturingEvents) stop()            {}
func TestAuditTerminalFailureHasObservation(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := validConfig()
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	events := &auditCapturingEvents{}
	l.events = events
	b := testBucket(t, l, "audit-key")
	initializeTestBucket(t, l, b, 100e6)
	allowed, _, _ := l.Allow("audit-key")
	if !allowed {
		t.Fatal("initial admission")
	}
	a, ok := l.prepareAttempt(b, c.Now())
	if !ok {
		t.Fatal("no attempt")
	}
	l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
	l.sampleMetrics()
	t.Logf("terminal=%s outstanding=%d logs=%d redis_errors=%v rejected_expired=%d", b.terminalReason, b.ledger.outstanding(), len(events.events), metricValue(t, m, MetricRedisErrors), m.lookup[metricKey{MetricRejected, Labels{TierLocal, ReasonAccountingExpired}}].value.Load())
	if b.terminalReason != ReasonAccountingExpired {
		t.Fatal("terminal not installed")
	}
	if len(events.events) == 0 {
		t.Fatal("terminal accounting failure emitted no event")
	}
}

func TestTerminalTransitionReasonCounter(t *testing.T) {
	for _, tc := range []struct {
		out    outcome
		reason string
	}{
		{outcomeSessionExpired, ReasonAccountingExpired},
		{outcomePolicyMismatch, ReasonPolicyMismatch},
		{outcomeStateCorrupt, ReasonAccountingError},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			m := NewPrometheusMetrics()
			cfg := validConfig()
			cfg.Metrics = m
			l, c, _ := newTestLimiter(t, cfg)
			events := &auditCapturingEvents{}
			l.events = events
			b := testBucket(t, l, "raw-secret-key")
			initializeTestBucket(t, l, b, 100e6)
			l.Allow("raw-secret-key")
			a, _ := l.prepareAttempt(b, c.Now())
			l.installReply(b, a, reply{Outcome: tc.out})
			l.installReply(b, a, reply{Outcome: tc.out}) // delayed duplicate cannot emit twice
			l.sampleMetrics()                            // no further admission needed to diagnose terminal debt
			name := fmt.Sprintf("rate_limit_terminal_transitions_total{tier=%q,reason=%q}", TierLocal, tc.reason)
			if got := metricValue(t, m, name); got != 1 {
				t.Fatal("terminal transition count", got)
			}
			if len(events.events) != 1 {
				t.Fatalf("events=%+v", events.events)
			}
			if b.ledger.outstanding() != 1 {
				t.Fatal("terminal transition discarded debt")
			}
		})
	}
}

// Check the public text format rather than the collector's internal ordering:
// one declaration precedes all samples in each contiguous metric family.
func TestMetricsTextExpositionFamilies(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprintf("populated=%v", populated), func(t *testing.T) {
			m := NewPrometheusMetrics()
			if populated {
				m.AddCounter(MetricAllowed, Labels{TierLocal, ReasonAllowed}, 2)
				m.AddCounter(MetricRejected, Labels{TierLocal, ReasonLocalLimit}, 3)
				m.AddCounter(MetricRejected, Labels{TierGlobal, ReasonGlobalLimit}, 4)
				m.AddCounter(MetricTerminalTransitions, Labels{TierLocal, ReasonAccountingExpired}, 1)
				m.SetGauge(MetricOutstanding, 5)
				m.SetGauge(MetricFallbackSeconds, 0.25)
				m.Observe(MetricSyncDuration, 0.001)
				m.Observe(MetricQueueDelay, 2)
			}
			declared := make(map[string]string)
			samples := make(map[string]int)
			current := ""
			for index, line := range strings.Split(scrapeMetrics(m), "\n") {
				if line == "" {
					continue
				}
				if strings.HasPrefix(line, "# TYPE ") {
					fields := strings.Fields(line)
					if len(fields) != 4 {
						t.Fatalf("line %d: malformed TYPE: %s", index+1, line)
					}
					family := fields[2]
					if _, exists := declared[family]; exists {
						t.Fatalf("line %d: duplicate TYPE for %s", index+1, family)
					}
					declared[family] = fields[3]
					current = family
					continue
				}
				if strings.HasPrefix(line, "#") {
					continue
				}
				end := strings.IndexAny(line, "{ \t")
				if end <= 0 {
					t.Fatalf("line %d: malformed sample: %s", index+1, line)
				}
				family := line[:end]
				if _, exists := declared[family]; !exists {
					for _, suffix := range []string{"_bucket", "_sum", "_count"} {
						if base := strings.TrimSuffix(family, suffix); base != family && declared[base] == "histogram" {
							family = base
							break
						}
					}
				}
				if _, exists := declared[family]; !exists {
					t.Fatalf("line %d: sample before TYPE: %s", index+1, line)
				}
				if family != current {
					t.Fatalf("line %d: family %s resumed after %s", index+1, family, current)
				}
				samples[family]++
			}
			if len(declared) == 0 {
				t.Fatal("empty exposition")
			}
			for family := range declared {
				if samples[family] == 0 {
					t.Errorf("family %s has no samples", family)
				}
			}
		})
	}
}
