package ratelimiter

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkSchedulerCapacitySmoke(b *testing.B) {
	for _, point := range []struct {
		keys  int
		delay time.Duration
	}{{1, time.Millisecond}, {100, time.Millisecond}, {1000, 10 * time.Millisecond}} {
		b.Run(fmt.Sprintf("keys%d_delay%s", point.keys, point.delay), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r := runCapacitySmoke(b, point.keys, point.delay, 300*time.Millisecond)
				b.ReportMetric(float64(r.admitted)/r.traffic.Seconds(), "admitted/s")
				b.ReportMetric(float64(r.offered-r.admitted)/float64(r.offered)*100, "denied_percent")
				b.ReportMetric(float64(r.ops), "backend_ops/run")
			}
		})
	}
}

// fixedBenchmarkClock avoids scheduler/refill variability in microbenchmarks.
type fixedBenchmarkClock struct {
	realClock
	now time.Time
}

func (c fixedBenchmarkClock) Now() time.Time { return c.now }

func newBenchmarkLimiter(t testing.TB, keys int) *Limiter {
	t.Helper()
	cfg, err := normalizeConfig(Config{GlobalRate: 1000000, GlobalBurst: 1000000, LocalBurst: 1000000, MaxOutstanding: 1000000, MaxKeys: keys, Redis: RedisConfig{Address: "unused:6379"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0)
	l := newLimiterCore(cfg.core(), context.Background(), fixedBenchmarkClock{now: now}, nil, NewPrometheusMetrics())
	t.Cleanup(func() { l.drainWork() })
	return l
}

// Authorization and ledger reset are test setup, outside timed finite batches.
// These measure admission computation only, not end-to-end sustainable capacity.
func benchmarkAllow(b *testing.B, keys int, outcome string) {
	l := newBenchmarkLimiter(b, keys)
	names := make([]string, keys)
	buckets := make([]*bucket, keys)
	for i := range names {
		names[i] = fmt.Sprint(i)
		buckets[i], _ = l.store.acquire(names[i], l.clock.Now())
	}
	b.Cleanup(func() {
		for _, bucket := range buckets {
			l.store.release(bucket)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for done := 0; done < b.N; {
		b.StopTimer()
		l.drainWork()
		for _, bucket := range buckets {
			bucket.initialized = true
			bucket.authorizedGeneration = 1
			bucket.freshUntil = l.clock.Now().Add(time.Second)
			bucket.tokens.tokens = float64(l.cfg.localBurst)
			bucket.ledger = ledger{}
			if outcome == ReasonLocalLimit {
				bucket.tokens.tokens = 0
			}
			if outcome == ReasonSyncPending {
				bucket.ledger.Pending = int64(l.cfg.maxOutstanding)
			}
		}
		count := min(8192, b.N-done)
		b.StartTimer()
		for i := 0; i < count; i++ {
			ok, reason, _ := l.Allow(names[i%keys])
			if reason != outcome || ok != (outcome == ReasonAllowed) {
				b.Fatalf("unexpected outcome %v %s", ok, reason)
			}
		}
		done += count
	}
}
func BenchmarkAllowHotKey(b *testing.B)      { benchmarkAllow(b, 1, ReasonAllowed) }
func BenchmarkAllowManyKeys(b *testing.B)    { benchmarkAllow(b, 1000, ReasonAllowed) }
func BenchmarkAllowLocalReject(b *testing.B) { benchmarkAllow(b, 1, ReasonLocalLimit) }
func BenchmarkAllowCapReject(b *testing.B)   { benchmarkAllow(b, 1, ReasonSyncPending) }
