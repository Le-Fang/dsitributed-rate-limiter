package ratelimiter

import (
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
