package ratelimiter_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	ratelimit "github.com/Le-Fang/dsitributed-rate-limiter"
	"github.com/redis/go-redis/v9"
)

type authenticatedUserKey struct{}

func ExampleNewLimiter() {
	run := func() error {
		l, err := ratelimit.NewLimiter(ratelimit.Config{
			GlobalRate: 1000, GlobalBurst: 100,
			MaxOutstanding: 8, LocalBurst: 8,
			Redis: ratelimit.RedisConfig{Address: "127.0.0.1:6379"},
		})
		if err != nil {
			return err
		}
		// Authentication middleware must set this trusted identity before throttling.
		keyFunc := func(r *http.Request) (string, error) {
			user, ok := r.Context().Value(authenticatedUserKey{}).(string)
			if !ok || user == "" {
				return "", errors.New("missing authenticated user")
			}
			route := "/items/{id}" // Route template from your router, never the raw URL.
			return fmt.Sprintf("%d:%s%d:%s", len(user), user, len(route), route), nil
		}
		appHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
		handler := l.Middleware(keyFunc)(appHandler)
		_ = handler // Mount on the application's HTTP server after authentication.
		// After stopping incoming application requests:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		return l.Close(shutdownCtx)
	}
	_ = run // No live Redis is needed to compile this example.
}

func ExampleNewLimiter_localOnly() {
	l, err := ratelimit.NewLimiter(ratelimit.Config{
		GlobalRate: 1000, GlobalBurst: 100, LocalRate: 100, LocalBurst: 8,
		MaxOutstanding: 8, FailurePolicy: ratelimit.LocalOnly,
		FallbackRate: 10, FallbackBurst: 4,
		Redis: ratelimit.RedisConfig{Address: "127.0.0.1:6379"},
	})
	if err != nil {
		return
	}
	// N fixed instances can serve up to N*(4+10*F) fallback tokens per key
	// during one uninterrupted F-second episode; this is extra to normal traffic.
	_, _, info := l.Allow("example-user")
	_ = info.Mode
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	_ = l.Close(ctx)
}

func ExampleNewLimiter_injectedClient() {
	timeout := 200 * time.Millisecond
	// Multiple Addrs select Cluster. For Sentinel instead, use Sentinel addresses
	// and set MasterName to your monitored primary name. For TLS standalone use
	// one address. Supply credentials through your application's secret provider.
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:                 []string{"redis-a.example:6379", "redis-b.example:6379"},
		TLSConfig:             &tls.Config{MinVersion: tls.VersionTLS12},
		ContextTimeoutEnabled: true, MaxRetries: -1,
		DialTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout, PoolTimeout: timeout,
		PoolSize: 4, MaxActiveConns: 4,
	})
	defer client.Close() // The caller owns injected clients; close after the limiter.
	l, err := ratelimit.NewLimiter(ratelimit.Config{GlobalRate: 1000, GlobalBurst: 100, Redis: ratelimit.RedisConfig{Client: client, Timeout: timeout}})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	_ = l.Close(ctx)
}

func ExampleLimiter_MetricsHandler() {
	l, err := ratelimit.NewLimiter(ratelimit.Config{GlobalRate: 1000, GlobalBurst: 100, Redis: ratelimit.RedisConfig{Address: "127.0.0.1:6379"}})
	if err != nil {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", l.MetricsHandler())
	// Mount on a protected monitoring listener. No per-user metric labels.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	_ = l.Close(ctx)
}

// A custom adapter owns exposition, uses bounded labels, and never blocks on I/O.
// This minimal example keeps only a total; a real adapter can keep more fixed bins.
type decisionTotals struct{ decisions atomic.Uint64 }

func (m *decisionTotals) AddCounter(name string, _ ratelimit.Labels, n uint64) {
	if name == ratelimit.MetricAllowed || name == ratelimit.MetricRejected {
		m.decisions.Add(n)
	}
}
func (*decisionTotals) SetGauge(string, float64) {}
func (*decisionTotals) Observe(string, float64)  {}

var _ ratelimit.Metrics = (*decisionTotals)(nil)

func ExampleConfig_metrics() {
	metrics := &decisionTotals{}
	cfg := ratelimit.Config{GlobalRate: 1000, GlobalBurst: 100, Metrics: metrics, Redis: ratelimit.RedisConfig{Address: "127.0.0.1:6379"}}
	_ = cfg // Custom Metrics disables the limiter-owned MetricsHandler (404).
}
