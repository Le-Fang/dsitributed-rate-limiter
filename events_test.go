package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type controlledLogHandler struct {
	mu           sync.Mutex
	records      []slog.Record
	entered      chan struct{}
	release      chan struct{}
	once         sync.Once
	fail, panics bool
}

func (h *controlledLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *controlledLogHandler) Handle(_ context.Context, r slog.Record) error {
	if h.entered != nil {
		h.once.Do(func() { close(h.entered) })
		<-h.release
	}
	if h.panics {
		panic("secret")
	}
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	if h.fail {
		return errors.New("secret")
	}
	return nil
}
func (h *controlledLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *controlledLogHandler) WithGroup(string) slog.Handler      { return h }
func TestEventsBoundedCoalescedDropsAndClose(t *testing.T) {
	m := NewPrometheusMetrics()
	cfg := fallbackConfig(LocalOnly)
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	h := &controlledLogHandler{entered: make(chan struct{}), release: make(chan struct{})}
	e := newEventLogger(slog.New(h), c, l.addCounter)
	l.events = e
	defer close(h.release)
	defer e.stop()
	l.recordFailure(&backendError{class: errorTransient, cause: errors.New("redis://username:secret@host raw-key")})
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("consumer did not start")
	}
	e.offer(logEvent{kind: eventRedisError})
	for i := 0; i < 1100; i++ {
		c.Advance(time.Second)
		e.offer(logEvent{kind: eventRedisError})
	}
	if got := metricValue(t, m, MetricLogDropped); got == 0 {
		t.Fatal("queue pressure was not counted")
	}
	if got := metricValue(t, m, MetricLogCoalesced); got == 0 {
		t.Fatal("repeats were not coalesced")
	}
	done := make(chan error, 1)
	go func() { done <- l.Close(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for blocking handler")
	}
}
func TestEventsSanitizedAndHandlerFailures(t *testing.T) {
	for _, mode := range []string{"ok", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			m := NewPrometheusMetrics()
			cfg := fallbackConfig(LocalOnly)
			cfg.Metrics = m
			l, c, _ := newTestLimiter(t, cfg)
			h := &controlledLogHandler{fail: mode == "error", panics: mode == "panic"}
			e := newEventLogger(slog.New(h), c, l.addCounter)
			defer e.stop()
			l.events = e
			l.recordFailure(&backendError{class: errorTransient, cause: errors.New("redis://secret raw-key")})
			deadline := time.Now().Add(time.Second)
			for {
				h.mu.Lock()
				n := len(h.records)
				h.mu.Unlock()
				drops := metricValue(t, m, MetricLogDropped)
				if (mode == "ok" && n >= 2) || (mode != "ok" && drops >= 2) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("log consumer did not report events/failures")
				}
				time.Sleep(time.Millisecond)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, r := range h.records {
				if strings.Contains(r.Message, "secret") || strings.Contains(r.Message, "raw-key") {
					t.Fatal("sensitive text")
				}
				r.Attrs(func(a slog.Attr) bool {
					if strings.Contains(a.String(), "secret") || strings.Contains(a.String(), "raw-key") {
						t.Fatal("sensitive attribute")
					}
					return true
				})
			}
		})
	}
}

// Removing adapter diagnostics or coalescing all dependency errors together must
// fail: each distinct safe cause is useful even within the same one-second window.
func TestEventsBackendDiagnostics(t *testing.T) {
	cases := []struct {
		err       error
		cause     string
		permanent bool
	}{
		{context.DeadlineExceeded, "timeout", false},
		{redis.ErrPoolTimeout, "pool_exhausted", false},
		{&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("secret-address raw-key")}, "network", false},
		{redisTestError("OOM secret-password raw-key"), "oom", false},
		{redisTestError("NOAUTH secret-password raw-key"), "authentication", true},
		{redisTestError("NOPERM secret-password raw-key"), "permission", true},
		{fmt.Errorf("secret-password: %w", errMalformedReply), "protocol", true},
		{redisTestError("ERR secret-password raw-key"), "script", true},
		{errors.New("secret-password secret-address raw-key"), "unknown", true},
		{redisTestError("LOADING secret-password"), "loading", false},
		{redisTestError("TRYAGAIN secret-password"), "try_again", false},
		{redisTestError("CLUSTERDOWN secret-password"), "cluster_down", false},
		{redisTestError("MASTERDOWN secret-password"), "master_down", false},
		{redisTestError("BUSY secret-password"), "busy", false},
		{redisTestError("READONLY secret-password"), "read_only", false},
	}
	l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	h := &controlledLogHandler{}
	e := newEventLogger(slog.New(h), c, l.addCounter)
	defer e.stop()
	l.events = e
	for _, tc := range cases {
		l.observeBackendError(classifyRedisError(tc.err))
	}
	l.observeBackendError(classifyRedisError(context.Canceled))
	deadline := time.Now().Add(time.Second)
	for {
		h.mu.Lock()
		n := len(h.records)
		h.mu.Unlock()
		if n == len(cases) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("distinct dependency events=%d want=%d", n, len(cases))
		}
		time.Sleep(time.Millisecond)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, r := range h.records {
		attrs := map[string]slog.Value{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value
			if strings.Contains(a.String(), "secret") || strings.Contains(a.String(), "raw-key") {
				t.Errorf("sensitive attribute: %s", a)
			}
			return true
		})
		if attrs["cause"].String() != cases[i].cause || attrs["permanent"].Kind() != slog.KindBool || attrs["permanent"].Bool() != cases[i].permanent {
			t.Errorf("event %d: %v", i, attrs)
		}
	}
}

type offerBarrierClock struct {
	clock
	entered, release chan struct{}
}

func (c offerBarrierClock) Now() time.Time { close(c.entered); <-c.release; return c.clock.Now() }

func TestEventsOfferRacingStopCountsLoss(t *testing.T) {
	c := offerBarrierClock{clock: realClock{}, entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var dropped atomic.Uint64
	e := &eventLogger{clock: c, queue: make(chan logEvent, 1024), ctx: ctx, cancel: cancel}
	e.counter = func(name string, _ Labels, n uint64) {
		// This callback must be able to reenter the event lock.
		e.mu.Lock()
		e.mu.Unlock()
		if name == MetricLogDropped {
			dropped.Add(n)
		}
	}
	consumed := make(chan struct{})
	go func() { e.run(); close(consumed) }()
	offered := make(chan struct{})
	go func() { e.offer(logEvent{kind: eventRedisError}); close(offered) }()
	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("offer did not reach acceptance barrier")
	}
	e.stop()
	select {
	case <-consumed:
	case <-time.After(time.Second):
		t.Fatal("consumer did not drain")
	}
	close(c.release)
	select {
	case <-offered:
	case <-time.After(time.Second):
		t.Fatal("offer or callback blocked")
	}
	if dropped.Load() != 1 || len(e.queue) != 0 {
		t.Fatalf("lost offer not accounted: dropped=%d queued=%d", dropped.Load(), len(e.queue))
	}
}

type terminalReentryMetrics struct {
	check  *lockCheckingMetrics
	called int
}

func (m *terminalReentryMetrics) AddCounter(name string, labels Labels, n uint64) {
	if name != MetricTerminalTransitions {
		return
	}
	m.check.check()
	m.called++
	if ok, reason, _ := m.check.l.Allow("secret-terminal-key"); ok || reason != labels.Reason {
		m.check.t.Error("callback saw uninstalled terminal")
	}
	panic("secret exporter failure")
}
func (*terminalReentryMetrics) SetGauge(string, float64) {}
func (*terminalReentryMetrics) Observe(string, float64)  {}
func TestEventsTerminalReentryRedactionAndStaleReplies(t *testing.T) {
	m := &terminalReentryMetrics{check: &lockCheckingMetrics{t: t}}
	cfg := validConfig()
	cfg.Metrics = m
	l, c, _ := newTestLimiter(t, cfg)
	m.check.l = l
	h := &controlledLogHandler{}
	e := newEventLogger(slog.New(h), c, l.addCounter)
	l.events = e
	defer e.stop()
	b := testBucket(t, l, "secret-terminal-key")
	initializeTestBucket(t, l, b, 100e6)
	l.Allow("secret-terminal-key")
	a, _ := l.prepareAttempt(b, c.Now())
	l.recordFailure(transientFailure())
	l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
	if m.called != 0 || b.terminalReason != "" {
		t.Fatal("old generation installed terminal")
	}
	l.beginProbe()
	l.finishProbe(nil)
	a, _ = l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
	l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
	if m.called != 1 {
		t.Fatal("transition callback count", m.called)
	}
	eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, r := range h.records {
			found := false
			r.Attrs(func(a slog.Attr) bool {
				found = found || a.Key == "cause" && a.Value.String() == ReasonAccountingExpired
				return true
			})
			if found {
				return true
			}
		}
		return false
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		r.Attrs(func(a slog.Attr) bool {
			if strings.Contains(a.String(), "secret") {
				t.Error("event leaked key or panic")
			}
			return true
		})
	}
}
