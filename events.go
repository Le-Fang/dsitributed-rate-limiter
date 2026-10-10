package ratelimiter

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// eventLogger owns one bounded consumer. stop cancels delivery without joining:
// an unsupported caller handler that blocks forever cannot hold up Close.
// At most that one goroutine can remain blocked; no goroutine is spawned per log.
type eventLogger struct {
	logger  *slog.Logger
	clock   clock
	counter func(string, Labels, uint64)
	queue   chan logEvent
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	last    [eventKindCount][diagnosticCount][2]time.Time
	stopped bool
}

func newEventLogger(logger *slog.Logger, clock clock, counter func(string, Labels, uint64)) *eventLogger {
	ctx, cancel := context.WithCancel(context.Background())
	e := &eventLogger{logger: logger, clock: clock, counter: counter, queue: make(chan logEvent, 1024), ctx: ctx, cancel: cancel}
	go e.run()
	return e
}
func (e *eventLogger) offer(event logEvent) {
	if event.kind >= eventKindCount {
		return
	}
	if event.diagnostic >= diagnosticCount {
		event.diagnostic = diagnosticUnknown
	}
	now := e.clock.Now()
	var metric string
	e.mu.Lock()
	// stop closes acceptance before the consumer's final drain. No offer can
	// enqueue after that drain, and no external callback runs under this lock.
	if e.stopped {
		metric = MetricLogDropped
	} else {
		permanence := 0
		if event.permanent {
			permanence = 1
		}
		last := &e.last[event.kind][event.diagnostic][permanence]
		if !last.IsZero() && now.Sub(*last) < time.Second {
			metric = MetricLogCoalesced
		} else {
			*last = now
			select {
			case e.queue <- event:
			default:
				metric = MetricLogDropped
			}
		}
	}
	e.mu.Unlock()
	if metric != "" {
		e.counter(metric, Labels{}, 1)
	}
}
func (e *eventLogger) stop() {
	e.mu.Lock()
	e.stopped = true
	e.cancel()
	e.mu.Unlock()
}
func (e *eventLogger) run() {
	for {
		if e.ctx.Err() != nil {
			e.dropQueued()
			return
		}
		select {
		case <-e.ctx.Done():
			e.dropQueued()
			return
		case kind := <-e.queue:
			if !e.deliver(kind) {
				e.counter(MetricLogDropped, Labels{}, 1)
			}
		}
	}
}
func (e *eventLogger) dropQueued() {
	var n uint64
	for {
		select {
		case <-e.queue:
			n++
		default:
			if n > 0 {
				e.counter(MetricLogDropped, Labels{}, n)
			}
			return
		}
	}
}
func (e *eventLogger) deliver(event logEvent) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	if e.ctx.Err() != nil {
		return false
	}
	h := e.logger.Handler()
	if !h.Enabled(e.ctx, slog.LevelWarn) {
		return true
	}
	cause := [...]string{"dependency_failure", "circuit_transition", "fallback_enter", "fallback_exit", ReasonAccountingExpired, ReasonPolicyMismatch, ReasonAccountingError}[event.kind]
	if event.kind == eventRedisError {
		cause = [...]string{"unknown", "timeout", "pool_exhausted", "network", "oom", "authentication", "permission", "protocol", "script", "loading", "try_again", "cluster_down", "master_down", "busy", "read_only"}[event.diagnostic]
	}
	r := slog.NewRecord(e.clock.Now(), slog.LevelWarn, "rate limiter event", 0)
	r.AddAttrs(slog.String("cause", cause))
	if event.kind == eventRedisError {
		r.AddAttrs(slog.Bool("permanent", event.permanent))
	}
	return h.Handle(e.ctx, r) == nil
}
