package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const shutdownBudget = 5 * time.Second

// ShutdownError reports normal consumption whose acknowledgement could not be
// confirmed. Counts include ambiguous batches, even if Redis applied them.
// Error deliberately omits keys and backend error text. Unwrap exposes the cause
// to errors.Is/As; callers should avoid logging that potentially sensitive cause.
type ShutdownError struct {
	UnflushedKeys   int
	UnflushedTokens int64
	cause           error
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf("rate limiter shutdown incomplete: %d unflushed keys, %d unflushed tokens", e.UnflushedKeys, e.UnflushedTokens)
}
func (e *ShutdownError) Unwrap() error { return e.cause }

// Close starts shared cleanup and waits for its stored result. ctx limits this
// caller's wait only. Parent cancellation starts the same cleanup automatically.
// Cleanup has an independent five-second budget covering worker join and final
// accounting. Injected clients must honor cancellation and I/O limits; otherwise
// joining their operations (and therefore cleanup completion) can exceed it.
func (l *Limiter) Close(ctx context.Context) error {
	l.beginShutdown()
	// A completed shutdown always returns its stored result, including when the
	// caller supplies an already-canceled context on a subsequent call.
	select {
	case <-l.shutdownDone:
		return l.shutdownErr
	default:
	}
	select {
	case <-l.shutdownDone:
		return l.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l *Limiter) beginShutdown() {
	l.shutdownOnce.Do(func() {
		ctx, cancel := l.shutdownContext()
		l.observations.transitionMu.Lock()
		l.circuitMu.Lock()
		previous := l.control.Load()
		next := *previous
		next.closed = true
		if next.stoppedAt.IsZero() {
			next.stoppedAt = l.clock.Now()
		}
		l.updateOutageTimeline(previous, &next)
		l.control.Store(&next)
		l.circuitMu.Unlock()
		l.finishObservationTransition(previous)
		l.enqueueMu.Lock()
		l.schedulingStopped = true
		l.enqueueMu.Unlock()
		// Internal domain/model users may never start workers. Competing start/close
		// calls still have exactly one owner and a fully initialized cancellation port.
		l.startOnce.Do(func() { l.cancelWorkers = func() {}; close(l.workersDone) })
		l.cancelWorkers()
		go l.shutdown(ctx, cancel)
	})
}
func (l *Limiter) shutdown(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	// Never race flush or client closure against an outstanding local operation.
	// Contract-conforming backends return promptly on worker cancellation. Time
	// spent joining consumes the already-running budget, never a new deadline.
	<-l.workersDone
	l.drainWork()
	var cause error
	for i := range l.store.shards {
		sh := &l.store.shards[i]
		sh.mu.Lock()
		buckets := make([]*bucket, 0, len(sh.keys))
		for _, b := range sh.keys {
			buckets = append(buckets, b)
		}
		sh.mu.Unlock()
		// Scheduling/eviction has joined. Admission may finish a pre-close decision;
		// taking each bucket mutex observes that final receipt before flushing it.
		for _, b := range buckets {
			if err := l.flushBucket(ctx, b); err != nil && cause == nil {
				cause = err
			}
		}
	}
	keys, tokens := l.unflushed()
	if ctx.Err() != nil {
		cause = ctx.Err()
	}
	if err := l.backend.Close(); err != nil {
		cause = errors.Join(cause, err)
	}
	if cause != nil || keys != 0 {
		l.shutdownErr = &ShutdownError{UnflushedKeys: keys, UnflushedTokens: tokens, cause: cause}
	}
	l.sampleMetrics()
	if l.events != nil {
		l.events.stop()
	}
	close(l.shutdownDone)
}

// Final accounting bypasses availability/authorization gates, never ledger
// identity or terminal safety gates. Only normal debt needs a final operation;
// cold/fallback-only keys need neither INIT nor REFRESH.
func (l *Limiter) flushBucket(ctx context.Context, b *bucket) error {
	for {
		b.mu.Lock()
		if b.ledger.outstanding() == 0 {
			b.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return err
		}
		if b.busy || !b.initialized || b.terminalReason != "" {
			b.mu.Unlock()
			return errors.New("unresolved accounting cannot be flushed")
		}
		if b.ledger.Flight == nil && b.ledger.Pending > 0 {
			b.flightSince = b.pendingSince
			b.pendingSince = time.Time{}
		}
		p, ok := b.ledger.begin()
		if !ok {
			b.mu.Unlock()
			return errors.New("accounting sequence exhausted")
		}
		c := command{Op: opApply, Session: b.session, Slot: b.slot, Sequence: p.Sequence, Delta: p.Delta, RateU: l.cfg.globalRateU, BurstU: int64(l.cfg.globalBurst) * 1e6}
		b.mu.Unlock()
		opCtx, cancel := context.WithTimeout(ctx, l.cfg.operationTimeout)
		r, err := l.backend.Execute(opCtx, b.key, c)
		cancel()
		if err != nil {
			l.observeBackendError(err)
		}
		if err == nil {
			if (r.Outcome == outcomeOK || r.Outcome == outcomeDuplicate) && r.CommittedSequence == p.Sequence {
				b.mu.Lock()
				b.ledger.acknowledge(p)
				b.flightSince = time.Time{}
				b.mu.Unlock()
				continue
			}
			return errors.New("final accounting was not acknowledged")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if backendErrorClass(err) != errorTransient {
			return err
		}
		// A bounded delay prevents a fast failing dependency from spinning through
		// retries. All retries retain the same session/slot/sequence/delta.
		timer := l.clock.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C():
			timer.Stop()
		}
	}
}
func (l *Limiter) unflushed() (keys int, tokens int64) {
	for i := range l.store.shards {
		sh := &l.store.shards[i]
		sh.mu.Lock()
		for _, b := range sh.keys {
			b.mu.Lock()
			n := b.ledger.outstanding()
			b.mu.Unlock()
			if n != 0 {
				keys++
				tokens += n
			}
		}
		sh.mu.Unlock()
	}
	return
}
