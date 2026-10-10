package ratelimiter

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

const scanChunk = 64
const registrationWindow = 30 * time.Second

func fullJitter(ceiling time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(ceiling)))
}

// start owns exactly four operation workers and one bounded scanner. The done
// signal includes draining queue references after cancellation. Lifecycle final
// flushing is layered on this cancellation/join boundary.
func (l *Limiter) start() {
	l.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(l.parent)
		l.cancelWorkers = cancel
		var workers sync.WaitGroup
		workers.Add(4)
		for i := 0; i < 4; i++ {
			go func() { defer workers.Done(); l.worker(ctx) }()
		}
		go func() {
			tick := min(10*time.Millisecond, l.cfg.syncInterval)
			timer := l.clock.NewTimer(tick)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					if l.parent.Err() != nil {
						l.beginShutdown()
					}
					// Publish stop and wait only for bounded enqueue sections.
					// Release the gate before joining workers or draining refs.
					l.enqueueMu.Lock()
					l.schedulingStopped = true
					l.enqueueMu.Unlock()
					workers.Wait()
					l.drainWork()
					close(l.workersDone)
					return
				case <-timer.C():
					l.scanDue(l.clock.Now())
					timer.Reset(tick)
				}
			}
		}()
	})
}

func (l *Limiter) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-l.work:
			if b == nil {
				l.runProbe(ctx)
				continue
			}
			b.mu.Lock()
			b.queued = false
			queuedAt := b.queuedAt
			b.mu.Unlock()
			l.observeDuration(MetricQueueDelay, max(0, l.clock.Now().Sub(queuedAt).Seconds()))
			if ctx.Err() == nil && l.due(b, l.clock.Now()) {
				if a, ok := l.prepareAttempt(b, l.clock.Now()); ok {
					l.runAttempt(ctx, b, a)
				}
			}
			// One command per queue turn; a continuously dirty key joins the tail.
			if ctx.Err() == nil && l.due(b, l.clock.Now()) {
				l.notify(b)
			}
			l.store.release(b)
		}
	}
}

func (l *Limiter) due(b *bucket, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := l.control.Load()
	if b.busy || b.terminalReason != "" || s.closed || s.permanent || s.circuit != circuitHealthy || l.parent.Err() != nil || now.Before(b.capacityUntil) {
		return false
	}
	// Stop refreshing fully refillable idle keys so even a slow backend cannot
	// pin them forever through continuous periodic work. Cleanup owns removal.
	tokens := b.tokens
	if b.ledger.outstanding() == 0 && !b.globalRejected && b.capacityUntil.IsZero() && now.Sub(b.lastActivity) >= l.cfg.idleTTL && tokens.safeToEvict(now) {
		return false
	}
	// Sequence exhaustion can rotate only a clean ledger. Pending/ambiguous
	// consumption is never moved under a different session identity.
	if b.initialized && b.ledger.Committed >= maxSequence && b.ledger.outstanding() == 0 {
		b.initialized = false
		b.init = nil
		b.session = ""
		b.slot = -1
		b.freshUntil = time.Time{}
		b.ledger = ledger{}
	}
	// A current negative observation resolves generation reconciliation for
	// scheduling, but grants no admission authority. Keep draining unresolved
	// recovery debt; clean rejected keys return to the periodic cadence.
	recoveringDebt := b.authorizedGeneration != s.generation && b.ledger.outstanding() != 0
	return !b.initialized || b.ledger.Flight != nil || b.ledger.Pending >= int64(l.cfg.syncThreshold) || b.checkedGeneration != s.generation || recoveringDebt || !now.Before(b.nextSyncAt)
}

// scanDue uses the store's persistent cursor and releases every scan reference.
// Notifications hold separate references only while actually queued.
func (l *Limiter) scanDue(now time.Time) {
	defer l.maybeSampleMetrics(now)
	s := l.control.Load()
	if s.closed || l.parent.Err() != nil {
		return
	}
	for _, b := range l.store.cleanup(now, scanChunk) {
		b.mu.Lock()
		l.observeBucketLocked(b, now)
		if b.observedFallback {
			b.observedFallback = false
			l.observations.fallbackExit.Add(1)
		}
		b.mu.Unlock()
	}
	l.flushObservations()
	if s.permanent {
		return
	}
	if s.circuit != circuitHealthy {
		l.scheduleProbe(now)
		return
	}
	for _, b := range l.store.scan(scanChunk) {
		if l.due(b, now) {
			l.notify(b)
		}
		l.store.release(b)
	}
}

func (l *Limiter) runAttempt(ctx context.Context, b *bucket, a attempt) {
	defer l.beginObservedWork()()
	if a.command.Op == 0 {
		// Ownership is already claimed before TIME, preventing concurrent init
		// samples for one key. Its deadline is based on the sample, never receipt.
		opCtx, cancel := context.WithTimeout(ctx, l.cfg.operationTimeout)
		remote, err := l.backend.Time(opCtx, b.key)
		cancel()
		if err != nil {
			l.finishAttempt(b, a)
			l.recordFailure(err)
			return
		}
		if l.clock.Now().Sub(a.start) >= registrationWindow {
			l.finishAttempt(b, a)
			return
		}
		id, err := newSessionID()
		if err != nil {
			l.finishAttempt(b, a)
			l.recordFailure(err)
			return
		}
		c := command{Op: opInit, Session: id, Slot: -1, Delta: int64(l.cfg.maxOutstanding), RateU: l.cfg.globalRateU, BurstU: int64(l.cfg.globalBurst) * 1e6, RegistrationDeadlineMS: remote.UnixMilli() + registrationWindow.Milliseconds()}
		b.mu.Lock()
		if !b.busy || b.active != a {
			b.mu.Unlock()
			return
		}
		b.init = &c
		a.command = c
		b.active = a
		b.mu.Unlock()
	}
	// TIME and script each have their own bounded operation budget. They share
	// one worker and per-key ownership; neither holds a storage/bucket lock.
	if ctx.Err() != nil {
		l.finishAttempt(b, a)
		return
	}
	s := l.control.Load()
	if s.closed || s.permanent || s.circuit != circuitHealthy || s.generation != a.generation {
		l.finishAttempt(b, a)
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, l.cfg.operationTimeout)
	r, err := l.backend.Execute(opCtx, b.key, a.command)
	cancel()
	if err != nil {
		l.finishAttempt(b, a)
		l.recordFailure(err)
		return
	}
	l.installReply(b, a, r)
}

func (l *Limiter) scheduleProbe(now time.Time) {
	l.enqueueMu.RLock()
	defer l.enqueueMu.RUnlock()
	if l.schedulingStopped {
		return
	}
	l.circuitMu.Lock()
	defer l.circuitMu.Unlock()
	s := l.control.Load()
	if s.closed || s.permanent || s.circuit != circuitOpen || l.probeQueued || l.probe != nil {
		return
	}
	if l.nextProbeAt.IsZero() {
		if l.probeBackoff == 0 {
			l.probeBackoff = 100 * time.Millisecond
		}
		l.nextProbeAt = now.Add(l.jitter(l.probeBackoff))
	}
	if now.Before(l.nextProbeAt) {
		return
	}
	select {
	case l.work <- nil:
		l.probeQueued = true
	default:
	}
}
func (l *Limiter) runProbe(ctx context.Context) {
	defer l.beginObservedWork()()
	if !l.beginProbe() {
		l.circuitMu.Lock()
		l.probeQueued = false
		l.circuitMu.Unlock()
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, l.cfg.operationTimeout)
	err := l.backend.Probe(opCtx)
	cancel()
	l.finishProbe(err)
	l.circuitMu.Lock()
	l.probeQueued = false
	if l.control.Load().circuit == circuitHealthy {
		l.probeBackoff = 0
		l.nextProbeAt = time.Time{}
	} else {
		l.probeBackoff = min(5*time.Second, max(100*time.Millisecond, l.probeBackoff*2))
		l.nextProbeAt = l.clock.Now().Add(l.jitter(l.probeBackoff))
	}
	l.circuitMu.Unlock()
}

// drainWork runs only after the enqueue gate closes and workers join.
func (l *Limiter) drainWork() {
	for {
		select {
		case b := <-l.work:
			if b != nil {
				b.mu.Lock()
				b.queued = false
				b.mu.Unlock()
				l.store.release(b)
			}
		default:
			return
		}
	}
}
