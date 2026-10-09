package ratelimiter

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type attempt struct {
	command        command
	batch          batch
	id, generation uint64
	start          time.Time
}

// Limiter performs bounded local admission and asynchronous shared accounting.
// The core retains policy and ports, never public Config or concrete adapters.
type Limiter struct {
	cfg       coreConfig
	parent    context.Context
	clock     clock
	backend   backend
	metrics   Metrics
	store     *bucketStore
	control   atomic.Pointer[controlState]
	work      chan *bucket
	circuitMu sync.Mutex
	probe     *controlState // protected by circuitMu; outstanding probe ownership
}

func newLimiterCore(c coreConfig, parent context.Context, clock clock, b backend, m Metrics) *Limiter {
	l := &Limiter{cfg: c, parent: parent, clock: clock, backend: b, metrics: m, store: newBucketStore(c), work: make(chan *bucket, 1024)}
	l.control.Store(&controlState{generation: 1})
	return l
}
func (l *Limiter) Allow(key string) (bool, string, LimitInfo) { return l.AllowN(key, 1) }
func (l *Limiter) AllowN(key string, n int) (bool, string, LimitInfo) {
	info := l.baseInfo()
	state := l.control.Load()
	if l.parent.Err() != nil || state.closed {
		return l.observe(false, ReasonClosed, info)
	}
	if key == "" || len(key) > 1024 || n <= 0 || n > l.cfg.localBurst || n > l.cfg.maxOutstanding {
		return l.observe(false, ReasonInvalidRequest, info)
	}
	b, ok := l.store.acquire(key, l.clock.Now())
	if !ok {
		return l.observe(false, ReasonCapacityExhausted, info)
	}
	b.mu.Lock()
	var reason string
	var allowed, notify bool
	for {
		priorTokens, priorFallback := b.tokens, b.fallback
		state = l.control.Load()
		now := l.clock.Now()
		b.lastActivity = now
		// Observe a lazy outage before any normal-rate refill. An already
		// entered fallback remains active until a successful reconciliation.
		// Recoverable admission latches still need conservative outage refill;
		// their rejection reasons take precedence below, independently of policy.
		unavailable := state.circuit != circuitHealthy ||
			(b.initialized && (b.authorizedGeneration != state.generation || !now.Before(b.freshUntil))) ||
			(!b.initialized && now.Sub(b.created) >= l.cfg.maxSyncAge)
		if l.cfg.failurePolicy == LocalOnly && unavailable && !b.fallback && !state.closed && !state.permanent && l.parent.Err() == nil && b.terminalReason == "" {
			b.tokens.setPolicyConservative(now, float64(l.cfg.fallbackRateU)/1e6, l.cfg.fallbackBurst)
			b.fallback = true
		} else {
			b.tokens.refill(now)
		}
		info = l.bucketInfo(b)
		reason = ReasonAllowed
		notify = false
		allowed = false
		switch {
		case l.parent.Err() != nil || state.closed:
			reason = ReasonClosed
		case state.permanent:
			reason = ReasonAccountingError
		case b.terminalReason != "":
			reason = b.terminalReason
		case b.globalRejected:
			reason = ReasonGlobalLimit
			info.Tier = TierGlobal
			info.RetryAfter = max(time.Duration(0), b.retryAt.Sub(now))
			info.RetryAfterKnown = true
			notify = true
		case !b.capacityUntil.IsZero():
			reason = ReasonCapacityExhausted
			notify = true
		case l.cfg.failurePolicy == LocalOnly && b.fallback:
			info.Mode = ModeLocalFallback
			notify = true
			if n > l.cfg.fallbackBurst || float64(n) > b.tokens.tokens {
				reason = ReasonFallbackLimit
				if n <= l.cfg.fallbackBurst {
					info.RetryAfter = durationSeconds((float64(n) - b.tokens.tokens) / b.tokens.rate)
					info.RetryAfterKnown = true
				}
			} else {
				allowed = true
				reason = ReasonLocalFallback
			}
		case state.circuit != circuitHealthy:
			reason = ReasonRedisUnavailable
		case !b.initialized:
			reason = ReasonSyncPending
			notify = true
			if now.Sub(b.created) >= l.cfg.maxSyncAge {
				reason = ReasonRedisUnavailable
			}
		case b.authorizedGeneration != state.generation || !now.Before(b.freshUntil):
			reason = ReasonRedisUnavailable
			notify = true
		case b.ledger.Committed >= maxSequence || (b.ledger.Flight != nil && b.ledger.Flight.Sequence == maxSequence):
			reason = ReasonSyncPending
			notify = true
		case int64(n) > int64(l.cfg.maxOutstanding)-b.ledger.outstanding():
			reason = ReasonSyncPending
			notify = true
		case float64(n) > b.tokens.tokens:
			reason = ReasonLocalLimit
			info.Mode = ModeNormal
			info.RetryAfter = durationSeconds((float64(n) - b.tokens.tokens) / b.tokens.rate)
			info.RetryAfterKnown = true
		default:
			allowed = true
			info.Mode = ModeNormal
		}
		// No tokens/delta were consumed. A changed snapshot restarts the decision.
		// The stable snapshot read is the linearization point; a transition after
		// it orders after this admission. Parent cancellation is checked alongside it.
		if l.parent.Err() != nil {
			allowed = false
			reason = ReasonClosed
			info.Mode = ModeBlocked
			break
		}
		if l.control.Load() != state {
			// Recompute refill and policy from the original balance as well as
			// the decision; speculative normal refill must not survive an outage.
			b.tokens, b.fallback = priorTokens, priorFallback
			continue
		}
		if allowed {
			b.tokens.take(n)
			if info.Mode == ModeNormal {
				b.ledger.admit(int64(n), int64(l.cfg.maxOutstanding))
			}
			info.Remaining = int(b.tokens.tokens)
			notify = notify || b.ledger.Pending >= int64(l.cfg.syncThreshold)
		}
		break
	}
	b.mu.Unlock()
	if notify {
		l.notify(b)
	}
	l.store.release(b)
	return l.observe(allowed, reason, info)
}
func (l *Limiter) baseInfo() LimitInfo {
	return LimitInfo{GlobalRate: float64(l.cfg.globalRateU) / 1e6, GlobalBurst: l.cfg.globalBurst, EffectiveRate: float64(l.cfg.localRateU) / 1e6, EffectiveBurst: l.cfg.localBurst, Tier: TierLocal, Mode: ModeBlocked}
}
func (l *Limiter) bucketInfo(b *bucket) LimitInfo {
	i := l.baseInfo()
	i.EffectiveRate = b.tokens.rate
	i.EffectiveBurst = b.tokens.burst
	i.Remaining = max(0, int(b.tokens.tokens))
	return i
}
func (l *Limiter) observe(ok bool, reason string, info LimitInfo) (bool, string, LimitInfo) {
	if l.metrics != nil {
		name := "rejected_total"
		if ok {
			name = "allowed_total"
		}
		l.metrics.AddCounter(name, Labels{Tier: info.Tier, Reason: reason}, 1)
	}
	return ok, reason, info
}

// notify owns a reference only on successful enqueue. Saturation loses a hint,
// never accounting; the scheduler's bounded scans rediscover work.
func (l *Limiter) notify(b *bucket) {
	if !l.store.retain(b) {
		return
	}
	b.mu.Lock()
	if b.queued || b.terminalReason != "" {
		b.mu.Unlock()
		l.store.release(b)
		return
	}
	b.queued = true
	b.mu.Unlock()
	select {
	case l.work <- b:
	default:
		b.mu.Lock()
		b.queued = false
		b.mu.Unlock()
		l.store.release(b)
	}
}

// prepareAttempt requires a retained bucket reference. For INIT, workers first
// obtain TIME and generate an identity outside locks, then stage the immutable
// command under b.mu after rechecking ownership. The core never does I/O.
func (l *Limiter) prepareAttempt(b *bucket, now time.Time) (attempt, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := l.control.Load()
	if b.busy || b.terminalReason != "" || s.closed || s.permanent || s.circuit != circuitHealthy || l.parent.Err() != nil || now.Before(b.capacityUntil) {
		return attempt{}, false
	}
	var c command
	var payload batch
	if !b.initialized {
		if b.init == nil {
			return attempt{}, false
		}
		c = *b.init
	} else {
		c = command{Session: b.session, Slot: b.slot, RateU: l.cfg.globalRateU, BurstU: int64(l.cfg.globalBurst) * 1e6}
		if p, ok := b.ledger.begin(); ok {
			payload = p
			c.Op, c.Sequence, c.Delta = opApply, p.Sequence, p.Delta
		} else {
			if b.ledger.Committed >= maxSequence {
				return attempt{}, false
			}
			c.Op = opRefresh
		}
	}
	b.attemptID++
	a := attempt{command: c, batch: payload, id: b.attemptID, generation: s.generation, start: now}
	b.active = a
	b.busy = true
	return a, true
}

// finishAttempt completes local transport ownership after loss/timeout. It does
// not discard the immutable INIT identity or unresolved APPLY batch.
func (l *Limiter) finishAttempt(b *bucket, a attempt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.busy && b.active == a {
		b.busy = false
	}
}
func (l *Limiter) installReply(b *bucket, a attempt, r reply) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.busy || b.active != a {
		return
	}
	b.busy = false
	success := r.Outcome == outcomeOK || r.Outcome == outcomeDuplicate
	if success && a.command.Op == opApply {
		if r.CommittedSequence != a.batch.Sequence {
			r = reply{Outcome: outcomeSequenceError}
			success = false
		} else {
			b.ledger.acknowledge(a.batch)
		}
	}
	s := l.control.Load()
	// Old generations may settle matching accounting, never alter authorization,
	// rejection, mode or deadlines. A new current-generation operation is needed.
	if a.generation != s.generation || s.closed || s.permanent {
		return
	}
	d := protocolResult(r, b.ledger.outstanding() != 0)
	if !success {
		if d.Terminal {
			b.terminalReason = d.Reason
		}
		if d.Reinitialize {
			b.initialized = false
			b.init = nil
			b.freshUntil = time.Time{}
			b.session = ""
			b.slot = -1
		}
		if d.RetryDelay > 0 {
			b.capacityUntil = l.clock.Now().Add(d.RetryDelay)
		}
		return
	}
	if a.command.Op == opInit {
		b.initialized = true
		b.session = a.command.Session
		b.slot = r.Slot
		b.init = nil
	}
	b.capacityUntil = time.Time{}
	// ACK and rejection install are one atomic transition. No caller can observe
	// freed capacity before a negative/fractional balance has blocked admission.
	b.globalRejected = !d.Authorized
	b.retryAt = a.start.Add(durationMilliseconds(r.RetryMS))
	// A successful quota rejection is still a fresh observation. Capacity is
	// independently guarded by globalRejected; a real outage/stale epoch below
	// must still apply conservative fallback policy before reconciliation.
	now := l.clock.Now()
	// This key may not have observed the outage through Allow at all. Apply
	// its lazy policy transition before reconciliation can resume normal refill.
	if l.cfg.failurePolicy == LocalOnly && !b.fallback && a.command.Op != opInit &&
		(b.authorizedGeneration != s.generation || !now.Before(b.freshUntil)) {
		b.tokens.setPolicyConservative(now, float64(l.cfg.fallbackRateU)/1e6, l.cfg.fallbackBurst)
		b.fallback = true
	}
	if (b.fallback || b.authorizedGeneration != s.generation) && b.ledger.outstanding() != 0 {
		return
	}
	b.authorizedGeneration = s.generation
	ttl := durationMilliseconds(r.SessionTTLMS)
	b.freshUntil = a.start.Add(min(l.cfg.maxSyncAge, max(time.Duration(0), ttl-time.Minute)))
	if d.Authorized && b.fallback && now.Before(b.freshUntil) {
		b.tokens.setPolicy(now, float64(l.cfg.localRateU)/1e6, l.cfg.localBurst)
		b.fallback = false
	}
}
func durationMilliseconds(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	if ms > math.MaxInt64/int64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ms) * time.Millisecond
}
func durationSeconds(seconds float64) time.Duration {
	ns := math.Ceil(seconds * float64(time.Second))
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	if ns <= 0 {
		return 0
	}
	return time.Duration(ns)
}
