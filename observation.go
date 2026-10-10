package ratelimiter

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics callbacks are synchronous by contract, but never run under a core
// lock. Each invocation has its own panic boundary so other observations survive.
func (l *Limiter) addCounter(name string, labels Labels, n uint64) {
	if l.metrics == nil {
		return
	}
	defer func() { _ = recover() }()
	l.metrics.AddCounter(name, labels, n)
}
func (l *Limiter) setGauge(name string, n float64) {
	if l.metrics == nil {
		return
	}
	defer func() { _ = recover() }()
	l.metrics.SetGauge(name, n)
}
func (l *Limiter) observeDuration(name string, n float64) {
	if l.metrics == nil {
		return
	}
	defer func() { _ = recover() }()
	l.metrics.Observe(name, n)
}
func (l *Limiter) sampleMetrics() {
	l.observations.sampleMu.Lock()
	now := l.clock.Now()
	var outstanding int64
	var oldest float64
	var fallback int
	for i := range l.store.shards {
		sh := &l.store.shards[i]
		sh.mu.Lock()
		for _, b := range sh.keys {
			b.mu.Lock()
			if l.observeBucketLocked(b, now) {
				fallback++
			}
			outstanding += b.ledger.outstanding()
			for _, at := range []time.Time{b.pendingSince, b.flightSince} {
				if !at.IsZero() {
					oldest = max(oldest, now.Sub(at).Seconds())
				}
			}
			b.mu.Unlock()
		}
		sh.mu.Unlock()
	}
	seconds := math.Float64frombits(l.observations.seconds.Load())
	l.observations.sampleMu.Unlock()
	l.flushObservations()
	l.setGauge(MetricOutstanding, float64(outstanding))
	l.setGauge(MetricOldestPending, oldest)
	l.setGauge(MetricFallbackKeys, float64(fallback))
	l.setGauge(MetricFallbackSeconds, seconds)
	l.setGauge(MetricCircuitState, float64(l.control.Load().circuit))
	l.setGauge(MetricQueueDepth, float64(len(l.work)))
	l.setGauge(MetricWorkers, float64(l.observations.workers.Load()))
}

// eventSink accepts sanitized finite events only. Implementations must enqueue
// without blocking; the application never delivers a logger callback directly.
type eventSink interface {
	offer(logEvent)
	stop()
}
type logEvent struct {
	kind       eventKind
	diagnostic backendDiagnostic
	permanent  bool
}

type eventKind uint8

const (
	eventRedisError eventKind = iota
	eventCircuitTransition
	eventFallbackEnter
	eventFallbackExit
	eventAccountingExpired
	eventPolicyMismatch
	eventAccountingError
	eventKindCount
)

// Each authorization epoch retains its first invalidation instant. A bucket can
// remain idle across arbitrarily many recoveries without losing that instant;
// no history list or per-key metrics map is needed.
type authorizationEpoch struct{ expired atomic.Pointer[time.Time] }
type observations struct {
	transitionMu  sync.Mutex // publication/creation ordering; never held by existing-key admission
	sampleMu      sync.Mutex
	nextSample    time.Time
	seconds       atomic.Uint64 // float64 seconds, retained across eviction
	fallbackEnter atomic.Uint64
	fallbackExit  atomic.Uint64
	freshness     atomic.Uint64
	circuit       atomic.Uint64
	workers       atomic.Int64
}

func addSeconds(dst *atomic.Uint64, seconds float64) {
	if seconds <= 0 {
		return
	}
	for old := dst.Load(); !dst.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+seconds)); old = dst.Load() {
	}
}

// observeBucketLocked accounts effective mode, including lazily noticed outage
// and freshness boundaries. It changes observation metadata only, never policy.
func (l *Limiter) observeBucketLocked(b *bucket, now time.Time) bool {
	s := l.control.Load()
	if b.observationStopped {
		return false
	}
	if !b.initialized && !b.fallback {
		return l.observeColdBucketLocked(b, s, now)
	}
	end := now
	if !s.stoppedAt.IsZero() && end.After(s.stoppedAt) {
		end = s.stoppedAt
	}
	if !b.terminalAt.IsZero() && end.After(b.terminalAt) {
		end = b.terminalAt
	}
	until := b.freshUntil
	if !b.initialized {
		until = maxTime(b.created.Add(l.cfg.maxSyncAge), b.coldStartedAt)
	}
	if b.initialized && !until.IsZero() && !end.Before(until) && b.expiryObserved != until {
		b.expiryObserved = until
		l.observations.freshness.Add(1)
	}
	start := until
	if b.initialized && start.IsZero() {
		start = b.unavailableAt
	}
	if b.epoch != nil {
		if expired := b.epoch.expired.Load(); expired != nil && (start.IsZero() || expired.Before(start)) {
			start = *expired
		}
	}
	if s.circuit != circuitHealthy && !s.outageAt.IsZero() && (start.IsZero() || s.outageAt.Before(start)) {
		start = s.outageAt
	}
	if start.Before(b.created) {
		start = b.created
	}
	active := l.cfg.failurePolicy == LocalOnly && (b.fallback || (!start.IsZero() && !end.Before(start)))
	if active && !b.observedFallback {
		b.observedFallback = true
		b.fallbackObservedAt = start
		l.observations.fallbackEnter.Add(1)
	}
	if b.observedFallback {
		addSeconds(&l.observations.seconds, end.Sub(b.fallbackObservedAt).Seconds())
		if end.After(b.fallbackObservedAt) {
			b.fallbackObservedAt = end
		}
	}
	if s.closed || s.permanent || b.terminalReason != "" {
		b.observationStopped = true
	}
	active = active && !s.closed && !s.permanent && b.terminalReason == ""
	if !active && b.observedFallback {
		b.observedFallback = false
		l.observations.fallbackExit.Add(1)
	}
	return active
}
func (l *Limiter) flushObservations() {
	if n := l.observations.fallbackEnter.Swap(0); n != 0 {
		l.addCounter(MetricFallbackTransitions, Labels{}, n)
		if l.events != nil {
			l.events.offer(logEvent{kind: eventFallbackEnter})
		}
	}
	if n := l.observations.fallbackExit.Swap(0); n != 0 {
		l.addCounter(MetricFallbackTransitions, Labels{}, n)
		if l.events != nil {
			l.events.offer(logEvent{kind: eventFallbackExit})
		}
	}
	if n := l.observations.freshness.Swap(0); n != 0 {
		l.addCounter(MetricFreshnessExpiries, Labels{}, n)
	}
	if n := l.observations.circuit.Swap(0); n != 0 {
		l.addCounter(MetricCircuitTransitions, Labels{}, n)
		if l.events != nil {
			l.events.offer(logEvent{kind: eventCircuitTransition})
		}
	}
}

func (l *Limiter) observeBackendError(err error) {
	if backendErrorClass(err) == errorShutdown {
		return
	}
	l.addCounter(MetricRedisErrors, Labels{}, 1)
	if l.events != nil {
		diagnostic := diagnosticUnknown
		var classified *backendError
		if errors.As(err, &classified) && classified.diagnostic < diagnosticCount {
			diagnostic = classified.diagnostic
		}
		l.events.offer(logEvent{kind: eventRedisError, diagnostic: diagnostic, permanent: backendErrorClass(err) == errorPermanent})
	}
}

// Background gauge refresh is at most once per second. Scrapes refresh on
// demand. Neither path is called by Allow, and all callbacks follow unlocking.
func (l *Limiter) maybeSampleMetrics(now time.Time) {
	l.observations.sampleMu.Lock()
	due := !now.Before(l.observations.nextSample)
	if due {
		l.observations.nextSample = now.Add(time.Second)
	}
	l.observations.sampleMu.Unlock()
	if due {
		l.sampleMetrics()
	}
}
func (l *Limiter) beginObservedWork() func() {
	start := l.clock.Now()
	n := l.observations.workers.Add(1)
	l.setGauge(MetricWorkers, float64(n))
	return func() {
		n := l.observations.workers.Add(-1)
		l.setGauge(MetricWorkers, float64(n))
		l.observeDuration(MetricSyncDuration, max(0, l.clock.Now().Sub(start).Seconds()))
	}
}

// outageTimeline stores cumulative completed outage time and the one live
// interval. Each control snapshot also keeps one preceding interval, not a chain.
// transitionMu orders publications and seals cold deadlines before that preceding
// interval can be replaced. It never nests circuitMu with a storage lock.
type outageTimeline struct {
	seconds        float64
	entries, exits uint64
	since          time.Time
}
type outageSample struct {
	seconds        float64
	entries, exits uint64
	active         bool
}

func (o outageTimeline) sample(at time.Time) outageSample {
	v := outageSample{seconds: o.seconds, entries: o.entries, exits: o.exits, active: !o.since.IsZero()}
	if v.active {
		v.seconds += max(0, at.Sub(o.since).Seconds())
	}
	return v
}
func (s *controlState) outageSampleAt(at time.Time) outageSample {
	if at.Before(s.outageChangedAt) {
		return s.previousOutages.sample(at)
	}
	return s.outages.sample(at)
}
func (l *Limiter) updateOutageTimeline(previous, next *controlState) {
	active := next.circuit != circuitHealthy && !next.closed && !next.permanent
	if active == !previous.outages.since.IsZero() {
		return
	}
	now := l.clock.Now()
	if !next.stoppedAt.IsZero() {
		now = next.stoppedAt
	}
	next.previousOutages = previous.outages
	next.outageChangedAt = now
	next.outages = previous.outages
	if active {
		next.outages.entries++
		next.outages.since = now
	} else {
		next.outages.seconds += max(0, now.Sub(next.outages.since).Seconds())
		next.outages.exits++
		next.outages.since = time.Time{}
	}
}

// finishObservationTransition runs after circuitMu is released and before the
// next publication or new-key birth. Unchanged availability needs no sweep.
// Terminal snapshots need no sweep: their
// preceding interval is never replaced by another outage transition.
func (l *Limiter) finishObservationTransition(previous *controlState) {
	defer l.observations.transitionMu.Unlock()
	s := l.control.Load()
	if l.cfg.failurePolicy != LocalOnly || s.closed || s.permanent || s.outages == previous.outages {
		return
	}
	now := l.clock.Now()
	for i := range l.store.shards {
		sh := &l.store.shards[i]
		sh.mu.Lock()
		for _, b := range sh.keys {
			b.mu.Lock()
			if !b.initialized && !b.coldCutoffKnown && !now.Before(maxTime(b.created.Add(l.cfg.maxSyncAge), b.coldStartedAt)) {
				b.coldCutoff = s.outageSampleAt(maxTime(b.created.Add(l.cfg.maxSyncAge), b.coldStartedAt))
				b.coldCutoffKnown = true
			}
			b.mu.Unlock()
		}
		sh.mu.Unlock()
	}
}

// Unlatched cold keys qualify only while the circuit is unavailable before
// MaxSyncAge. Unlike initialized or actually latched fallback keys, they cease
// qualifying at recovery. Integrating the cumulative circuit timeline makes
// both time and transition totals independent of observer sampling frequency.
func (l *Limiter) observeColdBucketLocked(b *bucket, s *controlState, now time.Time) bool {
	end := now
	if !s.stoppedAt.IsZero() && end.After(s.stoppedAt) {
		end = s.stoppedAt
	}
	if !b.terminalAt.IsZero() && end.After(b.terminalAt) {
		end = b.terminalAt
	}
	if end.Before(b.coldObservedAt) {
		return b.observedFallback
	}
	stopped := s.closed || s.permanent || b.terminalReason != ""
	if l.cfg.failurePolicy != LocalOnly {
		b.observationStopped = stopped
		return false
	}
	cutoff := maxTime(b.created.Add(l.cfg.maxSyncAge), b.coldStartedAt)
	at := minTime(end, cutoff)
	v := s.outageSampleAt(at)
	if !end.Before(cutoff) {
		if !b.coldCutoffKnown {
			b.coldCutoff = v
			b.coldCutoffKnown = true
		}
		v = b.coldCutoff
	}
	seconds := max(0, v.seconds-b.coldBaseline.seconds)
	entries := v.entries - b.coldBaseline.entries
	exits := v.exits - b.coldBaseline.exits
	if b.coldBaseline.active {
		entries++
	}
	active := v.active
	if !end.Before(cutoff) {
		seconds += end.Sub(cutoff).Seconds()
		if !active {
			entries++
		}
		active = true
	}
	if stopped && active {
		exits++
		active = false
	}
	// A caller may have been created after this terminal transition. Such a key
	// has no exposure before its creation, even when end predates its deadline.
	if end.Before(b.created) {
		seconds = 0
		entries = 0
		exits = 0
		active = false
	}
	addSeconds(&l.observations.seconds, seconds-b.coldAccountedSeconds)
	if entries > b.coldEntries {
		l.observations.fallbackEnter.Add(entries - b.coldEntries)
	}
	if exits > b.coldExits {
		l.observations.fallbackExit.Add(exits - b.coldExits)
	}
	b.coldAccountedSeconds = max(b.coldAccountedSeconds, seconds)
	b.coldEntries = max(b.coldEntries, entries)
	b.coldExits = max(b.coldExits, exits)
	b.coldObservedAt = end
	b.fallbackObservedAt = end
	b.observedFallback = active
	b.observationStopped = stopped
	return active
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// A clean session loss begins a new cold observation segment. Preserve an
// already observed active mode without recounting its entry or prior duration.
func (l *Limiter) resetColdObservationLocked(b *bucket, s *controlState, now time.Time) {
	b.coldStartedAt = now
	b.coldBaseline = s.outageSampleAt(now)
	b.coldCutoffKnown = false
	b.coldObservedAt = now
	b.coldAccountedSeconds = 0
	b.coldEntries = 0
	b.coldExits = 0
	if b.observedFallback {
		if b.fallback || s.circuit != circuitHealthy || !now.Before(b.created.Add(l.cfg.maxSyncAge)) {
			b.coldEntries = 1
		} else {
			b.observedFallback = false
			l.observations.fallbackExit.Add(1)
		}
	}
	b.fallbackObservedAt = now
}

// Terminal causes are finite application values; no key or backend error text
// crosses this boundary. Each installed transition emits once without requiring
// another Allow. Delivery retains the sink's bounded coalescing/drop policy.
func (l *Limiter) observeTerminalTransition(reason string) {
	var kind eventKind
	switch reason {
	case ReasonAccountingExpired:
		kind = eventAccountingExpired
	case ReasonPolicyMismatch:
		kind = eventPolicyMismatch
	case ReasonAccountingError:
		kind = eventAccountingError
	default:
		return
	}
	l.addCounter(MetricTerminalTransitions, Labels{Tier: TierLocal, Reason: reason}, 1)
	if l.events != nil {
		defer func() { _ = recover() }()
		l.events.offer(logEvent{kind: kind})
	}
}
