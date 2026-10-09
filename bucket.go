package ratelimiter

import (
	"math"
	"sync"
	"time"
)

// tokenBucket contains only local policy state. Its enclosing bucket mutex
// protects every method and field, including the eviction deadlines.
type tokenBucket struct {
	tokens             float64
	lastRefill         time.Time
	rate               float64
	burst              int
	normalRate         float64
	normalBurst        int
	fallbackRate       float64
	fallbackBurst      int
	fallback           bool
	normalFullAt       time.Time
	fallbackFullAt     time.Time
	normalFullTooFar   bool
	fallbackFullTooFar bool
}

func newTokenBucket(now time.Time, rate float64, burst int) tokenBucket {
	return tokenBucket{
		tokens: float64(burst), lastRefill: now, rate: rate, burst: burst,
		normalRate: rate, normalBurst: burst, normalFullAt: now,
	}
}

func (b *tokenBucket) refill(now time.Time) {
	b.refillAtRate(now, b.rate)
}

func (b *tokenBucket) refillAtRate(now time.Time, rate float64) {
	elapsed := now.Sub(b.lastRefill)
	if elapsed <= 0 {
		return
	}
	capacity := float64(b.burst)
	if b.tokens >= capacity {
		b.tokens = capacity
	} else if rate > 0 {
		seconds := float64(elapsed) / float64(time.Second)
		// Saturate before multiplying so a long idle gap cannot overflow the
		// refill increment or leave a balance above the burst.
		if seconds >= (capacity-b.tokens)/rate {
			b.tokens = capacity
		} else {
			b.tokens += seconds * rate
		}
	}
	b.lastRefill = now
}

func (b *tokenBucket) take(n int) bool {
	if n <= 0 || n > b.burst || float64(n) > b.tokens {
		return false
	}
	b.tokens -= float64(n)
	b.markFullDeadlines(b.lastRefill)
	return true
}

func (b *tokenBucket) setPolicy(now time.Time, rate float64, burst int) {
	b.refill(now)
	b.applyPolicy(now, rate, burst)
}

// setPolicyConservative is for a lazily observed outage: the exact moment
// normal refill stopped being valid is unknown, so use the lower rate for
// the entire unprocessed interval.
func (b *tokenBucket) setPolicyConservative(now time.Time, rate float64, burst int) {
	refillRate := b.rate
	if rate < refillRate {
		refillRate = rate
	}
	b.refillAtRate(now, refillRate)
	b.applyPolicy(now, rate, burst)
}

func (b *tokenBucket) applyPolicy(now time.Time, rate float64, burst int) {
	// A rejected backward refill must not move either safe-eviction clock back.
	if now.Before(b.lastRefill) {
		now = b.lastRefill
	}
	b.rate, b.burst = rate, burst
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}
	b.fallback = rate != b.normalRate || burst != b.normalBurst
	if b.fallback {
		b.fallbackRate, b.fallbackBurst = rate, burst
	}
	b.markFullDeadlines(now)
}

// safeToEvict checks only local refill safety; the owner must also check
// inactivity, outstanding accounting, references, and rejection latches.
func (b *tokenBucket) safeToEvict(now time.Time) bool {
	b.refill(now)
	return b.tokens >= float64(b.burst) &&
		!b.normalFullTooFar && !b.fallbackFullTooFar &&
		!now.Before(b.normalFullAt) && !now.Before(b.fallbackFullAt)
}

func (b *tokenBucket) markFullDeadlines(now time.Time) {
	b.normalFullAt, b.normalFullTooFar = laterFullDeadline(
		b.normalFullAt, b.normalFullTooFar, now, b.normalRate, b.normalBurst, b.tokens)
	if b.fallbackBurst > 0 {
		b.fallbackFullAt, b.fallbackFullTooFar = laterFullDeadline(
			b.fallbackFullAt, b.fallbackFullTooFar, now, b.fallbackRate, b.fallbackBurst, b.tokens)
	}
}

func laterFullDeadline(previous time.Time, tooFar bool, now time.Time, rate float64, burst int, tokens float64) (time.Time, bool) {
	if tooFar {
		return previous, true
	}
	deficit := float64(burst) - tokens
	if deficit <= 0 {
		if now.After(previous) {
			return now, false
		}
		return previous, false
	}
	if rate <= 0 {
		return previous, true
	}
	ns := math.Ceil(deficit / rate * float64(time.Second))
	if ns >= float64(math.MaxInt64) {
		return previous, true
	}
	at := now.Add(time.Duration(ns))
	if at.After(previous) {
		return at, false
	}
	return previous, false
}

// bucket owns one incarnation of a key. Callers retain a store reference while
// accessing it. The mutex couples local spending, the ledger and authorization.
type bucket struct {
	mu                    sync.Mutex
	key                   string
	refs                  int     // shard lock
	next, prev            *bucket // shard lock
	tokens                tokenBucket
	ledger                ledger
	created, lastActivity time.Time
	initialized           bool
	init                  *command // immutable registration identity/deadline, staged by worker
	session               string
	slot                  int
	freshUntil            time.Time
	authorizedGeneration  uint64
	globalRejected        bool
	retryAt               time.Time
	terminalReason        string
	capacityUntil         time.Time
	busy, queued          bool
	attemptID             uint64
	active                attempt
}
