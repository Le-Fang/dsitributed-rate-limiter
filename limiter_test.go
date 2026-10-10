package ratelimiter

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiterColdKey(t *testing.T) {
	l, _, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 32})
	ok, reason, _ := l.Allow("user:42")
	if ok || reason != ReasonSyncPending {
		t.Fatalf("cold key: %v %s", ok, reason)
	}
	ok, reason, _ = l.AllowN("user:42", 0)
	if ok || reason != ReasonInvalidRequest {
		t.Fatal("invalid batch admitted")
	}
}
func TestLimiterNormalBoundaries(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, LocalRate: 2, LocalBurst: 4, MaxOutstanding: 5})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	ok, r, i := l.AllowN("k", 3)
	if !ok || r != ReasonAllowed || i.GlobalRate != 100 || i.GlobalBurst != 10 || i.EffectiveRate != 2 || i.EffectiveBurst != 4 || i.Remaining != 1 || i.RetryAfterKnown || i.RetryAfter != 0 || i.Tier != TierLocal || i.Mode != ModeNormal {
		t.Fatalf("decision %v %s %+v", ok, r, i)
	}
	ok, r, i = l.AllowN("k", 2)
	if ok || r != ReasonLocalLimit || !i.RetryAfterKnown || i.RetryAfter != 500*time.Millisecond {
		t.Fatalf("local %v %s %+v", ok, r, i)
	}
	c.Advance(500 * time.Millisecond)
	if ok, _, _ := l.AllowN("k", 2); !ok {
		t.Fatal("refill")
	}
	c.Advance(500 * time.Millisecond)
	if ok, r, _ := l.Allow("k"); ok || r != ReasonRedisUnavailable {
		t.Fatalf("stale %v %s", ok, r)
	}
}
func TestLimiterCapConcurrentAndAtomicACK(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, LocalBurst: 5, MaxOutstanding: 5})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for j := 0; j < 2; j++ {
		wg.Add(1)
		go func() { defer wg.Done(); ok, _, _ := l.AllowN("k", 3); results <- ok }()
	}
	wg.Wait()
	close(results)
	n := 0
	for ok := range results {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatal(n)
	}
	a, ok := l.prepareAttempt(b, c.Now())
	if !ok || a.batch.Delta != 3 {
		t.Fatalf("attempt %+v", a)
	}
	if _, ok := l.prepareAttempt(b, c.Now()); ok {
		t.Fatal("overlapping attempt")
	}
	if ok, _, _ := l.AllowN("k", 2); !ok {
		t.Fatal("delta during flight")
	}
	c.Advance(50 * time.Millisecond)
	if ok, r, _ := l.Allow("k"); ok || r != ReasonSyncPending {
		t.Fatalf("cap %v %s", ok, r)
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: a.batch.Sequence, BalanceU: 999999, RetryMS: 1, SessionTTLMS: 86400000})
	b.mu.Lock()
	if b.ledger.Pending != 2 || b.ledger.Flight != nil || !b.globalRejected {
		t.Fatal("ACK/latch")
	}
	b.mu.Unlock()
	if ok, r, i := l.Allow("k"); ok || r != ReasonGlobalLimit || i.Tier != TierGlobal || i.Mode != ModeBlocked || !i.RetryAfterKnown {
		t.Fatalf("negative %v %s %+v", ok, r, i)
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 10e6, SessionTTLMS: 86400000})
	if ok, r, _ := l.Allow("k"); ok || r != ReasonGlobalLimit {
		t.Fatal("duplicate reopened")
	}
}
func TestLimiterFreshnessUsesAttemptStart(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	a, _ := l.prepareAttempt(b, c.Now())
	c.Advance(time.Second)
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 10e6, SessionTTLMS: 86400000})
	if ok, r, _ := l.Allow("k"); ok || r != ReasonRedisUnavailable {
		t.Fatalf("late %v %s", ok, r)
	}
	a, _ = l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 10e6, SessionTTLMS: 60001})
	c.Advance(time.Millisecond)
	if ok, r, _ := l.Allow("k"); ok || r != ReasonRedisUnavailable {
		t.Fatalf("session guard %v %s", ok, r)
	}
}
func TestLimiterOldGenerationACKOnly(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	l.AllowN("k", 3)
	a, _ := l.prepareAttempt(b, c.Now())
	next := *l.control.Load()
	next.generation++
	l.control.Store(&next)
	b.mu.Lock()
	b.globalRejected = true
	prior := b.freshUntil
	b.mu.Unlock()
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 10e6, SessionTTLMS: 86400000})
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ledger.outstanding() != 0 || !b.globalRejected || !b.freshUntil.Equal(prior) || b.authorizedGeneration == next.generation {
		t.Fatal("old generation changed authority or lost ACK")
	}
}
func TestLimiterTimeoutPreservesAttemptIdentity(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	l.AllowN("k", 3)
	a, _ := l.prepareAttempt(b, c.Now())
	l.finishAttempt(b, a)
	retry, _ := l.prepareAttempt(b, c.Now())
	if a.command != retry.command || a.batch != retry.batch || a.id == retry.id {
		t.Fatal("retry identity")
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 10e6, SessionTTLMS: 86400000})
	b.mu.Lock()
	if !b.busy || b.ledger.outstanding() != 3 {
		t.Fatal("completed attempt changed state")
	}
	b.mu.Unlock()
	l.installReply(b, retry, reply{Outcome: outcomeDuplicate, CommittedSequence: 1, BalanceU: -1, RetryMS: math.MaxInt64, SessionTTLMS: math.MaxInt64})
	if ok, r, i := l.Allow("k"); ok || r != ReasonGlobalLimit || i.RetryAfter <= 0 {
		t.Fatalf("saturation %v %s %+v", ok, r, i)
	}
}
func TestLimiterKeysLifecycleAndTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l, _, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, Parent: ctx})
	for _, key := range []string{"", strings.Repeat("x", 1025)} {
		if ok, r, _ := l.Allow(key); ok || r != ReasonInvalidRequest {
			t.Fatal("invalid key")
		}
	}
	for _, key := range []string{"\x00 {opaque} \xff", strings.Repeat("x", 1024)} {
		if ok, r, _ := l.Allow(key); ok || r != ReasonSyncPending {
			t.Fatal("opaque key")
		}
	}
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	a, _ := l.prepareAttempt(b, l.clock.Now())
	l.installReply(b, a, reply{Outcome: outcomePolicyMismatch})
	if ok, r, _ := l.Allow("k"); ok || r != ReasonPolicyMismatch {
		t.Fatal("terminal")
	}
	if _, ok := l.prepareAttempt(b, l.clock.Now()); ok {
		t.Fatal("terminal scheduled")
	}
	cancel()
	if ok, r, _ := l.Allow("k"); ok || r != ReasonClosed {
		t.Fatal("cancel")
	}
	if len(f.queued()) != 0 {
		t.Fatal("admission IO")
	}
}
func TestLimiterNewConstructorCold(t *testing.T) {
	l, err := NewLimiter(Config{GlobalRate: 10, GlobalBurst: 10, Redis: RedisConfig{Address: "unused:6379"}})
	if err != nil {
		t.Fatal(err)
	}
	defer l.backend.Close()
	defer func() { l.cancelWorkers(); <-l.workersDone }()
	if ok, r, _ := l.Allow("k"); ok || r != ReasonSyncPending {
		t.Fatal("first admission must initialize asynchronously")
	}
}

// A clock barrier changes the external control state after the initial snapshot
// read but before the decision commits. No production test hook is necessary.
type decisionClock struct {
	clock
	count  atomic.Int64
	change func()
}

func (c *decisionClock) Now() time.Time {
	if c.count.Add(1) == 2 {
		c.change()
	}
	return c.clock.Now()
}
func TestLimiterAdmissionSnapshotHandshake(t *testing.T) {
	for _, transition := range []string{"closed", "permanent", "generation", "cancel"} {
		t.Run(transition, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, Parent: ctx})
			b := testBucket(t, l, "k")
			initializeTestBucket(t, l, b, 10e6)
			l.clock = &decisionClock{clock: c, change: func() {
				next := *l.control.Load()
				switch transition {
				case "closed":
					next.closed = true
				case "permanent":
					next.permanent = true
				case "generation":
					next.generation++
				case "cancel":
					cancel()
				}
				l.control.Store(&next)
			}}
			ok, r, _ := l.Allow("k")
			want := ReasonRedisUnavailable
			if transition == "closed" || transition == "cancel" {
				want = ReasonClosed
			}
			if transition == "permanent" {
				want = ReasonAccountingError
			}
			if ok || r != want {
				t.Fatalf("decision %v %s want %s", ok, r, want)
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.tokens.tokens != 10 || b.ledger.outstanding() != 0 {
				t.Fatal("changed snapshot consumed tokens")
			}
		})
	}
}

type checkingMetrics struct{ check func() }

func (m checkingMetrics) AddCounter(string, Labels, uint64) { m.check() }
func (checkingMetrics) SetGauge(string, float64)            {}
func (checkingMetrics) Observe(string, float64)             {}
func TestLimiterCallbacksOutsideLocks(t *testing.T) {
	l, _, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	called := false
	l.metrics = checkingMetrics{check: func() {
		called = true
		if !b.mu.TryLock() {
			t.Fatal("bucket held during callback")
		}
		b.mu.Unlock()
		sh := &l.store.shards[shardIndex("k")]
		if !sh.mu.TryLock() {
			t.Fatal("shard held during callback")
		}
		sh.mu.Unlock()
	}}
	l.Allow("k")
	if !called {
		t.Fatal("no observation")
	}
}
func TestLimiterNotificationReferencesBounded(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, MaxKeys: 1100})
	for j := 0; j < 1100; j++ {
		b, _ := l.store.acquire(fmt.Sprint(j), c.Now())
		l.notify(b)
		l.notify(b)
		l.store.release(b)
	}
	if len(l.work) != 1024 {
		t.Fatal(len(l.work))
	}
	b, _ := l.store.acquire("1099", c.Now())
	b.mu.Lock()
	queued := b.queued
	b.mu.Unlock()
	sh := &l.store.shards[shardIndex(b.key)]
	sh.mu.Lock()
	refs := b.refs
	sh.mu.Unlock()
	l.store.release(b)
	if queued || refs != 1 {
		t.Fatalf("failed enqueue leaked reference/flag: %v %d", queued, refs)
	}
}
func TestLimiterSessionErrorsPreserveDebt(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		t.Run(fmt.Sprint(dirty), func(t *testing.T) {
			l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10})
			b := testBucket(t, l, "k")
			initializeTestBucket(t, l, b, 10e6)
			if dirty {
				l.Allow("k")
			}
			a, _ := l.prepareAttempt(b, c.Now())
			l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
			b.mu.Lock()
			defer b.mu.Unlock()
			if dirty {
				if b.terminalReason != ReasonAccountingExpired || b.ledger.outstanding() != 1 {
					t.Fatal("expired debt discarded")
				}
			} else if b.initialized || b.init != nil || b.terminalReason != "" {
				t.Fatal("clean session did not require initialization")
			}
		})
	}
}
func TestLimiterPermanentBatchValidation(t *testing.T) {
	l, _, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, LocalBurst: 5, MaxOutstanding: 5})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 10e6)
	for _, n := range []int{-1, 0, 6, math.MaxInt} {
		if ok, r, i := l.AllowN("k", n); ok || r != ReasonInvalidRequest || i.RetryAfterKnown {
			t.Fatalf("invalid %d: %v %s %+v", n, ok, r, i)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ledger.outstanding() != 0 || b.tokens.tokens != 5 {
		t.Fatal("invalid request consumed")
	}
}

func TestLimiterFreshCapExhaustionReason(t *testing.T) {
	l, _, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 5})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 5e6)
	if ok, _, _ := l.AllowN("k", 5); !ok {
		t.Fatal("initial burst")
	}
	if ok, r, _ := l.Allow("k"); ok || r != ReasonSyncPending {
		t.Fatalf("fresh full accounting cap: %v %s", ok, r)
	}
}
