package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func fallbackConfig(policy FailurePolicy) Config {
	c := Config{GlobalRate: 100, GlobalBurst: 20, LocalRate: 10, LocalBurst: 5, MaxOutstanding: 5, FailurePolicy: policy}
	if policy == LocalOnly {
		c.FallbackRate, c.FallbackBurst = 2, 3
	}
	return c
}
func transientFailure() error {
	return &backendError{class: errorTransient, cause: context.DeadlineExceeded}
}
func TestCircuitGenerationsAndProbeOwnership(t *testing.T) {
	l, _, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	prior := l.control.Load()
	l.recordFailure(transientFailure())
	if s := l.control.Load(); s == prior || s.circuit != circuitOpen || s.generation != 2 || prior.circuit != circuitHealthy {
		t.Fatalf("open: %+v", s)
	}
	l.recordFailure(transientFailure())
	if l.control.Load().generation != 2 {
		t.Fatal("repeated error advanced generation")
	}
	var wg sync.WaitGroup
	wins := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); wins <- l.beginProbe() }()
	}
	wg.Wait()
	close(wins)
	count := 0
	for won := range wins {
		if won {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("probe owners: %d", count)
	}
	l.recordFailure(transientFailure()) // Reopen while old probe is still owned.
	if l.beginProbe() {
		t.Fatal("overlapped obsolete probe")
	}
	l.finishProbe(nil)
	if s := l.control.Load(); s.circuit != circuitOpen || s.generation != 3 {
		t.Fatalf("obsolete probe reopened: %+v", s)
	}
	if !l.beginProbe() {
		t.Fatal("new probe blocked")
	}
	l.finishProbe(nil)
	if s := l.control.Load(); s.circuit != circuitHealthy || s.generation != 3 {
		t.Fatalf("recovery: %+v", s)
	}
	l.recordFailure(errors.New("unknown backend error"))
	if ok, r, _ := l.Allow("k"); ok || r != ReasonAccountingError {
		t.Fatalf("permanent: %v %s", ok, r)
	}
	if l.beginProbe() {
		t.Fatal("permanent probe")
	}
	l.recordFailure(transientFailure())
	l.finishProbe(nil)
	if !l.control.Load().permanent {
		t.Fatal("permanent latch cleared")
	}
}
func TestCircuitCancellationIsClosure(t *testing.T) {
	for _, policy := range []FailurePolicy{FailClosed, LocalOnly} {
		l, _, _ := newTestLimiter(t, fallbackConfig(policy))
		l.recordFailure(context.Canceled)
		if s := l.control.Load(); !s.closed || s.circuit != circuitHealthy || s.generation != 1 {
			t.Fatalf("cancellation outage: %+v", s)
		}
		if ok, r, _ := l.Allow("k"); ok || r != ReasonClosed {
			t.Fatalf("cancel: %v %s", ok, r)
		}
	}
}
func TestFallbackPolicyTable(t *testing.T) {
	for _, policy := range []FailurePolicy{FailClosed, LocalOnly} {
		for _, row := range []string{"cold", "startup stall", "open cold", "probing", "stale", "session unsafe", "fresh cap", "old cap", "global", "permanent", "terminal", "capacity"} {
			t.Run(map[FailurePolicy]string{FailClosed: "closed", LocalOnly: "local"}[policy]+"/"+row, func(t *testing.T) {
				l, c, _ := newTestLimiter(t, fallbackConfig(policy))
				b := testBucket(t, l, "k")
				if row != "cold" && row != "startup stall" && row != "open cold" {
					initializeTestBucket(t, l, b, 20e6)
				}
				want := ReasonRedisUnavailable
				fallback := true
				switch row {
				case "cold":
					want = ReasonSyncPending
					fallback = false
				case "startup stall", "stale":
					c.Advance(time.Second)
				case "open cold":
					l.recordFailure(transientFailure())
				case "probing":
					l.recordFailure(transientFailure())
					l.beginProbe()
				case "session unsafe":
					a, _ := l.prepareAttempt(b, c.Now())
					l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 20e6, SessionTTLMS: 60000})
				case "fresh cap", "old cap":
					l.AllowN("k", 5)
					if row == "fresh cap" {
						want = ReasonSyncPending
						fallback = false
					} else {
						l.recordFailure(transientFailure())
						c.Advance(500 * time.Millisecond)
					}
				case "global":
					a, _ := l.prepareAttempt(b, c.Now())
					l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: -1, SessionTTLMS: 86400000})
					l.recordFailure(transientFailure())
					want = ReasonGlobalLimit
					fallback = false
				case "permanent":
					l.recordFailure(errors.New("unclassified"))
					want = ReasonAccountingError
					fallback = false
				case "terminal":
					a, _ := l.prepareAttempt(b, c.Now())
					l.installReply(b, a, reply{Outcome: outcomeStateCorrupt})
					l.recordFailure(transientFailure())
					want = ReasonAccountingError
					fallback = false
				case "capacity":
					a, _ := l.prepareAttempt(b, c.Now())
					l.installReply(b, a, reply{Outcome: outcomeSessionCapacity})
					l.recordFailure(transientFailure())
					want = ReasonCapacityExhausted
					fallback = false
				}
				b.mu.Lock()
				before := b.ledger.outstanding()
				b.mu.Unlock()
				ok, r, i := l.Allow("k")
				expected := fallback && policy == LocalOnly
				if expected {
					want = ReasonLocalFallback
				}
				if ok != expected || r != want {
					t.Fatalf("decision %v %s %+v, expected %v %s", ok, r, i, expected, want)
				}
				if expected && (i.Mode != ModeLocalFallback || i.EffectiveRate != 2 || i.EffectiveBurst != 3) {
					t.Fatalf("fallback info %+v", i)
				}
				b.mu.Lock()
				defer b.mu.Unlock()
				if b.ledger.outstanding() != before {
					t.Fatal("fallback entered ledger")
				}
			})
		}
	}
}
func TestFallbackSameBalanceAndExhaustion(t *testing.T) {
	l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.AllowN("k", 5)
	l.recordFailure(transientFailure())
	c.Advance(500 * time.Millisecond)
	if ok, r, i := l.Allow("k"); !ok || r != ReasonLocalFallback || i.Remaining != 0 {
		t.Fatalf("conservative refill %v %s %+v", ok, r, i)
	}
	if ok, r, i := l.Allow("k"); ok || r != ReasonFallbackLimit || !i.RetryAfterKnown || i.RetryAfter != 500*time.Millisecond {
		t.Fatalf("exhaustion %v %s %+v", ok, r, i)
	}
	if ok, r, i := l.AllowN("k", 4); ok || r != ReasonFallbackLimit || i.RetryAfterKnown {
		t.Fatalf("oversized fallback %v %s %+v", ok, r, i)
	}
	if ok, r, _ := l.AllowN("k", 6); ok || r != ReasonInvalidRequest {
		t.Fatalf("oversized normal %v %s", ok, r)
	}
	for j := 0; j < 3; j++ {
		if !l.beginProbe() {
			t.Fatal("probe")
		}
		l.finishProbe(nil)
		a, ok := l.prepareAttempt(b, c.Now())
		if !ok {
			t.Fatal("prepare")
		}
		l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: a.batch.Sequence, BalanceU: 20e6, SessionTTLMS: 86400000})
		b.mu.Lock()
		tokens := b.tokens.tokens
		b.mu.Unlock()
		if tokens != 0 {
			t.Fatalf("exit minted %v", tokens)
		}
		if ok, r, _ := l.Allow("k"); ok || r != ReasonLocalLimit {
			t.Fatalf("normal refill reset %v %s", ok, r)
		}
		l.recordFailure(transientFailure())
		if ok, r, _ := l.Allow("k"); ok || r != ReasonFallbackLimit {
			t.Fatalf("entry reset %v %s", ok, r)
		}
	}
}
func TestFallbackReconcilesFlightThenPending(t *testing.T) {
	for _, negative := range []bool{false, true} {
		t.Run(map[bool]string{true: "negative", false: "positive"}[negative], func(t *testing.T) {
			l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
			b := testBucket(t, l, "k")
			initializeTestBucket(t, l, b, 20e6)
			l.AllowN("k", 2)
			old, _ := l.prepareAttempt(b, c.Now())
			l.Allow("k")
			l.recordFailure(transientFailure())
			if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
				t.Fatal("fallback")
			}
			l.beginProbe()
			l.finishProbe(nil)
			l.installReply(b, old, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 20e6, SessionTTLMS: 86400000})
			b.mu.Lock()
			if b.authorizedGeneration == l.control.Load().generation || b.ledger.Pending != 1 {
				t.Fatal("old reply authorized")
			}
			b.mu.Unlock()
			if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
				t.Fatal("ping authorized or replay")
			}
			a, ok := l.prepareAttempt(b, c.Now())
			if !ok || a.command.Op != opApply || a.command.Delta != 1 {
				t.Fatalf("pending reconcile %+v", a)
			}
			balance := int64(20e6)
			if negative {
				balance = -1
			}
			l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 2, BalanceU: balance, SessionTTLMS: 86400000})
			want := ReasonLocalLimit
			if negative {
				want = ReasonGlobalLimit
			}
			if ok, r, _ := l.Allow("k"); ok || r != want {
				t.Fatalf("recovery %v %s want %s", ok, r, want)
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.ledger.outstanding() != 0 || b.tokens.tokens != 0 {
				t.Fatal("replayed fallback or minted tokens")
			}
		})
	}
}

// A key may miss the entire outage on its admission path. Reconciliation must
// still account for the unobserved interval conservatively before normal refill.
func TestFallbackUnobservedOutagePreservesBalance(t *testing.T) {
	l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.AllowN("k", 5)
	l.recordFailure(transientFailure())
	c.Advance(500 * time.Millisecond)
	l.beginProbe()
	l.finishProbe(nil)
	a, _ := l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 20e6, SessionTTLMS: 86400000})
	if ok, r, i := l.Allow("k"); !ok || r != ReasonAllowed || i.Remaining != 0 {
		t.Fatalf("unobserved outage credited normal rate: %v %s %+v", ok, r, i)
	}
}
func TestFallbackLostACKAndSameGenerationReconciliation(t *testing.T) {
	l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.AllowN("k", 2)
	old, _ := l.prepareAttempt(b, c.Now())
	l.Allow("k")
	l.finishAttempt(b, old)
	c.Advance(time.Second) // Staleness, without a circuit error, starts fallback.
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
		t.Fatal("stale fallback")
	}
	retry, _ := l.prepareAttempt(b, c.Now())
	if retry.command != old.command {
		t.Fatal("lost ACK changed batch")
	}
	l.installReply(b, retry, reply{Outcome: outcomeDuplicate, CommittedSequence: 1, BalanceU: 20e6, SessionTTLMS: 86400000})
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
		t.Fatalf("pending normal debt skipped: %v %s", ok, r)
	}
	final, _ := l.prepareAttempt(b, c.Now())
	if final.command.Delta != 1 || final.command.Sequence != 2 {
		t.Fatalf("fallback replayed %+v", final)
	}
	l.installReply(b, final, reply{Outcome: outcomeOK, CommittedSequence: 2, BalanceU: 20e6, SessionTTLMS: 86400000})
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonAllowed {
		t.Fatalf("final APPLY did not authorize: %v %s", ok, r)
	}
}
func TestFallbackSessionCapacityAndTerminalPrecedence(t *testing.T) {
	for _, policy := range []FailurePolicy{FailClosed, LocalOnly} {
		l, c, _ := newTestLimiter(t, fallbackConfig(policy))
		b := testBucket(t, l, "k")
		initializeTestBucket(t, l, b, 20e6)
		l.Allow("k")
		a, _ := l.prepareAttempt(b, c.Now())
		l.installReply(b, a, reply{Outcome: outcomeSessionCapacity})
		if l.control.Load().circuit != circuitHealthy {
			t.Fatal("capacity opened circuit")
		}
		if _, ok := l.prepareAttempt(b, c.Now()); ok {
			t.Fatal("early capacity retry")
		}
		c.Advance(time.Second)
		a, ok := l.prepareAttempt(b, c.Now())
		if !ok {
			t.Fatal("capacity never retried")
		}
		l.installReply(b, a, reply{Outcome: outcomeSessionExpired})
		l.recordFailure(transientFailure())
		if ok, r, _ := l.Allow("k"); ok || r != ReasonAccountingExpired {
			t.Fatalf("expired debt fallback: %v %s", ok, r)
		}
		if _, ok := l.prepareAttempt(b, c.Now()); ok {
			t.Fatal("terminal retry")
		}
		b.mu.Lock()
		if b.ledger.outstanding() != 1 {
			t.Fatal("expired debt discarded")
		}
		b.mu.Unlock()
	}
}

func TestCircuitObsoleteProbeStillReportsPermanentFailure(t *testing.T) {
	l, _, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	l.recordFailure(transientFailure())
	l.beginProbe()
	l.recordFailure(transientFailure())
	l.finishProbe(&backendError{class: errorPermanent, cause: errors.New("permission denied")})
	if ok, r, _ := l.Allow("k"); ok || r != ReasonAccountingError {
		t.Fatalf("obsolete permanent failure ignored: %v %s", ok, r)
	}
}
func TestFallbackEqualPolicyStillRequiresReconciliation(t *testing.T) {
	cfg := fallbackConfig(LocalOnly)
	cfg.FallbackRate = cfg.LocalRate
	cfg.FallbackBurst = cfg.LocalBurst
	l, c, _ := newTestLimiter(t, cfg)
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.Allow("k")
	l.recordFailure(transientFailure())
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
		t.Fatal("equal policy not fallback")
	}
	l.beginProbe()
	l.finishProbe(nil)
	a, _ := l.prepareAttempt(b, c.Now())
	if a.command.Delta != 1 {
		t.Fatalf("fallback replay %+v", a)
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 20e6, SessionTTLMS: 86400000})
	if ok, r, i := l.Allow("k"); !ok || r != ReasonAllowed || i.Remaining != 2 {
		t.Fatalf("equal-policy recovery %v %s %+v", ok, r, i)
	}
}

func TestFallbackSnapshotRetryDoesNotCreditNormalRefill(t *testing.T) {
	l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 20e6)
	l.AllowN("k", 5)
	c.Advance(500 * time.Millisecond)
	l.clock = &decisionClock{clock: c, change: func() { l.recordFailure(transientFailure()) }}
	if ok, r, i := l.Allow("k"); !ok || r != ReasonLocalFallback || i.Remaining != 0 {
		t.Fatalf("snapshot retry minted refill: %v %s %+v", ok, r, i)
	}
}
func TestFallbackCleanupUnobservedOutage(t *testing.T) {
	cfg := fallbackConfig(LocalOnly)
	cfg.IdleTTL = time.Millisecond
	cfg.SyncThreshold = 5
	l, c, _ := newTestLimiter(t, cfg)
	b, ok := l.store.acquire("k", c.Now())
	if !ok {
		t.Fatal("acquire")
	}
	initializeTestBucket(t, l, b, 20e6)
	l.AllowN("k", 4)
	a, _ := l.prepareAttempt(b, c.Now())
	l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 20e6, SessionTTLMS: 86400000})
	l.store.release(b)
	l.recordFailure(transientFailure())
	c.Advance(250 * time.Millisecond)
	l.store.cleanup(c.Now(), 64)
	b2, ok := l.store.acquire("k", c.Now())
	if !ok || b2 != b {
		t.Fatal("early eviction")
	}
	b2.mu.Lock()
	tokens := b2.tokens.tokens
	b2.mu.Unlock()
	l.store.release(b2)
	if tokens != 1 {
		t.Fatalf("cleanup mutated live balance %v", tokens)
	}
	c.Advance(250 * time.Millisecond)
	l.store.cleanup(c.Now(), 64)
	b2, ok = l.store.acquire("k", c.Now())
	if !ok || b2 != b {
		t.Fatal("eviction before fallback refill deadline")
	}
	l.store.release(b2)
	if ok, r, i := l.Allow("k"); !ok || r != ReasonLocalFallback || i.Remaining != 1 {
		t.Fatalf("cleanup credited normal refill %v %s %+v", ok, r, i)
	}
}

func TestFallbackBlockedLatchConservativeRecovery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome outcome
		reason  string
		outage  time.Duration
		tokens  int
	}{
		{"global rejection", outcomeOK, ReasonGlobalLimit, 500 * time.Millisecond, 1},
		{"session capacity", outcomeSessionCapacity, ReasonCapacityExhausted, time.Second, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
			b := testBucket(t, l, "k")
			initializeTestBucket(t, l, b, 20e6)
			if ok, _, _ := l.AllowN("k", 5); !ok {
				t.Fatal("initial spend")
			}
			a, ok := l.prepareAttempt(b, c.Now())
			if !ok {
				t.Fatal("initial apply")
			}
			l.installReply(b, a, reply{Outcome: tc.outcome, CommittedSequence: 1, BalanceU: -1, SessionTTLMS: 86400000})
			l.recordFailure(transientFailure())
			c.Advance(tc.outage)
			if ok, r, i := l.Allow("k"); ok || r != tc.reason || i.Mode != ModeBlocked {
				t.Fatalf("latch precedence: %v %s %+v", ok, r, i)
			}
			b.mu.Lock()
			tokens := b.tokens.tokens
			b.mu.Unlock()
			if tokens != float64(tc.tokens) {
				t.Errorf("latched outage credited normal refill: got %v want %d", tokens, tc.tokens)
			}
			if !l.beginProbe() {
				t.Fatal("probe")
			}
			l.finishProbe(nil)
			a, ok = l.prepareAttempt(b, c.Now())
			if !ok {
				t.Fatal("recovery preparation")
			}
			l.installReply(b, a, reply{Outcome: outcomeOK, CommittedSequence: a.batch.Sequence, BalanceU: 20e6, SessionTTLMS: 86400000})
			if ok, r, i := l.AllowN("k", 3); ok || r != ReasonLocalLimit || i.Mode != ModeNormal || i.Remaining != tc.tokens {
				t.Fatalf("early recovered burst: %v %s %+v, want %d tokens", ok, r, i, tc.tokens)
			}
			if ok, r, i := l.AllowN("k", tc.tokens); !ok || r != ReasonAllowed || i.Remaining != 0 || i.EffectiveBurst != 5 || i.EffectiveRate != 10 {
				t.Fatalf("recovery balance: %v %s %+v", ok, r, i)
			}
			c.Advance(100 * time.Millisecond)
			if ok, r, i := l.Allow("k"); !ok || r != ReasonAllowed || i.Remaining != 0 {
				t.Fatalf("normal refill did not resume: %v %s %+v", ok, r, i)
			}
		})
	}
}

func TestFreshNegativePreservesNormalBurst(t *testing.T) {
	for _, balance := range []int64{-1000000, 0, 999999} {
		t.Run(fmt.Sprint(balance), func(t *testing.T) {
			l, c, _ := newTestLimiter(t, fallbackConfig(LocalOnly))
			b := testBucket(t, l, "fresh-negative")
			initializeTestBucket(t, l, b, 20e6)
			c.Advance(200 * time.Millisecond)
			a, _ := l.prepareAttempt(b, c.Now())
			l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: balance, SessionTTLMS: 86400000})
			if ok, r, _ := l.Allow("fresh-negative"); ok || r != ReasonGlobalLimit {
				t.Fatal(ok, r)
			}
			c.Advance(10 * time.Millisecond)
			a, _ = l.prepareAttempt(b, c.Now())
			l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 20e6, SessionTTLMS: 86400000})
			if ok, r, info := l.AllowN("fresh-negative", 5); !ok || r != ReasonAllowed {
				t.Fatalf("untouched burst lost: %v %s %+v", ok, r, info)
			}
		})
	}
}
