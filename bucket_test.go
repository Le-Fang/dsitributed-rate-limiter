package ratelimiter

import (
	"math"
	"testing"
	"time"
)

func TestBucketRefillAndBurst(t *testing.T) {
	now := time.Unix(0, 0)
	b := newTokenBucket(now, 2, 3)
	if !b.take(3) || b.take(1) {
		t.Fatal("burst violated")
	}
	b.refill(now.Add(250 * time.Millisecond))
	if b.take(1) {
		t.Fatal("fractional token authorized")
	}
	b.refill(now.Add(500 * time.Millisecond))
	if !b.take(1) || b.take(1) {
		t.Fatal("refill violated")
	}
}

func TestBucketRefillCases(t *testing.T) {
	start := time.Now()
	cases := []struct {
		name    string
		spent   int
		advance time.Duration
		want    float64
		request int
		allowed bool
	}{
		{"fractional", 2, 250 * time.Millisecond, 0.5, 1, false},
		{"negative elapsed", 2, -time.Second, 0, 1, false},
		{"huge idle", 2, time.Duration(math.MaxInt64), 2, 2, true},
		{"saturation", 1, 100 * time.Second, 2, 2, true},
		{"failed atomic batch", 1, 250 * time.Millisecond, 1.5, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newTokenBucket(start, 2, 2)
			if !b.take(tc.spent) {
				t.Fatal("setup consumption failed")
			}
			b.refill(start.Add(tc.advance))
			if b.tokens != tc.want {
				t.Fatalf("tokens = %v, want %v", b.tokens, tc.want)
			}
			if got := b.take(tc.request); got != tc.allowed {
				t.Fatalf("take = %v, want %v", got, tc.allowed)
			}
			if !tc.allowed && b.tokens != tc.want {
				t.Fatalf("failed take changed tokens to %v", b.tokens)
			}
		})
	}
}

func TestBucketBackwardTimeDoesNotMoveRefillPoint(t *testing.T) {
	start := time.Now()
	b := newTokenBucket(start, 1, 2)
	b.take(2)
	b.refill(start.Add(time.Second))
	b.refill(start.Add(-time.Hour))
	b.refill(start.Add(1500 * time.Millisecond))
	if b.tokens != 1.5 {
		t.Fatalf("tokens = %v, want 1.5", b.tokens)
	}
}

func TestBucketPolicyTransitionsPreserveBalance(t *testing.T) {
	start := time.Now()
	b := newTokenBucket(start, 4, 4)
	if !b.take(4) {
		t.Fatal("normal burst unavailable")
	}
	b.setPolicy(start.Add(250*time.Millisecond), 1, 2)
	if b.tokens != 1 {
		t.Fatalf("fallback entry tokens = %v, want 1", b.tokens)
	}
	if !b.take(1) {
		t.Fatal("fallback token unavailable")
	}
	b.refill(start.Add(750 * time.Millisecond))
	if b.take(1) {
		t.Fatal("fallback fractional token authorized")
	}
	b.setPolicy(start.Add(1250*time.Millisecond), 4, 4)
	if b.tokens != 1 {
		t.Fatalf("normal return tokens = %v, want 1", b.tokens)
	}
	for i := 0; i < 100; i++ {
		b.setPolicy(start.Add(1250*time.Millisecond), 1, 2)
		b.setPolicy(start.Add(1250*time.Millisecond), 4, 4)
	}
	if b.tokens != 1 {
		t.Fatalf("mode churn minted tokens: %v", b.tokens)
	}
}

func TestBucketLazyFallbackCreditsLowerRate(t *testing.T) {
	start := time.Now()
	b := newTokenBucket(start, 10, 10)
	b.take(10)
	b.setPolicyConservative(start.Add(time.Second), 1, 5)
	if b.tokens != 1 {
		t.Fatalf("lazy transition tokens = %v, want 1", b.tokens)
	}
}

func TestBucketEvictionWaitsForBothModes(t *testing.T) {
	start := time.Now()
	b := newTokenBucket(start, 2, 4)
	b.take(4)
	b.setPolicy(start, 0.5, 2)
	b.refill(start.Add(2 * time.Second))
	b.take(1)
	b.setPolicy(start.Add(2*time.Second), 2, 4)
	if b.safeToEvict(start.Add(3 * time.Second)) {
		t.Fatal("tiny idle TTL reset a burst")
	}
	if b.safeToEvict(start.Add(4 * time.Second)) {
		t.Fatal("fallback burst not yet safe despite full normal bucket")
	}
	if b.tokens != 4 {
		t.Fatalf("normal tokens = %v, want full burst", b.tokens)
	}
	if !b.safeToEvict(start.Add(6 * time.Second)) {
		t.Fatal("both mode deadlines elapsed")
	}
}

func TestBucketPolicyClampsWithoutMinting(t *testing.T) {
	start := time.Now()
	b := newTokenBucket(start, 4, 4)
	if !b.take(1) {
		t.Fatal("setup consumption failed")
	}
	b.setPolicy(start, 1, 2)
	if b.tokens != 2 {
		t.Fatalf("fallback tokens = %v, want clamped burst 2", b.tokens)
	}
	b.setPolicy(start, 4, 4)
	if b.tokens != 2 {
		t.Fatalf("normal return minted tokens: %v", b.tokens)
	}
}

func TestBucketBackwardPolicyTimeCannotAdvanceEviction(t *testing.T) {
	cases := []struct {
		name   string
		change func(*tokenBucket, time.Time, float64, int)
	}{
		{"exact transition", (*tokenBucket).setPolicy},
		{"lazy transition", (*tokenBucket).setPolicyConservative},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			spentAt := start.Add(10 * time.Second)
			b := newTokenBucket(spentAt, 100, 10)
			if !b.take(10) {
				t.Fatal("normal burst unavailable")
			}
			// Both timestamps precede the last accepted refill/consumption.
			tc.change(&b, start, 0.1, 10)
			tc.change(&b, start.Add(time.Second), 100, 10)
			if b.safeToEvict(start.Add(101 * time.Second)) {
				t.Fatal("backward transition made fallback burst evictable early")
			}
			if !b.safeToEvict(start.Add(110 * time.Second)) {
				t.Fatal("fallback burst had time to refill")
			}
		})
	}
}
