package ratelimiter

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStoreExactCrossShardCapacity(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 10, GlobalBurst: 10, MaxKeys: 1})
	keys := []string{"a", "b"}
	if shardIndex(keys[0]) == shardIndex(keys[1]) {
		t.Fatal("requires different shards")
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan *bucket, 2)
	for _, k := range keys {
		wg.Add(1)
		go func(k string) { defer wg.Done(); <-start; b, _ := l.store.acquire(k, c.Now()); results <- b }(k)
	}
	close(start)
	wg.Wait()
	close(results)
	n := 0
	for b := range results {
		if b != nil {
			n++
			l.store.release(b)
		}
	}
	if n != 1 || l.store.count.Load() != 1 {
		t.Fatalf("inserted %d count %d", n, l.store.count.Load())
	}
	if ok, r, _ := l.Allow("other"); ok || r != ReasonCapacityExhausted {
		t.Fatal("capacity")
	}
}
func TestStoreEvictionSafety(t *testing.T) {
	for _, state := range []string{"reference", "pending", "flight", "busy", "queued", "global", "terminal", "refill", "clean"} {
		t.Run(state, func(t *testing.T) {
			l, c, _ := newTestLimiter(t, Config{GlobalRate: 1, GlobalBurst: 10, IdleTTL: time.Nanosecond})
			b, _ := l.store.acquire("k", c.Now())
			b.mu.Lock()
			switch state {
			case "pending":
				b.ledger.Pending = 1
			case "flight":
				b.ledger.Flight = &batch{Sequence: 1, Delta: 1}
			case "busy":
				b.busy = true
			case "queued":
				b.queued = true
			case "global":
				b.globalRejected = true
			case "terminal":
				b.terminalReason = ReasonAccountingError
			case "refill":
				b.tokens.take(1)
			}
			b.mu.Unlock()
			if state != "reference" {
				l.store.release(b)
			}
			c.Advance(time.Millisecond)
			for i := 0; i < 64; i++ {
				l.store.cleanup(c.Now(), 1)
			}
			want := int64(1)
			if state == "clean" {
				want = 0
			}
			if l.store.count.Load() != want {
				t.Fatalf("count=%d", l.store.count.Load())
			}
			if state == "reference" {
				l.store.release(b)
			}
			if state == "refill" {
				c.Advance(time.Second)
				for i := 0; i < 64; i++ {
					l.store.cleanup(c.Now(), 1)
				}
				if l.store.count.Load() != 0 {
					t.Fatal("fully refilled retained")
				}
			}
		})
	}
}
func TestStoreBoundedScanCursor(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 10, GlobalBurst: 10})
	for i := 0; i < 100; i++ {
		b, _ := l.store.acquire(fmt.Sprint(i), c.Now())
		l.store.release(b)
	}
	seen := map[string]bool{}
	for rounds := 0; len(seen) < 100; rounds++ {
		if rounds > 1000 {
			t.Fatal("cursor starved keys")
		}
		chunk := l.store.scan(7)
		if len(chunk) > 7 || len(chunk) == 0 {
			t.Fatalf("chunk %d", len(chunk))
		}
		for _, b := range chunk {
			seen[b.key] = true
			l.store.release(b)
		}
	}
}
