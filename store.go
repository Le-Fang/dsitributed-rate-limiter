package ratelimiter

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 64

// Lock order: scanMu -> shard.mu -> bucket.mu. The creation-only birthMu
// precedes shard.mu; the lookup fast path releases shard.mu before taking it.
// Admission/attempt code only
// takes bucket.mu; it must drop that lock before retaining/releasing references
// or delivering callbacks. The linked lists make scan work independent of map
// size. References and list links belong to the shard, all policy to the bucket.
type bucketStore struct {
	// Only the miss path joins observation transition ordering. These are wired
	// before publication by the application constructor; standalone stores omit it.
	birthMu       *sync.Mutex
	control       *atomic.Pointer[controlState]
	clock         clock
	shards        [shardCount]storeShard
	count         atomic.Int64
	max           int64
	rate          float64
	burst         int
	idleTTL       time.Duration
	fallbackRate  float64
	fallbackBurst int
	scanMu        sync.Mutex
	scanShard     int
	cleanupShard  int
}
type storeShard struct {
	mu                          sync.Mutex
	keys                        map[string]*bucket
	head, cursor, cleanupCursor *bucket
}

func newBucketStore(c coreConfig) *bucketStore {
	s := &bucketStore{max: int64(c.maxKeys), rate: float64(c.localRateU) / 1e6, burst: c.localBurst, idleTTL: c.idleTTL}
	if c.failurePolicy == LocalOnly {
		s.fallbackRate, s.fallbackBurst = float64(c.fallbackRateU)/1e6, c.fallbackBurst
	}
	for i := range s.shards {
		s.shards[i].keys = make(map[string]*bucket)
	}
	return s
}
func shardIndex(key string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return h & (shardCount - 1)
}
func (s *bucketStore) acquire(key string, now time.Time) (*bucket, bool) {
	sh := &s.shards[shardIndex(key)]
	sh.mu.Lock()
	if b := sh.keys[key]; b != nil {
		b.refs++
		sh.mu.Unlock()
		return b, true
	}
	sh.mu.Unlock()
	if s.birthMu != nil {
		s.birthMu.Lock()
		defer s.birthMu.Unlock()
		now = s.clock.Now()
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if b := sh.keys[key]; b != nil {
		b.refs++
		return b, true
	}

	for {
		n := s.count.Load()
		if n >= s.max {
			return nil, false
		}
		if s.count.CompareAndSwap(n, n+1) {
			break
		}
	}
	// Copy bounded key bytes; a short substring must not retain a large caller buffer.
	key = strings.Clone(key)
	b := &bucket{key: key, refs: 1, tokens: newTokenBucket(now, s.rate, s.burst), created: now, lastActivity: now, slot: -1}
	// Track both refill deadlines from birth. Cleanup has no circuit knowledge;
	// this conservatively retains normal keys through potential lazy outages.
	b.tokens.fallbackRate, b.tokens.fallbackBurst = s.fallbackRate, s.fallbackBurst
	if s.control != nil {
		b.coldBaseline = s.control.Load().outageSampleAt(now)
		b.coldStartedAt = now
	}
	if sh.head == nil {
		b.next, b.prev = b, b
		sh.head, sh.cursor, sh.cleanupCursor = b, b, b
	} else {
		b.next, b.prev = sh.head, sh.head.prev
		sh.head.prev.next = b
		sh.head.prev = b
	}
	sh.keys[key] = b
	return b, true
}
func (s *bucketStore) retain(b *bucket) bool {
	sh := &s.shards[shardIndex(b.key)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.keys[b.key] != b {
		return false
	}
	b.refs++
	return true
}
func (s *bucketStore) release(b *bucket) {
	sh := &s.shards[shardIndex(b.key)]
	sh.mu.Lock()
	b.refs--
	sh.mu.Unlock()
}

// scan returns at most limit retained references; the caller releases each.
// It inspects at most one node per shard visit, so hot shards cannot starve
// others. A pass touches at most 64 empty shards between populated shards.
func (s *bucketStore) scan(limit int) []*bucket {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	var out []*bucket
	empty := 0
	for len(out) < limit && empty < shardCount {
		sh := &s.shards[s.scanShard]
		s.scanShard = (s.scanShard + 1) % shardCount
		sh.mu.Lock()
		if b := sh.cursor; b != nil {
			sh.cursor = b.next
			b.refs++
			out = append(out, b)
			empty = 0
		} else {
			empty++
		}
		sh.mu.Unlock()
	}
	return out
}
func (s *bucketStore) cleanup(now time.Time, limit int) (removed []*bucket) {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	visited, empty := 0, 0
	for visited < limit && empty < shardCount {
		sh := &s.shards[s.cleanupShard]
		s.cleanupShard = (s.cleanupShard + 1) % shardCount
		sh.mu.Lock()
		b := sh.cleanupCursor
		if b == nil {
			empty++
			sh.mu.Unlock()
			continue
		}
		empty = 0
		visited++
		sh.cleanupCursor = b.next
		b.mu.Lock()
		// Eligibility checks must not credit the live balance at a stale normal
		// rate before admission has observed a circuit/policy transition.
		tokens := b.tokens
		eligible := b.refs == 0 && !b.busy && !b.queued && b.ledger.outstanding() == 0 && !b.globalRejected && b.terminalReason == "" && b.capacityUntil.IsZero() && now.Sub(b.lastActivity) >= s.idleTTL && tokens.safeToEvict(now)
		if eligible {
			if b.next == b {
				sh.head, sh.cursor, sh.cleanupCursor = nil, nil, nil
			} else {
				b.prev.next = b.next
				b.next.prev = b.prev
				if sh.head == b {
					sh.head = b.next
				}
				if sh.cursor == b {
					sh.cursor = b.next
				}
			}
			removed = append(removed, b)
			delete(sh.keys, b.key)
			s.count.Add(-1)
		}
		b.mu.Unlock()
		sh.mu.Unlock()
	}
	return removed
}
