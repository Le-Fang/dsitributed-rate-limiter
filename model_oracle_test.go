package ratelimiter

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"testing"
)

// modelFailure is intentionally data, so reducers preserve an invariant category
// rather than matching unstable balances, event indices or error prose.
type modelFailure struct {
	Category      string
	Seed          uint64
	Config        modelConfig
	Index         int
	Detail, State string
	Events        []modelEvent
}

func (e *modelFailure) Error() string { data, _ := json.Marshal(e); return string(data) }
func modelFailureCategory(format string) string {
	for _, p := range []struct{ words, category string }{
		{"envelope", "envelope"}, {"partition", "partition"}, {"backend accounting", "conservation"}, {"conservation/cap", "conservation"},
		{"local balance", "local-balance"}, {"admission=", "admission"}, {"generation", "generation"}, {"batch=", "identity"}, {"command=", "identity"},
		{"fair", "liveness"}, {"restore normal", "liveness"}, {"shutdown", "lifecycle"}, {"shared", "lifecycle"}, {"bound", "bounds"},
	} {
		if strings.Contains(format, p.words) {
			return p.category
		}
	}
	return "protocol"
}
func knownModelEvent(kind string) bool {
	switch kind {
	case "CreateKey", "Initialize", "AllowN", "AdvanceTime", "AdvanceLocalTime", "AdvanceRedisTime", "QueueSync", "BeginAttempt", "ExecuteRedis", "DeliverReply", "LoseReply", "TimeoutAttempt", "RetrySameBatch", "ProbeSuccess", "ProbeFailure", "OpenCircuit", "ExpireSession", "EvictCleanKey", "CancelParent", "Close", "CloseBudgetExpired", "CrashInstance", "RestartInstance", "RequireNormal", "DrainNotifications", "SaturateQueue", "ScanUntilQueued":
		return true
	}
	return false
}

// Values have units of 1e-15 tokens. The minimum of A(s)-R*s is sufficient
// to check every prior boundary. observe inserts the left and right sides of
// each admission, even at equal timestamps; no instantaneous burst is hidden.
// Short traces additionally compare every pair using independently formed bounds.
type modelEnvelope struct {
	rate, burst, admitted int64
	min                   *big.Int
	points                []modelEnvelopePoint
	allowance             *big.Int
}
type modelEnvelopePoint struct{ ns, admitted int64 }

func newModelEnvelope(rate, burst int64, quantized bool) *modelEnvelope {
	e := &modelEnvelope{rate: rate, burst: burst, allowance: new(big.Int), min: new(big.Int)}
	if quantized {
		e.allowance.Add(new(big.Int).Mul(big.NewInt(rate), big.NewInt(1_000_000)), big.NewInt(1_000_000_000))
	}
	return e
}
func (e *modelEnvelope) value(ns, a int64) *big.Int {
	return new(big.Int).Sub(new(big.Int).Mul(big.NewInt(a), big.NewInt(1_000_000_000_000_000)), new(big.Int).Mul(big.NewInt(e.rate), big.NewInt(ns)))
}
func (e *modelEnvelope) limit() *big.Int {
	return new(big.Int).Add(new(big.Int).Mul(big.NewInt(e.burst), big.NewInt(1_000_000_000_000_000)), e.allowance)
}
func (e *modelEnvelope) observe(ns, total int64, pairwise bool) error {
	left := e.value(ns, e.admitted)
	if left.Cmp(e.min) < 0 {
		e.min.Set(left)
	}
	right := e.value(ns, total)
	linear := new(big.Int).Sub(right, e.min).Cmp(e.limit()) <= 0
	if pairwise {
		e.points = append(e.points, modelEnvelopePoint{ns, e.admitted})
		quadratic := true
		for _, p := range e.points {
			used := new(big.Int).Mul(big.NewInt(total-p.admitted), big.NewInt(1_000_000_000_000_000))
			bound := new(big.Int).Add(e.limit(), new(big.Int).Mul(big.NewInt(e.rate), big.NewInt(ns-p.ns)))
			if used.Cmp(bound) > 0 {
				quadratic = false
			}
		}
		if linear != quadratic {
			return fmt.Errorf("linear/pairwise disagreement")
		}
		e.points = append(e.points, modelEnvelopePoint{ns, total})
	}
	e.admitted = total
	if !linear {
		return fmt.Errorf("admitted=%d at ns=%d exceeds burst=%d rateU=%d", total, ns, e.burst, e.rate)
	}
	if right.Cmp(e.min) < 0 {
		e.min.Set(right)
	}
	return nil
}
func (e *modelEnvelope) admit(ns, cost int64) error { return e.observe(ns, e.admitted+cost, false) }

// This is a lossless textual state key for the finite search alphabet. It includes
// immutable transport payloads and reply order, receipts, Redis clocks/sessions,
// local clocks and generations, authorization/latches, notification order and
// envelope minima. Pointer identities and mutex bookkeeping are not semantics.
func modelStateSnapshot(clocks []*fakeClock, generations []uint64, open, closed, completed, crashed []bool, incarnations []int, subjects map[modelBucketID]*foundationSubject, oracles map[modelBucketID]*foundationOracle, attempts map[uint64]*foundationAttempt, remotes map[string]*redisReference, retired map[*foundationOracle]*foundationSubject, envelopes map[string]*modelEnvelope, redisNS int64) string {
	var out strings.Builder
	fmt.Fprintf(&out, "redis=%d gen=%v open=%v closed=%v completed=%v crash=%v incarnation=%v;", redisNS, generations, open, closed, completed, crashed, incarnations)
	for i, c := range clocks {
		fmt.Fprintf(&out, "clock%d=%d;", i, c.Now().UnixNano())
	}
	ids := make([]modelBucketID, 0, len(subjects))
	for id := range subjects {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].instance != ids[j].instance {
			return ids[i].instance < ids[j].instance
		}
		return ids[i].key < ids[j].key
	})
	seen := map[*Limiter]bool{}
	for _, id := range ids {
		s, o := subjects[id], oracles[id]
		b := s.bucket
		flat := *o
		flat.tokens = nil
		flat.init = nil
		flat.fallbackEnvelope = nil
		fmt.Fprintf(&out, "bucket%v:o=%+v tokens=%s init=%v accepted=%d/%d/%d;", id, flat, o.tokens, o.init, s.accepted, s.allAccepted, s.fallbackAccepted)
		if e := o.fallbackEnvelope; e != nil {
			fmt.Fprintf(&out, "fallbackEnv=%d/%v;", e.admitted, e.min)
		}
		if !seen[s.limiter] {
			l := s.limiter
			seen[l] = true
			fmt.Fprintf(&out, "store=%d scan=%d cleanup=%d stopped=%v queue=", l.store.count.Load(), l.store.scanShard, l.store.cleanupShard, l.schedulingStopped)
			for n := len(l.work); n > 0; n-- {
				q := <-l.work
				if q == nil {
					out.WriteString("nil,")
				} else {
					fmt.Fprintf(&out, "%s,", q.key)
				}
				l.work <- q
			}
		}
		token := b.tokens
		fmt.Fprintf(&out, "tokens=%v/%v/%d normal=%v/%d fallback=%v/%d/%v times=%v/%v/%v tooFar=%v/%v;", token.tokens, token.rate, token.burst, token.normalRate, token.normalBurst, token.fallbackRate, token.fallbackBurst, token.fallback, token.lastRefill, token.normalFullAt, token.fallbackFullAt, token.normalFullTooFar, token.fallbackFullTooFar)
		fmt.Fprintf(&out, "production=%d/%d busy=%v queued=%v active=%s init=%v session=%s slot=%d initialized=%v auth=%d checked=%d fresh=%v rejected=%v terminal=%s capacity=%v next=%v activity=%v;", b.ledger.Pending, b.ledger.Committed, b.busy, b.queued, modelAttemptState(b.active), b.init, b.session, b.slot, b.initialized, b.authorizedGeneration, b.checkedGeneration, b.freshUntil, b.globalRejected, b.terminalReason, b.capacityUntil, b.nextSyncAt, b.lastActivity)
		if b.ledger.Flight != nil {
			fmt.Fprintf(&out, "flight=%+v;", *b.ledger.Flight)
		}
	}
	keys := make([]string, 0, len(remotes))
	for k := range remotes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := remotes[k]
		fmt.Fprintf(&out, "remote%s balance=%v logical=%d expires=%d sessions=%v applied=%v;", k, r.balanceNumerator, r.logical, r.expiresAt, r.sessions, r.applied)
	}
	nums := make([]uint64, 0, len(attempts))
	for n := range attempts {
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	for _, n := range nums {
		a := attempts[n]
		fmt.Fprintf(&out, "attempt%d owner=%v payload=%s response=%+v seq=%d gen=%d flags=%v/%v/%v/%v;", n, a.owner, modelAttemptState(a.payload), a.response, a.sequence, a.generation, a.executed, a.finished, a.timedOut, a.failed)
	}
	for _, k := range keys {
		if e := envelopes[k]; e != nil {
			fmt.Fprintf(&out, "env%s min=%v admitted=%d;", k, e.min, e.admitted)
		}
	}
	archived := []string{}
	for o, s := range retired {
		archived = append(archived, fmt.Sprintf("%s/%v/%d", o.session, o.receipts, s.accepted))
	}
	sort.Strings(archived)
	fmt.Fprintf(&out, "retired=%v", archived)
	return out.String()
}

func (o *foundationOracle) markFull(now int64, cfg modelConfig) {
	deadline := func(rate int64, burst int) int64 {
		deficit := new(big.Rat).Sub(big.NewRat(int64(burst), 1), o.tokens)
		if deficit.Sign() <= 0 {
			return now
		}
		ns := new(big.Rat).Mul(deficit, big.NewRat(1_000_000_000_000_000, rate))
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(ns.Num(), ns.Denom(), r)
		if r.Sign() > 0 {
			q.Add(q, big.NewInt(1))
		}
		if !q.IsInt64() || q.Int64() > (1<<63-1)-now {
			return 1<<63 - 1
		}
		return now + q.Int64()
	}
	o.normalFullNS = max(o.normalFullNS, deadline(cfg.LocalRateU, cfg.LocalBurst))
	if cfg.Policy == LocalOnly && cfg.FallbackBurst > 0 {
		o.fallbackFullNS = max(o.fallbackFullNS, deadline(cfg.FallbackRateU, cfg.FallbackBurst))
	}
}

func (o *foundationOracle) actualApplied(r *redisReference) int64 {
	sum := r.applied[o.session]
	for _, id := range o.previousSessions {
		sum += r.applied[id]
	}
	return sum
}

func TestStateMachineEnvelopeOracle(t *testing.T) {
	for _, quantized := range []bool{false, true} {
		e := newModelEnvelope(1_000_000, 2, quantized)
		for _, point := range []modelEnvelopePoint{{0, 0}, {0, 1}, {0, 2}, {1_000_000_000, 3}, {2_000_000_000, 4}} {
			if err := e.observe(point.ns, point.admitted, true); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.observe(2_000_000_000, 5, true); err == nil {
			t.Fatal("instantaneous envelope violation was missed")
		}
	}
	// Fractional refill is exact, including the two sides of simultaneous events.
	e := newModelEnvelope(333333, 1, false)
	if err := e.observe(3_000_003_001, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := e.observe(3_000_003_001, 2, true); err == nil {
		t.Fatal("late first boundary hid a burst")
	}
}

func modelAttemptState(a attempt) string {
	return fmt.Sprintf("%+v/%+v/%d/%d/%v", a.command, a.batch, a.id, a.generation, a.start)
}

func TestStateMachineStateHash(t *testing.T) {
	base := modelTrace{Config: modelConfig{Instances: 1, Cap: 2}, Events: []modelEvent{{Kind: "CreateKey", Key: "k"}, {Kind: "Initialize", Key: "k"}}}
	state := func(tr modelTrace) string {
		var result string
		if err := inspectTrace(tr, (*redisReference).execute, func(s string) { result = s }); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := state(base)
	if first != state(base) {
		t.Fatal("state snapshot depends on allocation identity")
	}
	for _, event := range []modelEvent{{Kind: "AdvanceLocalTime", AdvanceNS: 1}, {Kind: "AdvanceRedisTime", AdvanceNS: 1}, {Kind: "QueueSync", Key: "k"}, {Kind: "AllowN", Key: "k", Cost: 1}, {Kind: "BeginAttempt", Key: "k", Attempt: 1}, {Kind: "ProbeFailure", Key: "k"}} {
		tr := base
		tr.Events = append(append([]modelEvent(nil), base.Events...), event)
		if first == state(tr) {
			t.Fatalf("state hash omitted %+v", event)
		}
	}
}
