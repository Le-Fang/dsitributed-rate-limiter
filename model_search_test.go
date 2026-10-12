package ratelimiter

import (
	"crypto/sha256"
	"reflect"
	"testing"
	"time"
)

// Exact finite domain: two authorized instances; one key; C=B=local burst=1;
// rate=1 token/s; costs {1,C}={1}; one attempt/instance; elapsed time {0,1s}.
// A refill tick and the freshness boundary both equal 1s. Larger costs, retries,
// drift, faults and recovery are covered by retained/generated schedules.
//
// BFS merges equal states only after assertions. A shortest-depth visit has at
// least as much suffix budget as a later visit. The state hash includes request/
// reply identities, queue order, clocks, generation, ledgers, latches, balances,
// refill deadlines, Redis expiry/dedup and envelope minima. The one-tick budget
// is represented by the clock. Invalid actions and zero-time strict no-ops are
// rejected from independent oracle state, without reconstructing the subject.
// The optimized and original enumerations must have identical depth-5 states;
// the full optimized search must retain the original depth-12 state count.
func TestStateMachineExhaustive(t *testing.T) {
	original := searchModelSchedules(t, 5, false)
	optimized := searchModelSchedules(t, 5, true)
	if !reflect.DeepEqual(original.states, optimized.states) {
		t.Fatal("frontier pruning changed reachable states")
	}
	full := searchModelSchedules(t, 12, true)
	if len(full.states) != 19070 || full.maxDepth != 12 {
		t.Fatalf("depth-12 reachability changed: states=%d depth=%d", len(full.states), full.maxDepth)
	}
	t.Logf("exhaustive states=%d proposed=%d replayed=%d maxDepth=%d; depth5 equivalence=%d", len(full.states), full.transitions, full.replayed, full.maxDepth, len(original.states))
}

type modelSearchResult struct {
	states                          map[[32]byte]bool
	transitions, replayed, maxDepth int
}

func searchModelSchedules(t *testing.T, depthLimit int, prune bool) modelSearchResult {
	t.Helper()
	base := modelTrace{Config: modelConfig{Instances: 2, Cap: 1, RateU: 1_000_000, Burst: 1, LocalRateU: 1_000_000, LocalBurst: 1, FreshnessNS: int64(time.Second)}}
	for i := 0; i < 2; i++ {
		base.Events = append(base.Events, modelEvent{Kind: "CreateKey", Instance: i, Key: "k"}, modelEvent{Kind: "Initialize", Instance: i, Key: "k"})
	}
	var alphabet []modelEvent
	for i := 0; i < 2; i++ {
		for _, kind := range []string{"AllowN", "BeginAttempt", "ExecuteRedis", "DeliverReply", "LoseReply", "TimeoutAttempt"} {
			alphabet = append(alphabet, modelEvent{Kind: kind, Instance: i, Key: "k", Cost: 1, Attempt: uint64(i + 1)})
		}
	}
	alphabet = append(alphabet, modelEvent{Kind: "AdvanceTime", Key: "k"}, modelEvent{Kind: "AdvanceTime", Key: "k", AdvanceNS: int64(time.Second)})
	capture := func(trace modelTrace) ([32]byte, uint16, error) {
		var state string
		var enabled uint16
		trace.searchCapture = func(mask uint16) { enabled = mask }
		err := inspectTrace(trace, (*redisReference).execute, func(s string) { state = s })
		return sha256.Sum256([]byte(state)), enabled, err
	}
	initial, mask, err := capture(base)
	if err != nil {
		t.Fatal(err)
	}
	type node struct {
		events  []modelEvent
		ticked  bool
		enabled uint16
	}
	frontier := []node{{enabled: mask}}
	result := modelSearchResult{states: map[[32]byte]bool{initial: true}}
	for depth := 1; depth <= depthLimit; depth++ {
		var next []node
		for _, prior := range frontier {
			for action, event := range alphabet {
				if prior.ticked && event.AdvanceNS > 0 {
					continue
				}
				result.transitions++
				if prune && prior.enabled&(1<<action) == 0 {
					continue
				}
				events := append(append([]modelEvent(nil), prior.events...), event)
				trace := base
				trace.Events = append(append([]modelEvent(nil), base.Events...), events...)
				hash, enabled, err := capture(trace)
				result.replayed++
				if err != nil {
					failModelTrace(t, trace, err)
				}
				if result.states[hash] {
					continue
				}
				result.states[hash] = true
				next = append(next, node{events, prior.ticked || event.AdvanceNS > 0, enabled})
			}
		}
		if len(next) > 0 {
			result.maxDepth = depth
		}
		frontier = next
	}
	return result
}

// This is a scheduling eligibility summary of the existing independent oracle,
// not another accounting model. Bit order matches the finite alphabet above.
func modelSearchEnabled(oracles map[modelBucketID]*foundationOracle, attempts map[uint64]*foundationAttempt) uint16 {
	var mask uint16 = 1 << 13 // Positive tick; the search separately bounds its count.
	for i := 0; i < 2; i++ {
		o := oracles[modelBucketID{i, "k"}]
		if o == nil {
			continue
		}
		shift := uint(i * 6)
		mask |= 1 << shift // Admission can change refill/activity/queue even if denied.
		a := attempts[uint64(i+1)]
		if a == nil && o.busy == 0 {
			mask |= 1 << (shift + 1)
		}
		if a == nil {
			continue
		}
		if !a.executed && !a.finished {
			mask |= 1 << (shift + 2)
		}
		if a.executed && !a.finished {
			mask |= 1<<(shift+3) | 1<<(shift+4)
		}
		if !a.finished && !a.timedOut && o.busy == uint64(i+1) {
			mask |= 1 << (shift + 5)
		}
	}
	// Bit12 (advance zero) is a strict no-op: no timers run in this finite domain;
	// adding identical envelope boundaries cannot change any pairwise constraint.
	return mask
}
