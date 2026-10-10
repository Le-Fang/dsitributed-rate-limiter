package ratelimiter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type modelEvent struct {
	Kind      string
	Instance  int
	Key       string
	Cost      int
	AdvanceNS int64
	Attempt   uint64
}
type modelTrace struct {
	Seed   uint64
	Config modelConfig
	Events []modelEvent
}
type modelConfig struct {
	Instances                             int
	RateU                                 int64
	Burst, Cap, LocalBurst, FallbackBurst int
	LocalRateU, FallbackRateU             int64
	SyncNS, FreshnessNS                   int64
	Policy                                FailurePolicy
}

func TestStateMachineFoundation(t *testing.T) {
	paths, err := filepath.Glob("testdata/model/*.json")
	if err != nil || len(paths) < 4 {
		t.Fatalf("foundation traces: %v, %v", paths, err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var trace modelTrace
			if err := json.Unmarshal(data, &trace); err != nil {
				t.Fatal(err)
			}
			if err := checkTrace(trace); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStateMachineFoundationRejectsUnsupportedAndInvalidEvents(t *testing.T) {
	for _, events := range [][]modelEvent{
		{{Kind: "CreateKey", Key: "k"}, {Kind: "FallbackAllow", Key: "k"}},
		{{Kind: "AllowN", Key: "k", Cost: 1}},
		{{Kind: "CreateKey", Key: "k"}, {Kind: "BeginAttempt", Key: "k", Attempt: 1}},
		{{Kind: "CreateKey", Key: "k"}, {Kind: "ExecuteRedis", Key: "k", Attempt: 1}},
	} {
		if err := checkTrace(modelTrace{Config: modelConfig{Instances: 1, Cap: 5}, Events: events}); err == nil {
			t.Fatalf("accepted invalid events: %+v", events)
		}
	}
}

// Requests leave independent receipts. The oracle uses rational local refill,
// receipt queries and the independent integer Redis reference, never production
// token/ledger/decision helpers. The subject runs real prepare/install/AllowN.
type modelReceipt struct {
	cost                  int64
	sequence              uint64
	applied, acknowledged bool
}
type foundationOracle struct {
	receipts                          []modelReceipt
	latestBatch, flight, acknowledged uint64
	tokens                            *big.Rat
	initialized, rejected             bool
	freshNS                           int64
	createdNS, refillNS               int64
	generation                        uint64
	fallback                          bool
	fallbackAccepted                  int64
	slot                              int
	busy                              uint64
}
type foundationSubject struct {
	limiter  *Limiter
	bucket   *bucket
	accepted int64
}
type modelBucketID struct {
	instance int
	key      string
}
type foundationAttempt struct {
	owner                        modelBucketID
	payload                      attempt
	response                     reply
	sequence                     uint64
	generation                   uint64
	executed, finished, timedOut bool
}
type envelopeBoundary struct {
	ns     int64
	totals map[string]int64
}

func checkTrace(trace modelTrace) error {
	return checkTraceWithExecution(trace, (*redisReference).execute)
}

// Execution remains independently replaceable so a regression can prove that
// actual backend effects, not merely a prepared batch, drive conservation.
func checkTraceWithExecution(trace modelTrace, execute func(*redisReference, command, int64) reply) error {
	cfg := trace.Config
	if cfg.Instances <= 0 || cfg.Cap <= 0 || cfg.Cap > 1_000_000 {
		return fmt.Errorf("seed %d: invalid model config %+v", trace.Seed, cfg)
	}
	if cfg.RateU == 0 {
		cfg.RateU = 1_000_000
	}
	if cfg.Burst == 0 {
		cfg.Burst = cfg.Cap
	}
	if cfg.LocalBurst == 0 {
		cfg.LocalBurst = cfg.Cap
	}
	if cfg.LocalRateU == 0 {
		cfg.LocalRateU = cfg.RateU
	}
	if cfg.FreshnessNS == 0 {
		cfg.FreshnessNS = int64(time.Second)
	}
	if cfg.SyncNS == 0 {
		cfg.SyncNS = int64(100 * time.Millisecond)
	}
	clocks := make([]*fakeClock, cfg.Instances)
	generations := make([]uint64, cfg.Instances)
	openCircuits := make([]bool, cfg.Instances)
	limiters := make([]*Limiter, cfg.Instances)
	cancels := make([]context.CancelFunc, cfg.Instances)
	closed := make([]bool, cfg.Instances)
	completed := make([]bool, cfg.Instances)
	results := make([]error, cfg.Instances)
	start := time.Unix(1700000000, 0)
	for i := range clocks {
		clocks[i] = newFakeClock(start)
		generations[i] = 1
		c := coreConfig{operationTimeout: time.Second, globalRateU: cfg.RateU, globalBurst: cfg.Burst, localRateU: cfg.LocalRateU, localBurst: cfg.LocalBurst, maxOutstanding: cfg.Cap, maxSyncAge: time.Duration(cfg.FreshnessNS), syncInterval: time.Duration(cfg.SyncNS), syncThreshold: cfg.Cap, maxKeys: 2048, idleTTL: time.Minute, failurePolicy: cfg.Policy, fallbackRateU: cfg.FallbackRateU, fallbackBurst: cfg.FallbackBurst}
		parent, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		defer cancel()
		limiters[i] = newLimiterCore(c, parent, clocks[i], &fakeBackend{clock: clocks[i]}, nil)
	}
	subjects := map[modelBucketID]*foundationSubject{}
	oracles := map[modelBucketID]*foundationOracle{}
	attempts := map[uint64]*foundationAttempt{}
	remotes := map[string]*redisReference{}
	boundaries := []envelopeBoundary{{totals: map[string]int64{}}}
	var nowNS int64
	fail := func(index int, format string, args ...interface{}) error {
		return fmt.Errorf("seed=%d config=%+v event=%d: %s; events=%+v", trace.Seed, cfg, index, fmt.Sprintf(format, args...), trace.Events[:index+1])
	}
	for index, event := range trace.Events {
		id := modelBucketID{event.Instance, event.Key}
		if event.Instance < 0 || event.Instance >= cfg.Instances {
			return fail(index, "invalid instance")
		}
		subject, oracle := subjects[id], oracles[id]
		if event.Kind != "CreateKey" && subject == nil {
			return fail(index, "event without a created key")
		}
		switch event.Kind {
		case "CreateKey":
			if subject != nil {
				return fail(index, "duplicate key")
			}
			l := limiters[id.instance]
			b, ok := l.store.acquire(id.key, clocks[id.instance].Now())
			if !ok {
				return fail(index, "store capacity")
			}
			subjects[id] = &foundationSubject{limiter: l, bucket: b}
			oracles[id] = &foundationOracle{tokens: big.NewRat(int64(cfg.LocalBurst), 1), createdNS: nowNS, refillNS: nowNS}
			if remotes[id.key] == nil {
				remotes[id.key] = &redisReference{rate: cfg.RateU, burst: int64(cfg.Burst) * 1e6}
			}
		case "Initialize":
			if oracle.initialized {
				return fail(index, "duplicate initialization")
			}
			b, l := subject.bucket, subject.limiter
			b.mu.Lock()
			b.init = &command{Op: opInit, Session: fmt.Sprintf("%032x", id.instance+1), Slot: -1, Delta: int64(cfg.Cap), RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6, RegistrationDeadlineMS: start.UnixMilli() + nowNS/int64(time.Millisecond) + 30000}
			b.mu.Unlock()
			a, ok := l.prepareAttempt(b, clocks[id.instance].Now())
			if !ok {
				return fail(index, "init preparation failed")
			}
			r := execute(remotes[id.key], a.command, start.UnixMilli()+nowNS/int64(time.Millisecond))
			l.installReply(b, a, r)
			oracle.slot = r.Slot
			oracle.initialized = true
			oracle.rejected = r.BalanceU < 1e6
			oracle.freshNS = nowNS + cfg.FreshnessNS
			oracle.generation = generations[id.instance]
		case "DrainNotifications":
			drainModelNotifications(subject.limiter, nil)
		case "SaturateQueue":
			l := subject.limiter
			for i := 0; i < 1024; i++ {
				b, ok := l.store.acquire(fmt.Sprintf("queue-filler-%d", i), clocks[id.instance].Now())
				if !ok {
					return fail(index, "filler capacity")
				}
				l.notify(b)
				l.store.release(b)
			}
			if len(l.work) != 1024 {
				return fail(index, "queue not saturated")
			}
		case "QueueSync":
			subject.limiter.notify(subject.bucket)
			if subject.bucket.queued {
				return fail(index, "saturated notification was not dropped")
			}
		case "ScanUntilQueued":
			found := false
			for pass := 0; pass < 128 && !found; pass++ {
				subject.limiter.scanDue(clocks[id.instance].Now())
				drainModelNotifications(subject.limiter, func(b *bucket) {
					if b == subject.bucket {
						found = true
					}
				})
			}
			if !found {
				return fail(index, "scan failed to rediscover dropped work")
			}
		case "AdvanceTime":
			if event.AdvanceNS < 0 {
				return fail(index, "negative time advance")
			}
			nowNS += event.AdvanceNS
			for _, c := range clocks {
				c.Advance(time.Duration(event.AdvanceNS))
			}
		case "CancelParent":
			cancels[id.instance]()
			closed[id.instance] = true
		case "Close", "CloseBudgetExpired":
			l := subject.limiter
			closed[id.instance] = true
			if completed[id.instance] {
				if got := l.Close(context.Background()); got != results[id.instance] {
					return fail(index, "changed shared shutdown result")
				}
				break
			}
			var modelErr error
			budgetCtx, expire := context.WithCancel(context.Background())
			defer expire()
			if event.Kind == "CloseBudgetExpired" {
				l.shutdownContext = func() (context.Context, context.CancelFunc) {
					return lifecycleDeadline{budgetCtx, time.Now().Add(5 * time.Second)}, expire
				}
			}
			transport := &lifecycleBackend{backend: l.backend}
			transport.execute = func(ctx context.Context, key string, c command) (reply, error) {
				owner := modelBucketID{id.instance, key}
				o := oracles[owner]
				if o == nil || o.busy != 0 {
					modelErr = fmt.Errorf("flush without joined ownership")
					return reply{}, modelErr
				}
				if o.flight == 0 {
					o.latestBatch++
					o.flight = o.latestBatch
					for i := range o.receipts {
						if o.receipts[i].sequence == 0 {
							o.receipts[i].sequence = o.flight
						}
					}
				}
				var delta int64
				for _, r := range o.receipts {
					if r.sequence == o.flight {
						delta += r.cost
					}
				}
				want := command{Op: opApply, Session: fmt.Sprintf("%032x", id.instance+1), Slot: o.slot, Sequence: o.flight, Delta: delta, RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6}
				if c != want {
					modelErr = fmt.Errorf("flush identity %+v expected %+v", c, want)
					return reply{}, modelErr
				}
				r := execute(remotes[key], c, start.UnixMilli()+nowNS/int64(time.Millisecond))
				if r.Outcome != outcomeOK && r.Outcome != outcomeDuplicate {
					modelErr = fmt.Errorf("flush backend rejected %+v", r)
					return reply{}, modelErr
				}
				for i := range o.receipts {
					if o.receipts[i].sequence == o.flight {
						o.receipts[i].applied = true
					}
				}
				if event.Kind == "CloseBudgetExpired" {
					expire()
					return reply{}, &backendError{class: errorTransient, cause: context.DeadlineExceeded}
				}
				for i := range o.receipts {
					if o.receipts[i].sequence == o.flight {
						o.receipts[i].acknowledged = true
					}
				}
				o.acknowledged, o.flight = o.flight, 0
				return r, nil
			}
			l.backend = transport
			result := l.Close(context.Background())
			if modelErr != nil {
				return fail(index, "%v", modelErr)
			}
			if event.Kind == "CloseBudgetExpired" {
				if !errors.Is(result, context.DeadlineExceeded) {
					return fail(index, "missing shutdown deadline: %v", result)
				}
			} else if result != nil {
				return fail(index, "shutdown: %v", result)
			}
			if transport.closeCount.Load() != 1 {
				return fail(index, "cleanup not shared")
			}
			completed[id.instance] = true
			results[id.instance] = result
		case "OpenCircuit":
			subject.limiter.recordFailure(transientFailure())
			if !openCircuits[id.instance] {
				generations[id.instance]++
			}
			openCircuits[id.instance] = true
		case "ProbeSuccess":
			if !openCircuits[id.instance] || !subject.limiter.beginProbe() {
				return fail(index, "probe unavailable")
			}
			subject.limiter.finishProbe(nil)
			openCircuits[id.instance] = false
		case "RequireNormal":
			if openCircuits[id.instance] || oracle.fallback || oracle.generation != generations[id.instance] || nowNS >= oracle.freshNS {
				return fail(index, "fair reconciliation did not restore normal admission")
			}
			subject.bucket.mu.Lock()
			normal := !subject.bucket.fallback && subject.bucket.authorizedGeneration == generations[id.instance] && clocks[id.instance].Now().Before(subject.bucket.freshUntil)
			subject.bucket.mu.Unlock()
			if !normal {
				return fail(index, "subject failed to restore normal admission")
			}
		case "AllowN":
			var outstanding int64
			for _, r := range oracle.receipts {
				if !r.acknowledged {
					outstanding += r.cost
				}
			}
			invalid := event.Cost <= 0 || event.Cost > cfg.LocalBurst || event.Cost > cfg.Cap
			unavailable := openCircuits[id.instance] || (oracle.initialized && (oracle.generation != generations[id.instance] || nowNS >= oracle.freshNS)) || (!oracle.initialized && nowNS-oracle.createdNS >= cfg.FreshnessNS)
			// Independent rational refill, delayed until the same observable
			// transition. No production token, ledger or admission helpers.
			if !invalid && !closed[id.instance] {
				if cfg.Policy == LocalOnly && unavailable && !oracle.fallback {
					oracle.refill(nowNS, cfg.FallbackRateU, cfg.LocalBurst)
					oracle.clamp(cfg.FallbackBurst)
					oracle.fallback = true
				} else if oracle.fallback {
					oracle.refill(nowNS, cfg.FallbackRateU, cfg.FallbackBurst)
				} else {
					oracle.refill(nowNS, cfg.LocalRateU, cfg.LocalBurst)
				}
			}
			wantReason, wantMode := ReasonAllowed, ModeNormal
			switch {
			case closed[id.instance]:
				wantReason, wantMode = ReasonClosed, ModeBlocked
			case invalid:
				wantReason, wantMode = ReasonInvalidRequest, ModeBlocked
			case oracle.rejected:
				wantReason, wantMode = ReasonGlobalLimit, ModeBlocked
			case cfg.Policy == LocalOnly && oracle.fallback:
				wantReason, wantMode = ReasonLocalFallback, ModeLocalFallback
				if event.Cost > cfg.FallbackBurst || oracle.tokens.Cmp(big.NewRat(int64(event.Cost), 1)) < 0 {
					wantReason = ReasonFallbackLimit
				}
			case openCircuits[id.instance]:
				wantReason, wantMode = ReasonRedisUnavailable, ModeBlocked
			case !oracle.initialized:
				wantReason, wantMode = ReasonSyncPending, ModeBlocked
				if unavailable {
					wantReason = ReasonRedisUnavailable
				}
			case unavailable:
				wantReason, wantMode = ReasonRedisUnavailable, ModeBlocked
			case int64(event.Cost) > int64(cfg.Cap)-outstanding:
				wantReason, wantMode = ReasonSyncPending, ModeBlocked
			case oracle.tokens.Cmp(big.NewRat(int64(event.Cost), 1)) < 0:
				wantReason = ReasonLocalLimit
			}
			want := wantReason == ReasonAllowed || wantReason == ReasonLocalFallback
			got, reason, info := subject.limiter.AllowN(id.key, event.Cost)
			if got != want || reason != wantReason || info.Mode != wantMode {
				return fail(index, "admission=%v reason=%s mode=%s oracle=%v expectedReason=%s mode=%s tokens=%s outstanding=%d", got, reason, info.Mode, want, wantReason, wantMode, oracle.tokens.String(), outstanding)
			}
			if want {
				oracle.tokens.Sub(oracle.tokens, big.NewRat(int64(event.Cost), 1))
				if wantReason == ReasonLocalFallback {
					oracle.fallbackAccepted += int64(event.Cost)
				} else {
					oracle.receipts = append(oracle.receipts, modelReceipt{cost: int64(event.Cost)})
					subject.accepted += int64(event.Cost)
				}
			}
		case "BeginAttempt", "RetrySameBatch":
			if event.Attempt == 0 || attempts[event.Attempt] != nil || oracle.busy != 0 || !oracle.initialized {
				return fail(index, "invalid attempt identity/state")
			}
			if event.Kind == "RetrySameBatch" && oracle.flight == 0 {
				return fail(index, "retry without flight")
			}
			if oracle.flight == 0 {
				var pending int64
				for _, r := range oracle.receipts {
					if r.sequence == 0 {
						pending += r.cost
					}
				}
				if pending > 0 {
					oracle.latestBatch++
					oracle.flight = oracle.latestBatch
					for i := range oracle.receipts {
						if oracle.receipts[i].sequence == 0 {
							oracle.receipts[i].sequence = oracle.flight
						}
					}
				}
			}
			a, ok := subject.limiter.prepareAttempt(subject.bucket, clocks[id.instance].Now())
			if !ok {
				return fail(index, "attempt rejected")
			}
			var delta int64
			for _, r := range oracle.receipts {
				if oracle.flight != 0 && r.sequence == oracle.flight {
					delta += r.cost
				}
			}
			if a.batch.Sequence != oracle.flight || a.batch.Delta != delta {
				return fail(index, "batch=%+v oracle sequence=%d delta=%d", a.batch, oracle.flight, delta)
			}
			wantCommand := command{Op: opRefresh, Session: fmt.Sprintf("%032x", id.instance+1), Slot: oracle.slot, RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6}
			if delta > 0 {
				wantCommand.Op, wantCommand.Sequence, wantCommand.Delta = opApply, oracle.flight, delta
			}
			if a.command != wantCommand || a.generation != generations[id.instance] {
				return fail(index, "command=%+v oracle=%+v", a.command, wantCommand)
			}
			attempts[event.Attempt] = &foundationAttempt{owner: id, payload: a, sequence: oracle.flight, generation: generations[id.instance]}
			oracle.busy = event.Attempt
		case "ExecuteRedis":
			a := attempts[event.Attempt]
			if a == nil || a.owner != id || a.executed || a.finished {
				return fail(index, "invalid execution")
			}
			a.response = execute(remotes[id.key], a.payload.command, start.UnixMilli()+nowNS/int64(time.Millisecond))
			if a.response.Outcome != outcomeOK && a.response.Outcome != outcomeDuplicate {
				return fail(index, "backend rejected %+v", a.response)
			}
			if a.sequence != 0 {
				for i := range oracle.receipts {
					if oracle.receipts[i].sequence == a.sequence {
						oracle.receipts[i].applied = true
					}
				}
			}
			a.executed = true
		case "TimeoutAttempt":
			a := attempts[event.Attempt]
			if a == nil || a.owner != id || a.finished || a.timedOut || oracle.busy != event.Attempt {
				return fail(index, "invalid timeout")
			}
			subject.limiter.finishAttempt(subject.bucket, a.payload)
			a.timedOut = true
			oracle.busy = 0
		case "DeliverReply", "LoseReply":
			a := attempts[event.Attempt]
			if a == nil || a.owner != id || !a.executed || a.finished {
				return fail(index, "invalid reply transition")
			}
			if event.Kind == "LoseReply" {
				subject.limiter.finishAttempt(subject.bucket, a.payload)
				if oracle.busy == event.Attempt {
					oracle.busy = 0
				}
			} else {
				subject.limiter.installReply(subject.bucket, a.payload, a.response)
				if !a.timedOut && oracle.busy == event.Attempt {
					if a.sequence != 0 && oracle.flight == a.sequence {
						for i := range oracle.receipts {
							if oracle.receipts[i].sequence == a.sequence {
								oracle.receipts[i].acknowledged = true
							}
						}
						oracle.acknowledged, oracle.flight = a.sequence, 0
					}
					if a.generation == generations[id.instance] {
						oracle.rejected = a.response.BalanceU < 1e6
						if oracle.rejected {
							oracle.freshNS = 0
						} else {
							if cfg.Policy == LocalOnly && !oracle.fallback && (oracle.generation != generations[id.instance] || nowNS >= oracle.freshNS) {
								oracle.refill(nowNS, cfg.FallbackRateU, cfg.LocalBurst)
								oracle.clamp(cfg.FallbackBurst)
								oracle.fallback = true
							}
							outstanding := int64(0)
							for _, receipt := range oracle.receipts {
								if !receipt.acknowledged {
									outstanding += receipt.cost
								}
							}
							if outstanding == 0 || (!oracle.fallback && oracle.generation == generations[id.instance]) {
								oracle.freshNS = a.payload.start.Sub(start).Nanoseconds() + cfg.FreshnessNS
								oracle.generation = generations[id.instance]
								if oracle.fallback && nowNS < oracle.freshNS {
									oracle.refill(nowNS, cfg.FallbackRateU, cfg.FallbackBurst)
									oracle.fallback = false
								}
							}
						}
					}
					oracle.busy = 0
				}
			}
			a.finished = true
		default:
			return fail(index, "unsupported foundation event %q", event.Kind)
		}
		totals := map[string]int64{}
		for id, subject := range subjects {
			o := oracles[id]
			var accepted, applied, unapplied, pending, outstanding, flight int64
			for _, r := range o.receipts {
				accepted += r.cost
				if r.applied {
					applied += r.cost
				} else {
					unapplied += r.cost
				}
				if !r.acknowledged {
					outstanding += r.cost
				}
				if r.sequence == 0 {
					pending += r.cost
				}
				if o.flight != 0 && r.sequence == o.flight {
					flight += r.cost
				}
				if r.acknowledged && !r.applied {
					return fail(index, "acknowledged unapplied receipt")
				}
			}
			actualApplied := remotes[id.key].applied[fmt.Sprintf("%032x", id.instance+1)]
			if actualApplied != applied || subject.accepted != actualApplied+unapplied {
				return fail(index, "backend accounting: applied=%d receipts=%d accepted=%d unapplied=%d", actualApplied, applied, subject.accepted, unapplied)
			}
			b := subject.bucket
			b.mu.Lock()
			l := b.ledger
			expectedTokens, _ := o.tokens.Float64()
			if math.Abs(b.tokens.tokens-expectedTokens) > 1e-9 || b.fallback != o.fallback {
				b.mu.Unlock()
				return fail(index, "local balance/mode tokens=%v expected=%v fallback=%v expected=%v", b.tokens.tokens, expectedTokens, b.fallback, o.fallback)
			}
			if subject.limiter.control.Load().generation != generations[id.instance] {
				b.mu.Unlock()
				return fail(index, "circuit generation mismatch")
			}
			var f batch
			if l.Flight != nil {
				f = *l.Flight
			}
			hasFlight := l.Flight != nil
			b.mu.Unlock()
			if accepted != applied+unapplied || subject.accepted != accepted || l.Pending != pending || l.outstanding() != outstanding || outstanding < 0 || outstanding > int64(cfg.Cap) || l.Committed != o.acknowledged || hasFlight != (o.flight != 0) || (hasFlight && (f.Sequence != o.flight || f.Delta != flight)) {
				return fail(index, "conservation/cap ledger=%+v oracle accepted=%d applied=%d unapplied=%d pending=%d outstanding=%d flight=%d", l, accepted, applied, unapplied, pending, outstanding, flight)
			}
			totals[id.key] += accepted
		}
		// Compare every pair of event boundaries in physical time. All intermediate
		// arithmetic is independent arbitrary precision, including the 1ms allowance.
		for _, prior := range boundaries {
			for key, total := range totals {
				used := new(big.Int).Mul(big.NewInt(total-prior.totals[key]), big.NewInt(1_000_000_000_000_000))
				bound := new(big.Int).Mul(big.NewInt(int64(cfg.Burst+2*cfg.Instances*cfg.Cap)), big.NewInt(1_000_000_000_000_000))
				bound.Add(bound, new(big.Int).Mul(big.NewInt(cfg.RateU), big.NewInt(nowNS-prior.ns+1_000_000)))
				bound.Add(bound, big.NewInt(1_000_000_000))
				if used.Cmp(bound) > 0 {
					return fail(index, "normal envelope violated key=%q admitted=%d intervalNS=%d", key, total-prior.totals[key], nowNS-prior.ns)
				}
			}
		}
		boundaries = append(boundaries, envelopeBoundary{ns: nowNS, totals: totals})
	}
	return nil
}

func (o *foundationOracle) refill(nowNS, rateU int64, burst int) {
	o.tokens.Add(o.tokens, new(big.Rat).SetFrac(new(big.Int).Mul(big.NewInt(rateU), big.NewInt(nowNS-o.refillNS)), big.NewInt(1_000_000_000_000_000)))
	o.clamp(burst)
	o.refillNS = nowNS
}
func (o *foundationOracle) clamp(burst int) {
	full := big.NewRat(int64(burst), 1)
	if o.tokens.Cmp(full) > 0 {
		o.tokens.Set(full)
	}
}

func TestStateMachineFoundationDelayedDuplicateAck(t *testing.T) {
	trace := modelTrace{Config: modelConfig{Instances: 1, Cap: 5}, Events: []modelEvent{
		{Kind: "CreateKey", Key: "k"},
		{Kind: "Initialize", Key: "k"},
		{Kind: "AllowN", Key: "k", Cost: 3},
		{Kind: "BeginAttempt", Key: "k", Attempt: 1},
		{Kind: "ExecuteRedis", Key: "k", Attempt: 1},
		{Kind: "TimeoutAttempt", Key: "k", Attempt: 1},
		{Kind: "RetrySameBatch", Key: "k", Attempt: 2},
		{Kind: "ExecuteRedis", Key: "k", Attempt: 2},
		{Kind: "DeliverReply", Key: "k", Attempt: 2},
		{Kind: "AllowN", Key: "k", Cost: 2},
		{Kind: "BeginAttempt", Key: "k", Attempt: 3},
		{Kind: "DeliverReply", Key: "k", Attempt: 1},
		{Kind: "ExecuteRedis", Key: "k", Attempt: 3},
		{Kind: "DeliverReply", Key: "k", Attempt: 3},
	}}
	if err := checkTrace(trace); err != nil {
		t.Fatal(err)
	}
}

// redisReference uses arbitrary-precision numerator arithmetic independent of
// Lua's bounded decomposition. Executed deductions are recorded separately from
// delivered acknowledgements; a lost reply cannot erase a committed deduction.
type redisReference struct {
	rate, burst      int64
	balanceNumerator *big.Int
	logical          int64
	sessions         map[string]referenceSession
	applied          map[string]int64 // Unique whole-token deductions, keyed by actual session.
}
type referenceSession struct {
	slot               int
	sequence           uint64
	delta, cap, expiry int64
}

func (m *redisReference) execute(c command, now int64) reply {
	if m.sessions == nil {
		m.sessions = make(map[string]referenceSession)
		m.applied = make(map[string]int64)
		m.balanceNumerator = new(big.Int).Mul(big.NewInt(m.burst), big.NewInt(1000))
		m.logical = now
	}
	logical := now
	if logical < m.logical {
		logical = m.logical
	}
	balance := new(big.Int).Add(m.balanceNumerator, new(big.Int).Mul(big.NewInt(logical-m.logical), big.NewInt(m.rate)))
	full := new(big.Int).Mul(big.NewInt(m.burst), big.NewInt(1000))
	if balance.Cmp(full) > 0 {
		balance.Set(full)
	}
	s, exists := m.sessions[c.Session]
	code := outcomeOK
	fresh := false
	if c.Op == opInit {
		if logical >= c.RegistrationDeadlineMS {
			return reply{Outcome: outcomeInitExpired}
		}
		if !exists {
			s = referenceSession{slot: len(m.sessions), cap: c.Delta}
			fresh = true
		}
	} else {
		if !exists || s.slot != c.Slot || s.expiry <= logical {
			return reply{Outcome: outcomeSessionExpired}
		}
		if c.Op == opApply {
			if c.Sequence == s.sequence && c.Delta == s.delta {
				code = outcomeDuplicate
			} else if c.Sequence == s.sequence+1 {
				balance.Sub(balance, new(big.Int).Mul(big.NewInt(c.Delta), big.NewInt(1_000_000_000)))
				m.applied[c.Session] += c.Delta
				s.sequence, s.delta = c.Sequence, c.Delta
			} else {
				return reply{Outcome: outcomeSequenceError}
			}
		}
	}
	s.expiry = logical + 86400000
	m.sessions[c.Session] = s
	m.balanceNumerator = balance
	m.logical = logical
	// Euclidean division gives floor for negative debt and a nonnegative carry.
	whole := new(big.Int).Div(new(big.Int).Set(balance), big.NewInt(1000)).Int64()
	need := new(big.Int).Sub(big.NewInt(1_000_000_000), balance)
	retry := int64(0)
	if need.Sign() > 0 {
		need.Add(need, big.NewInt(m.rate-1))
		retry = need.Div(need, big.NewInt(m.rate)).Int64()
	}
	return reply{Outcome: code, Slot: s.slot, CommittedSequence: s.sequence, BalanceU: whole, RetryMS: retry, SessionTTLMS: s.expiry - now, LogicalMS: logical, FreshInit: fresh}
}

func TestStateMachineFoundationRedisDifferential(t *testing.T) {
	client, key := redisFixture(t)
	data, err := os.ReadFile("testdata/redis/foundation.json")
	if err != nil {
		t.Fatal(err)
	}
	var events []struct {
		Kind    string
		Cost    int64
		Attempt int
	}
	if err = json.Unmarshal(data, &events); err != nil {
		t.Fatal(err)
	}
	base := initCommand(t, client)
	base.RateU = 333333
	model := redisReference{rate: base.RateU, burst: base.BurstU}
	var ledger ledger
	type attempt struct {
		command            command
		response           reply
		executed, finished bool
	}
	attempts := map[int]*attempt{}
	var admitted, applied int64
	compare := func(c command) reply {
		actual := evalProtocol(t, client, key, redisLua, c)
		want := model.execute(c, actual.LogicalMS)
		if actual != want {
			t.Fatalf("command=%+v got=%+v oracle=%+v", c, actual, want)
		}
		return actual
	}
	for index, e := range events {
		t.Logf("event %d: %+v", index, e)
		switch e.Kind {
		case "initialize":
			r := compare(base)
			base.Slot = r.Slot
			base.RegistrationDeadlineMS = 0
		case "admit":
			if !ledger.admit(e.Cost, 10) {
				t.Fatal("unexpected rejection")
			}
			admitted += e.Cost
		case "begin":
			payload, ok := ledger.begin()
			if !ok {
				t.Fatal("no batch")
			}
			c := base
			c.Op, c.Sequence, c.Delta = opApply, payload.Sequence, payload.Delta
			attempts[e.Attempt] = &attempt{command: c}
		case "execute":
			a := attempts[e.Attempt]
			if a == nil || a.executed {
				t.Fatal("invalid execution")
			}
			a.response = compare(a.command)
			a.executed = true
			if a.response.Outcome == outcomeOK {
				applied += a.command.Delta
			}
		case "lose", "deliver":
			a := attempts[e.Attempt]
			if a == nil || !a.executed || a.finished {
				t.Fatal("invalid delivery")
			}
			a.finished = true
			if e.Kind == "deliver" {
				if !ledger.acknowledge(batch{Sequence: a.response.CommittedSequence, Delta: a.command.Delta}) {
					t.Fatal("ack failed")
				}
			}
		case "refresh":
			c := base
			c.Op, c.Sequence, c.Delta = opRefresh, 0, 0
			compare(c)
		default:
			t.Fatalf("unsupported event %s", e.Kind)
		}
		if applied > admitted || ledger.outstanding() < 0 || ledger.outstanding() > 10 {
			t.Fatalf("conservation/cap admitted=%d applied=%d ledger=%+v", admitted, applied, ledger)
		}
	}
	if admitted != 5 || applied != admitted || ledger.outstanding() != 0 {
		t.Fatalf("lost or double charged consumption: admitted=%d applied=%d ledger=%+v", admitted, applied, ledger)
	}
}

func TestStateMachineFoundationRedisNumericDifferential(t *testing.T) {
	for _, rate := range []int64{1, 999, 1000, 1001, 333333, 999999999999, 1000000000000} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			client, key := redisFixture(t)
			c := initCommand(t, client)
			now := c.RegistrationDeadlineMS - 30000
			c.RateU, c.BurstU, c.Delta = rate, 1_000_000_000_000, 1_000_000
			model := redisReference{rate: c.RateU, burst: c.BurstU}
			compare := func(c command, now int64) reply {
				actual := evalProtocol(t, client, key, controlledLua(now), c)
				want := model.execute(c, now)
				if actual != want {
					t.Fatalf("rate=%d c=%+v time=%d got=%+v want=%+v", rate, c, now, actual, want)
				}
				return actual
			}
			r := compare(c, now)
			c.Op, c.Slot, c.Sequence, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 0
			compare(c, now)
			c.Sequence = 2
			compare(c, now)
			compare(c, now)
			c.Op, c.Sequence, c.Delta = opRefresh, 0, 0
			for _, elapsed := range []int64{1, 2, 999, 1000, 1001, 1_000_001, 86_399_999} {
				compare(c, now+elapsed)
			}
		})
	}
}

func TestStateMachineFoundationDetectsBackendUndercharge(t *testing.T) {
	trace := modelTrace{Config: modelConfig{Instances: 1, Cap: 5}, Events: []modelEvent{
		{Kind: "CreateKey", Key: "k"}, {Kind: "Initialize", Key: "k"},
		{Kind: "AllowN", Key: "k", Cost: 3}, {Kind: "BeginAttempt", Key: "k", Attempt: 1},
		{Kind: "ExecuteRedis", Key: "k", Attempt: 1},
	}}
	err := checkTraceWithExecution(trace, func(remote *redisReference, c command, now int64) reply {
		if c.Op == opApply {
			c.Delta = 1
		}
		return remote.execute(c, now)
	})
	if err == nil || !strings.Contains(err.Error(), "backend accounting: applied=1 receipts=3") {
		t.Fatalf("undercharged backend was not detected by conservation: %v", err)
	}
}

// Deterministic scheduling traces consume retained hints without starting
// goroutines; preparation/execution/delivery remain separate oracle events.
func drainModelNotifications(l *Limiter, visit func(*bucket)) {
	for {
		select {
		case b := <-l.work:
			if b != nil {
				b.mu.Lock()
				b.queued = false
				b.mu.Unlock()
				if visit != nil {
					visit(b)
				}
				l.store.release(b)
			}
		default:
			return
		}
	}
}
