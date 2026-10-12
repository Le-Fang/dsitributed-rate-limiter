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
	Kind          string
	Instance      int
	Key           string
	Cost          int
	AdvanceNS     int64
	Attempt       uint64
	DeltaOverride int64  // Adversarial wire payload, zero preserves the immutable command.
	Checkpoint    string // before/after the atomic backend commit; test transport only.
}
type modelTrace struct {
	Seed          uint64
	Config        modelConfig
	Events        []modelEvent
	coverage      *modelCoverage
	searchCapture func(uint16)
}
type modelConfig struct {
	Instances                             int
	RateU                                 int64
	Burst, Cap, LocalBurst, FallbackBurst int
	LocalRateU, FallbackRateU             int64
	SyncNS, FreshnessNS                   int64
	IdleNS                                int64
	MaxKeys                               int
	Policy                                FailurePolicy
}

// Model defaults are separate from production configuration normalization.
func normalizeModelConfig(cfg modelConfig) modelConfig {
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
	if cfg.IdleNS == 0 {
		cfg.IdleNS = int64(time.Minute)
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = 2048
	}
	return cfg
}

func TestStateMachineTraces(t *testing.T) { TestStateMachineFoundation(t) }

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

func TestStateMachineFoundationRejectsUnsupportedAndSkipsInvalidEvents(t *testing.T) {
	if err := checkTrace(modelTrace{Config: modelConfig{Instances: 1, Cap: 5}, Events: []modelEvent{{Kind: "Unknown"}}}); err == nil {
		t.Fatal("unknown event accepted")
	}
	for _, events := range [][]modelEvent{
		{{Kind: "AllowN", Key: "k", Cost: 1}},
		{{Kind: "CreateKey", Key: "k"}, {Kind: "ExecuteRedis", Key: "k", Attempt: 1}},
		{{Kind: "CreateKey", Key: "k"}, {Kind: "DeliverReply", Key: "k", Attempt: 1}},
	} {
		if err := checkTrace(modelTrace{Config: modelConfig{Instances: 1, Cap: 5}, Events: events}); err != nil {
			t.Fatal(err)
		}
	}
}

// Requests leave independent receipts. The oracle uses rational local refill,
// receipt queries and the independent integer Redis reference, never production
// token/ledger/decision helpers. The subject runs real prepare/install/AllowN.
type modelReceipt struct {
	cost                        int64
	session                     string
	sequence                    uint64
	applied, acknowledged, lost bool
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
	session                           string
	previousSessions                  []string
	init                              *command
	terminal                          string
	capacityNS                        int64
	lastActivityNS                    int64
	normalFullNS, fallbackFullNS      int64
	fallbackEnvelope                  *modelEnvelope
}
type foundationSubject struct {
	limiter                       *Limiter
	bucket                        *bucket
	accepted                      int64
	allAccepted, fallbackAccepted int64
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
	oracle                       *foundationOracle
	failed                       bool
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
	return inspectTrace(trace, execute, nil)
}

func inspectTrace(trace modelTrace, execute func(*redisReference, command, int64) reply, capture func(string)) error {
	cfg := trace.Config
	if cfg.Instances <= 0 || cfg.Instances > 257 || cfg.Cap <= 0 || cfg.Cap > 1_000_000 {
		return fmt.Errorf("seed %d: invalid model config %+v", trace.Seed, cfg)
	}
	cfg = normalizeModelConfig(cfg)
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
		c := coreConfig{operationTimeout: time.Second, globalRateU: cfg.RateU, globalBurst: cfg.Burst, localRateU: cfg.LocalRateU, localBurst: cfg.LocalBurst, maxOutstanding: cfg.Cap, maxSyncAge: time.Duration(cfg.FreshnessNS), syncInterval: time.Duration(cfg.SyncNS), syncThreshold: cfg.Cap, maxKeys: cfg.MaxKeys, idleTTL: time.Duration(cfg.IdleNS), failurePolicy: cfg.Policy, fallbackRateU: cfg.FallbackRateU, fallbackBurst: cfg.FallbackBurst}
		parent, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		defer cancel()
		limiters[i] = newLimiterCore(c, parent, clocks[i], &fakeBackend{clock: clocks[i]}, nil)
	}
	subjects := map[modelBucketID]*foundationSubject{}
	oracles := map[modelBucketID]*foundationOracle{}
	attempts := map[uint64]*foundationAttempt{}
	remotes := map[string]*redisReference{}
	envelopes := map[string]*modelEnvelope{}
	retired := map[*foundationOracle]*foundationSubject{}
	incarnations := make([]int, cfg.Instances)
	crashed := make([]bool, cfg.Instances)
	var redisNS int64
	snapshot := func() string {
		return modelStateSnapshot(clocks, generations, openCircuits, closed, completed, crashed, incarnations, subjects, oracles, attempts, remotes, retired, envelopes, redisNS)
	}
	var nowNS int64
	resetEnvelope := false
	fail := func(index int, format string, args ...interface{}) error {
		return &modelFailure{Category: modelFailureCategory(format), Seed: trace.Seed, Config: cfg, Index: index, Detail: fmt.Sprintf(format, args...), State: snapshot(), Events: append([]modelEvent(nil), trace.Events[:index+1]...)}
	}
	for index, event := range trace.Events {
		id := modelBucketID{event.Instance, event.Key}
		if !knownModelEvent(event.Kind) {
			return fail(index, "unsupported event %q", event.Kind)
		}
		if event.Instance < 0 || event.Instance >= cfg.Instances {
			continue
		}
		nowNS = clocks[id.instance].Now().Sub(start).Nanoseconds()
		subject, oracle := subjects[id], oracles[id]
		global := event.Kind == "ExecuteRedis" || event.Kind == "AdvanceTime" || event.Kind == "AdvanceLocalTime" || event.Kind == "AdvanceRedisTime" || event.Kind == "RestartInstance" || event.Kind == "CrashInstance"
		if !global && event.Kind != "CreateKey" && subject == nil {
			continue
		}
		if crashed[id.instance] && event.Kind != "RestartInstance" && event.Kind != "AdvanceTime" && event.Kind != "AdvanceLocalTime" && event.Kind != "AdvanceRedisTime" && event.Kind != "ExecuteRedis" {
			continue
		}
		switch event.Kind {
		case "CrashInstance":
			if crashed[id.instance] {
				continue
			}
			for bid, o := range oracles {
				if bid.instance == id.instance {
					for i := range o.receipts {
						if !o.receipts[i].applied {
							o.receipts[i].lost = true
						}
					}
					retired[o] = subjects[bid]
					delete(subjects, bid)
					delete(oracles, bid)
				}
			}
			cancels[id.instance]()
			drainModelNotifications(limiters[id.instance], nil)
			crashed[id.instance] = true
			closed[id.instance] = true
			resetEnvelope = true
		case "RestartInstance":
			if !crashed[id.instance] {
				continue
			}
			old := limiters[id.instance]
			parent, cancel := context.WithCancel(context.Background())
			cancels[id.instance] = cancel
			defer cancel()
			limiters[id.instance] = newLimiterCore(old.cfg, parent, clocks[id.instance], &fakeBackend{clock: clocks[id.instance]}, nil)
			incarnations[id.instance]++
			generations[id.instance] = 1
			openCircuits[id.instance] = false
			closed[id.instance] = false
			completed[id.instance] = false
			crashed[id.instance] = false
		case "ExpireSession":
			// Expiration is a clock-driven backend transition, never an arbitrary
			// discard of intact Redis debt. A slot remains occupied until reuse.
			r := remotes[id.key]
			ss, ok := r.sessions[oracle.session]
			if !ok {
				continue
			}
			target := ss.expiry - start.UnixMilli()
			if target > redisNS/int64(time.Millisecond) {
				redisNS = target * int64(time.Millisecond)
			}
		case "EvictCleanKey":
			l := subject.limiter
			drainModelNotifications(l, nil)
			outstanding := int64(0)
			for _, r := range oracle.receipts {
				if !r.acknowledged {
					outstanding += r.cost
				}
			}
			can := oracle.busy == 0 && outstanding == 0 && !oracle.rejected && oracle.terminal == "" && oracle.capacityNS == 0 && nowNS-oracle.lastActivityNS >= cfg.IdleNS && nowNS >= oracle.normalFullNS && nowNS >= oracle.fallbackFullNS
			l.store.release(subject.bucket)
			removed := l.store.cleanup(clocks[id.instance].Now(), max(1, int(l.store.count.Load())*64))
			got := false
			for _, b := range removed {
				if b == subject.bucket {
					got = true
				}
			}
			if got != can {
				return fail(index, "local balance eviction=%v expected=%v", got, can)
			}
			if got {
				retired[oracle] = subject
				delete(subjects, id)
				delete(oracles, id)
				incarnations[id.instance]++
			} else {
				l.store.retain(subject.bucket)
			}
		case "CreateKey":
			if subject != nil {
				continue
			}
			l := limiters[id.instance]
			b, ok := l.store.acquire(id.key, clocks[id.instance].Now())
			if !ok {
				if l.store.count.Load() != int64(cfg.MaxKeys) {
					return fail(index, "store capacity mismatch")
				}
				continue
			}
			subjects[id] = &foundationSubject{limiter: l, bucket: b}
			oracles[id] = &foundationOracle{tokens: big.NewRat(int64(cfg.LocalBurst), 1), createdNS: nowNS, refillNS: nowNS, lastActivityNS: nowNS, session: fmt.Sprintf("%032x", (incarnations[id.instance]*cfg.Instances)+id.instance+1)}
			if remotes[id.key] == nil {
				remotes[id.key] = &redisReference{rate: cfg.RateU, burst: int64(cfg.Burst) * 1e6}
			}
		case "Initialize":
			if oracle.initialized || oracle.busy != 0 || openCircuits[id.instance] || closed[id.instance] || oracle.terminal != "" {
				continue
			}
			b, l := subject.bucket, subject.limiter
			b.mu.Lock()
			b.init = &command{Op: opInit, Session: oracle.session, Slot: -1, Delta: int64(cfg.Cap), RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6, RegistrationDeadlineMS: start.UnixMilli() + redisNS/int64(time.Millisecond) + 30000}
			oracle.init = b.init
			b.mu.Unlock()
			a, ok := l.prepareAttempt(b, clocks[id.instance].Now())
			if !ok {
				return fail(index, "init preparation failed")
			}
			r := execute(remotes[id.key], a.command, start.UnixMilli()+redisNS/int64(time.Millisecond))
			l.installReply(b, a, r)
			if r.Outcome != outcomeOK {
				return fail(index, "initialization outcome %+v", r)
			}
			oracle.init = nil
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
			if len(subject.limiter.work) > 1024 {
				return fail(index, "queue capacity exceeded")
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
		case "AdvanceTime", "AdvanceLocalTime", "AdvanceRedisTime":
			if event.AdvanceNS < 0 {
				continue
			}
			if event.Kind != "AdvanceLocalTime" {
				redisNS += event.AdvanceNS
			}
			if event.Kind == "AdvanceLocalTime" {
				clocks[id.instance].Advance(time.Duration(event.AdvanceNS))
			}
			if event.Kind == "AdvanceTime" {
				for _, c := range clocks {
					c.Advance(time.Duration(event.AdvanceNS))
				}
			}
		case "CancelParent":
			cancels[id.instance]()
			closed[id.instance] = true
		case "Close", "CloseBudgetExpired":
			if event.Kind == "CloseBudgetExpired" {
				unresolved := false
				for _, r := range oracle.receipts {
					unresolved = unresolved || !r.acknowledged
				}
				if !unresolved {
					continue
				}
			}
			if oracle.busy != 0 || oracle.terminal != "" || !oracle.initialized {
				continue
			}
			l := subject.limiter
			closed[id.instance] = true
			if completed[id.instance] {
				if got := l.Close(context.Background()); got != results[id.instance] {
					return fail(index, "changed shared shutdown result")
				}
				break
			}
			var modelErr error
			finalRejected := false
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
							o.receipts[i].session = o.session
						}
					}
				}
				var delta int64
				for _, r := range o.receipts {
					if r.sequence == o.flight && r.session == o.session {
						delta += r.cost
					}
				}
				want := command{Op: opApply, Session: o.session, Slot: o.slot, Sequence: o.flight, Delta: delta, RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6}
				if c != want {
					modelErr = fmt.Errorf("flush identity %+v expected %+v", c, want)
					return reply{}, modelErr
				}
				r := execute(remotes[key], c, start.UnixMilli()+redisNS/int64(time.Millisecond))
				if r.Outcome != outcomeOK && r.Outcome != outcomeDuplicate {
					finalRejected = true
					return r, nil
				}
				for i := range o.receipts {
					if o.receipts[i].sequence == o.flight && o.receipts[i].session == o.session {
						o.receipts[i].applied = true
					}
				}
				if event.Kind == "CloseBudgetExpired" {
					expire()
					return reply{}, &backendError{class: errorTransient, cause: context.DeadlineExceeded}
				}
				for i := range o.receipts {
					if o.receipts[i].sequence == o.flight && o.receipts[i].session == o.session {
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
			} else if finalRejected {
				var unflushed *ShutdownError
				if !errors.As(result, &unflushed) || unflushed.UnflushedTokens == 0 {
					return fail(index, "shutdown missing unresolved report: %v", result)
				}
			} else if result != nil {
				return fail(index, "shutdown: %v", result)
			}
			if transport.closeCount.Load() != 1 {
				return fail(index, "cleanup not shared")
			}
			completed[id.instance] = true
			results[id.instance] = result
		case "OpenCircuit", "ProbeFailure":
			if closed[id.instance] {
				continue
			}
			subject.limiter.recordFailure(transientFailure())
			if !openCircuits[id.instance] {
				generations[id.instance]++
			}
			openCircuits[id.instance] = true
		case "ProbeSuccess":
			if !openCircuits[id.instance] || !subject.limiter.beginProbe() {
				continue
			}
			subject.limiter.finishProbe(nil)
			openCircuits[id.instance] = false
		case "RequireNormal":
			for _, r := range oracle.receipts {
				if !r.acknowledged {
					return fail(index, "fair drain left pending accounting")
				}
			}
			if openCircuits[id.instance] || oracle.fallback || oracle.generation != generations[id.instance] || nowNS >= oracle.freshNS || oracle.rejected || oracle.flight != 0 {
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
			invalid := event.Key == "" || len(event.Key) > 1024 || event.Cost <= 0 || event.Cost > cfg.LocalBurst || event.Cost > cfg.Cap
			unavailable := openCircuits[id.instance] || (oracle.initialized && (oracle.generation != generations[id.instance] || nowNS >= oracle.freshNS)) || (!oracle.initialized && nowNS-oracle.createdNS >= cfg.FreshnessNS)
			// Independent rational refill, delayed until the same observable
			// transition. No production token, ledger or admission helpers.
			if !invalid && !closed[id.instance] {
				oracle.lastActivityNS = nowNS
				if cfg.Policy == LocalOnly && unavailable && !oracle.fallback && oracle.terminal == "" {
					oracle.refill(nowNS, min(cfg.LocalRateU, cfg.FallbackRateU), cfg.LocalBurst)
					oracle.clamp(cfg.FallbackBurst)
					oracle.fallback = true
					oracle.fallbackEnvelope = newModelEnvelope(cfg.FallbackRateU, int64(cfg.FallbackBurst), false)
					oracle.markFull(nowNS, cfg)
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
			case oracle.terminal != "":
				wantReason, wantMode = oracle.terminal, ModeBlocked
			case oracle.rejected:
				wantReason, wantMode = ReasonGlobalLimit, ModeBlocked
			case oracle.capacityNS != 0:
				wantReason, wantMode = ReasonCapacityExhausted, ModeBlocked
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
				if trace.coverage != nil {
					if wantReason == ReasonLocalFallback {
						trace.coverage.Fallback++
					} else {
						trace.coverage.Normal++
					}
				}
				subject.allAccepted += int64(event.Cost)
				oracle.tokens.Sub(oracle.tokens, big.NewRat(int64(event.Cost), 1))
				oracle.markFull(nowNS, cfg)
				if wantReason == ReasonLocalFallback {
					oracle.fallbackAccepted += int64(event.Cost)
					subject.fallbackAccepted += int64(event.Cost)
					if err := oracle.fallbackEnvelope.admit(nowNS, int64(event.Cost)); err != nil {
						return fail(index, "fallback envelope: %v", err)
					}
				} else {
					oracle.receipts = append(oracle.receipts, modelReceipt{cost: int64(event.Cost)})
					subject.accepted += int64(event.Cost)
				}
			}
		case "BeginAttempt", "RetrySameBatch":
			if event.Attempt == 0 || attempts[event.Attempt] != nil || oracle.busy != 0 || openCircuits[id.instance] || closed[id.instance] || oracle.terminal != "" || nowNS < oracle.capacityNS {
				continue
			}
			if event.Kind == "RetrySameBatch" && oracle.flight == 0 {
				continue
			}
			if oracle.initialized && oracle.flight == 0 {
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
							oracle.receipts[i].session = oracle.session
						}
					}
				}
			}
			if !oracle.initialized && oracle.init == nil {
				oracle.init = &command{Op: opInit, Session: oracle.session, Slot: -1, Delta: int64(cfg.Cap), RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6, RegistrationDeadlineMS: start.UnixMilli() + redisNS/int64(time.Millisecond) + 30000}
				copyInit := *oracle.init
				subject.bucket.init = &copyInit
			}
			a, ok := subject.limiter.prepareAttempt(subject.bucket, clocks[id.instance].Now())
			if !ok {
				return fail(index, "attempt rejected")
			}
			var delta int64
			for _, r := range oracle.receipts {
				if oracle.flight != 0 && r.sequence == oracle.flight && r.session == oracle.session {
					delta += r.cost
				}
			}
			if a.batch.Sequence != oracle.flight || a.batch.Delta != delta {
				return fail(index, "batch=%+v oracle sequence=%d delta=%d", a.batch, oracle.flight, delta)
			}
			wantCommand := command{Op: opRefresh, Session: oracle.session, Slot: oracle.slot, RateU: cfg.RateU, BurstU: int64(cfg.Burst) * 1e6}
			if delta > 0 {
				wantCommand.Op, wantCommand.Sequence, wantCommand.Delta = opApply, oracle.flight, delta
			}
			if !oracle.initialized {
				wantCommand = *oracle.init
			}
			if a.command != wantCommand || a.generation != generations[id.instance] || !a.start.Equal(clocks[id.instance].Now()) {
				return fail(index, "command=%+v oracle=%+v", a.command, wantCommand)
			}
			attempts[event.Attempt] = &foundationAttempt{owner: id, payload: a, sequence: oracle.flight, generation: generations[id.instance], oracle: oracle}
			oracle.busy = event.Attempt
		case "ExecuteRedis":
			a := attempts[event.Attempt]
			if a == nil || a.owner != id || a.executed || a.finished {
				continue
			}
			if event.DeltaOverride != 0 {
				prior, exists := remotes[id.key].sessions[a.payload.command.Session]
				// This is the mismatched DUPLICATE scenario, not permission to
				// forge a different first deduction than production prepared.
				if a.payload.command.Op != opApply || !exists || prior.sequence != a.sequence || event.DeltaOverride == prior.delta {
					continue
				}
			}
			if event.Checkpoint == "before" {
				a.failed = true
				a.executed = true
				break
			}
			wire := a.payload.command
			if event.DeltaOverride != 0 {
				wire.Delta = event.DeltaOverride
			}
			a.response = execute(remotes[id.key], wire, start.UnixMilli()+redisNS/int64(time.Millisecond))
			if event.Checkpoint == "after" {
				a.failed = true
			}
			if a.sequence != 0 && (a.response.Outcome == outcomeOK || a.response.Outcome == outcomeDuplicate) {
				oracle = a.oracle
				for i := range oracle.receipts {
					if oracle.receipts[i].sequence == a.sequence && oracle.receipts[i].session == a.payload.command.Session {
						oracle.receipts[i].applied = true
						oracle.receipts[i].lost = false
					}
				}
			}
			a.executed = true
		case "TimeoutAttempt":
			a := attempts[event.Attempt]
			if a == nil || a.owner != id || a.oracle != oracle || a.finished || a.timedOut || oracle.busy != event.Attempt {
				continue
			}
			subject.limiter.finishAttempt(subject.bucket, a.payload)
			a.timedOut = true
			oracle.busy = 0
		case "DeliverReply", "LoseReply":
			a := attempts[event.Attempt]
			if a == nil || a.owner != id || a.oracle != oracle || !a.executed || a.finished {
				continue
			}
			if event.Kind == "LoseReply" || a.failed {
				subject.limiter.finishAttempt(subject.bucket, a.payload)
				if oracle.busy == event.Attempt {
					oracle.busy = 0
				}
			} else {
				subject.limiter.installReply(subject.bucket, a.payload, a.response)
				if !a.timedOut && oracle.busy == event.Attempt {
					success := a.response.Outcome == outcomeOK || a.response.Outcome == outcomeDuplicate
					if success && a.sequence != 0 && oracle.flight == a.sequence {
						for i := range oracle.receipts {
							if oracle.receipts[i].sequence == a.sequence && oracle.receipts[i].session == a.payload.command.Session {
								oracle.receipts[i].acknowledged = true
							}
						}
						oracle.acknowledged, oracle.flight = a.sequence, 0
					}
					if a.generation == generations[id.instance] && !completed[id.instance] {
						if !success {
							outstanding := int64(0)
							for _, r := range oracle.receipts {
								if !r.acknowledged {
									outstanding += r.cost
								}
							}
							switch a.response.Outcome {
							case outcomeSessionCapacity:
								oracle.capacityNS = nowNS + int64(time.Second)
							case outcomeInitExpired, outcomeSessionExpired:
								if outstanding > 0 {
									oracle.terminal = ReasonAccountingExpired
								} else {
									oracle.initialized = false
									oracle.init = nil
									oracle.freshNS = 0
									oracle.flight = 0
									oracle.latestBatch = 0
									oracle.acknowledged = 0
									oracle.slot = -1
									oracle.previousSessions = append(oracle.previousSessions, oracle.session)
									oracle.session = fmt.Sprintf("%032x", uint64(index+1)<<32|uint64(id.instance+1))
								}
							default:
								oracle.terminal = ReasonAccountingError
							}
							oracle.busy = 0
							a.finished = true
							break
						}
						if a.payload.command.Op == opInit {
							oracle.initialized = true
							oracle.init = nil
							oracle.slot = a.response.Slot
						}
						oracle.capacityNS = 0
						oracle.rejected = a.response.BalanceU < 1e6
						// Successful negative balances refresh observation age too;
						// the separate rejection latch still forbids admission.
						if cfg.Policy == LocalOnly && !oracle.fallback && a.payload.command.Op != opInit && (oracle.generation != generations[id.instance] || nowNS >= oracle.freshNS) {
							oracle.refill(nowNS, min(cfg.LocalRateU, cfg.FallbackRateU), cfg.LocalBurst)
							oracle.clamp(cfg.FallbackBurst)
							oracle.fallback = true
							oracle.fallbackEnvelope = newModelEnvelope(cfg.FallbackRateU, int64(cfg.FallbackBurst), false)
							oracle.markFull(nowNS, cfg)
						}
						outstanding := int64(0)
						for _, receipt := range oracle.receipts {
							if !receipt.acknowledged {
								outstanding += receipt.cost
							}
						}
						if outstanding == 0 || (!oracle.fallback && oracle.generation == generations[id.instance]) {
							oracle.freshNS = a.payload.start.Sub(start).Nanoseconds() + min(cfg.FreshnessNS, max(int64(0), a.response.SessionTTLMS-60000)*int64(time.Millisecond))
							oracle.generation = generations[id.instance]
							if !oracle.rejected && oracle.fallback && nowNS < oracle.freshNS {
								oracle.refill(nowNS, cfg.FallbackRateU, cfg.FallbackBurst)
								oracle.fallback = false
								oracle.markFull(nowNS, cfg)
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
		if trace.coverage != nil {
			trace.coverage.Events[event.Kind]++
		}
		totals := map[string]int64{}
		for o, s := range retired {
			var applied, lost int64
			for _, r := range o.receipts {
				if r.applied {
					applied += r.cost
				} else if r.lost {
					lost += r.cost
				} else {
					return fail(index, "conservation retired unresolved receipt")
				}
			}
			key := s.bucket.key
			actual := o.actualApplied(remotes[key])
			if actual != applied || s.accepted != actual+lost {
				return fail(index, "backend accounting retired accepted=%d applied=%d lost=%d receipts=%d", s.accepted, actual, lost, applied)
			}
			totals[key] += s.accepted
		}
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
				if o.flight != 0 && r.sequence == o.flight && r.session == o.session {
					flight += r.cost
				}
				if r.acknowledged && !r.applied {
					return fail(index, "acknowledged unapplied receipt")
				}
			}
			actualApplied := o.actualApplied(remotes[id.key])
			if subject.allAccepted != subject.accepted+subject.fallbackAccepted || subject.fallbackAccepted != o.fallbackAccepted {
				return fail(index, "normal/fallback partition mismatch")
			}
			if actualApplied != applied || subject.accepted != actualApplied+unapplied {
				return fail(index, "backend accounting: applied=%d receipts=%d accepted=%d unapplied=%d", actualApplied, applied, subject.accepted, unapplied)
			}
			b := subject.bucket
			b.mu.Lock()
			l := b.ledger
			expectedTokens, _ := o.tokens.Float64()
			expectedFresh := time.Time{}
			if o.freshNS != 0 {
				expectedFresh = start.Add(time.Duration(o.freshNS))
			}
			if b.authorizedGeneration != o.generation || !b.freshUntil.Equal(expectedFresh) {
				b.mu.Unlock()
				return fail(index, "generation/freshness got=%d/%v expected=%d/%v", b.authorizedGeneration, b.freshUntil, o.generation, expectedFresh)
			}
			if b.initialized != o.initialized || b.globalRejected != o.rejected || b.terminalReason != o.terminal || b.busy != (o.busy != 0) {
				b.mu.Unlock()
				return fail(index, "generation/latch mismatch initialized=%v/%v rejected=%v/%v terminal=%s/%s busy=%v/%v", b.initialized, o.initialized, b.globalRejected, o.rejected, b.terminalReason, o.terminal, b.busy, o.busy)
			}
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
		for key, total := range totals {
			env := envelopes[key]
			if env == nil {
				env = newModelEnvelope(cfg.RateU, int64(cfg.Burst+2*cfg.Instances*cfg.Cap), true)
				envelopes[key] = env
			}
			if resetEnvelope {
				env.min = env.value(redisNS, total)
				env.admitted = total
				env.points = nil
			}
			if err := env.observe(redisNS, total, len(trace.Events) <= 64); err != nil {
				return fail(index, "normal envelope: %v", err)
			}
		}
		resetEnvelope = false
		for _, l := range limiters {
			if l.store.count.Load() > int64(cfg.MaxKeys) || len(l.work) > 1024 {
				return fail(index, "memory bound exceeded")
			}
		}
		for _, r := range remotes {
			if len(r.sessions) > 256 {
				return fail(index, "session bound exceeded")
			}
		}

	}
	if capture != nil {
		capture(snapshot())
	}
	if trace.searchCapture != nil {
		trace.searchCapture(modelSearchEnabled(oracles, attempts))
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
	expiresAt        int64
	sessions         map[string]referenceSession
	applied          map[string]int64 // Unique whole-token deductions, keyed by actual session.
}
type referenceSession struct {
	slot               int
	sequence           uint64
	delta, cap, expiry int64
}

func (m *redisReference) execute(c command, now int64) reply {
	logical := max(now, m.logical)
	sessions := m.sessions
	balance := new(big.Int)
	if sessions == nil || (m.expiresAt != 0 && now >= m.expiresAt) {
		sessions = map[string]referenceSession{}
		balance.Mul(big.NewInt(m.burst), big.NewInt(1000))
		logical = now
	} else {
		balance.Add(m.balanceNumerator, new(big.Int).Mul(big.NewInt(logical-m.logical), big.NewInt(m.rate)))
		full := new(big.Int).Mul(big.NewInt(m.burst), big.NewInt(1000))
		if balance.Cmp(full) > 0 {
			balance.Set(full)
		}
	}
	ss, exists := sessions[c.Session]
	code := outcomeOK
	fresh := false
	if c.Op == opInit {
		if logical >= c.RegistrationDeadlineMS {
			return reply{Outcome: outcomeInitExpired}
		}
		if c.RegistrationDeadlineMS-logical > 30000 {
			return reply{Outcome: outcomeInitExpired}
		}
		if exists {
			if ss.expiry <= logical {
				return reply{Outcome: outcomeSessionExpired}
			}
			if ss.cap != c.Delta {
				return reply{Outcome: outcomeInvalidArgument}
			}
		} else {
			occupied := [256]bool{}
			for _, old := range sessions {
				if old.expiry > logical {
					occupied[old.slot] = true
				}
			}
			free := -1
			for i, used := range occupied {
				if !used {
					free = i
					break
				}
			}
			if free < 0 {
				return reply{Outcome: outcomeSessionCapacity}
			}
			ss = referenceSession{slot: free, cap: c.Delta}
			fresh = true
		}
	} else {
		if !exists || ss.slot != c.Slot || ss.expiry <= logical {
			return reply{Outcome: outcomeSessionExpired}
		}
		if c.Op == opApply {
			if c.Delta > ss.cap {
				return reply{Outcome: outcomeInvalidArgument}
			}
			if c.Sequence == ss.sequence && c.Delta == ss.delta {
				code = outcomeDuplicate
			} else if c.Sequence == ss.sequence+1 {
				balance.Sub(balance, new(big.Int).Mul(big.NewInt(c.Delta), big.NewInt(1_000_000_000)))
				if balance.Cmp(big.NewInt(-1_000_000_000_000_000)) < 0 {
					return reply{Outcome: outcomeNumericLimit}
				}
				ss.sequence, ss.delta = c.Sequence, c.Delta
			} else {
				return reply{Outcome: outcomeSequenceError}
			}
		}
	}
	if m.applied == nil {
		m.applied = map[string]int64{}
	}
	if c.Op == opApply && code == outcomeOK {
		m.applied[c.Session] += c.Delta
	}
	// Commit replaces only a reused slot; cumulative applied history is retained
	// outside the expiring record for independent conservation assertions.
	for id, old := range sessions {
		if old.slot == ss.slot && id != c.Session {
			delete(sessions, id)
		}
	}
	ss.expiry = logical + 86400000
	sessions[c.Session] = ss
	m.sessions = sessions
	m.balanceNumerator = balance
	m.logical = logical
	whole := new(big.Int).Div(new(big.Int).Set(balance), big.NewInt(1000)).Int64()
	ceilDiv := func(n *big.Int) int64 {
		if n.Sign() <= 0 {
			return 0
		}
		return new(big.Int).Div(new(big.Int).Add(n, big.NewInt(m.rate-1)), big.NewInt(m.rate)).Int64()
	}
	retry := ceilDiv(new(big.Int).Sub(big.NewInt(1_000_000_000), balance))
	fill := ceilDiv(new(big.Int).Sub(new(big.Int).Mul(big.NewInt(m.burst), big.NewInt(1000)), balance))
	m.expiresAt = logical + max(int64(86400000), fill+60000)
	return reply{Outcome: code, Slot: ss.slot, CommittedSequence: ss.sequence, BalanceU: whole, RetryMS: retry, SessionTTLMS: ss.expiry - now, LogicalMS: logical, FreshInit: fresh}
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
