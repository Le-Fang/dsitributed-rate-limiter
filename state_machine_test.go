package ratelimiter

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
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

// The foundation runs the real ledger against an independent receipt oracle.
// Each accepted request leaves a receipt; expected pending, applied, unapplied,
// and acknowledged totals are queries over receipts, never production helpers.
// Redis execution is a controlled integer backend at this stage. The real Lua
// and application scheduling will replace that subject seam in later tasks.
type modelReceipt struct {
	cost                  int64
	sequence              uint64
	applied, acknowledged bool
}
type foundationOracle struct {
	receipts                          []modelReceipt
	latestBatch, flight, acknowledged uint64
}
type foundationSubject struct {
	ledger            ledger
	accepted, applied int64
	remoteSequence    uint64
	remoteDelta       int64
}
type modelBucketID struct {
	instance int
	key      string
}
type foundationAttempt struct {
	owner              modelBucketID
	payload            batch
	sequence           uint64
	executed, finished bool
}

func checkTrace(trace modelTrace) error {
	if trace.Config.Instances <= 0 || trace.Config.Cap <= 0 || trace.Config.Cap > 1_000_000 {
		return fmt.Errorf("seed %d: invalid model config %+v", trace.Seed, trace.Config)
	}
	subjects := make(map[modelBucketID]*foundationSubject)
	oracles := make(map[modelBucketID]*foundationOracle)
	attempts := make(map[uint64]*foundationAttempt)
	fail := func(index int, format string, args ...interface{}) error {
		return fmt.Errorf("seed=%d config=%+v event=%d: %s; events=%+v", trace.Seed, trace.Config, index, fmt.Sprintf(format, args...), trace.Events[:index+1])
	}
	for index, event := range trace.Events {
		id := modelBucketID{event.Instance, event.Key}
		if event.Instance < 0 || event.Instance >= trace.Config.Instances {
			return fail(index, "invalid instance")
		}
		subject, oracle := subjects[id], oracles[id]
		if event.Kind != "CreateKey" && subject == nil {
			return fail(index, "event without a created key")
		}
		switch event.Kind {
		case "CreateKey":
			if subject != nil {
				return fail(index, "duplicate key creation")
			}
			subjects[id], oracles[id] = &foundationSubject{}, &foundationOracle{}
		case "AllowN":
			var unresolved int64
			for _, receipt := range oracle.receipts {
				if !receipt.acknowledged {
					unresolved += receipt.cost
				}
			}
			want := event.Cost > 0 && int64(event.Cost) <= int64(trace.Config.Cap)-unresolved
			got := subject.ledger.admit(int64(event.Cost), int64(trace.Config.Cap))
			if got != want {
				return fail(index, "admission=%v, oracle=%v", got, want)
			}
			if want {
				oracle.receipts = append(oracle.receipts, modelReceipt{cost: int64(event.Cost)})
				subject.accepted += int64(event.Cost)
			}
		case "BeginAttempt", "RetrySameBatch":
			if event.Attempt == 0 || attempts[event.Attempt] != nil {
				return fail(index, "attempt identity must be new and nonzero")
			}
			if event.Kind == "RetrySameBatch" && oracle.flight == 0 {
				return fail(index, "retry without flight")
			}
			if oracle.flight == 0 {
				var pending int64
				for _, receipt := range oracle.receipts {
					if receipt.sequence == 0 {
						pending += receipt.cost
					}
				}
				if pending == 0 {
					return fail(index, "begin without pending consumption")
				}
				oracle.latestBatch++
				oracle.flight = oracle.latestBatch
				for i := range oracle.receipts {
					if oracle.receipts[i].sequence == 0 {
						oracle.receipts[i].sequence = oracle.flight
					}
				}
			}
			payload, ok := subject.ledger.begin()
			var expectedDelta int64
			for _, receipt := range oracle.receipts {
				if receipt.sequence == oracle.flight {
					expectedDelta += receipt.cost
				}
			}
			if !ok || payload.Sequence != oracle.flight || payload.Delta != expectedDelta {
				return fail(index, "batch=%+v,%v; oracle sequence=%d delta=%d", payload, ok, oracle.flight, expectedDelta)
			}
			attempts[event.Attempt] = &foundationAttempt{owner: id, payload: payload, sequence: oracle.flight}
		case "ExecuteRedis":
			attempt := attempts[event.Attempt]
			if attempt == nil || attempt.owner != id || attempt.executed || attempt.finished {
				return fail(index, "invalid execution")
			}
			// Controlled backend: executes the production payload, deduplicating the
			// sequence as Redis will. The oracle never reads this payload or counters.
			payload := attempt.payload
			if payload.Sequence == subject.remoteSequence+1 {
				subject.applied += payload.Delta
				subject.remoteSequence, subject.remoteDelta = payload.Sequence, payload.Delta
			} else if payload.Sequence != subject.remoteSequence || payload.Delta != subject.remoteDelta {
				return fail(index, "invalid foundation execution order")
			}
			for i := range oracle.receipts {
				if oracle.receipts[i].sequence == attempt.sequence {
					oracle.receipts[i].applied = true
				}
			}
			attempt.executed = true
		case "DeliverReply", "LoseReply":
			attempt := attempts[event.Attempt]
			if attempt == nil || attempt.owner != id || !attempt.executed || attempt.finished {
				return fail(index, "invalid reply transition")
			}
			if event.Kind == "DeliverReply" {
				want := oracle.flight == attempt.sequence
				if got := subject.ledger.acknowledge(attempt.payload); got != want {
					return fail(index, "ack=%v, oracle=%v", got, want)
				}
				if want {
					for i := range oracle.receipts {
						if oracle.receipts[i].sequence == attempt.sequence {
							oracle.receipts[i].acknowledged = true
						}
					}
					oracle.acknowledged, oracle.flight = attempt.sequence, 0
				}
			}
			attempt.finished = true
		default:
			return fail(index, "unsupported foundation event %q", event.Kind)
		}
		for id, subject := range subjects {
			oracle := oracles[id]
			var accepted, applied, unapplied, pending, outstanding, flight int64
			for _, receipt := range oracle.receipts {
				accepted += receipt.cost
				if receipt.applied {
					applied += receipt.cost
				} else {
					unapplied += receipt.cost
				}
				if !receipt.acknowledged {
					outstanding += receipt.cost
				}
				if receipt.sequence == 0 {
					pending += receipt.cost
				}
				if oracle.flight != 0 && receipt.sequence == oracle.flight {
					flight += receipt.cost
				}
				if receipt.acknowledged && !receipt.applied {
					return fail(index, "oracle acknowledged unapplied consumption")
				}
			}
			l := &subject.ledger
			if accepted != applied+unapplied || subject.accepted != accepted || subject.applied != applied || subject.accepted != subject.applied+unapplied {
				return fail(index, "conservation: accepted=%d/%d applied=%d/%d unapplied=%d", subject.accepted, accepted, subject.applied, applied, unapplied)
			}
			if l.Pending != pending || l.outstanding() != outstanding || outstanding < 0 || outstanding > int64(trace.Config.Cap) || l.Committed != oracle.acknowledged {
				return fail(index, "ledger=%+v outstanding=%d; oracle pending=%d outstanding=%d committed=%d", *l, l.outstanding(), pending, outstanding, oracle.acknowledged)
			}
			if (l.Flight == nil) != (oracle.flight == 0) || (l.Flight != nil && (l.Flight.Sequence != oracle.flight || l.Flight.Delta != flight)) {
				return fail(index, "flight=%+v; oracle sequence=%d delta=%d", l.Flight, oracle.flight, flight)
			}
		}
	}
	return nil
}

func TestStateMachineFoundationDelayedDuplicateAck(t *testing.T) {
	trace := modelTrace{Config: modelConfig{Instances: 1, Cap: 5}, Events: []modelEvent{
		{Kind: "CreateKey", Key: "k"},
		{Kind: "AllowN", Key: "k", Cost: 3},
		{Kind: "BeginAttempt", Key: "k", Attempt: 1},
		{Kind: "ExecuteRedis", Key: "k", Attempt: 1},
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
}
type referenceSession struct {
	slot               int
	sequence           uint64
	delta, cap, expiry int64
}

func (m *redisReference) execute(c command, now int64) reply {
	if m.sessions == nil {
		m.sessions = make(map[string]referenceSession)
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
