package ratelimiter

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func reduceTrace(trace modelTrace) modelTrace { return reduceTraceWith(trace, checkTrace) }
func reduceTraceWith(trace modelTrace, check func(modelTrace) error) modelTrace {
	original := check(trace)
	var first *modelFailure
	if !errors.As(original, &first) {
		return trace
	}
	if first.Index >= 0 && first.Index+1 < len(trace.Events) {
		trace.Events = trace.Events[:first.Index+1]
	}
	fails := func(candidate modelTrace) bool {
		var err *modelFailure
		return errors.As(check(candidate), &err) && err.Category == first.Category
	}
	// Deterministic contiguous deletion, coarse to fine; repeat at each width.
	for width := len(trace.Events) / 2; width >= 1; width /= 2 {
		for start := 0; start+width <= len(trace.Events); {
			candidate := trace
			candidate.Events = append(append([]modelEvent(nil), trace.Events[:start]...), trace.Events[start+width:]...)
			if fails(candidate) {
				trace = candidate
			} else {
				start++
			}
		}
	}
	for i := range trace.Events {
		for _, cost := range []int{0, 1, trace.Events[i].Cost / 2} {
			if cost < 0 || cost >= trace.Events[i].Cost {
				continue
			}
			candidate := trace
			candidate.Events = append([]modelEvent(nil), trace.Events...)
			candidate.Events[i].Cost = cost
			if fails(candidate) {
				trace = candidate
			}
		}
		for _, ns := range []int64{0, 1, int64(time.Millisecond), trace.Events[i].AdvanceNS / 2} {
			if ns < 0 || ns >= trace.Events[i].AdvanceNS {
				continue
			}
			candidate := trace
			candidate.Events = append([]modelEvent(nil), trace.Events...)
			candidate.Events[i].AdvanceNS = ns
			if fails(candidate) {
				trace = candidate
			}
		}
	}
	return trace
}
func failModelTrace(t testing.TB, trace modelTrace, err error) {
	t.Helper()
	reduced := reduceTrace(trace)
	data, marshalErr := json.MarshalIndent(reduced, "", "  ")
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	digest := sha256.Sum256(data)
	path := filepath.Join("testdata", "model", fmt.Sprintf("regression-%x.json", digest[:8]))
	saveErr := os.WriteFile(path, append(data, '\n'), 0644)
	t.Fatalf("%v\nminimized regression %s (save error %v):\n%s", err, path, saveErr, data)
}

// SplitMix64 is specified here instead of math/rand: seeds and corpus bytes
// retain exactly the same interpretation across supported Go versions.
func modelRandom(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	z := *state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

var modelKinds = []string{"AllowN", "AllowN", "BeginAttempt", "ExecuteRedis", "DeliverReply", "LoseReply", "TimeoutAttempt", "RetrySameBatch", "AdvanceTime", "AdvanceLocalTime", "AdvanceRedisTime", "QueueSync", "ProbeFailure", "ProbeSuccess", "ExpireSession", "EvictCleanKey", "CancelParent", "Close", "CrashInstance", "RestartInstance", "CreateKey"}
var modelEpisode = []modelEvent{
	{Kind: "CrashInstance"}, {Kind: "CrashInstance", Instance: 1},
	{Kind: "RestartInstance"}, {Kind: "RestartInstance", Instance: 1},
	{Kind: "CreateKey"}, {Kind: "CreateKey", Instance: 1},
	{Kind: "BeginAttempt"}, {Kind: "ExecuteRedis"}, {Kind: "DeliverReply"},
	{Kind: "BeginAttempt", Instance: 1}, {Kind: "ExecuteRedis", Instance: 1}, {Kind: "DeliverReply", Instance: 1},
	{Kind: "AllowN", Cost: 1}, {Kind: "BeginAttempt"}, {Kind: "ExecuteRedis"}, {Kind: "LoseReply"},
	{Kind: "AllowN", Cost: 1}, {Kind: "RetrySameBatch"}, {Kind: "ExecuteRedis"}, {Kind: "DeliverReply"},
	{Kind: "BeginAttempt"}, {Kind: "ExecuteRedis"}, {Kind: "DeliverReply"},
	{Kind: "AllowN", Instance: 1, Cost: 1}, {Kind: "BeginAttempt", Instance: 1}, {Kind: "ExecuteRedis", Instance: 1}, {Kind: "DeliverReply", Instance: 1},
	{Kind: "ProbeFailure"}, {Kind: "AdvanceTime", AdvanceNS: 2_000_000_000}, {Kind: "AllowN", Cost: 1},
	{Kind: "ProbeSuccess"}, {Kind: "BeginAttempt"}, {Kind: "ExecuteRedis"}, {Kind: "DeliverReply"},
	{Kind: "AllowN", Cost: 1}, {Kind: "CancelParent", Instance: 1}, {Kind: "AllowN", Instance: 1, Cost: 1}, {Kind: "Close", Instance: 1},
}

func modelTraceBytes(seed uint64, data []byte) modelTrace {
	trace := modelTrace{Seed: seed, Config: modelConfig{Instances: 2, Cap: 2, RateU: 1_000_000, Burst: 3, LocalRateU: 2_000_000, LocalBurst: 2, FallbackRateU: 500_000, FallbackBurst: 1, Policy: FailurePolicy(seed % 2)}}
	trace.Config = modelFuzzSeedConfig(trace.Config)
	for i := 0; i < 2; i++ {
		trace.Events = append(trace.Events, modelEvent{Kind: "CreateKey", Instance: i, Key: "k"}, modelEvent{Kind: "Initialize", Instance: i, Key: "k"})
	}
	var last [2]uint64
	for off := 0; off+8 <= len(data); off += 8 {
		b := data[off : off+8]
		instance := int(b[1] % 2)
		kind := modelKinds[int(b[0])%len(modelKinds)]
		e := modelEvent{Kind: kind, Instance: instance, Key: "k", Cost: []int{0, 1, 2, 3}[b[2]%4], AdvanceNS: []int64{0, 1_000_000, 500_000_000, 1_000_000_000}[b[3]%4]}
		// A valid retry/recovery spine opens each 64-event episode. The remainder
		// is fully adversarial. Restarts bound accumulated terminal/closed states.
		phase := (off / 8) % 64
		scripted := phase < len(modelEpisode)
		if scripted {
			e = modelEpisode[phase]
			e.Key = "k"
			instance = e.Instance
			kind = e.Kind
		}
		e.Attempt = last[instance]
		if kind == "BeginAttempt" || kind == "RetrySameBatch" {
			e.Attempt = uint64(off/8 + 1)
			last[instance] = e.Attempt
		}
		if !scripted && b[4]%8 == 0 && off >= 8 {
			e.Attempt = uint64(b[5])%uint64(off/8) + 1
		}
		if !scripted && kind == "ExecuteRedis" {
			switch b[6] % 16 {
			case 0:
				e.Checkpoint = "before"
			case 1:
				e.Checkpoint = "after"
			}
		}
		trace.Events = append(trace.Events, e)
	}
	return trace
}

type modelCoverage struct {
	Events           map[string]int
	Normal, Fallback int
}

func TestStateMachineFixedSeeds(t *testing.T) {
	coverage := &modelCoverage{Events: map[string]int{}}
	for seed := uint64(0); seed < 1000; seed++ {
		state := seed
		data := make([]byte, 996*8)
		for off := 0; off < len(data); off += 8 {
			binary.LittleEndian.PutUint64(data[off:], modelRandom(&state))
		}
		trace := modelTraceBytes(seed, data)
		trace.coverage = coverage
		if len(trace.Events) != 1000 {
			t.Fatal("seed schedule length changed")
		}
		if err := checkTrace(trace); err != nil {
			failModelTrace(t, trace, err)
		}
	}
	if coverage.Normal < 10000 || coverage.Fallback < 5000 {
		t.Fatalf("insufficient real admission coverage: %+v", coverage)
	}
	for _, kind := range modelKinds {
		if coverage.Events[kind] == 0 {
			t.Fatalf("unexercised event %s", kind)
		}
	}
	for _, kind := range []string{"RetrySameBatch", "ExecuteRedis", "DeliverReply", "ProbeSuccess", "CrashInstance", "RestartInstance"} {
		if coverage.Events[kind] < 10000 {
			t.Fatalf("insufficient lifecycle coverage %s=%d", kind, coverage.Events[kind])
		}
	}
	t.Logf("executed events=%v normal=%d fallback=%d", coverage.Events, coverage.Normal, coverage.Fallback)
}
func FuzzStateMachine(f *testing.F) {
	f.Add(uint64(0), make([]byte, 64*8))
	f.Add(uint64(1), make([]byte, 64*8))
	paths, _ := filepath.Glob("testdata/model/*.json")
	// JSON seed traces preserve their exact event ordering. Non-JSON fuzz bytes
	// use the fixed eight-byte mapping above, capped at 1000 events.
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			f.Fatal(err)
		}
		var seedTrace modelTrace
		if err = json.Unmarshal(data, &seedTrace); err != nil {
			f.Fatal(err)
		}
		seedTrace.Config = modelFuzzSeedConfig(seedTrace.Config)
		if !validModelFuzzConfig(seedTrace.Config) {
			continue
		}
		// Assertions about a fair suffix are replayed by deterministic tests. An
		// arbitrary mutation need not preserve their scheduling preconditions.
		events := seedTrace.Events[:0]
		for _, e := range seedTrace.Events {
			if e.Kind != "RequireNormal" && e.Kind != "ScanUntilQueued" && e.Kind != "SaturateQueue" {
				events = append(events, e)
			}
		}
		seedTrace.Events = events
		data, err = json.Marshal(seedTrace)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(seedTrace.Seed, data)
	}
	f.Fuzz(func(t *testing.T, seed uint64, data []byte) {

		var trace modelTrace
		if len(data) > 256000 {
			return
		}
		if bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) && json.Unmarshal(data, &trace) == nil {
			if !validModelFuzzConfig(trace.Config) || len(trace.Events) > 1000 {
				return
			}
			for _, e := range trace.Events {
				if !knownModelEvent(e.Kind) || e.Kind == "RequireNormal" || e.Kind == "ScanUntilQueued" || e.Kind == "SaturateQueue" || e.AdvanceNS < 0 || e.AdvanceNS > int64(25*time.Hour) {
					return
				}
			}
		} else {
			if len(data) > 996*8 {
				data = data[:996*8]
			}
			trace = modelTraceBytes(seed, data)
		}
		if err := checkTrace(trace); err != nil {
			failModelTrace(t, trace, err)
		}
	})
}
func FuzzProtocolReply(f *testing.F) {
	for _, s := range []string{"0", "1", "255", "256", "9007199254740991", "9007199254740992", "-1", "01", "NaN", "1e6"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		n, ok := new(big.Int).SetString(s, 10)
		canonical := ok && n.String() == s
		wire := []interface{}{"OK", "0", "0", "1000000", "0", "86400000", "1700000000000", "0", "1"}
		wire[2] = s
		r, err := decodeReply(wire)
		valid := canonical && n.Sign() >= 0 && n.Cmp(new(big.Int).SetUint64(1<<53-1)) <= 0
		if (err == nil) != valid {
			t.Fatalf("sequence %q valid=%v err=%v", s, valid, err)
		}
		if valid && r.CommittedSequence != n.Uint64() {
			t.Fatal("sequence altered")
		}
		wire[2] = "0"
		wire[1] = s
		_, err = decodeReply(wire)
		valid = canonical && n.Sign() >= 0 && n.Cmp(big.NewInt(255)) <= 0
		if (err == nil) != valid {
			t.Fatalf("slot %q valid=%v err=%v", s, valid, err)
		}
	})
}
func FuzzNumericBoundaries(f *testing.F) {
	for _, n := range []int64{-1, 0, 1, 999, 1000, 1001, 999999999999, 1000000000000, 1<<53 - 1} {
		f.Add(n, int64(1))
	}
	f.Fuzz(func(t *testing.T, n, d int64) {
		s := strconv.FormatInt(n, 10)
		got, ok := parseWireInt(s, 0, 1<<53-1)
		if ok != (n >= 0 && n <= 1<<53-1) || (ok && got != n) {
			t.Fatal("wire bound")
		}
		// Integer/rational conversion oracle for duration saturation.
		ms := new(big.Int).Mul(big.NewInt(n), big.NewInt(1_000_000))
		maxNS := big.NewInt(1<<63 - 1)
		if ms.Sign() < 0 {
			ms.SetInt64(0)
		}
		if ms.Cmp(maxNS) > 0 {
			ms.Set(maxNS)
		}
		if int64(durationMilliseconds(n)) != ms.Int64() {
			t.Fatal("duration overflow")
		}
		rate := int64(uint64(n)%1_000_000_000_000) + 1
		delta := int64(uint64(d)%1_000_000) + 1
		remote := redisReference{rate: rate, burst: 1_000_000_000_000}
		c := command{Op: opInit, Session: "numeric", Delta: delta, RegistrationDeadlineMS: 30000}
		r := remote.execute(c, 0)
		c.Op = opApply
		c.Slot = r.Slot
		c.Sequence = 1
		once := remote.execute(c, 0)
		twice := remote.execute(c, 0)
		if twice.Outcome != outcomeDuplicate || once.BalanceU != twice.BalanceU || remote.applied[c.Session] != delta {
			t.Fatal("numeric duplicate")
		}
	})
}

func TestStateMachineReducerAndDiagnostics(t *testing.T) {
	trace := modelTrace{Seed: 10, Config: modelConfig{Instances: 1, Cap: 5}, Events: []modelEvent{{Kind: "CreateKey", Key: "k"}, {Kind: "Initialize", Key: "k"}, {Kind: "AdvanceTime", Key: "k", AdvanceNS: 100}, {Kind: "AllowN", Key: "k", Cost: 3}, {Kind: "BeginAttempt", Key: "k", Attempt: 1}, {Kind: "ExecuteRedis", Key: "k", Attempt: 1}}}
	check := func(tr modelTrace) error {
		return checkTraceWithExecution(tr, func(r *redisReference, c command, now int64) reply {
			if c.Op == opApply {
				c.Delta = 1
			}
			return r.execute(c, now)
		})
	}
	var e *modelFailure
	if !errors.As(check(trace), &e) || e.Category != "conservation" || e.State == "" || len(e.Events) == 0 {
		t.Fatalf("missing diagnostics: %v", e)
	}
	reduced := reduceTraceWith(trace, check)
	if len(reduced.Events) >= len(trace.Events) {
		t.Fatal("range deletion did not reduce")
	}
	b1, _ := json.Marshal(reduced)
	b2, _ := json.Marshal(reduceTraceWith(trace, check))
	if !bytes.Equal(b1, b2) {
		t.Fatal("nondeterministic reduction")
	}
	if !strings.Contains(check(reduced).Error(), "conservation") {
		t.Fatal("category changed")
	}
}

// The bounded fuzz domain follows supported policy relationships independently
// of normalizeConfig. Fixed core-only fixtures may deliberately use a broader
// domain; they still execute unfiltered in TestStateMachineTraces.
func validModelFuzzConfig(cfg modelConfig) bool {
	cfg = normalizeModelConfig(cfg)
	if cfg.Instances < 1 || cfg.Instances > 4 || cfg.Cap < 1 || cfg.Cap > 8 || cfg.MaxKeys < 1 || cfg.MaxKeys > 2048 {
		return false
	}
	if cfg.RateU < 1 || cfg.RateU > 1e12 || cfg.LocalRateU < 1 || cfg.LocalRateU > 1e12 || cfg.Burst < 1 || cfg.Burst > 1e6 || cfg.LocalBurst < 1 || cfg.LocalBurst > cfg.Burst || cfg.LocalBurst > cfg.Cap {
		return false
	}
	if cfg.FreshnessNS < 1 || cfg.FreshnessNS > int64(30*time.Second) || cfg.SyncNS < 1 || cfg.SyncNS >= cfg.FreshnessNS || cfg.IdleNS < 1 {
		return false
	}
	switch cfg.Policy {
	case FailClosed:
		return cfg.FallbackRateU == 0 && cfg.FallbackBurst == 0
	case LocalOnly:
		return cfg.FallbackRateU >= 1 && cfg.FallbackRateU <= cfg.LocalRateU && cfg.FallbackBurst >= 1 && cfg.FallbackBurst <= cfg.LocalBurst
	default:
		return false
	}
}

// Seed preparation removes only inert FailClosed fallback fields from historical
// core fixtures. Arbitrary fuzz JSON is validated as received, never repaired.
func modelFuzzSeedConfig(cfg modelConfig) modelConfig {
	if cfg.Policy == FailClosed {
		cfg.FallbackRateU, cfg.FallbackBurst = 0, 0
	}
	return cfg
}

// Removing supported-domain checks must expose invalid configurations before
// they can produce misleading production/oracle comparisons.
func TestStateMachineFuzzConfig(t *testing.T) {
	base := modelConfig{Instances: 1, Cap: 1}
	cases := []struct {
		name   string
		change func(*modelConfig)
		valid  bool
	}{
		{"defaults", func(c *modelConfig) {}, true},
		{"24-hour freshness", func(c *modelConfig) { c.FreshnessNS = int64(24 * time.Hour) }, false},
		{"freshness upper boundary", func(c *modelConfig) { c.FreshnessNS = int64(30 * time.Second) }, true},
		{"freshness above boundary", func(c *modelConfig) { c.FreshnessNS = int64(30*time.Second) + 1 }, false},
		{"negative freshness", func(c *modelConfig) { c.FreshnessNS = -1 }, false},
		{"sync equals freshness", func(c *modelConfig) { c.SyncNS = int64(time.Second) }, false},
		{"default sync exceeds freshness", func(c *modelConfig) { c.FreshnessNS = int64(time.Millisecond) }, false},
		{"sync below freshness", func(c *modelConfig) { c.SyncNS = int64(time.Second) - 1 }, true},
		{"short supported intervals", func(c *modelConfig) { c.SyncNS = 1; c.FreshnessNS = 2 }, true},
		{"negative sync", func(c *modelConfig) { c.SyncNS = -1 }, false},
		{"negative idle", func(c *modelConfig) { c.IdleNS = -1 }, false},
		{"small idle", func(c *modelConfig) { c.IdleNS = 1 }, true},
		{"unknown policy", func(c *modelConfig) { c.Policy = 2 }, false},
		{"closed fallback rate", func(c *modelConfig) { c.FallbackRateU = 1 }, false},
		{"closed fallback burst", func(c *modelConfig) { c.FallbackBurst = 1 }, false},
		{"missing local fallback", func(c *modelConfig) { c.Policy = LocalOnly }, false},
		{"fallback equals local", func(c *modelConfig) { c.Policy = LocalOnly; c.FallbackRateU = 1e6; c.FallbackBurst = 1 }, true},
		{"fallback rate exceeds local", func(c *modelConfig) { c.Policy = LocalOnly; c.FallbackRateU = 1e6 + 1; c.FallbackBurst = 1 }, false},
		{"fallback burst exceeds local", func(c *modelConfig) { c.Policy = LocalOnly; c.FallbackRateU = 1; c.FallbackBurst = 2 }, false},
		{"local burst exceeds cap", func(c *modelConfig) { c.LocalBurst = 2 }, false},
		{"default local burst exceeds global", func(c *modelConfig) { c.Cap = 2; c.Burst = 1 }, false},
		{"cap may exceed global", func(c *modelConfig) { c.Cap = 2; c.Burst = 1; c.LocalBurst = 1 }, true},
		{"minimum rates", func(c *modelConfig) { c.RateU = 1; c.LocalRateU = 1 }, true},
		{"maximum rates", func(c *modelConfig) { c.RateU = 1e12; c.LocalRateU = 1e12 }, true},
		{"global rate overflow", func(c *modelConfig) { c.RateU = 1e12 + 1 }, false},
		{"local rate overflow", func(c *modelConfig) { c.LocalRateU = 1e12 + 1 }, false},
		{"negative rate", func(c *modelConfig) { c.RateU = -1 }, false},
		{"maximum global burst", func(c *modelConfig) { c.Burst = 1e6 }, true},
		{"global burst overflow", func(c *modelConfig) { c.Burst = 1e6 + 1 }, false},
		{"negative local burst", func(c *modelConfig) { c.LocalBurst = -1 }, false},
		{"instance cap", func(c *modelConfig) { c.Instances = 4 }, true},
		{"too many instances", func(c *modelConfig) { c.Instances = 5 }, false},
		{"no instances", func(c *modelConfig) { c.Instances = 0 }, false},
		{"cap limit", func(c *modelConfig) { c.Cap = 8 }, true},
		{"cap overflow", func(c *modelConfig) { c.Cap = 9 }, false},
		{"zero cap", func(c *modelConfig) { c.Cap = 0 }, false},
		{"key cap", func(c *modelConfig) { c.MaxKeys = 2048 }, true},
		{"key overflow", func(c *modelConfig) { c.MaxKeys = 2049 }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.change(&cfg)
			if got := validModelFuzzConfig(cfg); got != tc.valid {
				t.Fatalf("config accepted=%v want=%v: %+v", got, tc.valid, cfg)
			}
			if tc.valid {
				trace := modelTrace{Config: cfg, Events: []modelEvent{{Kind: "CreateKey", Key: "k"}, {Kind: "Initialize", Key: "k"}}}
				if err := checkTrace(trace); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStateMachineFuzzSeedDomain(t *testing.T) {
	// These fixed fixtures intentionally exceed the supported fuzz domain. No
	// discovered regression is excluded; all five remain eligible JSON seeds.
	coreOnly := map[string]bool{
		"all-256-session-slots.json":               true,
		"fallback-negative-recovery-closed.json":   true,
		"fallback-negative-recovery-local.json":    true,
		"fallback-latched-outage-recovery.json":    true,
		"expired-slot-retained-negative-debt.json": true,
	}
	paths, err := filepath.Glob("testdata/model/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("fixtures: %v", err)
	}
	regressions := 0
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var trace modelTrace
		if err := json.Unmarshal(data, &trace); err != nil {
			t.Fatal(err)
		}
		trace.Config = modelFuzzSeedConfig(trace.Config)
		if got := validModelFuzzConfig(trace.Config); got == coreOnly[filepath.Base(path)] {
			t.Errorf("unexpected seed eligibility %s: %v", path, got)
		}
		if strings.HasPrefix(filepath.Base(path), "regression-") {
			regressions++
			if err := checkTrace(trace); err != nil {
				t.Fatal(err)
			}
		}
	}
	if regressions < 5 {
		t.Fatalf("lost regression seeds: %d", regressions)
	}
	for seed := uint64(0); seed < 1000; seed++ {
		if !validModelFuzzConfig(modelTraceBytes(seed, nil).Config) {
			t.Fatalf("invalid mapped config seed=%d", seed)
		}
	}
}
