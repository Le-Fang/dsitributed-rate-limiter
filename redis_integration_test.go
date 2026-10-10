package ratelimiter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisFixture(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		if os.Getenv("REQUIRE_REDIS") == "1" {
			t.Fatal("REQUIRE_REDIS=1 requires REDIS_ADDR")
		}
		t.Skip("Redis integration skipped: set REDIS_ADDR")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("rate-limiter-test:%d:%s", time.Now().UnixNano(), t.Name())
	t.Cleanup(func() { client.Del(context.Background(), key); client.Close() })
	return client, key
}
func evalProtocol(t *testing.T, client *redis.Client, key, script string, c command) reply {
	t.Helper()
	data, err := client.Eval(context.Background(), script, []string{key}, encodeCommand(c)...).Result()
	if err != nil {
		t.Fatal(err)
	}
	r, err := decodeReply(data)
	if err != nil {
		t.Fatalf("decode %v: %v", data, err)
	}
	return r
}
func initCommand(t *testing.T, client *redis.Client) command {
	t.Helper()
	now, err := client.Time(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	return command{Op: opInit, Session: "0123456789abcdef0123456789abcdef", Slot: -1, Delta: 10, RateU: 1, BurstU: 1_000_000, RegistrationDeadlineMS: now.UnixMilli() + 30_000}
}
func TestRedisIntegrationFoundation(t *testing.T) {
	client, key := redisFixture(t)
	script, err := os.ReadFile("redis.lua")
	if err != nil {
		t.Fatalf("production Lua missing: %v", err)
	}
	c := initCommand(t, client)
	initial := evalProtocol(t, client, key, string(script), c)
	if initial.Outcome != outcomeOK || initial.BalanceU != 1_000_000 || !initial.FreshInit {
		t.Fatalf("INIT: %+v", initial)
	}
	c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, initial.Slot, 1, 3, 0
	first := evalProtocol(t, client, key, string(script), c)
	if first.Outcome != outcomeOK || first.BalanceU > -1_999_900 {
		t.Fatalf("debt: %+v", first)
	}
	duplicate := evalProtocol(t, client, key, string(script), c)
	if duplicate.Outcome != outcomeDuplicate || duplicate.CommittedSequence != first.CommittedSequence {
		t.Fatalf("not a duplicate acknowledgement: %+v", duplicate)
	}
	if duplicate.BalanceU < first.BalanceU {
		t.Fatalf("duplicate charged again: first=%+v duplicate=%+v", first, duplicate)
	}
	c.Delta = 2
	if r := evalProtocol(t, client, key, string(script), c); r.Outcome != outcomeSequenceError {
		t.Fatalf("conflicting delta: %+v", r)
	}
	c.Session = "ffffffffffffffffffffffffffffffff"
	if r := evalProtocol(t, client, key, string(script), c); r.Outcome != outcomeSessionExpired {
		t.Fatalf("missing session: %+v", r)
	}
}

// Controlled clock replacement exists only in test code; production accepts
// exactly eight arguments and always uses the server's real TIME.
func controlledLua(now int64) string {
	return strings.Replace(redisLua, "local clock = redis.call('TIME')", fmt.Sprintf("local clock = {'%d', '%d'}", now/1000, now%1000*1000), 1)
}
func storedSession(t *testing.T, client *redis.Client, key string, slot int) map[string]string {
	t.Helper()
	raw, err := client.HGet(context.Background(), key, fmt.Sprintf("s:%d", slot)).Result()
	if err != nil {
		t.Fatal(err)
	}
	var s map[string]string
	if err = json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatal(err)
	}
	return s
}
func setSession(t *testing.T, client *redis.Client, key string, slot int, s map[string]string) {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(context.Background(), key, fmt.Sprintf("s:%d", slot), raw).Err(); err != nil {
		t.Fatal(err)
	}
}
func assertOutcome(t *testing.T, r reply, want outcome) {
	t.Helper()
	if r.Outcome != want {
		t.Fatalf("outcome=%+v want=%v", r, want)
	}
}

func TestRedisIntegrationSessionsAndClock(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	now := c.RegistrationDeadlineMS - 30000
	initial := evalProtocol(t, client, key, controlledLua(now), c)
	for i := 1; i < 256; i++ {
		c.Session = fmt.Sprintf("%032x", i)
		r := evalProtocol(t, client, key, controlledLua(now), c)
		assertOutcome(t, r, outcomeOK)
		if r.Slot != i {
			t.Fatalf("slot %d: %+v", i, r)
		}
	}
	c.Session = fmt.Sprintf("%032x", 256)
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeSessionCapacity)
	if n := client.HLen(context.Background(), key).Val(); n != 262 {
		t.Fatalf("unbounded fields %d", n)
	}
	s := storedSession(t, client, key, 0)
	oldID := s["id"]
	s["expires_ms"] = fmt.Sprint(now)
	setSession(t, client, key, 0, s)
	old := c
	old.Session = oldID
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), old), outcomeSessionExpired)
	replacement := evalProtocol(t, client, key, controlledLua(now), c)
	assertOutcome(t, replacement, outcomeOK)
	if replacement.Slot != initial.Slot {
		t.Fatal("did not reuse expired slot")
	}
	old.Op, old.Slot, old.Sequence, old.Delta, old.RegistrationDeadlineMS = opApply, 0, 1, 1, 0
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), old), outcomeSessionExpired)
	c.RegistrationDeadlineMS = now
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeInitExpired)
	c.RegistrationDeadlineMS = now + 30001
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeInitExpired)
	// A delayed original INIT cannot resurrect a slot after retention has elapsed.
	old.Op, old.Slot, old.Sequence, old.Delta, old.RegistrationDeadlineMS = opInit, -1, 0, 10, now+30000
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now+86400001), old), outcomeInitExpired)
}

func TestRedisIntegrationRefillAndTTL(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	now := c.RegistrationDeadlineMS - 30000
	c.RateU = 333333
	r := evalProtocol(t, client, key, controlledLua(now), c)
	c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 2, 0
	r = evalProtocol(t, client, key, controlledLua(now), c)
	if r.BalanceU != -1000000 || r.RetryMS != 6001 {
		t.Fatalf("debt %+v", r)
	}
	c.Op, c.Sequence, c.Delta = opRefresh, 0, 0
	r = evalProtocol(t, client, key, controlledLua(now+1), c)
	if r.BalanceU != -999667 || client.HGet(context.Background(), key, "rem").Val() != "333" {
		t.Fatalf("fractional carry %+v", r)
	}
	r = evalProtocol(t, client, key, controlledLua(now+4500), c)
	if r.BalanceU != 499998 || r.authorizes() {
		t.Fatalf("fractional authorization %+v", r)
	}
	r = evalProtocol(t, client, key, controlledLua(now-2000), c)
	if r.BalanceU != 499998 || r.LogicalMS != now+4500 || r.SessionTTLMS != 86406500 {
		t.Fatalf("backward clock %+v", r)
	}
	ttl := client.PTTL(context.Background(), key).Val()
	if ttl < 86406*time.Second || ttl > 86406500*time.Millisecond {
		t.Fatalf("backward TTL %v", ttl)
	}
	r = evalProtocol(t, client, key, controlledLua(now+10_000_000_000), c)
	assertOutcome(t, r, outcomeSessionExpired)
	// Refresh while alive shortcuts products which would overflow on long idle.
	s := storedSession(t, client, key, 0)
	s["expires_ms"] = fmt.Sprint(now + 20_000_000_000)
	setSession(t, client, key, 0, s)
	r = evalProtocol(t, client, key, controlledLua(now+10_000_000_000), c)
	if r.BalanceU != 1_000_000 || client.HGet(context.Background(), key, "rem").Val() != "0" {
		t.Fatalf("full refill %+v", r)
	}
}

func TestRedisIntegrationValidationPreservesState(t *testing.T) {
	tests := []struct {
		name, field, value string
		want               outcome
	}{
		{"version", "v", "2", outcomeVersionMismatch}, {"policy", "rate_u", "2", outcomePolicyMismatch},
		{"noncanonical", "balance_u", "01", outcomeStateCorrupt}, {"range", "balance_u", "1000000000001", outcomeStateCorrupt},
		{"missing", "last_ms", "", outcomeStateCorrupt}, {"remainder", "rem", "1000", outcomeStateCorrupt},
		{"json-extra", "s:0", `{"id":"0123456789abcdef0123456789abcdef","seq":"0","delta":"0","cap":"10","expires_ms":"9999999999999","extra":"1"}`, outcomeStateCorrupt},
		{"json-duplicate", "s:0", `{"id":"0123456789abcdef0123456789abcdef","seq":"0","seq":"0","delta":"0","cap":"10","expires_ms":"9999999999999"}`, outcomeStateCorrupt},
		{"json-number", "s:0", `{"id":"0123456789abcdef0123456789abcdef","seq":0,"delta":"0","cap":"10","expires_ms":"9999999999999"}`, outcomeStateCorrupt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, key := redisFixture(t)
			c := initCommand(t, client)
			r := evalProtocol(t, client, key, redisLua, c)
			c.Op, c.Slot, c.Delta, c.RegistrationDeadlineMS = opRefresh, r.Slot, 0, 0
			client.HSet(context.Background(), key, tc.field, tc.value)
			before := client.HGetAll(context.Background(), key).Val()
			assertOutcome(t, evalProtocol(t, client, key, redisLua, c), tc.want)
			if after := client.HGetAll(context.Background(), key).Val(); !reflect.DeepEqual(before, after) {
				t.Fatalf("error mutated state: %v -> %v", before, after)
			}
		})
	}
	t.Run("wrong-type", func(t *testing.T) {
		client, key := redisFixture(t)
		client.Set(context.Background(), key, "keep", time.Minute)
		assertOutcome(t, evalProtocol(t, client, key, redisLua, initCommand(t, client)), outcomeStateCorrupt)
		if client.Get(context.Background(), key).Val() != "keep" {
			t.Fatal("wrong-type changed")
		}
	})
	t.Run("unknown-field", func(t *testing.T) {
		client, key := redisFixture(t)
		c := initCommand(t, client)
		r := evalProtocol(t, client, key, redisLua, c)
		client.HSet(context.Background(), key, "unknown", "preserve")
		c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 1, 0
		r = evalProtocol(t, client, key, redisLua, c)
		assertOutcome(t, r, outcomeOK)
		if r.BalanceU > 100 || client.HGet(context.Background(), key, "unknown").Val() != "preserve" {
			t.Fatalf("unknown field affected accounting: %+v", r)
		}
	})
}

func TestRedisIntegrationNumericBoundaries(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	now := c.RegistrationDeadlineMS - 30000
	c.Delta = 1_000_000
	c.RateU = 1
	c.BurstU = 1_000_000_000_000
	r := evalProtocol(t, client, key, controlledLua(now), c)
	c.Op, c.Slot, c.Sequence, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 0
	r = evalProtocol(t, client, key, controlledLua(now), c)
	if r.BalanceU != 0 {
		t.Fatal(r)
	}
	c.Sequence = 2
	r = evalProtocol(t, client, key, controlledLua(now), c)
	if r.BalanceU != -1_000_000_000_000 {
		t.Fatal(r)
	}
	ttl := client.Do(context.Background(), "PTTL", key).Val().(int64)
	if ttl < 2_000_000_000_059_000 {
		t.Fatalf("debt TTL truncated: %v", ttl)
	}
	before := client.HGetAll(context.Background(), key).Val()
	c.Sequence = 3
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeNumericLimit)
	if !reflect.DeepEqual(before, client.HGetAll(context.Background(), key).Val()) {
		t.Fatal("numeric failure mutated debt")
	}
	s := storedSession(t, client, key, 0)
	s["seq"] = "9007199254740990"
	setSession(t, client, key, 0, s)
	client.HSet(context.Background(), key, "balance_u", "1000000000000")
	c.Sequence = maxSequence
	c.Delta = 1
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeOK)
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeDuplicate)
	c.Sequence++
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeInvalidArgument)
}

func TestRedisIntegrationCommitBoundaries(t *testing.T) {
	points := []struct {
		name, needle     string
		after, committed bool
	}{
		{"before-persist", "redis.call('PERSIST', KEYS[1])", false, false},
		{"after-persist", "redis.call('PERSIST', KEYS[1])", true, false},
		{"after-hset", "redis.call('PEXPIRE', KEYS[1], ttl_s)", false, true},
		{"after-expire", "redis.call('PEXPIRE', KEYS[1], ttl_s)", true, true},
	}
	for _, point := range points {
		t.Run(point.name, func(t *testing.T) {
			client, key := redisFixture(t)
			c := initCommand(t, client)
			now := c.RegistrationDeadlineMS - 30000
			r := evalProtocol(t, client, key, controlledLua(now), c)
			c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 3, 0
			client.PExpire(context.Background(), key, time.Minute)
			fail := "error('injected commit failure')"
			replacement := fail + "\n" + point.needle
			if point.after {
				replacement = point.needle + "\n" + fail
			}
			script := strings.Replace(controlledLua(now), point.needle, replacement, 1)
			if err := client.Eval(context.Background(), script, []string{key}, encodeCommand(c)...).Err(); err == nil {
				t.Fatal("fault did not execute")
			}
			balance := client.HGet(context.Background(), key, "balance_u").Val()
			want := "1000000"
			if point.committed {
				want = "-2000000"
			}
			if balance != want {
				t.Fatalf("partial commit: %s want %s", balance, want)
			}
			ttl := client.PTTL(context.Background(), key).Val()
			if point.name == "after-persist" || point.name == "after-hset" {
				if ttl != -1 {
					t.Fatalf("unsafe TTL after PERSIST: %v", ttl)
				}
			}
			if point.name == "after-expire" && ttl < time.Hour*24 {
				t.Fatalf("debt expired early: %v", ttl)
			}
			retry := evalProtocol(t, client, key, controlledLua(now), c)
			out := outcomeOK
			if point.committed {
				out = outcomeDuplicate
			}
			assertOutcome(t, retry, out)
			if retry.BalanceU != -2_000_000 || retry.CommittedSequence != 1 {
				t.Fatalf("retry lost or doubled debt %+v", retry)
			}
		})
	}
}

func TestRedisIntegrationBackendNOSCRIPT(t *testing.T) {
	client, _ := redisFixture(t)
	// SCRIPT FLUSH is server-wide. It is allowed only on the dedicated fixture.
	if os.Getenv("REDIS_ISOLATED") != "1" {
		t.Skip("NOSCRIPT flush requires dedicated fixture REDIS_ISOLATED=1")
	}
	c, err := normalizeConfig(Config{GlobalRate: 1, GlobalBurst: 1, Redis: RedisConfig{Client: client, Prefix: fmt.Sprintf("rl-noscript-%d", time.Now().UnixNano()), Timeout: time.Second}, MaxSyncAge: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	b, err := newRedisBackend(c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	key := "noscript"
	t.Cleanup(func() { client.Del(context.Background(), redisKey(c.Redis.Prefix, key)) })
	now, err := b.Time(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	cmd := command{Op: opInit, Session: "0123456789abcdef0123456789abcdef", Slot: -1, Delta: 1, RateU: 1_000_000, BurstU: 1_000_000, RegistrationDeadlineMS: now.UnixMilli() + 30_000}
	if err = client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	r, err := b.Execute(context.Background(), key, cmd)
	if err != nil {
		t.Fatal(err)
	}
	assertOutcome(t, r, outcomeOK)
	cmd.Op, cmd.Slot, cmd.Sequence, cmd.Delta, cmd.RegistrationDeadlineMS = opApply, r.Slot, 1, 1, 0
	r, err = b.Execute(context.Background(), key, cmd)
	if err != nil {
		t.Fatal(err)
	}
	assertOutcome(t, r, outcomeOK)
	if err = client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	r, err = b.Execute(context.Background(), key, cmd)
	if err != nil {
		t.Fatal(err)
	}
	assertOutcome(t, r, outcomeDuplicate)
	if err = b.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if err = client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("injected client closed: %v", err)
	}
}

func TestRedisIntegrationInvalidArguments(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	for _, tc := range []struct {
		index int
		value string
	}{{0, "UNKNOWN"}, {1, "ABCDEF0123456789abcdef0123456789ab"}, {2, "-0"}, {3, "00"}, {4, "1.0"}, {5, "1e6"}, {6, "999999"}, {7, "9007199254740992"}} {
		args := encodeCommand(c)
		args[tc.index] = tc.value
		raw, err := client.Eval(context.Background(), redisLua, []string{key}, args...).Result()
		if err != nil {
			t.Fatal(err)
		}
		r, err := decodeReply(raw)
		if err != nil {
			t.Fatal(err)
		}
		assertOutcome(t, r, outcomeInvalidArgument)
		if client.Exists(context.Background(), key).Val() != 0 {
			t.Fatal("invalid INIT created state")
		}
	}
	for _, args := range [][]interface{}{encodeCommand(c)[:7], append(encodeCommand(c), "fault")} {
		raw, err := client.Eval(context.Background(), redisLua, []string{key}, args...).Result()
		if err != nil {
			t.Fatal(err)
		}
		r, err := decodeReply(raw)
		if err != nil {
			t.Fatal(err)
		}
		assertOutcome(t, r, outcomeInvalidArgument)
	}
}

func TestRedisIntegrationInitRetryAndExpiryBounds(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	now := c.RegistrationDeadlineMS - 30000
	initial := evalProtocol(t, client, key, controlledLua(now), c)
	apply := c
	apply.Op, apply.Slot, apply.Sequence, apply.Delta, apply.RegistrationDeadlineMS = opApply, initial.Slot, 1, 2, 0
	evalProtocol(t, client, key, controlledLua(now), apply)
	retry := evalProtocol(t, client, key, controlledLua(now), c)
	if retry.FreshInit || retry.CommittedSequence != 1 || retry.BalanceU != -1_000_000 {
		t.Fatalf("INIT reset existing session: %+v", retry)
	}
	c.Delta++
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), c), outcomeInvalidArgument)
	apply.Sequence = 3
	before := client.HGetAll(context.Background(), key).Val()
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), apply), outcomeSequenceError)
	if !reflect.DeepEqual(before, client.HGetAll(context.Background(), key).Val()) {
		t.Fatal("sequence gap mutated state")
	}
	client.HSet(context.Background(), key, "last_ms", "9007199254740990")
	apply.Op, apply.Sequence, apply.Delta = opRefresh, 0, 0
	before = client.HGetAll(context.Background(), key).Val()
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(now), apply), outcomeNumericLimit)
	if !reflect.DeepEqual(before, client.HGetAll(context.Background(), key).Val()) {
		t.Fatal("overflow TTL mutated state")
	}
}

func TestRedisIntegrationRegistrationClockDiscontinuity(t *testing.T) {
	client, key := redisFixture(t)
	const sample int64 = 100000
	c := initCommand(t, client)
	c.RegistrationDeadlineMS = sample + 30000
	for _, now := range []int64{sample - 1, sample - 5, sample - 100, sample + 30000} {
		r := evalProtocol(t, client, key, controlledLua(now), c)
		if r.Outcome != outcomeInitExpired {
			t.Fatalf("clock=%d: got %+v; want retriable INIT_EXPIRED", now, r)
		}
		if client.Exists(context.Background(), key).Val() != 0 {
			t.Fatal("invalid window created state")
		}
	}
	c.RegistrationDeadlineMS = sample - 100 + 30000
	assertOutcome(t, evalProtocol(t, client, key, controlledLua(sample-100), c), outcomeOK)
}
