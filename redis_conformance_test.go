package ratelimiter

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These vectors catch drift in real Lua serialization, fractional refill,
// deduplication and TTL installation against the independent rational oracle.
func TestRedisIntegrationProtocolVectors(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	c.RateU, c.BurstU = 333333, 2000000
	model := redisReference{rate: c.RateU, burst: c.BurstU}
	run := func(c command) reply {
		t.Helper()
		got := evalProtocol(t, client, key, redisLua, c)
		now := got.LogicalMS
		// Error replies deliberately omit time.
		if now == 0 {
			now = model.logical
		}
		want := model.execute(c, now)
		authorized := (want.Outcome == outcomeOK || want.Outcome == outcomeDuplicate) && want.BalanceU >= 1000000
		if got != want || got.authorizes() != authorized {
			t.Fatalf("command=%+v actual=%+v oracle=%+v", c, got, want)
		}
		assertRedisReferenceState(t, client, key, &model)
		return got
	}
	initial := run(c)
	c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, initial.Slot, 1, 3, 0
	run(c)
	// The actual committed reply is deliberately discarded. Retry the exact
	// command: a new sequence would double charge these already served tokens.
	run(c)
	conflict := c
	conflict.Delta++
	run(conflict)
	gap := c
	gap.Sequence += 2
	run(gap)
	for i := 0; i < 20; i++ {
		c.Sequence++
		c.Delta = int64(i%3 + 1)
		run(c)
		refresh := c
		refresh.Op, refresh.Sequence, refresh.Delta = opRefresh, 0, 0
		run(refresh)
	}
	missing := c
	missing.Session = "ffffffffffffffffffffffffffffffff"
	run(missing)
}

func assertRedisReferenceState(t *testing.T, client *redis.Client, key string, m *redisReference) {
	t.Helper()
	ctx := context.Background()
	fields, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	whole, rem := new(big.Int), new(big.Int)
	whole.DivMod(m.balanceNumerator, big.NewInt(1000), rem)
	expected := map[string]string{"v": "1", "rate_u": strconv.FormatInt(m.rate, 10), "burst_u": strconv.FormatInt(m.burst, 10), "balance_u": whole.String(), "rem": rem.String(), "last_ms": strconv.FormatInt(m.logical, 10)}
	if len(fields) != len(expected)+len(m.sessions) {
		t.Fatalf("hash fields=%v", fields)
	}
	for field, want := range expected {
		if fields[field] != want {
			t.Fatalf("%s=%s oracle=%s", field, fields[field], want)
		}
	}
	for id, s := range m.sessions {
		want := map[string]string{"id": id, "seq": strconv.FormatUint(s.sequence, 10), "delta": strconv.FormatInt(s.delta, 10), "cap": strconv.FormatInt(s.cap, 10), "expires_ms": strconv.FormatInt(s.expiry, 10)}
		if got := storedSession(t, client, key, s.slot); !reflect.DeepEqual(got, want) {
			t.Fatalf("session=%v oracle=%v", got, want)
		}
	}
	before, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	ttl, err := client.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	after, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	// Bound network elapsed time instead of sleeping or assuming a host clock.
	if ms := ttl.Milliseconds(); ms < m.expiresAt-after.UnixMilli()-1 || ms > m.expiresAt-before.UnixMilli()+1 {
		t.Fatalf("TTL=%d expiry=%d server interval=[%d,%d]", ms, m.expiresAt, before.UnixMilli(), after.UnixMilli())
	}
}

func integrationBackend(t *testing.T, client redis.UniversalClient, timeout time.Duration) (*redisBackend, string) {
	t.Helper()
	cfg, err := normalizeConfig(Config{GlobalRate: 1, GlobalBurst: 100, Redis: RedisConfig{Client: client, Prefix: fmt.Sprintf("rl-integration-%d", time.Now().UnixNano()), Timeout: timeout}, MaxSyncAge: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	b, err := newRedisBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	key := "account"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := client.Del(ctx, redisKey(cfg.Redis.Prefix, key)).Err(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b.(*redisBackend), key
}

func TestRedisIntegrationConcurrentClients(t *testing.T) {
	client, key := redisFixture(t)
	ctx := context.Background()
	const clients = 8
	commands := make([]command, clients)
	for i := range commands {
		c := initCommand(t, client)
		c.Session = fmt.Sprintf("%032x", i+1)
		c.RateU, c.BurstU = 1, 1000000
		r := evalProtocol(t, client, key, redisLua, c)
		assertOutcome(t, r, outcomeOK)
		c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 3, 0
		commands[i] = c
	}
	type result struct {
		r   reply
		err error
	}
	results := make(chan result, clients*2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, c := range commands {
		peer := redis.NewClient(client.Options())
		t.Cleanup(func() { peer.Close() })
		for n := 0; n < 2; n++ {
			wg.Add(1)
			go func(c command) {
				defer wg.Done()
				<-start
				raw, err := redis.NewScript(redisLua).Run(ctx, peer, []string{key}, encodeCommand(c)...).Result()
				var r reply
				if err == nil {
					r, err = decodeReply(raw)
				}
				results <- result{r, err}
			}(c)
		}
	}
	close(start)
	wg.Wait()
	close(results)
	counts := map[outcome]int{}
	firstLogical := int64(1<<63 - 1)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		counts[result.r.Outcome]++
		if result.r.LogicalMS < firstLogical {
			firstLogical = result.r.LogicalMS
		}
	}
	if counts[outcomeOK] != clients || counts[outcomeDuplicate] != clients {
		t.Fatalf("concurrent results: %v", counts)
	}
	c := commands[0]
	c.Op, c.Sequence, c.Delta = opRefresh, 0, 0
	final := evalProtocol(t, client, key, redisLua, c)
	expected := int64(1000000-clients*3*1000000) + (final.LogicalMS-firstLogical)/1000
	if final.BalanceU != expected || final.authorizes() {
		t.Fatalf("lost/doubled concurrent deduction: %+v expected=%d", final, expected)
	}
	for i := 0; i < clients; i++ {
		s := storedSession(t, client, key, i)
		if s["seq"] != "1" || s["delta"] != "3" {
			t.Fatalf("session %d: %v", i, s)
		}
	}
}

func TestRedisIntegrationPoolTimeout(t *testing.T) {
	fixture, _ := redisFixture(t)
	options := *fixture.Options()
	options.PoolSize, options.MaxActiveConns = 1, 1
	options.PoolTimeout = 30 * time.Millisecond
	client := redis.NewClient(&options)
	t.Cleanup(func() { client.Close() })
	// A pinned connection exhausts the actual pool; no fake transport error.
	conn := client.Conn()
	defer conn.Close()
	if err := conn.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	b, key := integrationBackend(t, client, 100*time.Millisecond)
	start := time.Now()
	_, err := b.Execute(context.Background(), key, initCommand(t, fixture))
	if err == nil || backendErrorClass(err) != errorTransient || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("pool bound/classification: %v elapsed=%v", err, time.Since(start))
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Probe(context.Background()); err != nil {
		t.Fatalf("pool did not recover: %v", err)
	}
}

func TestRedisIntegrationStateExpiry(t *testing.T) {
	client, key := redisFixture(t)
	c := initCommand(t, client)
	r := evalProtocol(t, client, key, redisLua, c)
	// Physically expire a full, clean record; exact safe 24-hour expiry boundaries
	// stay in the deterministic model rather than waiting a day in CI.
	if err := client.PExpire(context.Background(), key, time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		n, err := client.Exists(context.Background(), key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("record did not expire")
		}
		time.Sleep(time.Millisecond)
	}
	c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 1, 0
	assertOutcome(t, evalProtocol(t, client, key, redisLua, c), outcomeSessionExpired)
	c.Op, c.Sequence, c.Delta = opRefresh, 0, 0
	assertOutcome(t, evalProtocol(t, client, key, redisLua, c), outcomeSessionExpired)
	if n := client.Exists(context.Background(), key).Val(); n != 0 {
		t.Fatal("old operation recreated missing state")
	}
	fresh := initCommand(t, client)
	fresh.Session = "ffffffffffffffffffffffffffffffff"
	r = evalProtocol(t, client, key, redisLua, fresh)
	if !r.FreshInit || r.BalanceU != fresh.BurstU {
		t.Fatalf("clean reinitialization: %+v", r)
	}
	c.Op, c.Sequence, c.Delta = opApply, 1, 1
	before := client.HGetAll(context.Background(), key).Val()
	assertOutcome(t, evalProtocol(t, client, key, redisLua, c), outcomeSessionExpired)
	if after := client.HGetAll(context.Background(), key).Val(); !reflect.DeepEqual(before, after) {
		t.Fatal("old identity mutated reused slot")
	}
}
