package ratelimiter

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisBackendConstructionOwnershipAndIdentity(t *testing.T) {
	c, err := normalizeConfig(Config{GlobalRate: 1, GlobalBurst: 1, Redis: RedisConfig{Address: "127.0.0.1:1", Timeout: 20 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := newRedisBackend(c)
	if err != nil {
		t.Fatalf("construction performed I/O: %v", err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: time.Millisecond})
	c.Redis.Client, c.Redis.Address = client, ""
	b, err = newRedisBackend(c)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err = client.Ping(context.Background()).Err(); err == nil || strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("injected client was closed: %v", err)
	}
	client.Close()
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, err := newSessionID()
		if err != nil {
			t.Fatal(err)
		}
		bytes, err := hex.DecodeString(id)
		if err != nil || len(bytes) != 16 || strings.ToLower(id) != id || seen[id] {
			t.Fatalf("bad identity %q", id)
		}
		seen[id] = true
	}
}

// A real peer accepts bytes but never sends a reply, exercising socket cancellation.
func TestRedisBackendBoundedInjectedClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			if _, err = conn.Read(buf); err != nil {
				return
			}
		}
	}()
	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 50 * time.Millisecond, ReadTimeout: 50 * time.Millisecond, WriteTimeout: 50 * time.Millisecond, PoolTimeout: 50 * time.Millisecond})
	defer func() { client.Close(); listener.Close(); <-done }()
	c, _ := normalizeConfig(Config{GlobalRate: 1, GlobalBurst: 1, Redis: RedisConfig{Client: client, Timeout: 20 * time.Millisecond}})
	b, err := newRedisBackend(c)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = b.Probe(context.Background())
	if err == nil || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("unbounded probe %v in %v", err, time.Since(start))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = b.Probe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestProtocolResultPolicy(t *testing.T) {
	cases := []struct {
		out                  outcome
		unresolved           bool
		reason               string
		terminal, initialize bool
		delay                time.Duration
	}{
		{outcomeInitExpired, false, ReasonSyncPending, false, true, 0},
		{outcomeInitExpired, true, ReasonAccountingError, true, false, 0},
		{outcomeInvalidArgument, false, ReasonAccountingError, true, false, 0},
		{outcomeVersionMismatch, false, ReasonPolicyMismatch, true, false, 0},
		{outcomePolicyMismatch, false, ReasonPolicyMismatch, true, false, 0},
		{outcomeStateCorrupt, false, ReasonAccountingError, true, false, 0},
		{outcomeNumericLimit, false, ReasonAccountingError, true, false, 0},
		{outcomeSequenceError, false, ReasonAccountingError, true, false, 0},
		{outcomeSessionExpired, true, ReasonAccountingExpired, true, false, 0},
		{outcomeSessionExpired, false, ReasonSyncPending, false, true, 0},
		{outcomeSessionCapacity, false, ReasonCapacityExhausted, false, false, time.Second},
	}
	for _, tc := range cases {
		got := protocolResult(reply{Outcome: tc.out, BalanceU: 1_000_000}, tc.unresolved)
		if got.Reason != tc.reason || got.Terminal != tc.terminal || got.Reinitialize != tc.initialize || got.RetryDelay != tc.delay || got.Authorized {
			t.Fatalf("%+v -> %+v", tc, got)
		}
	}
	for _, o := range []outcome{outcomeOK, outcomeDuplicate} {
		for _, balance := range []int64{-1, 0, 999999, 1000000} {
			got := protocolResult(reply{Outcome: o, BalanceU: balance}, false)
			if got.Authorized != (balance >= 1_000_000) {
				t.Fatalf("%v %d -> %+v", o, balance, got)
			}
		}
	}
}

// Only the unavailable network boundary is substituted. A canceled first
// command must not grant NOSCRIPT fallback a new budget or identity.
type budgetHook struct {
	t      *testing.T
	cancel context.CancelFunc
	first  context.Context
	args   []interface{}
	calls  int
}

func (h *budgetHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *budgetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *budgetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.calls++
		if _, ok := ctx.Deadline(); !ok {
			h.t.Fatal("attempt has no deadline")
		}
		if h.calls == 1 {
			h.first = ctx
			h.args = append([]interface{}(nil), cmd.Args()[2:]...)
			h.cancel()
			err := testRedisError("NOSCRIPT missing")
			cmd.SetErr(err)
			return err
		}
		if ctx != h.first || !reflect.DeepEqual(h.args, cmd.Args()[2:]) {
			h.t.Fatal("fallback changed attempt or arguments")
		}
		err := ctx.Err()
		if err == nil {
			h.t.Fatal("canceled attempt restarted")
		}
		cmd.SetErr(err)
		return err
	}
}
func TestRedisBackendNOSCRIPTBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	hook := &budgetHook{t: t, cancel: cancel}
	client.AddHook(hook)
	c, _ := normalizeConfig(Config{GlobalRate: 1, GlobalBurst: 1, Redis: RedisConfig{Client: client}})
	b, _ := newRedisBackend(c)
	_, err := b.Execute(ctx, "key", command{Op: opApply, Session: "0123456789abcdef0123456789abcdef", Slot: 1, Sequence: 1, Delta: 1, RateU: 1, BurstU: 1000000})
	if !errors.Is(err, context.Canceled) || hook.calls != 2 {
		t.Fatalf("fallback budget: calls=%d err=%v", hook.calls, err)
	}
}

type testRedisError string

func (e testRedisError) Error() string { return string(e) }
func (e testRedisError) RedisError()   {}
