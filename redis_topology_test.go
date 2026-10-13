package ratelimiter

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func topologyAddresses(t *testing.T, variable, required string) []string {
	t.Helper()
	value := os.Getenv(variable)
	if value == "" {
		if os.Getenv(required) == "1" {
			t.Fatalf("%s=1 requires %s", required, variable)
		}
		t.Skipf("Redis topology integration skipped: set %s", variable)
	}
	return strings.Split(value, ",")
}

// Routing across all three masters exercises hashed keys and catches adapter
// changes which bypass UniversalClient slot routing.
func TestRedisClusterIntegration(t *testing.T) {
	addrs := topologyAddresses(t, "REDIS_CLUSTER_ADDRS", "REQUIRE_REDIS_CLUSTER")
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, MaxRetries: -1, MaxRedirects: 3, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolTimeout: time.Second})
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	masters := map[string]bool{}
	for _, slot := range slots {
		if len(slot.Nodes) > 0 {
			masters[slot.Nodes[0].Addr] = false
		}
	}
	if len(masters) != 3 {
		t.Fatalf("expected three masters: %v", masters)
	}
	b, _ := integrationBackend(t, client, time.Second)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("route:{raw-%d}", i)
		wireKey := redisKey(b.prefix, key)
		slot, err := client.ClusterKeySlot(ctx, wireKey).Result()
		if err != nil {
			t.Fatal(err)
		}
		owner := ""
		for _, rangeSlots := range slots {
			if slot >= int64(rangeSlots.Start) && slot <= int64(rangeSlots.End) {
				owner = rangeSlots.Nodes[0].Addr
			}
		}
		if owner == "" {
			t.Fatal("unassigned slot")
		}
		if masters[owner] {
			continue
		}
		func() {
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
				defer cleanupCancel()
				if err := client.Del(cleanupCtx, wireKey).Err(); err != nil {
					t.Error(err)
				}
			}()
			now, err := b.Time(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			c := command{Op: opInit, Session: fmt.Sprintf("%032x", i+1), Slot: -1, Delta: 3, RateU: 1, BurstU: 1000000, RegistrationDeadlineMS: now.UnixMilli() + 30000}
			r, err := b.Execute(ctx, key, c)
			if err != nil {
				t.Fatal(err)
			}
			assertOutcome(t, r, outcomeOK)
			c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 3, 0
			r, err = b.Execute(ctx, key, c)
			if err != nil {
				t.Fatal(err)
			}
			assertOutcome(t, r, outcomeOK)
			r, err = b.Execute(ctx, key, c)
			if err != nil {
				t.Fatal(err)
			}
			assertOutcome(t, r, outcomeDuplicate)
			if r.CommittedSequence != 1 || r.BalanceU >= 0 {
				t.Fatalf("routed accounting: %+v", r)
			}
			direct := redis.NewClient(&redis.Options{Addr: owner, MaxRetries: -1, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
			defer direct.Close()
			if n, err := direct.Exists(ctx, wireKey).Result(); err != nil || n != 1 {
				t.Fatalf("owner %s missing hash: n=%d err=%v", owner, n, err)
			}
		}()
		masters[owner] = true
	}
	for owner, seen := range masters {
		if !seen {
			t.Errorf("master %s was not exercised", owner)
		}
	}
}

// Sentinel promotion is deliberately requested on an isolated fixture. WAIT
// confirms replication before losing the reply; this checks intact-history
// retry safety, not a guarantee against asynchronous-replication write loss.
func TestRedisSentinelIntegration(t *testing.T) {
	addrs := topologyAddresses(t, "REDIS_SENTINEL_ADDRS", "REQUIRE_REDIS_SENTINEL")
	if os.Getenv("REDIS_SENTINEL_ISOLATED") != "1" {
		t.Fatal("failover requires dedicated fixture REDIS_SENTINEL_ISOLATED=1")
	}
	masterName := os.Getenv("REDIS_SENTINEL_MASTER")
	if masterName == "" {
		t.Fatal("REDIS_SENTINEL_MASTER is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sentinel := redis.NewSentinelClient(&redis.Options{Addr: addrs[0], MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer sentinel.Close()
	address := func() (string, error) {
		parts, err := sentinel.GetMasterAddrByName(ctx, masterName).Result()
		if err != nil {
			return "", err
		}
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid master address: %v", parts)
		}
		return net.JoinHostPort(parts[0], parts[1]), nil
	}
	original, err := address()
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: masterName, SentinelAddrs: addrs, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, PoolTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { client.Close() })
	b, key := integrationBackend(t, client, 500*time.Millisecond)
	now, err := b.Time(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	c := command{Op: opInit, Session: "0123456789abcdef0123456789abcdef", Slot: -1, Delta: 3, RateU: 1, BurstU: 1000000, RegistrationDeadlineMS: now.UnixMilli() + 30000}
	r, err := b.Execute(ctx, key, c)
	if err != nil {
		t.Fatal(err)
	}
	assertOutcome(t, r, outcomeOK)
	c.Op, c.Slot, c.Sequence, c.Delta, c.RegistrationDeadlineMS = opApply, r.Slot, 1, 3, 0
	// Commit on a pinned connection, WAIT on that same connection, then discard
	// the reply before promotion. Retry goes through the production adapter.
	conn := client.Conn()
	raw, err := redis.NewScript(redisLua).Run(ctx, conn, []string{redisKey(b.prefix, key)}, encodeCommand(c)...).Result()
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	committed, err := decodeReply(raw)
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	assertOutcome(t, committed, outcomeOK)
	replicated, err := conn.Do(ctx, "WAIT", 1, 5000).Int()
	conn.Close()
	if err != nil || replicated != 1 {
		t.Fatalf("replication before promotion: %d %v", replicated, err)
	}
	before := append([]interface{}(nil), encodeCommand(c)...)
	if err := sentinel.Failover(ctx, masterName).Err(); err != nil {
		t.Fatal(err)
	}
	oldMaster := redis.NewClient(&redis.Options{Addr: original, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer oldMaster.Close()
	var promoted string
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, e := address()
		role, roleErr := oldMaster.Do(ctx, "ROLE").Slice()
		// Discovery can change before the former master is demoted. Do not accept
		// a successful retry on that old writable node as evidence of recovery.
		demoted := roleErr == nil && len(role) > 0 && role[0] == "slave"
		if e == nil && current != original && demoted {
			promoted = current
			r, err = b.Execute(ctx, key, c)
			if err == nil {
				break
			}
			if backendErrorClass(err) != errorTransient {
				t.Fatalf("permanent recovery error: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no finite Sentinel recovery: address=%s error=%v", current, err)
		case <-ticker.C:
		}
	}
	assertOutcome(t, r, outcomeDuplicate)
	if r.CommittedSequence != 1 || r.BalanceU != committed.BalanceU+(r.LogicalMS-committed.LogicalMS)/1000 {
		t.Fatalf("promotion lost/doubled deduction: committed=%+v recovered=%+v", committed, r)
	}
	if !reflect.DeepEqual(before, encodeCommand(c)) {
		t.Fatal("retry changed batch identity")
	}
	c.Sequence++
	r, err = b.Execute(ctx, key, c)
	if err != nil {
		t.Fatal(err)
	}
	assertOutcome(t, r, outcomeOK)
	if r.CommittedSequence != 2 {
		t.Fatal("recovered master did not accept next batch")
	}
	direct := redis.NewClient(&redis.Options{Addr: promoted, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer direct.Close()
	session := storedSession(t, direct, redisKey(b.prefix, key), c.Slot)
	if session["id"] != c.Session || session["seq"] != "2" || session["delta"] != "3" {
		t.Fatalf("promoted master %s has incorrect accounting: %v", promoted, session)
	}
	t.Logf("recovered from %s to %s with unchanged batch identity", original, promoted)
}
