package ratelimiter

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed redis.lua
var redisLua string

// redisBackend owns wire serialization and transport only. Script outcomes are
// application values; transport errors remain errors and never carry a balance.
type redisBackend struct {
	client  redis.UniversalClient
	script  *redis.Script
	prefix  string
	timeout time.Duration
	owned   bool
}

func newRedisBackend(c normalizedConfig) (backend, error) {
	b := &redisBackend{client: c.Redis.Client, script: redis.NewScript(redisLua), prefix: c.Redis.Prefix, timeout: c.Redis.Timeout}
	if b.client == nil {
		b.owned = true
		b.client = redis.NewClient(&redis.Options{
			Addr: c.Redis.Address, MaxRetries: -1, ContextTimeoutEnabled: true,
			DialTimeout: b.timeout, ReadTimeout: b.timeout, WriteTimeout: b.timeout, PoolTimeout: b.timeout,
			PoolSize: 4, MaxActiveConns: 4,
		})
	}
	return b, nil
}
func (b *redisBackend) Time(ctx context.Context) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.client.Time(ctx).Result()
}
func (b *redisBackend) Probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.client.Ping(ctx).Err()
}
func (b *redisBackend) Execute(ctx context.Context, key string, c command) (reply, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	// Script.Run performs EVALSHA -> NOSCRIPT -> EVAL with the SAME context and
	// arguments. It does not grant the fallback a new operation budget.
	value, err := b.script.Run(ctx, b.client, []string{redisKey(b.prefix, key)}, encodeCommand(c)...).Result()
	if err != nil {
		return reply{}, err
	}
	return decodeReply(value)
}
func (b *redisBackend) Close() error {
	if b.owned {
		return b.client.Close()
	}
	return nil
}

// newSessionID is used during worker initialization, never on the admission path.
func newSessionID() (string, error) {
	var bits [16]byte
	if _, err := rand.Read(bits[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bits[:]), nil
}
