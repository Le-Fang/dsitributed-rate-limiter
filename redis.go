package ratelimiter

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed redis.lua
var redisLua string

// redisBackend owns wire serialization and transport only. Script outcomes are
// application values; transport errors are classified at this boundary and never carry a balance.
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
func (b *redisBackend) Time(ctx context.Context, key string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	// Declaring the same derived key as INIT routes TIME to its Cluster owner.
	// EVAL avoids a separate NOSCRIPT round trip for this tiny read-only script.
	values, err := b.client.Eval(ctx, "return redis.call('TIME')", []string{redisKey(b.prefix, key)}).StringSlice()
	if err != nil {
		return time.Time{}, classifyRedisError(err)
	}
	if len(values) != 2 {
		return time.Time{}, classifyRedisError(errMalformedReply)
	}
	seconds, secErr := strconv.ParseInt(values[0], 10, 64)
	micros, microErr := strconv.ParseInt(values[1], 10, 64)
	if secErr != nil || microErr != nil || seconds < 0 || micros < 0 || micros >= 1000000 {
		return time.Time{}, classifyRedisError(errMalformedReply)
	}
	return time.Unix(seconds, micros*1000), nil
}
func (b *redisBackend) Probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return classifyRedisError(b.client.Ping(ctx).Err())
}
func (b *redisBackend) Execute(ctx context.Context, key string, c command) (reply, error) {
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	// Script.Run performs EVALSHA -> NOSCRIPT -> EVAL with the SAME context and
	// arguments. It does not grant the fallback a new operation budget.
	value, err := b.script.Run(ctx, b.client, []string{redisKey(b.prefix, key)}, encodeCommand(c)...).Result()
	if err != nil {
		return reply{}, classifyRedisError(err)
	}
	r, err := decodeReply(value)
	return r, classifyRedisError(err)
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

// classifyRedisFailure owns all Redis-specific interpretation. Exact RESP
// codes form an allowlist; unknown errors fail closed at the dependency level.
func classifyRedisFailure(err error) (errorClass, backendDiagnostic) {
	if errors.Is(err, context.Canceled) {
		return errorShutdown, diagnosticUnknown
	}
	if errors.Is(err, errMalformedReply) {
		return errorPermanent, diagnosticProtocol
	}
	if errors.Is(err, redis.ErrPoolTimeout) {
		return errorTransient, diagnosticPoolExhausted
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorTransient, diagnosticTimeout
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errorTransient, diagnosticNetwork
	}
	var wire redis.Error
	if errors.As(err, &wire) {
		code, _, _ := strings.Cut(wire.Error(), " ")
		switch code {
		case "LOADING":
			return errorTransient, diagnosticLoading
		case "TRYAGAIN":
			return errorTransient, diagnosticTryAgain
		case "CLUSTERDOWN":
			return errorTransient, diagnosticClusterDown
		case "MASTERDOWN":
			return errorTransient, diagnosticMasterDown
		case "BUSY":
			return errorTransient, diagnosticBusy
		case "READONLY":
			return errorTransient, diagnosticReadOnly
		case "OOM":
			return errorTransient, diagnosticOOM
		case "NOAUTH", "WRONGPASS":
			return errorPermanent, diagnosticAuthentication
		case "NOPERM":
			return errorPermanent, diagnosticPermission
		case "ERR", "NOSCRIPT":
			return errorPermanent, diagnosticScript
		}
		return errorPermanent, diagnosticUnknown
	}
	var network net.Error
	if errors.As(err, &network) {
		if network.Timeout() {
			return errorTransient, diagnosticTimeout
		}
		return errorTransient, diagnosticNetwork
	}
	return errorPermanent, diagnosticUnknown
}
func classifyBackendError(err error) errorClass {
	class, _ := classifyRedisFailure(err)
	return class
}
func classifyRedisError(err error) error {
	if err == nil {
		return nil
	}
	class, diagnostic := classifyRedisFailure(err)
	return &backendError{class: class, diagnostic: diagnostic, cause: err}
}
