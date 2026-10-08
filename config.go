package ratelimiter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// FailurePolicy controls admission when shared accounting is unavailable.
type FailurePolicy uint8

const (
	FailClosed FailurePolicy = iota
	LocalOnly
)

type RedisConfig struct {
	Address string
	Prefix  string
	Timeout time.Duration
	Client  redis.UniversalClient
}

// Config contains public construction settings. Rates are tokens per second.
type Config struct {
	GlobalRate     float64
	GlobalBurst    int
	LocalRate      float64
	LocalBurst     int
	SyncInterval   time.Duration
	SyncThreshold  int
	MaxOutstanding int
	MaxSyncAge     time.Duration
	MaxKeys        int
	IdleTTL        time.Duration
	Redis          RedisConfig
	Parent         context.Context
	FailurePolicy  FailurePolicy
	FallbackRate   float64
	FallbackBurst  int
	Logger         *slog.Logger
	Metrics        Metrics
}

type normalizedConfig struct {
	Config
	globalRateU int64
}

// coreConfig holds immutable policy and scheduling values without adapter state.
type coreConfig struct {
	globalRateU, localRateU, fallbackRateU int64
	globalBurst, localBurst, fallbackBurst int
	syncInterval                           time.Duration
	syncThreshold, maxOutstanding          int
	maxSyncAge                             time.Duration
	maxKeys                                int
	idleTTL                                time.Duration
	failurePolicy                          FailurePolicy
}

func (c normalizedConfig) core() coreConfig {
	return coreConfig{
		globalRateU:   c.globalRateU,
		localRateU:    int64(math.Round(c.LocalRate * 1e6)),
		fallbackRateU: int64(math.Round(c.FallbackRate * 1e6)),
		globalBurst:   c.GlobalBurst, localBurst: c.LocalBurst, fallbackBurst: c.FallbackBurst,
		syncInterval: c.SyncInterval, syncThreshold: c.SyncThreshold,
		maxOutstanding: c.MaxOutstanding, maxSyncAge: c.MaxSyncAge,
		maxKeys: c.MaxKeys, idleTTL: c.IdleTTL, failurePolicy: c.FailurePolicy,
	}
}

func normalizeConfig(c Config) (normalizedConfig, error) {
	bad := func(field string) (normalizedConfig, error) {
		return normalizedConfig{}, fmt.Errorf("invalid %s", field)
	}
	if c.FailurePolicy != FailClosed && c.FailurePolicy != LocalOnly {
		return bad("FailurePolicy")
	}
	for _, rate := range []struct {
		name  string
		value float64
	}{{"GlobalRate", c.GlobalRate}, {"LocalRate", c.LocalRate}, {"FallbackRate", c.FallbackRate}} {
		if math.IsNaN(rate.value) || math.IsInf(rate.value, 0) || rate.value < 0 || rate.value > 1e6 {
			return bad(rate.name)
		}
	}
	if c.GlobalRate == 0 {
		return bad("GlobalRate")
	}
	for _, count := range []struct {
		name  string
		value int
	}{{"GlobalBurst", c.GlobalBurst}, {"LocalBurst", c.LocalBurst}, {"SyncThreshold", c.SyncThreshold}, {"MaxOutstanding", c.MaxOutstanding}, {"MaxKeys", c.MaxKeys}, {"FallbackBurst", c.FallbackBurst}} {
		if count.value < 0 {
			return bad(count.name)
		}
	}
	if c.GlobalBurst == 0 || c.GlobalBurst > 1e6 {
		return bad("GlobalBurst")
	}
	for _, d := range []struct {
		name  string
		value time.Duration
	}{{"SyncInterval", c.SyncInterval}, {"MaxSyncAge", c.MaxSyncAge}, {"IdleTTL", c.IdleTTL}, {"Redis.Timeout", c.Redis.Timeout}} {
		if d.value < 0 {
			return bad(d.name)
		}
	}
	if c.LocalRate == 0 {
		c.LocalRate = c.GlobalRate
	}
	if c.MaxOutstanding == 0 {
		c.MaxOutstanding = min(c.GlobalBurst, 32)
	}
	if c.LocalBurst == 0 {
		c.LocalBurst = min(c.GlobalBurst, c.MaxOutstanding)
	}
	if c.SyncThreshold == 0 {
		c.SyncThreshold = min(16, c.MaxOutstanding)
	}
	if c.SyncInterval == 0 {
		c.SyncInterval = 100 * time.Millisecond
	}
	if c.MaxSyncAge == 0 {
		c.MaxSyncAge = time.Second
	}
	if c.Redis.Timeout == 0 {
		c.Redis.Timeout = 200 * time.Millisecond
	}
	if c.MaxKeys == 0 {
		c.MaxKeys = 10000
	}
	if c.IdleTTL == 0 {
		c.IdleTTL = 5 * time.Minute
	}
	if c.Redis.Prefix == "" {
		c.Redis.Prefix = "rate_limiter:v1"
	}
	if c.Parent == nil {
		c.Parent = context.Background()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}

	globalU, ok := normalizedRate(c.GlobalRate)
	if !ok {
		return bad("GlobalRate")
	}
	localU, ok := normalizedRate(c.LocalRate)
	if !ok {
		return bad("LocalRate")
	}
	c.GlobalRate, c.LocalRate = float64(globalU)/1e6, float64(localU)/1e6
	if c.MaxOutstanding <= 0 || c.MaxOutstanding > 1e6 {
		return bad("MaxOutstanding")
	}
	if c.LocalBurst <= 0 || c.LocalBurst > c.GlobalBurst || c.LocalBurst > c.MaxOutstanding {
		return bad("LocalBurst")
	}
	if c.SyncThreshold <= 0 || c.SyncThreshold > c.MaxOutstanding {
		return bad("SyncThreshold")
	}
	if c.MaxSyncAge <= 0 || c.MaxSyncAge > 30*time.Second {
		return bad("MaxSyncAge")
	}
	if c.SyncInterval <= 0 || c.SyncInterval >= c.MaxSyncAge {
		return bad("SyncInterval")
	}
	if c.Redis.Timeout <= 0 || c.Redis.Timeout >= c.MaxSyncAge {
		return bad("Redis.Timeout")
	}
	if c.MaxKeys <= 0 {
		return bad("MaxKeys")
	}
	if c.IdleTTL <= 0 {
		return bad("IdleTTL")
	}
	if strings.ContainsAny(c.Redis.Prefix, "{}") || uint64(len(c.Redis.Prefix)) > math.MaxUint32 {
		return bad("Redis.Prefix")
	}
	if (c.Redis.Address == "") == (c.Redis.Client == nil) {
		return bad("Redis connection")
	}
	if c.FailurePolicy == FailClosed {
		if c.FallbackRate != 0 || c.FallbackBurst != 0 {
			return bad("fallback settings")
		}
	} else {
		fallbackU, valid := normalizedRate(c.FallbackRate)
		if !valid || fallbackU > localU || c.FallbackBurst <= 0 || c.FallbackBurst > c.LocalBurst {
			return bad("fallback settings")
		}
		c.FallbackRate = float64(fallbackU) / 1e6
	}
	return normalizedConfig{Config: c, globalRateU: globalU}, nil
}

func normalizedRate(rate float64) (int64, bool) {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 || rate > 1e6 {
		return 0, false
	}
	u := int64(math.Round(rate * 1e6))
	return u, u > 0
}
