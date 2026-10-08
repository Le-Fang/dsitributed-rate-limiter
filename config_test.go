package ratelimiter

import (
	"context"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func validConfig() Config {
	return Config{GlobalRate: 100, GlobalBurst: 100, Redis: RedisConfig{Address: "127.0.0.1:6379"}}
}

func TestConfigDefaults(t *testing.T) {
	c, err := normalizeConfig(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	if c.LocalRate != 100 || c.LocalBurst != 32 || c.MaxOutstanding != 32 ||
		c.SyncThreshold != 16 || c.SyncInterval != 100*time.Millisecond ||
		c.MaxSyncAge != time.Second || c.Redis.Timeout != 200*time.Millisecond ||
		c.MaxKeys != 10000 || c.IdleTTL != 5*time.Minute || c.FailurePolicy != FailClosed ||
		c.Redis.Prefix != "rate_limiter:v1" || c.Parent == nil || c.Logger != slog.Default() || c.globalRateU != 100000000 {
		t.Fatalf("incorrect defaults: %+v", c)
	}
}

func TestConfigSmallBurstDefaults(t *testing.T) {
	c := validConfig()
	c.GlobalBurst = 7
	n, err := normalizeConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if n.LocalBurst != 7 || n.MaxOutstanding != 7 || n.SyncThreshold != 7 {
		t.Fatalf("defaults: %+v", n)
	}
}

func TestConfigRateRounding(t *testing.T) {
	c := validConfig()
	c.GlobalRate = 1.0000005
	c.LocalRate = 0.0000015
	n, err := normalizeConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if n.globalRateU != 1000001 || n.GlobalRate != 1.000001 || n.LocalRate != 0.000002 {
		t.Fatalf("effective rates: %+v", n)
	}
	core := n.core()
	if core.globalRateU != 1000001 || core.localRateU != 2 {
		t.Fatalf("core rates: %+v", core)
	}
}

func TestConfigLocalOnly(t *testing.T) {
	c := validConfig()
	c.FailurePolicy = LocalOnly
	c.FallbackRate = 0.0000015
	c.FallbackBurst = 10
	n, err := normalizeConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if n.FallbackRate != 0.000002 || n.core().fallbackRateU != 2 {
		t.Fatalf("fallback rounding: %+v", n)
	}
}

func TestConfigRedisClient(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()
	c := validConfig()
	c.Redis.Address = ""
	c.Redis.Client = client
	if _, err := normalizeConfig(c); err != nil {
		t.Fatal(err)
	}
}

func TestConfigCoreExcludesAdapters(t *testing.T) {
	c := validConfig()
	c.Logger = slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	c.Parent = context.WithValue(context.Background(), struct{}{}, "value")
	n, err := normalizeConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	core := n.core()
	if core.globalBurst != 100 || core.localBurst != 32 || core.maxOutstanding != 32 || core.failurePolicy != FailClosed {
		t.Fatalf("core values: %+v", core)
	}
	typ := reflect.TypeOf(core)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Type == reflect.TypeOf(RedisConfig{}) || field.Type == reflect.TypeOf(Config{}) || field.Type == reflect.TypeOf((*slog.Logger)(nil)) || field.Type == reflect.TypeOf((*redis.UniversalClient)(nil)).Elem() || field.Type == reflect.TypeOf((*context.Context)(nil)).Elem() {
			t.Fatalf("adapter or lifecycle field in core: %s", field.Name)
		}
	}
}

func TestConfigRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"missing global rate", func(c *Config) { c.GlobalRate = 0 }},
		{"negative global rate", func(c *Config) { c.GlobalRate = -1 }},
		{"nan global rate", func(c *Config) { c.GlobalRate = math.NaN() }},
		{"infinite global rate", func(c *Config) { c.GlobalRate = math.Inf(1) }},
		{"rounded zero global rate", func(c *Config) { c.GlobalRate = 0.0000004 }},
		{"global rate too high", func(c *Config) { c.GlobalRate = 1000000.1 }},
		{"missing global burst", func(c *Config) { c.GlobalBurst = 0 }},
		{"negative global burst", func(c *Config) { c.GlobalBurst = -1 }},
		{"global burst too high", func(c *Config) { c.GlobalBurst = 1000001 }},
		{"negative local rate", func(c *Config) { c.LocalRate = -1 }},
		{"nan local rate", func(c *Config) { c.LocalRate = math.NaN() }},
		{"infinite local rate", func(c *Config) { c.LocalRate = math.Inf(1) }},
		{"rounded zero local rate", func(c *Config) { c.LocalRate = 0.0000004 }},
		{"local rate too high", func(c *Config) { c.LocalRate = 1000000.1 }},
		{"negative local burst", func(c *Config) { c.LocalBurst = -1 }},
		{"local burst above global", func(c *Config) { c.LocalBurst = 101 }},
		{"local burst above cap", func(c *Config) { c.LocalBurst = 33 }},
		{"negative cap", func(c *Config) { c.MaxOutstanding = -1 }},
		{"cap above limit", func(c *Config) { c.MaxOutstanding = 1000001 }},
		{"negative threshold", func(c *Config) { c.SyncThreshold = -1 }},
		{"threshold above cap", func(c *Config) { c.SyncThreshold = 33 }},
		{"negative interval", func(c *Config) { c.SyncInterval = -time.Nanosecond }},
		{"interval at freshness", func(c *Config) { c.SyncInterval = time.Second }},
		{"negative freshness", func(c *Config) { c.MaxSyncAge = -time.Nanosecond }},
		{"freshness too high", func(c *Config) { c.MaxSyncAge = 30*time.Second + time.Nanosecond }},
		{"negative timeout", func(c *Config) { c.Redis.Timeout = -time.Nanosecond }},
		{"timeout at freshness", func(c *Config) { c.Redis.Timeout = time.Second }},
		{"negative max keys", func(c *Config) { c.MaxKeys = -1 }},
		{"negative ttl", func(c *Config) { c.IdleTTL = -time.Nanosecond }},
		{"prefix opening brace", func(c *Config) { c.Redis.Prefix = "bad{" }},
		{"prefix closing brace", func(c *Config) { c.Redis.Prefix = "bad}" }},
		{"missing redis connection", func(c *Config) { c.Redis.Address = "" }},
		{"both redis connections", func(c *Config) { c.Redis.Client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"}) }},
		{"unknown failure policy", func(c *Config) { c.FailurePolicy = FailurePolicy(2) }},
		{"failclosed fallback rate", func(c *Config) { c.FallbackRate = 1 }},
		{"failclosed fallback burst", func(c *Config) { c.FallbackBurst = 1 }},
		{"negative fallback rate", func(c *Config) { c.FallbackRate = -1 }},
		{"nan fallback rate", func(c *Config) { c.FallbackRate = math.NaN() }},
		{"infinite fallback rate", func(c *Config) { c.FallbackRate = math.Inf(1) }},
		{"negative fallback burst", func(c *Config) { c.FallbackBurst = -1 }},
		{"localonly missing rate", func(c *Config) { c.FailurePolicy = LocalOnly; c.FallbackBurst = 1 }},
		{"localonly missing burst", func(c *Config) { c.FailurePolicy = LocalOnly; c.FallbackRate = 1 }},
		{"localonly rounded zero", func(c *Config) { c.FailurePolicy = LocalOnly; c.FallbackRate = 0.0000004; c.FallbackBurst = 1 }},
		{"localonly high rate", func(c *Config) { c.FailurePolicy = LocalOnly; c.FallbackRate = 101; c.FallbackBurst = 1 }},
		{"localonly high burst", func(c *Config) { c.FailurePolicy = LocalOnly; c.FallbackRate = 1; c.FallbackBurst = 33 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.change(&c)
			if _, err := normalizeConfig(c); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
