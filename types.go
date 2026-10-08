package ratelimiter

import "time"

const (
	ReasonAllowed           = "allowed"
	ReasonLocalLimit        = "local_limit"
	ReasonGlobalLimit       = "global_limit"
	ReasonSyncPending       = "sync_pending"
	ReasonRedisUnavailable  = "redis_unavailable"
	ReasonInvalidRequest    = "invalid_request"
	ReasonCapacityExhausted = "capacity_exhausted"
	ReasonAccountingExpired = "accounting_expired"
	ReasonPolicyMismatch    = "policy_mismatch"
	ReasonAccountingError   = "accounting_error"
	ReasonClosed            = "closed"
	ReasonLocalFallback     = "local_fallback"
	ReasonFallbackLimit     = "fallback_limit"
	TierLocal               = "local"
	TierGlobal              = "global"
	ModeNormal              = "normal"
	ModeLocalFallback       = "local_fallback"
	ModeBlocked             = "blocked"
)

type LimitInfo struct {
	GlobalRate      float64
	GlobalBurst     int
	EffectiveRate   float64
	EffectiveBurst  int
	Remaining       int
	RetryAfter      time.Duration
	RetryAfterKnown bool
	Tier            string
	Mode            string
}
