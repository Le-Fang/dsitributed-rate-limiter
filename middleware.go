package ratelimiter

import (
	"net/http"
	"strconv"
	"time"
)

// KeyFunc extracts a bounded application key. Errors are returned as HTTP 400;
// their potentially sensitive text is never included in the response.
type KeyFunc func(*http.Request) (string, error)

// Middleware applies Allow before invoking the next handler. A nil KeyFunc
// panics at wrapper construction. Limit is effective local tokens/second;
// Remaining is a floored local estimate, never cached global quota.
func (l *Limiter) Middleware(keyFunc KeyFunc) func(http.Handler) http.Handler {
	if keyFunc == nil {
		panic("ratelimiter: nil KeyFunc")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, err := keyFunc(r)
			var reason string
			var info LimitInfo
			if err != nil {
				reason = ReasonInvalidRequest
				info = l.baseInfo()
			} else {
				_, reason, info = l.Allow(key)
			}
			w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(info.EffectiveRate, 'f', -1, 64))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(info.Remaining))
			w.Header().Del("Retry-After")
			if info.RetryAfterKnown && info.RetryAfter > 0 {
				seconds := info.RetryAfter / time.Second
				if info.RetryAfter%time.Second != 0 {
					seconds++
				}
				w.Header().Set("Retry-After", strconv.FormatInt(int64(seconds), 10))
			}
			switch reason {
			case ReasonAllowed, ReasonLocalFallback:
				next.ServeHTTP(w, r)
			case ReasonInvalidRequest:
				http.Error(w, reason, http.StatusBadRequest)
			case ReasonLocalLimit, ReasonGlobalLimit, ReasonFallbackLimit:
				http.Error(w, reason, http.StatusTooManyRequests)
			case ReasonSyncPending, ReasonRedisUnavailable, ReasonCapacityExhausted, ReasonAccountingExpired, ReasonPolicyMismatch, ReasonAccountingError, ReasonClosed:
				http.Error(w, reason, http.StatusServiceUnavailable)
			default:
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
			}
		})
	}
}
