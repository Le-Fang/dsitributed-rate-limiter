package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMiddlewareMatrix(t *testing.T) {
	for _, row := range []struct {
		name, reason            string
		status                  int
		limit, remaining, retry string
	}{
		{"allowed", ReasonAllowed, 204, "10", "4", ""},
		{"local", ReasonLocalLimit, 429, "10", "0", "1"},
		{"global", ReasonGlobalLimit, 429, "10", "5", "1"},
		{"cold", ReasonSyncPending, 503, "10", "5", ""},
		{"pending", ReasonSyncPending, 503, "10", "0", ""},
		{"outage", ReasonRedisUnavailable, 503, "10", "5", ""},
		{"capacity", ReasonCapacityExhausted, 503, "10", "0", ""},
		{"expired", ReasonAccountingExpired, 503, "10", "5", ""},
		{"policy", ReasonPolicyMismatch, 503, "10", "5", ""},
		{"accounting", ReasonAccountingError, 503, "10", "5", ""},
		{"closed", ReasonClosed, 503, "10", "0", ""},
		{"fallback", ReasonLocalFallback, 204, "2", "2", ""},
		{"fallback_limit", ReasonFallbackLimit, 429, "2", "0", "1"},
		{"empty", ReasonInvalidRequest, 400, "10", "0", ""},
		{"oversized", ReasonInvalidRequest, 400, "10", "0", ""},
		{"extractor", ReasonInvalidRequest, 400, "10", "0", ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			cfg := fallbackConfig(FailClosed)
			cfg.MaxKeys = 1
			m := NewPrometheusMetrics()
			if strings.HasPrefix(row.name, "fallback") {
				cfg = fallbackConfig(LocalOnly)
			}
			if row.name == "local" {
				cfg.MaxOutstanding = 10
			}
			cfg.Metrics = m
			l, c, _ := newTestLimiter(t, cfg)
			key := "k"
			if row.name != "capacity" && row.name != "empty" && row.name != "oversized" && row.name != "extractor" && row.name != "closed" {
				b := testBucket(t, l, key)
				if row.name != "cold" {
					initializeTestBucket(t, l, b, 20e6)
				}
				switch row.name {
				case "local", "pending":
					l.AllowN(key, 5)
				case "global":
					a, _ := l.prepareAttempt(b, c.Now())
					l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 0, RetryMS: 125, SessionTTLMS: 86400000})
				case "outage":
					l.recordFailure(transientFailure())
				case "expired", "policy", "accounting":
					outcome := outcomeSessionExpired
					if row.name == "policy" {
						outcome = outcomePolicyMismatch
					}
					if row.name == "accounting" {
						outcome = outcomeSequenceError
					}
					if row.name == "expired" {
						l.Allow(key)
					}
					a, _ := l.prepareAttempt(b, c.Now())
					l.installReply(b, a, reply{Outcome: outcome})
					if row.name == "expired" {
						c.Advance(100 * time.Millisecond)
					}
				case "fallback", "fallback_limit":
					l.recordFailure(transientFailure())
					if row.name == "fallback_limit" {
						l.AllowN(key, 3)
					}
				}
			}
			switch row.name {
			case "capacity":
				l.Allow("other")
			case "closed":
				if err := l.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "empty":
				key = ""
			case "oversized":
				key = strings.Repeat("x", 1025)
			}
			calls := 0
			h := l.Middleware(func(*http.Request) (string, error) {
				if row.name == "extractor" {
					return "", errors.New("secret")
				}
				return key, nil
			})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) }))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
			if rr.Code != row.status || (calls == 1) != (row.status == 204) {
				t.Fatalf("status=%d calls=%d body=%s", rr.Code, calls, rr.Body.String())
			}
			if row.name != "extractor" {
				tier := TierLocal
				if row.reason == ReasonGlobalLimit {
					tier = TierGlobal
				}
				name := MetricRejected
				if row.status == 204 {
					name = MetricAllowed
				}
				label := fmt.Sprintf("%s{tier=%q,reason=%q}", name, tier, row.reason)
				if got := metricValue(t, m, label); got != 1 {
					t.Errorf("decision %s counted %v times", row.reason, got)
				}
			}
			for name, want := range map[string]string{"X-RateLimit-Limit": row.limit, "X-RateLimit-Remaining": row.remaining, "Retry-After": row.retry} {
				if got := rr.Header().Get(name); got != want {
					t.Errorf("%s=%q want %q", name, got, want)
				}
			}
		})
	}
}
func TestMiddlewareNilExtractorPanicsAtConstruction(t *testing.T) {
	l, _, _ := newTestLimiter(t, fallbackConfig(FailClosed))
	defer func() {
		if recover() == nil {
			t.Fatal("nil extractor accepted")
		}
	}()
	l.Middleware(nil)
}
