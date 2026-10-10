package ratelimiter

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
)

type Labels struct{ Tier, Reason string }

// Metrics receives aggregate observations with bounded labels. Implementations
// must be thread-safe, fast and nonblocking. Calls run outside internal locks;
// panics are isolated, but a blocking callback still blocks its calling goroutine.
type Metrics interface {
	AddCounter(name string, labels Labels, delta uint64)
	SetGauge(name string, value float64)
	Observe(name string, seconds float64)
}

const (
	MetricAllowed                = "rate_limit_allowed_total"
	MetricRejected               = "rate_limit_rejected_total"
	MetricTerminalTransitions    = "rate_limit_terminal_transitions_total"
	MetricRedisErrors            = "rate_limit_redis_errors_total"
	MetricSyncDuration           = "rate_limit_sync_duration_seconds"
	MetricCircuitState           = "rate_limit_circuit_state"
	MetricCircuitTransitions     = "rate_limit_circuit_transitions_total"
	MetricOutstanding            = "rate_limit_normal_outstanding_tokens"
	MetricOldestPending          = "rate_limit_normal_oldest_pending_seconds"
	MetricFallbackKeys           = "rate_limit_fallback_keys"
	MetricFallbackSeconds        = "rate_limit_fallback_key_seconds_total"
	MetricFallbackTransitions    = "rate_limit_fallback_transitions_total"
	MetricFallbackAllowed        = "rate_limit_fallback_allowed_total"
	MetricFallbackRejected       = "rate_limit_fallback_rejected_total"
	MetricQueueDepth             = "rate_limit_queue_depth"
	MetricQueueDelay             = "rate_limit_queue_delay_seconds"
	MetricNotificationsCoalesced = "rate_limit_notifications_coalesced_total"
	MetricNotificationsDropped   = "rate_limit_notifications_dropped_total"
	MetricFreshnessExpiries      = "rate_limit_freshness_expiries_total"
	MetricWorkers                = "rate_limit_worker_occupancy"
	MetricLogDropped             = "rate_limit_log_events_dropped_total"
	MetricLogCoalesced           = "rate_limit_log_events_coalesced_total"
)

var durationBuckets = [...]float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5}

type metricKey struct {
	name   string
	labels Labels
}
type metricCounter struct {
	key   metricKey
	value atomic.Uint64
}
type metricGauge struct {
	name string
	bits atomic.Uint64
}
type metricHistogram struct {
	name string
	bins [11]atomic.Uint64
	sum  atomic.Uint64
}

// PrometheusMetrics is a fixed-cardinality, dependency-free text collector.
// Unknown names/label combinations and nonfinite observations are ignored.
// Its zero value is inert; construct it with NewPrometheusMetrics.
type PrometheusMetrics struct {
	counters   []*metricCounter
	lookup     map[metricKey]*metricCounter // immutable after construction
	gauges     []*metricGauge
	histograms []*metricHistogram
}

func NewPrometheusMetrics() *PrometheusMetrics {
	m := &PrometheusMetrics{lookup: make(map[metricKey]*metricCounter)}
	add := func(name string, labels Labels) {
		c := &metricCounter{key: metricKey{name, labels}}
		m.counters = append(m.counters, c)
		m.lookup[c.key] = c
	}
	for _, r := range []string{ReasonAllowed, ReasonLocalFallback} {
		add(MetricAllowed, Labels{TierLocal, r})
	}
	for _, r := range []string{ReasonLocalLimit, ReasonSyncPending, ReasonRedisUnavailable, ReasonInvalidRequest, ReasonCapacityExhausted, ReasonAccountingExpired, ReasonPolicyMismatch, ReasonAccountingError, ReasonClosed, ReasonFallbackLimit} {
		add(MetricRejected, Labels{TierLocal, r})
	}
	add(MetricRejected, Labels{TierGlobal, ReasonGlobalLimit})
	// Parent cancellation can supersede a global decision at linearization.
	add(MetricRejected, Labels{TierGlobal, ReasonClosed})
	for _, reason := range []string{ReasonAccountingExpired, ReasonPolicyMismatch, ReasonAccountingError} {
		add(MetricTerminalTransitions, Labels{Tier: TierLocal, Reason: reason})
	}
	for _, name := range []string{MetricRedisErrors, MetricCircuitTransitions, MetricFallbackTransitions, MetricFallbackAllowed, MetricFallbackRejected, MetricNotificationsCoalesced, MetricNotificationsDropped, MetricFreshnessExpiries, MetricLogDropped, MetricLogCoalesced} {
		add(name, Labels{})
	}
	for _, name := range []string{MetricCircuitState, MetricOutstanding, MetricOldestPending, MetricFallbackKeys, MetricFallbackSeconds, MetricQueueDepth, MetricWorkers} {
		m.gauges = append(m.gauges, &metricGauge{name: name})
	}
	for _, name := range []string{MetricSyncDuration, MetricQueueDelay} {
		m.histograms = append(m.histograms, &metricHistogram{name: name})
	}
	return m
}
func (m *PrometheusMetrics) AddCounter(name string, labels Labels, delta uint64) {
	if c := m.lookup[metricKey{name, labels}]; c != nil {
		c.value.Add(delta)
	}
}
func (m *PrometheusMetrics) SetGauge(name string, value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return
	}
	for _, g := range m.gauges {
		if g.name == name {
			if name == MetricFallbackSeconds {
				for old := g.bits.Load(); math.Float64frombits(old) < value; old = g.bits.Load() {
					if g.bits.CompareAndSwap(old, math.Float64bits(value)) {
						break
					}
				}
			} else {
				g.bits.Store(math.Float64bits(value))
			}
			return
		}
	}
}
func (m *PrometheusMetrics) Observe(name string, seconds float64) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 {
		return
	}
	for _, h := range m.histograms {
		if h.name != name {
			continue
		}
		idx := len(durationBuckets)
		for i, v := range durationBuckets {
			if seconds <= v {
				idx = i
				break
			}
		}
		h.bins[idx].Add(1)
		for old := h.sum.Load(); !h.sum.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+seconds)); old = h.sum.Load() {
		}
		return
	}
}
func (m *PrometheusMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	last := ""
	for _, c := range m.counters {
		if c.key.name != last {
			fmt.Fprintf(w, "# TYPE %s counter\n", c.key.name)
			last = c.key.name
		}
		if c.key.labels == (Labels{}) {
			fmt.Fprintf(w, "%s %d\n", c.key.name, c.value.Load())
		} else {
			fmt.Fprintf(w, "%s{tier=%q,reason=%q} %d\n", c.key.name, c.key.labels.Tier, c.key.labels.Reason, c.value.Load())
		}
	}
	for _, g := range m.gauges {
		typ := "gauge"
		if g.name == MetricFallbackSeconds {
			typ = "counter"
		}
		fmt.Fprintf(w, "# TYPE %s %s\n%s %s\n", g.name, typ, g.name, strconv.FormatFloat(math.Float64frombits(g.bits.Load()), 'g', -1, 64))
	}
	for _, h := range m.histograms {
		fmt.Fprintf(w, "# TYPE %s histogram\n", h.name)
		var count uint64
		for i := range h.bins {
			count += h.bins[i].Load()
			bound := "+Inf"
			if i < len(durationBuckets) {
				bound = strconv.FormatFloat(durationBuckets[i], 'g', -1, 64)
			}
			fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", h.name, bound, count)
		}
		fmt.Fprintf(w, "%s_sum %s\n%s_count %d\n", h.name, strconv.FormatFloat(math.Float64frombits(h.sum.Load()), 'g', -1, 64), h.name, count)
	}
}

// MetricsHandler exposes the default collector. Config.Metrics adapters own
// their exposition; for any custom adapter this method returns a 404 handler.
func (l *Limiter) MetricsHandler() http.Handler {
	if !l.exposeMetrics {
		return http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.sampleMetrics()
		l.metrics.(*PrometheusMetrics).ServeHTTP(w, r)
	})
}
