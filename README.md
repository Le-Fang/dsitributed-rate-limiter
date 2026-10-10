# Distributed rate limiter

An embedded Go library for local admission with asynchronous Redis accounting.
The source compatibility floor is Go 1.21; use a currently supported Go toolchain
in production. Implementation and release validation are still in progress.

## Shutdown

Call `limiter.Close(ctx)` to stop admission and finish normal accounting. Canceling
`Config.Parent` starts the same cleanup automatically. Every admission after
cancellation returns `ReasonClosed`.

One shared cleanup stops scheduling, cancels and joins the workers, then flushes
normal consumption serially. It has an independent five-second deadline starting
before the join. A canceled parent does not cancel the final flush. A caller's
`ctx` limits only that caller's wait; it does not stop cleanup. Concurrent callers
share the result, and later calls return the stored result.

Unacknowledged batches keep their session, slot, sequence and token count on
retries. New normal consumption accumulated behind an earlier batch is flushed
after that batch is acknowledged. Local fallback traffic is never replayed into
Redis. Shutdown does not initialize cold keys or refresh clean keys.

A `*ShutdownError` reports `UnflushedKeys` and `UnflushedTokens`. These counts
include ambiguous batches that Redis might already have applied. The error's
text contains aggregate counts only. `errors.Is` and `errors.As` can inspect the
underlying cause through `Unwrap`; that cause may contain sensitive dependency
information and should not be logged without redaction. The library does not
persist an accounting journal for replay after process exit.

Owned Redis clients close once after cleanup. Injected clients remain caller
owned and must honor operation contexts, cancellation, and bounded pool, dial,
read, and write timeouts. Clients or hooks that ignore these limits are unsupported:
worker join can exceed the shutdown budget. Cleanup never overlaps final flushing
or client closure with a still-running local worker.

## HTTP middleware

`limiter.Middleware(keyFunc)(handler)` calls the handler only for admitted
requests. `keyFunc` has type `func(*http.Request) (string, error)`; a nil function
panics when constructing the wrapper. Extractor errors and empty or over-1,024-byte
keys return 400. Local/global quota and fallback exhaustion return 429. Cold or
pending synchronization, unavailable accounting under FailClosed, capacity limits,
terminal accounting errors, and a closed limiter return 503. Successful LocalOnly
fallback requests proceed normally.

`X-RateLimit-Limit` is the effective local rate in tokens per second, including
fractional rates. `X-RateLimit-Remaining` is a floored local token estimate, not
shared Redis quota. Fallback headers reflect `FallbackRate` and `FallbackBurst`.
`Retry-After` appears only for a known positive delay, rounded up to whole seconds.
Errors returned by the extractor are not exposed in response bodies.

## Metrics and logs

With `Config.Metrics == nil`, each limiter owns a fixed-cardinality Prometheus text
collector exposed by `limiter.MetricsHandler()`. Register that handler on your
HTTP server. No Prometheus library is required. `NewPrometheusMetrics()` also
creates a standalone collector. With any custom `Config.Metrics`, including an
explicitly supplied Prometheus collector, `MetricsHandler()` returns 404; the
custom adapter owns exposition.

Metric names are exported as `Metric*` constants. Families include decisions by
fixed tier/reason, Redis errors, synchronization duration, circuit state and
transitions, normal outstanding tokens and oldest receipt age, fallback keys and
cumulative key-seconds, fallback transitions/decisions, notification queue depth,
delay/coalescing/drops, freshness expiries, worker occupancy, and dropped/coalesced
log events. Circuit state is 0 healthy, 1 open, or 2 probing. Transitions count each
actual change once. Histograms use second buckets 0.0001, 0.0005, 0.001, 0.005,
0.01, 0.05, 0.1, 0.5, 1, 5, and +Inf. Unknown collector names/labels and invalid
numeric values are ignored; application keys never become labels.

Gauges refresh in the background at most once per second and on default-handler
scrapes. A scrape walks the bounded retained-key store; `Allow` never scans it.
Fallback key-seconds include lazy outage and freshness transitions from their
observed clock boundaries, continue through reconciliation, survive eviction, and
stop at closure or terminal failure. Sampling does not change admission policy.
Gauges are concurrent local estimates, not an atomic global accounting snapshot.
The cumulative duration does not decrease. Uninitialized keys which have not
actually entered fallback count only circuit-outage intervals before MaxSyncAge;
recovery ends those intervals even if no scrape occurred. At MaxSyncAge their
exposure becomes continuous. An actual fallback admission latches fallback until
reconciliation, as usual. Cumulative outage totals and one preceding interval,
plus fixed per-bucket deadline snapshots, preserve these boundaries without an
unbounded transition history. Availability changes perform a bounded O(retained
keys) deadline walk outside the circuit mutex. New-key creation can briefly wait
on this in-memory observation gate; existing-key lookup does not take that gate.
No request performs a store-wide scan or backend operation, or waits for Redis
synchronization.

Sync duration covers a worker's attempt
(including cold-key TIME) or recovery probe; queue delay covers queued key work.

Custom Metrics methods must be thread-safe, fast, and nonblocking. They run outside
internal locks with panic isolation. Arbitrarily blocking callbacks still block
their calling goroutine and are unsupported; there is no asynchronous metrics
buffer that silently loses decision counts.

Structured slog delivery uses one background consumer and a 1,024-event queue.
Dependency events identify a finite diagnostic cause (timeout, pool exhaustion,
network, OOM, authentication, permission, protocol, script, loading, retry, cluster
or master availability, busy, read-only, or unknown) and a `permanent` flag. They
never contain keys, credentials, addresses, or arbitrary dependency error text.
Repeats are coalesced per cause and permanence over one second; pressure and handler errors
or panics increment the drop counter. `Config.Logger` defaults to `slog.Default()`.
Caller handlers must be nonblocking and thread-safe. Shutdown cancels log delivery
without waiting for a blocked handler; queued logs may be dropped. A handler that
never returns can retain the single consumer goroutine until process exit, but
cannot delay admission or shutdown. Caller-added logger attributes remain the
caller's responsibility.
