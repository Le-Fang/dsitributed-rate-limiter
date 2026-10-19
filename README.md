# Distributed rate limiter

An embedded Go library for local admission with asynchronous Redis accounting.
The source compatibility floor is Go 1.21; use a currently supported Go toolchain
in production (Go 1.27.3 is the current CI toolchain). See
[BENCHMARKS.md](BENCHMARKS.md) for measured workloads and verification limits.

## Usage and admission

```go
import ratelimit "github.com/Le-Fang/dsitributed-rate-limiter"

l, err := ratelimit.NewLimiter(ratelimit.Config{
    GlobalRate: 1000, GlobalBurst: 100,
    MaxOutstanding: 8, LocalBurst: 8,
    Redis: ratelimit.RedisConfig{Address: "127.0.0.1:6379"},
})
```

Handle the constructor error and always arrange `Close` on shutdown. Construction
validates settings without contacting Redis. Complete, externally compiled
examples in [example_test.go](example_test.go) define authenticated extraction,
HTTP handling, a shutdown context, LocalOnly, TLS/Cluster/Sentinel injection,
and metrics. Examples have no `Output` directive and need no live Redis at test time.

`Allow(key)` consumes one token; `AllowN(key, n)` atomically consumes all `n` or
none. Keys must be nonempty and at most 1,024 bytes. Nonpositive costs, costs above
LocalBurst or MaxOutstanding, and invalid keys return `invalid_request`.
Every newly created key starts with `sync_pending` in a healthy circuit. A worker
performs a keyed TIME Lua call then INIT before normal admission can begin.
Both operations route by the same derived Redis key, selecting the same Cluster
owner. If a clock or topology change moves the immutable registration deadline
outside the valid 30-second window, INIT returns `INIT_EXPIRED`; only clean
registration restarts with a new identity and time sample. Ambiguous retries keep
the original identity/deadline, and unresolved APPLY consumption never
reinitializes. This library has no public warm-up API. Retry through your application's bounded request policy;
`RetryAfterKnown == false` does not promise when the dependency will recover.

Normal admission consumes a local token and records normal consumption for later
Redis deduction. An observed positive Redis balance is **not a reservation**.
Normal admission requires fresh authorization from the current circuit generation,
a permitting global result, local tokens, and room under MaxOutstanding. The cap
includes both pending consumption and the immutable in-flight batch. Reaching a
fresh cap returns `sync_pending`; it does not activate fallback. Allow performs
no Redis calls, logging, or synchronization waits, though short mutex contention
and synchronous custom metrics callbacks are possible.

Constructor policy applies independently to every key in that limiter. It is not
an aggregate limit across all keys, nor a dynamic per-request policy selector.
Use separate limiters and deliberate namespaces for distinct policy classes.
Every writer to the same Redis key must use compatible protocol/global policy.
Local limits are per instance: uneven routing can leave global capacity unused,
and one busy instance cannot borrow another instance's local burst.

## Failure policies and guarantees

FailClosed is the zero-value policy. Transient Redis errors or stale authorization
return `redis_unavailable`. LocalOnly is an explicit availability choice:

```go
FailurePolicy: ratelimit.LocalOnly,
LocalRate: 100, LocalBurst: 8, MaxOutstanding: 8,
FallbackRate: 10, FallbackBurst: 4,
```

Fallback values must be explicitly positive and no greater than their normal
local values; FailClosed requires both to remain zero. A cold key may enter
fallback immediately during a known transient outage, or after MaxSyncAge while
initialization stalls. Initialized keys may enter on stale authorization even
when Redis itself is healthy but the scheduler is overloaded. Fallback success
returns `local_fallback` and `ModeLocalFallback`; exhaustion returns
`fallback_limit`. A valid normal request larger than FallbackBurst has no finite
refill-based retry delay while fallback persists.

Modes share one local token balance. Entry clamps to fallback burst; recovery
preserves the balance and never grants a new burst. Fallback consumption never
enters Redis debt, batch identities, or the normal outstanding cap. Previously
admitted normal consumption remains unresolved until acknowledged. A full old
cap does not prevent otherwise eligible fallback. Known `global_limit`, memory
or session capacity, permanent errors, terminal key errors, and closure override
fallback. A fresh negative global result does not activate fallback or reduce
the normal local burst; its freshness and global rejection are tracked independently.
A negative global result stays latched until a new successful current
check clears it; a retry timer alone cannot reopen admission.

For effective global rate R, burst B, fixed participating instances N, and
MaxOutstanding C, normal tokens admitted over physical duration T satisfy:

```
normal <= B + R*(T + 0.001 seconds) + 2*N*C + 0.000001
```

In logical Redis millisecond time the bound is `B+R*T+2*N*C+0.000001`.
This is a token-bucket envelope, not an exact sliding-window quota. It assumes
intact compatible Redis history, a fixed fleet, correct deduplication, no
forward server-clock jumps, and no unaccounted process loss. For desired excess
allowance E, choose `C <= floor((E - (0.001*R+0.000001))/(2*N))`; a result below
one cannot be promised by this asynchronous configuration.

One uninterrupted F-second fallback episode adds at most
`N*(FallbackBurst+FallbackRate*F)` tokens **per key**. Sum per-episode bounds or use
actual fallback counters for multiple episodes. Thus N instances at fallback
rate f can add approximately N*f tokens/second plus bursts. Unlimited outage time
has no fixed global overshoot allowance. Fleet growth and restarts add exposure;
LocalOnly is protective throttling rather than strict usage accounting.

Rates round once to the nearest microtoken/second (10^-6); a rounded zero is
invalid. LimitInfo exposes the effective rates. Rates, GlobalBurst and
MaxOutstanding are bounded by 1,000,000; LocalBurst must fit GlobalBurst and C.
Redis refill uses integer microtokens, milliseconds and carried remainder;
local refill uses monotonic nanosecond elapsed time. Backward Redis time clamps
to the last logical timestamp. Forward clock jumps, Redis eviction/state loss,
or failover losing writes invalidate the fixed physical-time envelope. Each
crashed/abandoned instance can lose up to C accepted normal tokens per key;
add cumulative losses explicitly. No disk journal or fleet membership is supplied.

## Keys, capacity and tuning

Derive keys from authenticated identity and normalized route templates, not an
untrusted arbitrary header or raw URL. Frame components unambiguously, for
example `fmt.Sprintf("%d:%s%d:%s", len(user), user, len(route), route)` with route
`/items/{id}`. Plain concatenation can collide. Redis keys use a framed prefix
and digest in a Cluster hash tag. Hashing avoids storing raw identifiers but is
not encryption and does not protect guessable low-entropy identities.

| Setting | Default / constraint |
| --- | --- |
| GlobalRate / GlobalBurst | Required positive |
| LocalRate | GlobalRate |
| MaxOutstanding C | min(GlobalBurst, 32) |
| LocalBurst | min(GlobalBurst, C) |
| SyncThreshold H | min(16, C); must be <= C |
| SyncInterval S | 100ms; less than MaxSyncAge |
| MaxSyncAge | 1s; at most 30s |
| Redis.Timeout | 200ms; less than MaxSyncAge |
| MaxKeys | 10,000 retained keys per instance |
| IdleTTL | 5 minutes; eligibility threshold, not forced expiry |
| Redis.Prefix | rate_limiter:v1; braces forbidden |
| Parent | context.Background() |

Zero optional numeric values select defaults; negative or incompatible values
are rejected. Supply exactly one Redis address or UniversalClient. The fixed
implementation uses 64 shards, four workers and a 1,024-entry notification queue.
A full queue coalesces/drops notifications, then bounded scans rediscover work;
it never discards accounting. Each scan/cleanup tick inspects at most 64 nodes.
MaxKeys bounds retained memory, **not** supported synchronization throughput.

For active K keys, normal rates lambda_i, backend operation occupancy L, estimate
`q ≈ K/S + sum(lambda_i/H) + q_init + q_probe + q_retry` and `mu <= 4/L`.
Each cold INIT includes TIME plus script. Keep target utilization `q/mu <= 0.60`
and validate with measured queue/scan delays. Timer/threshold overlap makes q a
conservative estimate, not an equality. The cap heuristic `C >= H + r*D` uses
local rate r and observed synchronization delay D; it trades throughput against
the fleet envelope. Neither interval nor threshold is a hard exposure bound;
C and MaxSyncAge provide the independent limits. See the complete measured matrix,
including unsupported points, in [BENCHMARKS.md](BENCHMARKS.md).

Clean buckets are evictable only after IdleTTL, no active references/work, no
pending/in-flight consumption, and sufficient inactivity to refill both normal
and fallback bursts. Global rejection and terminal errors remain retained;
unresolved debt is never evicted. Full storage rejects new keys with
`capacity_exhausted`, including during fallback. Account for 1,024-byte keys and
these retained states when sizing memory.

## Redis operations and repair

Use dedicated, adequately provisioned Redis with `maxmemory-policy noeviction`.
Do not share the limiter's protocol keys with arbitrary writers. Redis OOM is a
transient failure-policy event, not permission to lose accounting. Replication,
persistence and failover durability remain deployment responsibilities; async
replication can lose acknowledged writes. Tested Sentinel intact-history retry
uses replication confirmation and does not promise zero-loss failover generally.

Each session has a random immutable identity, one slot, and monotonic batch
sequence. Retries preserve identity/sequence/delta, including ambiguous replies.
Successful operations, including zero-delta refreshes, renew 24-hour retention.
At most 256 unexpired sessions exist per shared key. Eviction, shutdown and process
exit do not release slots early; churn can exhaust them even with fewer than 256
live instances. Session capacity retries at least one second later and never
activates fallback. Clean session loss reinitializes; unresolved session loss
latches `accounting_expired`. Global TTL is at least 24h and long enough to refill
all debt to burst plus 60s, including backward-clock gaps. Never shorten it manually.

Owned clients disable automatic retries and have bounded pool/dial/read/write
settings. Injected TLS, Cluster and Sentinel clients remain caller-owned; enable
ContextTimeoutEnabled, finite pool/dial/read/write timeouts no greater than
Redis.Timeout, bounded retries/routing, and cancellation-aware hooks. Cluster
routing and NOSCRIPT EVAL fallback share the attempt's original timeout. Four
local workers bound local attempts, not delayed remote executions after timeouts.
Recovery uses one probe with full-jitter exponential backoff from 100ms to 5s;
normal reconciliation then uses the same pool. SyncInterval is not an outage
detection guarantee.

For `policy_mismatch`, align every writer's configuration and protocol version.
For `accounting_error`, investigate corrupt state, numeric/sequence errors, or
(nonretryable dependency-wide) authentication, ACL or script errors. Unknown
errors fail closed under both policies. For `accounting_expired`, investigate
state loss, retention and dependency availability before replacement. Terminal
keys stop automatic work and retain their local capacity; unaffected keys continue
unless the entire dependency is blocked. There is no reset API.

Repair the dependency first. Deliberately replace the limiter only if abandoning
up to C unresolved tokens per affected key per retired instance is acceptable,
and record that loss. Replacement is cold initialization, not reconciliation or
exactly-once recovery. **Do not delete shared Redis keys as a repair step.** An
incompatible protocol requires a planned namespace migration: simply changing
the prefix can create a second budget during rolling deployment.

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
log events. `rate_limit_terminal_transitions_total` counts installed per-key
terminal transitions once with fixed reasons `accounting_expired`,
`policy_mismatch`, and `accounting_error`, even if no later request arrives.
A sanitized structured event is offered for each transition through the same
bounded log queue, with coalescing and drop counters. Circuit state is 0 healthy, 1 open, or 2 probing. Transitions count each
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

## gRPC integration without a dependency

Call Allow/AllowN from an interceptor after authentication, using the same framed
identity/normalized-method key, and invoke the next handler only when allowed.
Use `LimitInfo.Mode` to distinguish fallback success. Suggested reason mapping:

| Reasons | gRPC status |
| --- | --- |
| invalid_request | InvalidArgument |
| local_limit, global_limit, fallback_limit | ResourceExhausted |
| sync_pending, redis_unavailable, capacity_exhausted | Unavailable |
| accounting_expired, policy_mismatch, accounting_error | Unavailable; alert operators |
| closed | Unavailable; stop routing to this instance |

Attach retry metadata only when RetryAfterKnown is true and the delay is positive.
Do not expose dependency error text or imply unavailable shared quota is known.
The application owns status/metadata conventions; this module imports no gRPC SDK.

## Verification

Use `go build ./...`, `go test ./...`, `go test -race -timeout 3m ./...`, and
`go vet ./...` on a supported toolchain. CI additionally requires Go 1.21 source
compatibility, pinned govulncheck, actual Redis 7/current integration, Cluster
routing, and Sentinel failover. Fixture setup/teardown and required-environment
flags are in [testdata/redis/README.md](testdata/redis/README.md). Run extended
model fuzzing with `go test -run '^$' -fuzz '^FuzzStateMachine$' -fuzztime=60s
-fuzzminimizetime=1s -parallel=4 .`. Capacity commands and the precise locally
verified scope are in [BENCHMARKS.md](BENCHMARKS.md); local results do not certify
hosted CI, arbitrary deployments, clock jumps, or state-loss recovery.
