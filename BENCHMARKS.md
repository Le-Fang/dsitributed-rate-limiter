# Early scheduler capacity smoke

These are short Task 7 measurements of the actual scheduler against a **simulated delayed backend**, not a Redis throughput claim or a supported production operating point. The full capacity matrix, sustained trials, memory profiles and real Redis baseline remain Task 12 work. The public constructor now starts background synchronization; shared Close/final-flush behavior remains Task 8 work.

## Environment and method

Measured on 2026-10-10: Apple M1 Pro, darwin/arm64, Darwin 24.0.0, Go 1.27.2, default GOMAXPROCS=8. This was a shared development laptop, not an isolated benchmark host. The module retains Go 1.21 source compatibility and pins go-redis/v9 v9.18.0. Production should use a currently supported Go toolchain. Available real fixtures were Redis 7.4.11 (Docker, localhost:16380) and Redis 8.10.0 (native, localhost:16379); these validate integration, but are **not** the backend used for the timings below.

The simulated backend delays every TIME, PING and script-equivalent operation by the labeled duration. It executes an independent arbitrary-precision accounting model, counting unique deductions separately from admitted receipts. Four workers serve every operation; the queue holds at most 1,024 key references. Due scans and cleanup each inspect at most 64 nodes per tick, with independent persistent cursors. Tick duration is min(10ms, SyncInterval). A fully refillable clean idle key stops periodic refresh so it can be evicted. Dirty, ambiguous, terminal, capacity-latched and globally rejected keys retain their state.

Configuration per key: global rate/burst 10,000/10,000; local rate/burst 100/20; MaxOutstanding 20; SyncThreshold 10; SyncInterval 100ms; MaxSyncAge 1s; operation timeout 200ms; FailClosed. MaxKeys equals the offered key count; IdleTTL uses the 5-minute default. Uniform traffic offers one request/key every 20ms for 300ms, after every key has initialized. This offers 50 requests/s/key, within the local and global budgets. Cold initialization is reported separately: each initial request returns sync_pending (1, 100 or 1,000 cold rejections) and requires TIME plus INIT. For the overloaded case, finishing initialization does not imply that all keys remain fresh.

After traffic, the backend becomes unavailable until the circuit detects it and then for another 100ms. Availability is restored and offered traffic stops. Recovery time runs from restoration until the circuit is healthy, every key has current-generation authorization and every pending/in-flight ledger is empty. Deterministic midpoint jitter is injected for reproducible measurements; production uses full jitter with 100ms base, exponential growth and 5s cap. All measured admitted receipts must equal actual unique backend deductions before completion. Backend counters are captured after canceling/joining the workers.

Test-only counters and one timestamp per key bound measurement storage. A 1ms sampler records queue depth, sampled queued-state spans, and overdue scheduling age while healthy/due/neither queued nor busy. Queue spans are a **coarse proxy**, not exact enqueue-to-worker latency: brief dequeue/requeue cycles and outage gaps can merge between samples. The previously emitted raw label `queue_wait_lower_bound` was corrected to `queue_sample_span`; the values below are spans, not proven lower bounds. Scan overdue age includes inherited recovery backlog, not just scanner execution time. Queue/scan maxima cover traffic, outage and drain. Operation timings cover actual elapsed simulated operation occupancy. No latency percentile or precise production-metrics claim is made.

## Results

Ranges include all logged runs, including Go benchmark calibration, from the final `-count=3` invocation. Each row's offered/admitted values refer only to its 300ms traffic window. Durations include startup separately and do not count rejections as throughput.

| Keys / simulated delay | Cold initialization | Offered | Normal admitted | Stale/freshness denied | Admission rate during traffic | Drain after restoration |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 / 1ms | 2.24–3.65ms | 15 | 15 | 0 | 50/s | 71.7–82.9ms |
| 100 / 1ms | 57.5–66.6ms | 1,500 | 1,500 | 0 | 5,000/s | 100.8–110.1ms |
| 1,000 / 10ms | 5.10–5.16s | 15,000 | 3,279–3,368 | 11,632–11,721 (77.55–78.14%) | 10,930–11,227/s | 2.70–2.72s |

All admitted receipts were deducted exactly once: respectively 15, 1,500 and 3,279–3,368 tokens. All other rejection reasons during measured traffic were zero, including sync_pending, local_limit, global_limit and capacity_exhausted. Fallback admissions were zero under FailClosed. The overloaded stale rejections did not open the circuit; only the deliberate backend outage did. Separate deterministic tests fill a freshly authorized cap and verify sync_pending, and verify stale LocalOnly fallback without adding Redis debt.

| Keys | Backend operations/run (TIME / PING included) | Peak active operations | Sampled queue peak | Max sampled queue span | Max sampled scan overdue age | Mean operation occupancy | Max operation occupancy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 8 (1 / 2) | 1 | 0 | 0ms | 71.0–81.8ms | 1.075–1.614ms | 1.15–5.14ms |
| 100 | 549–566 (100 / 2) | 4 | 56–57 | 169–174ms | 199.9–233.4ms | 1.085–1.186ms | 1.19–7.12ms |
| 1,000 | 3,154–3,156 (1,000 / 2) | 4 | 962–963 | 2.415–2.426s | 5.016–5.077s | 10.176–10.271ms | 14.92–26.58ms |

Benchmark `ns/op` measures a complete initialization/traffic/outage/drain trial, not an Allow call: 0.489–0.494s, 0.568–0.573s and 8.216–8.294s respectively. Harness allocations per trial were approximately 61–64KB / 384–401 allocations, 728–738KB / 10,276–10,458 allocations, and 5.01MB / 58,747–58,758 allocations. These include the test backend, reference arithmetic and sampler; they are not heap-per-key or Allow allocation figures.

The 1- and 100-key smoke cases meet the early gate: useful admission with 0% capacity/freshness denial after initialization (<0.1%). The 1,000-key case is deliberately unsupported at these settings. Its periodic target alone is 10,000 operations/s while four 10ms workers supply at most roughly 400 operations/s, before initialization, deductions or probes. Stale authorization and multi-second backlog are expected; temporary burst admissions do not establish sustainable throughput. No receipts were discarded to recover. The bounded barrier test additionally saturates the full queue with more than 1,024 distinct notifications and proves eventual rediscovery and drain.

## Reproduction and limits

```sh
GOTOOLCHAIN=go1.27.2 GOCACHE=/tmp/rate-limiter-go-cache \
  go test -race -run '^TestLoadCapacitySmoke$' -count=1 .
GOTOOLCHAIN=go1.27.2 GOCACHE=/tmp/rate-limiter-go-cache \
  go test -run '^$' -bench '^BenchmarkSchedulerCapacitySmoke$' -benchmem -count=3 .
```

Ordinary CI runs the bounded-resource/accounting smoke test, without hardware-specific latency assertions. The final full race suite also exercised actual Lua/Redis 7 and the functional constructor. A focused constructor/scheduler test passed against Redis 8. Go 1.21 compatibility and `go vet` were checked. Cluster coverage, a new vulnerability scan, sustained capacity, real Redis timing, detailed memory profiles and the full Task 12 matrix were not performed for this task. These short measurements do not establish production readiness.

## Review-fix regression

After separating current-generation observations from admission authority (so negative INIT/recovery responses follow the refresh cadence) and synchronizing enqueue with cancellation/drain, the same three points were rerun once with `-benchtime=1x -benchmem -count=1`. The 1-key and 100-key cases again admitted 15/15 and 1,500/1,500; overload admitted 3,288/15,000 with 11,712 explicit freshness denials (78.08%). Every admission matched an actual unique deduction after recovery. Initialization was 3.79ms / 59.48ms / 5.160s, recovery was 78.31ms / 99.09ms / 2.726s, operation counts were 8 / 559 / 3,153 including TIME counts 1 / 100 / 1,000 and two probes each, peak operations were 1 / 4 / 4, and sampled queue peaks were 0 / 56 / 962. The focused scheduler/admission/model/load race suite passed. This single regression run supplements the three-run baseline above; it is not a new sustained-capacity claim.
