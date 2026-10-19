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

# Task 12 capacity and release evidence (2026-10-14)

This section extends the historical Task 7 results above. The completed library
now includes shared shutdown, metrics, model tests, and Redis/Cluster/Sentinel
conformance. Historical statements about unfinished tasks describe their original
measurement dates. The following are bounded local experiments, not universal
latency guarantees or a certification of arbitrary production deployments.

## Current method and environment

Apple M1 Pro, Darwin 24.0.0 arm64, Go 1.27.2, GOMAXPROCS=8, go-redis/v9
v9.18.0. Redis baseline/memory uses Redis 7.4.11 in Docker on localhost:16380.
The separate conformance gate also uses official-image Redis 8.10.2 and its
Cluster/Sentinel fixtures. All run on the shared development laptop. Capacity
measurements ran before the CPU-heavy full release checks. Current toolchain and
image versions were verified against [Go downloads](https://go.dev/dl/) and the
[official Redis image manifest](https://github.com/docker-library/official-images/blob/master/library/redis).
Production should use a supported patched toolchain; Go 1.21 is only the source
compatibility floor.

Every matrix cell uses the **actual four-worker scheduler**, default aggregate
metrics and the independent accounting backend. Configuration remains the Task 7
policy: global 10,000/s burst 10,000 per key, local 100/s burst 20, C=20, H=10,
S=100ms, MaxSyncAge=1s, operation timeout=200ms, FailClosed, MaxKeys=K,
IdleTTL=5 minutes. Requested simulated delays apply to every TIME/PING/script
operation. The measured mean operation occupancy includes timer scheduling and
model processing; it is not assumed equal to the requested delay.

After creating all K keys, wait a measured one-second warmup. The initial K
requests all return sync_pending and are separate from measured traffic.
The warmup is intentionally bounded: it does **not** imply 10,000 keys have
initialized or remain fresh. The total operation/TIME columns include this cold
phase, whereas q and operations/normal use the traffic window. INIT requires TIME
plus script; ongoing initialization remains included in the traffic counters.
Then offer one batch every 20ms, for 50 rounds in matrix cells. G goroutines split
max(K,G) requests per round, so K=1/G=16 offers 800/s against a 100/s local limit;
other uniform rows offer 50/s/key. Hot distribution is a deterministic mixture of
90% key zero and 10% rotating uniform keys (the uniform fraction can also hit
zero). This intentionally exceeds the hot key's local budget in many cells.
All results, including local quota rejections, remain counted. Offered/second and
normal/second use the actual elapsed window, not a nominal duration.

Every caller has one fixed 64-bin power-of-two-nanosecond histogram, merged only
after joining. Allow p50/p95/p99 are **upper bucket boundaries**, with at most
2x relative bucket width and 1ns minimum; no unbounded sample list is retained.
Each measurement includes Allow itself, time reads and histogram measurement
overhead, unlike the separate microbenchmarks. Queue and sync p95 are the upper
bounds of the production histogram buckets (0.1/0.5/1/5/10/50/100/500ms, 1/5s;
-1 denotes overflow). They cover cold warmup plus traffic. Sync includes TIME on
INIT. A 10ms sampler locks each retained bucket to check its cap and sample scan
overdue age and queue depth; this harness overhead is included. Sampled scan age
is backlog age, not scanner execution duration, and peaks can miss short events.

Backend occupancy is summed completed-operation duration/window, approximately
the mean busy worker count. Boundary-crossing operations are counted whole, so
short-window estimates can slightly exceed four (up to about four operation
durations of edge bias). Independently measured peak concurrency never exceeds
four. Reported allocation bytes/objects include all Go process activity during
the traffic interval: scheduler, independent model, sampler and harness, including
unfinished initialization. They are **not Allow allocations** or steady-state
heap/key estimates. Counter/clock boundaries also have small sampling skew.

After the traffic join, simulated delay is set to zero **only for cleanup**.
Close cancels/joins workers and flushes using unchanged batch identities. At that
barrier every retained ledger is empty and independent unique deductions equal
normal admissions; fallback is excluded. No outstanding work is dropped. This
accelerated cleanup is neither a same-delay drain benchmark nor a recovery time.
The real Redis baseline retains real transport latency and verifies successful
Close/empty ledgers; actual Lua deduplication is additionally covered by the
required conformance suite. Measured outage recovery below retains configured
delay throughout. A bounded watchdog diagnoses stalled progress; cap and
conservation assertions use locked state and the completion barrier.

## Full 48-cell matrix

Each cell has one run, not a confidence interval. U=uniform; H=90% hot. Delay is
requested simulated milliseconds. A=normal admissions; L=local_limit;
P=sync_pending (cold or fresh-cap); F=redis_unavailable (stale/unfinished cold
initialization here, **not an observed Redis outage**). All other rejection
reasons are zero: global_limit, capacity_exhausted, invalid_request,
accounting_expired, policy_mismatch, accounting_error, closed, fallback_limit.
Fallback admissions/share and probes are zero in every matrix row. Mode totals
are normal=A+L and blocked=P+F; every offered request is accounted for.

| ID | K | Mix | G | Delay ms | Window s | Offered/s | Normal/s | A / offered | L | P | F | Allow p50/p95/p99 us |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |
| 1 | 1 | U | 1 | 0.2 | 1.0012 | 50 | 50 | 50/50 | 0 | 0 | 0 | 16.384/131.072/262.144 |
| 2 | 1 | U | 1 | 1 | 1.0009 | 50 | 50 | 50/50 | 0 | 0 | 0 | 8.192/32.768/65.536 |
| 3 | 1 | U | 1 | 10 | 1.0002 | 50 | 50 | 50/50 | 0 | 0 | 0 | 16.384/131.072/131.072 |
| 4 | 1 | U | 16 | 0.2 | 1.0008 | 799 | 117 | 117/800 | 683 | 0 | 0 | 2.048/16.384/65.536 |
| 5 | 1 | U | 16 | 1 | 1.0007 | 799 | 117 | 117/800 | 683 | 0 | 0 | 2.048/32.768/65.536 |
| 6 | 1 | U | 16 | 10 | 1.0005 | 800 | 117 | 117/800 | 683 | 0 | 0 | 2.048/16.384/65.536 |
| 7 | 1 | H | 1 | 0.2 | 1.0003 | 50 | 50 | 50/50 | 0 | 0 | 0 | 16.384/65.536/65.536 |
| 8 | 1 | H | 1 | 1 | 1.0007 | 50 | 50 | 50/50 | 0 | 0 | 0 | 16.384/65.536/131.072 |
| 9 | 1 | H | 1 | 10 | 1.0023 | 50 | 50 | 50/50 | 0 | 0 | 0 | 16.384/65.536/131.072 |
| 10 | 1 | H | 16 | 0.2 | 1.0002 | 800 | 117 | 117/800 | 683 | 0 | 0 | 2.048/32.768/131.072 |
| 11 | 1 | H | 16 | 1 | 1.0018 | 799 | 118 | 118/800 | 682 | 0 | 0 | 2.048/32.768/131.072 |
| 12 | 1 | H | 16 | 10 | 1.0004 | 800 | 117 | 117/800 | 683 | 0 | 0 | 2.048/16.384/131.072 |
| 13 | 100 | U | 1 | 0.2 | 1.0009 | 4995 | 4995 | 5000/5000 | 0 | 0 | 0 | 2.048/4.096/8.192 |
| 14 | 100 | U | 1 | 1 | 1.0004 | 4998 | 4998 | 5000/5000 | 0 | 0 | 0 | 1.024/4.096/8.192 |
| 15 | 100 | U | 1 | 10 | 1.0012 | 4994 | 4994 | 5000/5000 | 0 | 0 | 0 | 2.048/4.096/8.192 |
| 16 | 100 | U | 16 | 0.2 | 1.0012 | 4994 | 4994 | 5000/5000 | 0 | 0 | 0 | 2.048/4.096/16.384 |
| 17 | 100 | U | 16 | 1 | 1.0006 | 4997 | 4997 | 5000/5000 | 0 | 0 | 0 | 2.048/4.096/16.384 |
| 18 | 100 | U | 16 | 10 | 1.0017 | 4992 | 4992 | 5000/5000 | 0 | 0 | 0 | 2.048/4.096/8.192 |
| 19 | 100 | H | 1 | 0.2 | 1.0002 | 4999 | 612 | 612/5000 | 4317 | 71 | 0 | 1.024/4.096/8.192 |
| 20 | 100 | H | 1 | 1 | 1.0008 | 4996 | 612 | 612/5000 | 4317 | 71 | 0 | 1.024/2.048/8.192 |
| 21 | 100 | H | 1 | 10 | 1.0008 | 4996 | 612 | 613/5000 | 2430 | 1957 | 0 | 1.024/4.096/8.192 |
| 22 | 100 | H | 16 | 0.2 | 1.0014 | 4993 | 612 | 613/5000 | 4316 | 71 | 0 | 1.024/8.192/32.768 |
| 23 | 100 | H | 16 | 1 | 1.0011 | 4995 | 612 | 613/5000 | 4316 | 71 | 0 | 2.048/8.192/65.536 |
| 24 | 100 | H | 16 | 10 | 1.0011 | 4994 | 612 | 613/5000 | 2575 | 1812 | 0 | 1.024/16.384/65.536 |
| 25 | 1000 | U | 1 | 0.2 | 1.0014 | 49929 | 49929 | 50000/50000 | 0 | 0 | 0 | 0.512/1.024/4.096 |
| 26 | 1000 | U | 1 | 1 | 1.0029 | 49857 | 49857 | 50000/50000 | 0 | 0 | 0 | 1.024/4.096/8.192 |
| 27 | 1000 | U | 1 | 10 | 1.0021 | 49894 | 5889 | 5902/50000 | 0 | 3260 | 40838 | 1.024/2.048/8.192 |
| 28 | 1000 | U | 16 | 0.2 | 1.0007 | 49965 | 49965 | 50000/50000 | 0 | 0 | 0 | 4.096/8.192/8.192 |
| 29 | 1000 | U | 16 | 1 | 1.0007 | 49966 | 49966 | 50000/50000 | 0 | 0 | 0 | 4.096/8.192/8.192 |
| 30 | 1000 | U | 16 | 10 | 1.0017 | 49916 | 5818 | 5828/50000 | 0 | 3186 | 40986 | 4.096/8.192/16.384 |
| 31 | 1000 | H | 1 | 0.2 | 1.0005 | 49976 | 5111 | 5113/50000 | 44585 | 302 | 0 | 0.512/1.024/2.048 |
| 32 | 1000 | H | 1 | 1 | 1.0010 | 49951 | 5108 | 5113/50000 | 35420 | 9467 | 0 | 1.024/2.048/8.192 |
| 33 | 1000 | H | 1 | 10 | 1.0021 | 49893 | 942 | 944/50000 | 0 | 0 | 49056 | 1.024/2.048/4.096 |
| 34 | 1000 | H | 16 | 0.2 | 1.0015 | 49927 | 5105 | 5113/50000 | 44784 | 103 | 0 | 4.096/131.072/262.144 |
| 35 | 1000 | H | 16 | 1 | 1.0010 | 49950 | 5108 | 5113/50000 | 37709 | 7178 | 0 | 4.096/131.072/262.144 |
| 36 | 1000 | H | 16 | 10 | 1.0034 | 49830 | 931 | 934/50000 | 0 | 0 | 49066 | 4.096/131.072/524.288 |
| 37 | 10000 | U | 1 | 0.2 | 1.0047 | 497639 | 266212 | 267475/500000 | 0 | 93256 | 139269 | 0.512/1.024/1.024 |
| 38 | 10000 | U | 1 | 1 | 1.0060 | 497002 | 57232 | 57577/500000 | 0 | 31242 | 411181 | 0.512/1.024/2.048 |
| 39 | 10000 | U | 1 | 10 | 1.0065 | 496772 | 5882 | 5920/500000 | 0 | 3265 | 490815 | 0.512/1.024/2.048 |
| 40 | 10000 | U | 16 | 0.2 | 1.0035 | 498237 | 253945 | 254844/500000 | 0 | 93180 | 151976 | 2.048/8.192/8.192 |
| 41 | 10000 | U | 16 | 1 | 1.0041 | 497968 | 54369 | 54591/500000 | 0 | 30163 | 415246 | 4.096/8.192/8.192 |
| 42 | 10000 | U | 16 | 10 | 1.0050 | 497535 | 5845 | 5874/500000 | 0 | 3221 | 490905 | 4.096/8.192/8.192 |
| 43 | 10000 | H | 1 | 0.2 | 1.0041 | 497934 | 36548 | 36700/500000 | 435650 | 13778 | 13872 | 0.512/0.512/1.024 |
| 44 | 10000 | H | 1 | 1 | 1.0052 | 497409 | 9478 | 9527/500000 | 0 | 188982 | 301491 | 0.512/1.024/1.024 |
| 45 | 10000 | H | 1 | 10 | 1.0068 | 496645 | 914 | 920/500000 | 0 | 0 | 499080 | 0.512/1.024/1.024 |
| 46 | 10000 | H | 16 | 0.2 | 1.0071 | 496459 | 36536 | 36797/500000 | 436299 | 10296 | 16608 | 2.048/65.536/131.072 |
| 47 | 10000 | H | 16 | 1 | 1.0110 | 494546 | 9726 | 9833/500000 | 0 | 199829 | 290338 | 4.096/131.072/262.144 |
| 48 | 10000 | H | 16 | 10 | 1.0122 | 493950 | 912 | 923/500000 | 0 | 0 | 499077 | 4.096/131.072/262.144 |

q is measured backend operations/second; op/A is traffic operations per normal
token. Total/TIME includes cold initialization through the end of traffic.
Q95/S95 are queue/sync p95 upper boundaries in ms. Scan is sampled max overdue
ms. Busy is approximate mean/true peak; queue is sampled peak. Alloc is traffic
bytes/objects (including model/harness), not retained memory.

| ID | q /s | op/A | Total/TIME | Actual mean L ms | Q95/S95 ms | Scan ms | Busy mean/peak | Queue | Alloc bytes/objects |
| --- | ---: | ---: | --- | ---: | --- | ---: | --- | ---: | --- |
| 1 | 9.0 | 0.1800 | 20/1 | 0.267 | 0.5/1 | 12.1 | 0.002/1 | 0 | 105232/636 |
| 2 | 9.0 | 0.1800 | 20/1 | 1.151 | 0.5/5 | 6.9 | 0.010/1 | 0 | 105376/636 |
| 3 | 9.0 | 0.1800 | 20/1 | 10.480 | 0.1/50 | 7.6 | 0.094/1 | 0 | 105696/637 |
| 4 | 11.0 | 0.0940 | 22/1 | 0.362 | 0.1/5 | 0.0 | 0.004/1 | 0 | 144776/861 |
| 5 | 11.0 | 0.0940 | 22/1 | 1.583 | 0.1/5 | 8.9 | 0.017/1 | 0 | 136752/837 |
| 6 | 11.0 | 0.0940 | 22/1 | 10.164 | 0.5/50 | 0.0 | 0.112/1 | 0 | 136960/833 |
| 7 | 9.0 | 0.1800 | 20/1 | 0.286 | 0.5/1 | 8.2 | 0.003/1 | 0 | 103808/628 |
| 8 | 9.0 | 0.1800 | 20/1 | 1.159 | 0.1/5 | 6.5 | 0.010/1 | 0 | 105552/639 |
| 9 | 9.0 | 0.1800 | 20/1 | 10.576 | 0.1/50 | 7.5 | 0.095/1 | 0 | 102704/628 |
| 10 | 11.0 | 0.0940 | 22/1 | 0.259 | 0.1/0.5 | 0.0 | 0.003/1 | 0 | 139920/850 |
| 11 | 11.0 | 0.0932 | 22/1 | 1.171 | 0.1/5 | 0.0 | 0.013/1 | 0 | 139552/849 |
| 12 | 11.0 | 0.0940 | 22/1 | 10.201 | 0.1/50 | 0.0 | 0.112/1 | 0 | 136560/839 |
| 13 | 902.2 | 0.1806 | 1931/100 | 0.282 | 5/1 | 21.5 | 0.255/4 | 15 | 1164864/23063 |
| 14 | 919.6 | 0.1840 | 1928/100 | 1.162 | 10/5 | 28.1 | 1.068/4 | 13 | 1182840/23450 |
| 15 | 375.6 | 0.0752 | 748/100 | 10.547 | 500/50 | 30.1 | 3.961/4 | 66 | 549464/9854 |
| 16 | 903.9 | 0.1810 | 1929/100 | 0.276 | 5/0.5 | 18.8 | 0.249/4 | 24 | 1199480/23297 |
| 17 | 917.4 | 0.1836 | 1928/100 | 1.187 | 10/5 | 27.7 | 1.089/4 | 18 | 1211160/23528 |
| 18 | 383.4 | 0.0768 | 752/100 | 10.434 | 500/50 | 26.0 | 4.000/4 | 65 | 583432/10193 |
| 19 | 904.8 | 1.4788 | 1951/100 | 0.272 | 5/0.5 | 18.0 | 0.246/4 | 33 | 1109488/20933 |
| 20 | 916.3 | 1.4984 | 1929/100 | 1.170 | 10/5 | 28.1 | 1.073/4 | 9 | 1123488/21269 |
| 21 | 375.7 | 0.6134 | 744/100 | 10.557 | 500/50 | 167.6 | 3.966/4 | 68 | 545720/9728 |
| 22 | 902.7 | 1.4747 | 1932/100 | 0.253 | 5/0.5 | 16.2 | 0.229/4 | 27 | 1142888/21086 |
| 23 | 917.0 | 1.4976 | 1929/100 | 1.169 | 10/5 | 28.0 | 1.072/4 | 16 | 1148536/21404 |
| 24 | 383.6 | 0.6264 | 748/100 | 10.388 | 500/50 | 26.5 | 3.985/4 | 62 | 579336/10052 |
| 25 | 6221.2 | 0.1246 | 12956/1000 | 0.241 | 100/1 | 111.9 | 1.498/4 | 58 | 7513240/156475 |
| 26 | 3380.3 | 0.0678 | 6798/1000 | 1.177 | 500/5 | 172.7 | 3.979/4 | 523 | 4230184/85830 |
| 27 | 387.2 | 0.0657 | 752/376 | 10.404 | 5000/50 | 176.5 | 4.028/4 | 975 | 562456/6988 |
| 28 | 6066.8 | 0.1214 | 13025/1000 | 0.256 | 50/0.5 | 114.0 | 1.550/4 | 61 | 7365696/152734 |
| 29 | 3395.7 | 0.0680 | 6742/1000 | 1.173 | 500/5 | 176.5 | 3.983/4 | 496 | 4259016/86125 |
| 30 | 375.4 | 0.0645 | 740/372 | 10.567 | 5000/50 | 183.8 | 3.967/4 | 974 | 575608/6942 |
| 31 | 6126.1 | 1.1987 | 13085/1000 | 0.245 | 50/0.5 | 112.2 | 1.502/4 | 60 | 7209152/146658 |
| 32 | 3364.7 | 0.6587 | 6731/1000 | 1.184 | 500/5 | 202.4 | 3.983/4 | 427 | 4142504/82909 |
| 33 | 379.2 | 0.4025 | 748/376 | 10.460 | 5000/50 | 190.0 | 3.966/4 | 972 | 555000/6890 |
| 34 | 6052.1 | 1.1854 | 12981/1000 | 0.260 | 50/0.5 | 114.9 | 1.574/4 | 60 | 7158960/145335 |
| 35 | 3524.5 | 0.6900 | 6915/1000 | 1.130 | 500/5 | 199.0 | 3.983/4 | 420 | 4352952/87053 |
| 36 | 386.7 | 0.4154 | 752/376 | 10.377 | 5000/50 | 184.3 | 4.012/4 | 974 | 586232/7127 |
| 37 | 16610.2 | 0.0624 | 29336/9289 | 0.237 | 100/1 | 1777.9 | 3.935/4 | 986 | 20652784/372581 |
| 38 | 3542.6 | 0.0619 | 7035/3366 | 1.126 | 1000/5 | 1668.3 | 3.988/4 | 1024 | 4433848/63426 |
| 39 | 389.5 | 0.0662 | 760/380 | 10.325 | 5000/50 | 1865.6 | 4.021/4 | 1024 | 570680/7084 |
| 40 | 14968.0 | 0.0589 | 27469/9364 | 0.263 | 500/1 | 1780.5 | 3.931/4 | 1024 | 18735536/328715 |
| 41 | 3352.3 | 0.0617 | 6797/3335 | 1.189 | 1000/5 | 1821.6 | 3.987/4 | 1024 | 4191672/58661 |
| 42 | 382.1 | 0.0654 | 752/376 | 10.500 | 5000/50 | 1910.4 | 4.012/4 | 1024 | 588152/7088 |
| 43 | 15016.7 | 0.4109 | 27641/10000 | 0.236 | 500/1 | 1454.3 | 3.542/4 | 988 | 18962352/318233 |
| 44 | 3537.6 | 0.3733 | 6976/3466 | 1.128 | 1000/5 | 1880.3 | 3.990/4 | 1024 | 4389464/61088 |
| 45 | 389.4 | 0.4261 | 760/380 | 10.328 | 5000/50 | 1910.4 | 4.022/4 | 1024 | 570680/7084 |
| 46 | 15083.4 | 0.4128 | 27679/10000 | 0.236 | 500/1 | 1460.4 | 3.557/4 | 1000 | 19178272/321244 |
| 47 | 3728.9 | 0.3834 | 7238/3594 | 1.070 | 1000/5 | 1905.1 | 3.989/4 | 1024 | 4681368/64992 |
| 48 | 395.2 | 0.4334 | 768/384 | 10.175 | 5000/50 | 1919.8 | 4.021/4 | 1024 | 603160/7355 |

Actual warmup wait across the matrix: 1.000011–1.001516s after cold-key creation. The matrix plus supported/Redis/memory tests completed in 109.629s.

## Supported measured point and capacity interpretation

The supported measured workload is **100 uniform active keys, 16 goroutines,
50 offered requests/s/key (5,000/s total), one process, the configuration above**,
after one-second warmup, at simulated 1ms or the measured local Redis baseline.
Both five-second traffic trials admitted all 25,000/25,000 requests as normal:
0% healthy capacity/freshness-induced denial or fallback, below the <0.1% target.
This is a five-second operating-point observation, not a long-duration soak or a
claim about other fleets/network paths. Test the intended deployment and sustained
load before relying on it. No quota/freshness outcomes were filtered out.

| Backend | Actual window s | Actual mean L ms | q /s | op/normal | Allow p50/p95/p99 us | Q95/S95 ms | Scan max ms | Busy mean/peak | Queue peak | Alloc bytes/objects | Total ops/TIME |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- | ---: | --- | --- |
| simulated | 5.000389 | 1.185 | 921.5 | 0.18432 | 2.048/4.096/8.192 | 5/5 | 28.28 | 1.092/4 | 20 | 5852296/117290 | 5616/100 |
| redis | 5.001094 | 1.333 | 916.2 | 0.18328 | 2.048/4.096/16.384 | 5/5 | 27.99 | 1.222/4 | 14 | 11715016/286217 | 5574/100 |

The conservative planning estimate is `q≈K/S+sum(lambda_i/H)+q_init+q_probe+q_retry`.
For the warmed supported point, `100/.1 + 5000/10 = 1500/s`, before extra cold
starts, outages or retries. Actual mean occupancy gives `mu<=4/L≈3376/s` for the
simulation and `≈3000/s` for local Redis: planned utilization is about 0.44/0.50,
below 0.60. Measured q is lower because periodic and threshold work overlap.
With planned synchronization delay D=100ms, `C>=H+r*D=10+100*.1=20` matches
C=20. Observed queue/sync p95 and scan max are smaller, but percentiles/sampled
maxima are not hard latency bounds. Budget longer tails and fleet excess together.

K=1/G=16 and many hot rows exceed the hot key's local rate: their local_limit
counts reflect offered quota overload, not successful hot-path throughput.
K>=1,000 is **not declared supported** at these settings, including rows with
few or zero short-window denials: the periodic estimate alone is 10,000/s or
100,000/s. At 10ms, four workers supply only about 380–400/s; unfinished cold
initialization and stale authorization dominate. At 0.2ms requested delay,
actual timer/model occupancy is larger and high-key-count utilization has
insufficient headroom. Small operation/normal ratios in cold/overloaded rows
also include consumption still waiting for post-window flushing; they are not
sustainable Redis-efficiency claims. The full queue remains bounded at 1,024,
the circuit stays healthy, and accounting survives overload.

## Outcome-specific microbenchmarks

Five repetitions, `-benchtime=100ms -benchmem`, default metrics enabled. Each
benchmark uses a fixed clock, prebuilt keys and finite burst/cap 1,000,000.
Authorization, replenishment, ledger reset and queue draining occur outside timed
batches of at most 8,192 requests; no backend or scheduler runs in the timed path.
Every measured call asserts its expected outcome. These are serial per-call
computational costs, not end-to-end capacity; G=1/16 scheduler measurements above
cover concurrent traffic. Setup allocations are excluded by StopTimer/StartTimer.

| Benchmark | Outcomes | Five-run ns/op range | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| AllowHotKey | 100% allowed, one key | 195.4–197.3 | 0 | 0 |
| AllowManyKeys | 100% allowed, 1,000 keys | 153.1–155.2 | 0 | 0 |
| AllowLocalReject | 100% local_limit | 124.5–126.6 | 0 | 0 |
| AllowCapReject | 100% sync_pending at fresh full cap | 177.8–179.7 | 0 | 0 |

## Outages, recovery and concurrent Close

The non-race Task 7 smoke was rerun once at 1x: K=1/100 at simulated 1ms admitted
15/15 and 1,500/1,500; K=1,000 at 10ms admitted 3,354/15,000, with 11,646 freshness
denials (77.64%). Unique deductions were exactly 15/1,500/3,354. Cold init took
3.87ms/68.61ms/5.138s; after restoration, **same-delay full accounting drain** took
71.59ms/105.96ms/2.683s. Backend operations were 8/576/3157, including TIME
1/100/1000 and two probes each. Peak workers were 1/4/4; queue peaks 0/57/960.
These preserve the original unsupported-overload regression baseline.

The concurrent load suite passed under the race detector: uniform 100 and 10,000
keys, 16 callers on one hot key with two transient outages/recoveries, both
FailClosed and LocalOnly, and eight simultaneous Close callers racing admission.
The outage trials retained simulated 1ms delay, local rate/burst 100/20; LocalOnly
used explicit fallback 20/5. Each offered 1,600 calls over 2.001s after one-second
warmup. Recovery is restoration until healthy circuit **and current-generation
per-key authorization with fallback exited**, while offered traffic continues.
It is not necessarily full drain; final Close provides the accounting barrier.
Midpoint jitter makes these reproducible experiments; production uses full jitter.

| Policy | Normal | Fallback | local_limit | fallback_limit | redis_unavailable | Fallback share | Recovery ms (two episodes) | Allow p50/p95/p99 us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| FailClosed | 210 | 0 | 1054 | 0 | 336 | 0% | 101.89 / 84.79 | 32.768/262.144/262.144 |
| LocalOnly | 182 | 7 | 1130 | 281 | 0 | 0.4375% | 79.45 / 79.30 | 65.536/524.288/524.288 |

Other reasons are zero. Modes were normal/blocked 1264/336 for FailClosed;
normal/local_fallback 1312/288 for LocalOnly. Applied deductions equal 210 and
182 respectively; the seven fallback tokens never reached the Redis ledger.
Race-detector timing is diagnostic, not comparable with the non-race matrix.

## Retained memory and session cost

GC-separated heap deltas for an unstarted core with 10,000 retained keys, no
backend/metrics, compare the empty core to the populated store. Unresolved keys
hold pending=10 and an in-flight=10 batch; rejected keys hold the global latch.
These are deliberately retained state profiles, not live throughput allocations.
An empty core measured 9,952–13,432 bytes separately. Runtime/allocator overhead
and sampling noise remain; RSS and active worker/client/model memory are excluded.
The global process baseline is subtracted, not divided into per-key estimates.

| State | Key bytes | Retained bytes | Bytes/key |
| --- | ---: | ---: | ---: |
| Clean | 16 | 10,821,912 | 1082.19 |
| Clean | 1024 | 20,903,928 | 2090.39 |
| Unresolved | 16 | 10,982,104 | 1098.21 |
| Unresolved | 1024 | 21,063,928 | 2106.39 |
| Global rejected | 16 | 10,822,296 | 1082.23 |
| Global rejected | 1024 | 20,905,128 | 2090.51 |

Optional heap profiles are written one per state/key length with
`CAPACITY_HEAP_DIR`; six profiles were generated and the unresolved/1024 profile
was inspected with `go tool pprof -top -sample_index=inuse_space`. Pprof is sampled
and includes process/runtime allocations; use the GC delta table for per-key
estimates. Byte counts vary by Go version, architecture and allocator.

Actual Redis 7.4.11 `MEMORY USAGE key SAMPLES 0` after successful INITs:

| Sessions | Total key bytes | Increment over one session | Encoding |
| --- | ---: | ---: | --- |
| 1 | 704 | 0 | hashtable |
| 32 | 5584 | 4880 | hashtable |
| 256 | 41424 | 40720 | hashtable |

The one-session total includes the key/global hash and allocator overhead.
Additional sessions average about 157–160 bytes each over this baseline; do not
charge all 704 bytes per session. These numbers include the fixture's key length,
not Redis server RSS, connection buffers, replication, persistence or fragmentation.
Production hashed-key length depends on the namespace. The limit remains 256
unexpired sessions per key, including evicted/shut-down incarnations, for 24h
since last successful operation. This is a lifetime/churn constraint, not merely
an instantaneous live-process count.

## Reproduction

```sh
export GOTOOLCHAIN=go1.27.2 GOCACHE=/tmp/rate-limiter-go-cache
# Non-race full 48-cell matrix and supported point (~110s with other measurements).
RUN_CAPACITY=1 REQUIRE_REDIS=1 REDIS_ADDR=127.0.0.1:16380 REDIS_ISOLATED=1 \
  go test -timeout 5m -run '^Test(LoadCapacityMatrix|LoadSupportedPoint|LoadRealRedis|CapacityMemory|CapacityRedisMemory)$' -count=1 -v .
go test -race -timeout 90s -run '^TestLoad' -count=1 -v .
go test -run '^$' -bench 'BenchmarkAllow' -benchtime=100ms -benchmem -count=5 .
go test -run '^$' -bench '^BenchmarkSchedulerCapacitySmoke$' -benchtime=1x -benchmem -count=1 .
mkdir -p /tmp/limiter-heap
RUN_CAPACITY=1 CAPACITY_HEAP_DIR=/tmp/limiter-heap \
  go test -run '^TestCapacityMemory$' -count=1 -v .
go tool pprof -top -sample_index=inuse_space /tmp/limiter-heap/unresolved-1024.pprof
```

The opt-in matrix is excluded from ordinary unit/race runs to bound CI duration;
all 48 cells were separately executed here without race instrumentation. The
race load command runs the deterministic/barrier smoke and concurrent load suite;
its three optional capacity measurement tests explicitly skip without RUN_CAPACITY.
Machine-readable CAPACITY JSON lines from `-v` contain every counter and actual
duration; table rounding does not alter their values. No hosted CI run is inferred
from these local checks.

## Final local release gate

Measurements above were taken on October 14; final shutdown-test repair and
build/unit/race/vet/security checks completed on October 19. Commands use
`GOTOOLCHAIN=go1.27.2 GOCACHE=/tmp/rate-limiter-go-cache` unless stated otherwise.
Standalone required coverage also sets `REQUIRE_REDIS=1 REDIS_ISOLATED=1` and
`REDIS_ADDR` to the dedicated fixture. The Go 1.21 checks used 1.21.13.

| Check | Command / required environment | Result |
| --- | --- | --- |
| Formatting | `gofmt -l .`; `git diff --check` | Empty output |
| Build and examples | `go build ./...`; `go test -run '^Example' -count=1 .` | Passed; examples compiled without live execution |
| Unit/model/Redis 7 | `REDIS_ADDR=127.0.0.1:16380 go test -timeout 3m ./...` with required flags | Passed, final run 20.255s |
| Full race/Redis 7 | Same required fixture, `go test -race -timeout 3m ./...` | Passed, final run 111.938s |
| Vet | `go vet ./...` | Passed |
| Vulnerabilities | `govulncheck ./...`, scanner v1.8.0 built on supported Go | No vulnerabilities found; refreshed October 19 |
| Compatibility | `GOTOOLCHAIN=go1.21.13 go build ./...`, `go test -timeout 3m ./...`, `go vet ./...` | Passed; test 20.195s |
| Redis 8.10.2 | Required fixture at 16381; `go test -race -timeout 3m -run '^Test(RedisIntegration\|StateMachineFoundationRedis\|SchedulerConstructorRedis)' -count=1 .` | Passed, 2.395s |
| Cluster/Sentinel | Required topology flags below; `go test -race -timeout 2m -run '^TestRedis(Cluster\|Sentinel)Integration$' -count=1 .` | Passed, 12.678s |
| Model fuzz | `go test -run '^$' -fuzz '^FuzzStateMachine$' -fuzztime=60s -fuzzminimizetime=1s -parallel=4 .` | Passed, 536,885 executions, 60.859s |
| Load race | `go test -race -timeout 90s -run '^TestLoad' -count=1 -v .` | Passed, 12.589s; opt-in measurement tests skipped here and run separately |
| Capacity / microbench / smoke | Reproduction commands above | 48 matrix cells, supported point, real Redis, six heap profiles, Redis session memory and all five-repeat microbenchmarks passed |

The first full race run exposed a pre-existing test-ordering error in
`TestLifecycleParentCancellationDuringFallback`: a canceled ordinary worker was
incorrectly asserted to be the independent final flush. A deterministic barrier
now cancels the worker before checking that the subsequent flush has a live
context, unchanged batch identity and joined workers. The original assertion was
reproduced failing; the corrected test passed 200 race repetitions and the full
race rerun. Production lifecycle behavior was unchanged.

Topology fixture creation used the distinct project `rate-limiter-task12`:

```sh
docker compose -p rate-limiter-task12 -f compose.test.yaml --profile current --profile topology \
  up -d --wait --wait-timeout 90 redis-current cluster sentinel
REQUIRE_REDIS_CLUSTER=1 REDIS_CLUSTER_ADDRS=127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002 \
REQUIRE_REDIS_SENTINEL=1 REDIS_SENTINEL_ADDRS=127.0.0.1:27379 \
REDIS_SENTINEL_MASTER=limiter-test REDIS_SENTINEL_ISOLATED=1 \
  go test -race -timeout 2m -run '^TestRedis(Cluster|Sentinel)Integration$' -count=1 .
docker compose -p rate-limiter-task12 -f compose.test.yaml --profile current --profile topology \
  down --volumes --remove-orphans
```

Only this project's fixtures were removed; the existing Redis 7 fixture remained.
No required local topology was omitted. Hosted CI, long-duration soak tests,
other hardware/network/fleet shapes, and durability under Redis data loss were
not established by these checks. Final whole-branch review remains a separate
checkpoint; these measurements do not by themselves declare production readiness.

## Component p99 follow-up (2026-10-20)

A focused rerun measured the existing supported simulated point: 100 uniform
active keys, 16 callers, 5,000 offered requests/s, four workers and a requested
1ms delay for each backend call. Environment: Apple M1 Pro, darwin/arm64,
Darwin 24.0.0, Go 1.27.2, default GOMAXPROCS=8, shared development laptop.
Warmup was 1.001395958s and traffic lasted 5.000772417s. All 25,000 requests were
normal admissions; observed stale-authorization, capacity denial and fallback
rates were 0%. This adds component timing evidence at the same operating point;
it does not expand the supported workload or establish long-duration tails.

| Component | p99 upper bin boundary | Samples |
| --- | ---: | ---: |
| Periodic due-to-enqueue scheduling delay | 33.554432ms | 5,229 |
| Queue delay | 50ms | 5,331 |
| Backend operation latency | 4.194304ms | 5,429 |

These histograms cover warmup plus traffic through their end-of-traffic
snapshots, before accelerated cleanup. Operation samples include every completed
TIME, PING or Execute call, not only Execute; this run had 100 TIME calls and no
PING calls. The test-only operation histogram measures elapsed adapter-call
occupancy. It is distinct from the existing sync-duration metric, which can
include both TIME and Execute in one cold-key attempt. The operation and scan
histograms each have 64 power-of-two nanosecond bins: values above are upper bin
boundaries, with up to 2x rounding for positive observations. Queue p99 uses the
existing production histogram, with boundaries 0.1/0.5/1/5/10/50/100/500ms and
1/5s, then +Inf; it has coarser precision. Snapshot populations can differ due to
in-flight work and queued turns that do not issue a backend call.

The scan measurement uses existing timestamps in the test harness, with no new
runtime instrumentation. Worker preparation advances `nextSyncAt` before calling
the backend, so the harness retains that next deadline for the following
operation, replacing it after every Execute, including early threshold work.
For each later operation whose enqueue timestamp is at or after the preceding
deadline, it records enqueue time minus that deadline. The 100 initial operations
had no preceding deadline and were excluded; there were zero early-trigger
exclusions in this run. The deadline map is bounded by the retained test key
population, and histogram storage is fixed. This measurement applies to healthy
runs where prepared operations reach the adapter; it does not reconstruct missed
attempts during cancellation or outages.

This is observed periodic scheduling delay, not scanner execution duration.
A notification can replace the enqueue timestamp in the short interval between
worker dequeue and attempt preparation, so that interval can be included in the
scan estimate. Once the attempt is busy, its timestamps are stable. Queue delay
uses the timestamp captured at dequeue. The additional test-only bucket lookup,
lock and histogram updates contribute measurement overhead; no timing callback
was added to Allow. Percentiles and their sum are not worst-case latency bounds.
The earlier sampled scan maximum remains a separate backlog-age observation.

Reproduce this focused measurement (without the 48-cell matrix):

```sh
RUN_CAPACITY=1 GOTOOLCHAIN=go1.27.2 GOCACHE=/tmp/rate-limiter-go-cache \
  go test -run '^Test(CapacityHistogram|CapacityScanTimingUsesPreviousDeadline|LoadSupportedPoint)$' -v -count=1 .
```
