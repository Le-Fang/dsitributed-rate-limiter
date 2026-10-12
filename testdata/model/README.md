# Executable accounting model

`go test -run '^TestStateMachine' -count=1 .` replays every JSON file here, runs
1,000 fixed seeds of exactly 1,000 events, and explores the finite schedule set
in `TestStateMachineExhaustive` through depth 12. No Redis is required for these
core tests. Existing Redis differential tests run when `REDIS_ADDR` is set.

The runner calls production admission, prepare/install, circuit, store and close
transitions. The independent receipt oracle uses rational local balances and
integer Redis numerators; it never uses production refill/ledger/Lua functions
to calculate an expected answer. The backend's cumulative actual deductions
are compared with receipt classifications after every valid event. Committed
but unacknowledged batches count as applied. Crash loss is explicit, and late
commits transfer consumption from loss to applied. Crashes start a new envelope
segment; no intact-history claim crosses a crash.

Requests, backend execution and replies are separate events. `Checkpoint`
`before` and `after` simulate failure around the test backend's atomic commit;
they do not inject arguments into production Lua. `DeltaOverride` tests a
mismatched duplicate without changing the original immutable attempt. Its event
is valid only after that same sequence has committed; changing a first deduction
would fabricate transport corruption outside the protocol contract.

Invalid transitions are skipped before either side advances. Unknown event
names are errors. `Initialize`, `AdvanceTime`, `RequireNormal`, queue helpers and
`CloseBudgetExpired` are legacy deterministic fixture conveniences; the public
schedule alphabet includes separately staged INIT and independent clocks.

Normal envelopes use the running minimum of `A(s)-R*s`, including the left and
right boundary of simultaneous admissions. Short traces cross-check every
pair with exact integers. Normal allowance is `B+2*N*C+R*1ms+0.000001`.
Fallback envelopes are independent per episode. Logical Redis time drives the
normal envelope; independent local clock drift drives authorization and local
refill tests. Physical-time claims still require the documented clock assumptions.

## Finite exhaustive search

The exact domain is two authorized instances, one key, `C=B=local burst=1`, and
rate one token/s. Costs `{1,C}` coincide. The refill tick and freshness boundary
both equal one second. Elapsed time is bounded to `{0,1s}`; there is one transport
attempt per instance. All admission/prepare/execute/deliver/loss/timeout choices,
zero advances and the one positive tick are enumerated to depth 12 after setup.
This search is exhaustive for that finite domain, not for arbitrary retries,
clock histories or capacities. Larger costs, retries, faults, restart, fallback
and lifecycle schedules are covered by the independent retained/generated suites.

BFS merges only identical serialized states after checking the transition.
The hash includes ledger/receipt state, request identities and responses, queue
order, clocks, generation, authorization/latches, local balances/refill deadlines,
Redis expiry/session state and envelope minima. Pointer identity is excluded.
A shortest-depth visit has at least as much remaining search budget as a later
visit. Invalid/self-loop schedules add no state. An independent frontier skips only
invalid transport actions and strict zero-time no-ops before subject replay.
Depth-5 optimized and original searches must produce exactly the same state
hashes, and depth 12 must retain the original 19,070-state count. The time budget is encoded by
the clock, so it cannot be pruned inconsistently.

## Generated schedules and fuzzing

SplitMix64 is defined in source; little-endian eight-byte words have a fixed
mapping. Each 64-event episode includes a 38-event valid retry/recovery schedule,
followed by 26 byte-mapped adversarial events. Restarting episodes prevents a
closed instance from making most of the run vacuous. Ordinary CI requires at
least 10,000 normal admissions, 5,000 fallback admissions, and 10,000 executions
of each retry, backend execution, reply delivery, successful probe, crash and
restart class. Every event kind must actually execute at least once.

`FuzzStateMachine` seeds compact, eligible traces plus both failure policies.
JSON configuration is independently checked after applying model defaults,
before any core execution: positive normalized rates up to 10^12 microtokens/s,
global burst up to 10^6, local burst no greater than global burst or outstanding
cap, freshness at most 30s, positive sync interval below freshness, positive idle
TTL, and the supported policy/fallback relationships. The resource domain stays
at 1–4 instances, outstanding cap 1–8, and at most 2,048 keys. Tiny freshness
intervals are valid when sync is smaller; attempt timing remains explicitly
controlled by events rather than the harness's transport timeout.

The fixed mapper and seed preparation clear inert fallback fields for FailClosed;
all other mapped values and the event mapping/PRNG are unchanged. Arbitrary JSON
is validated as received and is never repaired. All five reduced regressions
remain eligible seeds. Model defaults are shared by the runner and domain filter,
independently of production normalization.

Core-only fixtures still run unfiltered in every deterministic trace run. The
257-instance slot fixture exceeds the fuzz resource bound; three fallback
negative-recovery fixtures and expired-slot-retained-negative-debt deliberately
use a local burst greater than global burst and therefore exceed the supported
configuration domain for JSON mutation. None is a discarded failing fuzz input.
Fair-suffix assertion helpers are removed from fuzz seeds because mutations need
not preserve their scheduling preconditions.

```sh
go test -run=^$ -fuzz=FuzzStateMachine -fuzztime=60s -fuzzminimizetime=1s -parallel=4 .
```

Bounding Go's coverage minimization prevents large JSON seeds from consuming
the entire fuzz interval. The model's own deterministic failure reducer still
deletes ranges and shrinks costs/durations while preserving the first invariant
category. It writes a minimized JSON before failing. The reducer promises a
deterministic category-preserving reduction, not a globally shortest trace.
`FuzzProtocolReply` and `FuzzNumericBoundaries` cover adapter integer limits
and numeric conversions independently.
