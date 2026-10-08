# Repository guidance

## Commits

- Keep commit messages concise: use an imperative subject, preferably under 50 characters and no longer than 72.
- Describe the change directly, for example `Fix duplicate batch accounting` or `Add Redis integration tests`.
- Add a short body only when needed to explain a non-obvious reason or trade-off. Omit task transcripts and routine test output.
- Keep each commit focused and reviewable. Do not commit unrelated changes or push unless requested.
- Fold fix commits into their original feature commits before marking a task complete; use fixup/autosquash during review so the final history has one coherent commit per feature task. Do not rewrite shared history or force-push without explicit authorization.

## Scope and design

- Build an embedded Go library, not a separate service. Preserve Go 1.21 source compatibility; recommend a currently supported Go toolchain for production.
- Read the approved spec and implementation plan under `docs/superpowers/` before implementation. Keep work within their scope; surface contradictions rather than silently changing the contract.
- `/docs/` is intentionally Git-ignored. Do not force-add it. Put public usage and operational documentation in tracked files such as `README.md` and `BENCHMARKS.md`.
- Prefer small, idiomatic components and minimal dependencies. Do not add deferred features or speculative abstractions.

## Clean Architecture

- Follow Clean Architecture: dependencies point inward toward the rate-limiting rules. Keep domain rules independent of Redis, HTTP, logging, and metrics implementations.
- Keep token-bucket/accounting rules in the domain; admission, synchronization, recovery, and lifecycle orchestration in the application layer; Redis, HTTP, and observability implementations in adapters.
- Define small interfaces at the consuming application boundary. Adapters implement those interfaces; translate Redis wire values and errors before they reach admission logic.
- Use the public constructor as the composition root. Concrete clients and public connection settings belong there, not in core policy/state types.
- Test the core without Redis or an HTTP server, and test adapters separately against their real dependencies. Keep the independent accounting model separate from production rules.
- Keep boundaries explicit without adding generic repositories, dependency-injection frameworks, or interfaces for every struct. Preserve the fast local path.

## Correctness and reliability

- Keep `Allow` free of network I/O, background-sync waits, and logging. Bound memory, queues, workers, retries, and operation durations.
- Preserve immutable batch identities on retries. Never discard unresolved normal consumption or replay fallback traffic into Redis.
- Treat admission, accounting, failure-policy transitions, and shutdown as concurrency-sensitive state changes. Follow a documented lock order and keep external callbacks outside locks.
- Keep metrics cardinality bounded and logs free of credentials and raw application keys. Dependency failures must follow the explicit failure policy.

## Verification and delivery

- Add meaningful tests for behavior changes. Use deterministic clocks and backend controls for correctness tests, and an independent model for accounting invariants.
- Establish CI and model checks early; expand them alongside implementation. Test actual Lua against Redis and measure capacity before making throughput claims.
- Run checks appropriate to the change. Before release, require formatting, unit and race tests, `go vet`, vulnerability scanning, required Redis integration coverage, and the plan's capacity gates.
- Report what was verified and what was skipped. Never describe unrun checks as passing or equate a written plan with production readiness.
- Preserve user changes and keep diffs focused. Update public documentation when behavior or operational limits change.
