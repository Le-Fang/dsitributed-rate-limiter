# Redis protocol fixtures

`foundation.json` separates admission, execution, reply loss and delivery. The
independent oracle uses arbitrary-precision integer numerators, while production
Lua uses bounded arithmetic. Differential tests compare returned logical Redis
timestamps, balances, retry time, sequence, identity and fresh initialization.

Run a dedicated fixture with `docker compose -f compose.test.yaml up -d --wait`.
Then run:

```
REQUIRE_REDIS=1 REDIS_ADDR=127.0.0.1:16380 REDIS_ISOLATED=1 go test -run 'Test(Redis|Protocol|StateMachineFoundation)' -count=1 .
```

`REDIS_ISOLATED=1` explicitly permits SCRIPT FLUSH for NOSCRIPT tests; use it only
on a disposable Redis instance. Each test deletes only its own keys. No test
uses FLUSHDB or FLUSHALL. Without REDIS_ADDR, tests explicitly skip; setting
REQUIRE_REDIS=1 makes a missing address fail. Redis 7 integration is required in CI.

Clock edge tests replace the TIME line in memory. Commit fault tests inject an
error at the write boundaries following the unique COMMIT marker. Production
Lua has no fault/clock arguments; unmodified production Lua also executes with
real Redis TIME in foundation, adapter and differential tests. A fault after
PERSIST can intentionally leave a persistent key; a successful retry restores
safe expiry. Extreme TTL assertions read integer milliseconds directly because
Go time.Duration cannot represent the full Redis expiry range.

## Required conformance and topology coverage

The standalone CI matrix pins `redis:7.4.11-alpine` and
`redis:8.10.2-alpine`. Both run the required integration suite with the race
detector. The independent rational model is compared with unmodified Lua using
returned logical timestamps, including every global hash field, fractional
remainder, session JSON, current decision, sequence, retry delay and a
server-time bracket for TTL. Separate tests cover simultaneous clients,
duplicate submissions, actual committed replies discarded before retry,
physical key expiry, missing/reused sessions, corrupt state and pool exhaustion.
The controlled-clock and commit-checkpoint tests remain the exact boundary tests.
`REQUIRE_REDIS=1` also requires `REDIS_ISOLATED=1`, so required NOSCRIPT coverage
cannot silently skip.

The topology fixtures use one disposable container per topology. Three Cluster
masters share a network namespace and advertise `127.0.0.1:17000` through `17002`;
the same ports are published on the host. This avoids Docker-internal addresses
that host-side clients cannot reach. Sentinel uses a master, one replica and one
Sentinel in its own namespace, with matching host/advertised ports 17379, 17380
and 27379. These are isolated test topologies, not production deployment examples.

```
docker compose -p limiter-check -f compose.test.yaml --profile current --profile topology up -d --wait --wait-timeout 90 redis-current cluster sentinel
REQUIRE_REDIS=1 REDIS_ADDR=127.0.0.1:16381 REDIS_ISOLATED=1 go test -race -timeout 3m -run '^TestRedisIntegration' -count=1 .
REQUIRE_REDIS_CLUSTER=1 REDIS_CLUSTER_ADDRS=127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002 REQUIRE_REDIS_SENTINEL=1 REDIS_SENTINEL_ADDRS=127.0.0.1:27379 REDIS_SENTINEL_MASTER=limiter-test REDIS_SENTINEL_ISOLATED=1 go test -race -timeout 2m -run '^TestRedis(Cluster|Sentinel)Integration$' -count=1 .
docker compose -p limiter-check -f compose.test.yaml --profile current --profile topology down --volumes --remove-orphans
```

Cluster tests exercise INIT/APPLY/duplicate retries across all three owners via
an injected `redis.UniversalClient`. The Sentinel test requests an actual
`SENTINEL FAILOVER`, observes a different master and demotion of the old master, verifies the next
sequence directly on the promoted node, and bounds discovery/recovery
to 30 seconds. It commits an APPLY and confirms replication with `WAIT` on the
same connection before discarding the reply; the unchanged session/slot/sequence/
delta retry must return DUPLICATE on the promoted master, followed by a successful
next batch. `WAIT` makes this an intact-history test. It does not promise durable
writes or protect the accounting envelope from arbitrary failover data loss.
Redis asynchronous replication can lose writes; that limitation still applies.

Only run the failover test on its disposable fixture: it deliberately changes
which server is master. Recreate the topology before a second run, so readiness
and replication start from the declared configuration. CI always tears down the
named project and its volumes, even after failure. Test keys have unique prefixes
and cleanup deletes only those keys. No fixture or test touches other projects.
Missing addresses explicitly skip locally; `REQUIRE_REDIS_CLUSTER=1` and
`REQUIRE_REDIS_SENTINEL=1` convert missing coverage into failures in CI.

Version pins were checked on 2026-10-13 against the
[official Redis image manifest](https://github.com/docker-library/official-images/blob/master/library/redis),
[official Go downloads](https://go.dev/dl/),
[checkout release](https://github.com/actions/checkout/releases/tag/v7.0.1) and
[setup-go release](https://github.com/actions/setup-go/releases/tag/v7.0.0).
CI keeps the Go 1.21.x source-compatibility floor and pins supported Go 1.27.2
for build/test/vet/race and the required vulnerability scan. The action commits
are full revision pins. Hosted CI results must still be checked before release;
a local run is not evidence of a hosted CI pass.
