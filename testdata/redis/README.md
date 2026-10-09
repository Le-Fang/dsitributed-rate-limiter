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
