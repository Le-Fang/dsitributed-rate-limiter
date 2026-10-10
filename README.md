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
