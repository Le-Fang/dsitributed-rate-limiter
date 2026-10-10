package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lifecycle tests control transport completion independently of cancellation.
// Blocking after cancellation models join time without sleeping for the budget.
type lifecycleBackend struct {
	backend
	execute    func(context.Context, string, command) (reply, error)
	closeCount atomic.Int64
	closeErr   error
}

func (b *lifecycleBackend) Execute(ctx context.Context, key string, c command) (reply, error) {
	return b.execute(ctx, key, c)
}
func (b *lifecycleBackend) Close() error { b.closeCount.Add(1); return b.closeErr }
func lifecycleReady(t *testing.T) (*Limiter, *bucket, *fakeClock, *fakeBackend) {
	t.Helper()
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, LocalBurst: 10, MaxOutstanding: 10, SyncThreshold: 3})
	b := testBucket(t, l, "private/application-key")
	initializeTestBucket(t, l, b, 100e6)
	return l, b, c, f
}
func lifecycleAck(c command) reply {
	return reply{Outcome: outcomeOK, CommittedSequence: c.Sequence, BalanceU: 100e6, SessionTTLMS: 86400000}
}

func TestLifecycleSharedWaitersAndUncertainBatch(t *testing.T) {
	l, b, _, f := lifecycleReady(t)
	first := make(chan command, 1)
	flushed := make(chan command, 2)
	release := make(chan struct{})
	var calls atomic.Int64
	var active atomic.Int64
	transport := &lifecycleBackend{backend: f}
	transport.execute = func(ctx context.Context, key string, c command) (reply, error) {
		if active.Add(1) != 1 {
			t.Error("flush overlaps worker")
		}
		defer active.Add(-1)
		if calls.Add(1) == 1 {
			first <- c
			<-ctx.Done()
			return reply{}, ctx.Err()
		}
		flushed <- c
		select {
		case <-release:
			return lifecycleAck(c), nil
		case <-ctx.Done():
			return reply{}, ctx.Err()
		}
	}
	l.backend = transport
	l.start()
	if ok, _, _ := l.AllowN(b.key, 3); !ok {
		t.Fatal("first admission")
	}
	original := <-first
	if ok, _, _ := l.AllowN(b.key, 2); !ok {
		t.Fatal("pending admission")
	}
	short, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Close(short); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled waiter: %v", err)
	}
	retry := <-flushed
	if retry != original {
		t.Fatalf("changed retry: %+v != %+v", retry, original)
	}
	if ok, r, _ := l.Allow(b.key); ok || r != ReasonClosed {
		t.Fatalf("closed admission %v %s", ok, r)
	}
	const waiters = 12
	results := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() { results <- l.Close(context.Background()) }()
	}
	close(release)
	next := <-flushed
	if next.Sequence != original.Sequence+1 || next.Delta != 2 || next.Session != original.Session {
		t.Fatal("pending behind uncertain batch lost", next)
	}
	for i := 0; i < waiters; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(short); err != nil {
		t.Fatal("completed result not reused", err)
	}
	if transport.closeCount.Load() != 1 || calls.Load() != 3 || b.ledger.outstanding() != 0 {
		t.Fatal("cleanup not shared or accounting lost")
	}
	l.scanDue(l.clock.Now())
	l.notify(b)
	if len(l.work) != 0 || calls.Load() != 3 {
		t.Fatal("work after completion")
	}
}

// Exposes a deterministic deadline error while using standard cancellation to
// unblock the transport. The production factory remains context.WithTimeout.
type lifecycleDeadline struct {
	context.Context
	at time.Time
}

func (c lifecycleDeadline) Deadline() (time.Time, bool) { return c.at, true }
func (c lifecycleDeadline) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}
func TestLifecycleBudgetIncludesJoin(t *testing.T) {
	l, b, c, f := lifecycleReady(t)
	ctx, expire := context.WithCancel(context.Background())
	defer expire()
	budget := lifecycleDeadline{ctx, c.Now().Add(5 * time.Second)}
	started := make(chan struct{})
	l.shutdownContext = func() (context.Context, context.CancelFunc) { close(started); return budget, expire }
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	transport := &lifecycleBackend{backend: f}
	transport.execute = func(ctx context.Context, key string, cmd command) (reply, error) {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return reply{}, ctx.Err()
	}
	l.backend = transport
	l.start()
	l.AllowN(b.key, 3)
	<-entered
	result := make(chan error, 1)
	go func() { result <- l.Close(context.Background()) }()
	<-started
	<-canceled
	c.Advance(5 * time.Second)
	expire()
	close(release)
	err := <-result
	var shutdown *ShutdownError
	if !errors.As(err, &shutdown) || !errors.Is(err, context.DeadlineExceeded) || shutdown.UnflushedKeys != 1 || shutdown.UnflushedTokens != 3 {
		t.Fatalf("aggregate timeout: %#v", err)
	}
	if calls.Load() != 1 || transport.closeCount.Load() != 1 {
		t.Fatal("fresh flush budget after join")
	}
	if strings.Contains(err.Error(), b.key) {
		t.Fatal("raw key in error")
	}
	if again := l.Close(context.Background()); again != err {
		t.Fatal("stored result changed")
	}
}

func TestLifecycleParentCancellationDuringFallback(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, LocalBurst: 5, MaxOutstanding: 5, FailurePolicy: LocalOnly, FallbackRate: 1, FallbackBurst: 2, Parent: parent})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 100e6)
	l.AllowN("k", 2)
	c.Advance(2 * time.Second)
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
		t.Fatalf("fallback %v %s", ok, r)
	}
	var debt atomic.Int64
	var calls atomic.Int64
	entered := make(chan struct{})
	var original command
	transport := &lifecycleBackend{backend: f, execute: func(ctx context.Context, key string, c command) (reply, error) {
		if calls.Add(1) == 1 {
			// The ordinary worker inherits parent cancellation. Hold it here so
			// this test cannot mistake its canceled attempt for the final flush.
			original = c
			close(entered)
			<-ctx.Done()
			return reply{}, ctx.Err()
		}
		select {
		case <-l.workersDone:
		default:
			t.Error("final flush started before workers joined")
		}
		if c != original {
			t.Errorf("final flush changed batch identity: got %+v, want %+v", c, original)
		}
		if ctx.Err() != nil {
			t.Error("parent canceled flush context")
		}
		debt.Add(c.Delta)
		return lifecycleAck(c), nil
	}}
	l.backend = transport
	l.start()
	// Cancellation also releases an entered worker when the entry watchdog fails.
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("ordinary worker did not enter before parent cancellation")
	}
	cancel()
	for i := 0; i < 100; i++ {
		if ok, r, _ := l.Allow("k"); ok || r != ReasonClosed {
			t.Fatalf("post-cancel %v %s", ok, r)
		}
	}
	select {
	case <-l.shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not clean up")
	}
	if err := l.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if debt.Load() != 2 || transport.closeCount.Load() != 1 || calls.Load() != 2 {
		t.Fatalf("fallback replay/cleanup: debt=%d closes=%d calls=%d", debt.Load(), transport.closeCount.Load(), calls.Load())
	}
}

func TestLifecycleSharedAggregateError(t *testing.T) {
	l, b, _, f := lifecycleReady(t)
	l.AllowN(b.key, 2)
	secret := errors.New("redis://user:password@host private/application-key")
	transport := &lifecycleBackend{backend: f, execute: func(context.Context, string, command) (reply, error) { return reply{}, secret }}
	l.backend = transport
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- l.Close(context.Background()) }()
	}
	wg.Wait()
	close(results)
	var first error
	for err := range results {
		if first == nil {
			first = err
		}
		if err != first {
			t.Fatal("distinct cleanup results")
		}
	}
	var shutdown *ShutdownError
	if !errors.As(first, &shutdown) || shutdown.UnflushedKeys != 1 || shutdown.UnflushedTokens != 2 || !errors.Is(first, secret) {
		t.Fatalf("result %#v", first)
	}
	if strings.Contains(first.Error(), "password") || strings.Contains(first.Error(), b.key) {
		t.Fatal("sensitive cause rendered", first)
	}
	if transport.closeCount.Load() != 1 {
		t.Fatal("multiple closes")
	}
}

func TestLifecycleUnstartedCoreDrainsQueueReferences(t *testing.T) {
	l, b, _, f := lifecycleReady(t)
	l.notify(b)
	l.backend = &lifecycleBackend{backend: f, execute: func(context.Context, string, command) (reply, error) {
		t.Error("clean shutdown performed I/O")
		return reply{}, nil
	}}
	if err := l.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	sh := &l.store.shards[shardIndex(b.key)]
	sh.mu.Lock()
	refs := b.refs
	sh.mu.Unlock()
	if refs != 1 || b.queued || len(l.work) != 0 {
		t.Fatalf("shutdown retained queued ref: refs=%d queued=%v queue=%d", refs, b.queued, len(l.work))
	}
}

func TestLifecycleShortWaiterDoesNotCancelFlush(t *testing.T) {
	l, b, _, f := lifecycleReady(t)
	l.AllowN(b.key, 2)
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	l.backend = &lifecycleBackend{backend: f, execute: func(ctx context.Context, key string, c command) (reply, error) {
		entered <- ctx
		select {
		case <-release:
			return lifecycleAck(c), nil
		case <-ctx.Done():
			return reply{}, ctx.Err()
		}
	}}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- l.Close(short) }()
	flushCtx := <-entered
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("short waiter", err)
	}
	if flushCtx.Err() != nil {
		t.Fatal("short waiter canceled cleanup")
	}
	close(release)
	if err := l.Close(context.Background()); err != nil {
		t.Fatal("later waiter", err)
	}
}

func TestLifecycleTransientFinalRetryPreservesIdentity(t *testing.T) {
	l, b, c, f := lifecycleReady(t)
	l.AllowN(b.key, 2)
	var calls []command
	var mu sync.Mutex
	l.backend = &lifecycleBackend{backend: f, execute: func(ctx context.Context, key string, c command) (reply, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, c)
		if len(calls) == 1 {
			return reply{}, transientFailure()
		}
		return lifecycleAck(c), nil
	}}
	result := make(chan error, 1)
	go func() { result <- l.Close(context.Background()) }()
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.timers) == 1 })
	c.Advance(99 * time.Millisecond)
	mu.Lock()
	n := len(calls)
	mu.Unlock()
	if n != 1 {
		t.Fatal("unpaced retry")
	}
	c.Advance(time.Millisecond)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != calls[1] {
		t.Fatal("changed retry identity", calls)
	}
}

func TestLifecycleTerminalDebtAndClientCloseError(t *testing.T) {
	l, b, _, f := lifecycleReady(t)
	l.AllowN(b.key, 2)
	b.mu.Lock()
	b.terminalReason = ReasonAccountingExpired
	b.mu.Unlock()
	closeCause := errors.New("credential in close error")
	l.backend = &lifecycleBackend{backend: f, closeErr: closeCause, execute: func(context.Context, string, command) (reply, error) {
		t.Error("replayed terminal debt")
		return reply{}, nil
	}}
	err := l.Close(context.Background())
	var shutdown *ShutdownError
	if !errors.As(err, &shutdown) || shutdown.UnflushedTokens != 2 || !errors.Is(err, closeCause) {
		t.Fatalf("terminal error %#v", err)
	}
	if strings.Contains(err.Error(), "credential") {
		t.Fatal("client close error exposed")
	}
}

func TestLifecycleCancellationAtAdmissionDecision(t *testing.T) {
	for _, parentCancel := range []bool{true, false} {
		t.Run(fmt.Sprint("parent=", parentCancel), func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, Parent: parent})
			b := testBucket(t, l, "k")
			initializeTestBucket(t, l, b, 10e6)
			l.backend = &lifecycleBackend{backend: f, execute: func(context.Context, string, command) (reply, error) {
				t.Error("closed decision created debt")
				return reply{}, nil
			}}
			l.clock = &decisionClock{clock: c, change: func() {
				if parentCancel {
					cancel()
				} else {
					l.beginShutdown()
				}
			}}
			if ok, r, _ := l.Allow("k"); ok || r != ReasonClosed {
				t.Fatalf("decision %v %s", ok, r)
			}
			if err := l.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if b.ledger.outstanding() != 0 || b.tokens.tokens != 10 {
				t.Fatal("admission consumed after cancellation")
			}
		})
	}
}

type lifecycleRedisClient struct {
	redis.UniversalClient
	closes atomic.Int64
}

func (c *lifecycleRedisClient) Close() error { c.closes.Add(1); return nil }
func TestLifecycleOwnedAndInjectedClientCloseCounts(t *testing.T) {
	for _, owned := range []bool{true, false} {
		t.Run(fmt.Sprint("owned=", owned), func(t *testing.T) {
			l, _, _, _ := lifecycleReady(t)
			client := &lifecycleRedisClient{}
			l.backend = &redisBackend{client: client, owned: owned}
			l.start()
			if err := l.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := l.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if owned {
				want = 1
			}
			if client.closes.Load() != want {
				t.Fatalf("Close calls=%d want=%d", client.closes.Load(), want)
			}
		})
	}
}

func TestLifecycleFreshBackgroundBudget(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	l, _, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 10, Parent: parent})
	before := time.Now()
	ctx, stop := l.shutdownContext()
	defer stop()
	deadline, ok := ctx.Deadline()
	if !ok || deadline.Before(before.Add(5*time.Second)) || deadline.After(time.Now().Add(5*time.Second)) || ctx.Err() != nil {
		t.Fatal("shutdown deadline is not an independent five seconds")
	}
	if err := l.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
