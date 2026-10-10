package ratelimiter

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		runtime.Gosched()
	}
}
func startTestScheduler(t *testing.T, l *Limiter) {
	t.Helper()
	l.start()
	t.Cleanup(func() { l.cancelWorkers(); <-l.workersDone })
}
func TestSchedulerBoundedQueueAndFairDrain(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 10000, GlobalBurst: 10000, LocalBurst: 5, MaxOutstanding: 5, SyncThreshold: 5, MaxKeys: 1200})
	hot := testBucket(t, l, "hot")
	initializeTestBucket(t, l, hot, 10000e6)
	if ok, _, _ := l.AllowN("hot", 5); !ok {
		t.Fatal("hot preauthorization")
	}
	startTestScheduler(t, l)
	for i := 0; i < 1100; i++ {
		l.Allow(fmt.Sprint(i))
	}
	eventually(t, func() bool { return len(f.queued()) == 4 })
	if f.MaxConcurrent() > 4 || len(l.work) > 1024 {
		t.Fatal("unbounded work")
	}
	if ok, r, _ := l.Allow("hot"); ok || r != ReasonSyncPending {
		t.Fatalf("cap: %v %s", ok, r)
	}
	c.Advance(2 * time.Second)
	if ok, r, _ := l.Allow("hot"); ok || r != ReasonRedisUnavailable {
		t.Fatalf("freshness: %v %s", ok, r)
	}
	if l.control.Load().circuit != circuitHealthy {
		t.Fatal("queue fabricated outage")
	}
	seen := map[string]bool{}
	delivered := 0
	eventually(t, func() bool {
		calls := f.queued()
		for ; delivered < len(calls); delivered++ {
			call := calls[delivered]
			seen[call.key] = true
			f.deliver(call, reply{Outcome: outcomeOK, Slot: 0, CommittedSequence: call.command.Sequence, BalanceU: 10000e6, SessionTTLMS: 86400000}, nil)
		}
		l.scanDue(c.Now())
		return len(seen) == 1101
	})
	if f.MaxConcurrent() > 4 {
		t.Fatal("worker bound")
	}
	eventually(t, func() bool { hot.mu.Lock(); defer hot.mu.Unlock(); return hot.ledger.outstanding() == 0 })
}
func TestSchedulerThresholdPeriodicAndBusy(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, LocalBurst: 10, MaxOutstanding: 10, SyncThreshold: 3})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 100e6)
	startTestScheduler(t, l)
	l.AllowN("k", 2)
	if len(f.queued()) != 0 {
		t.Fatal("below threshold")
	}
	c.Advance(l.cfg.syncInterval)
	l.scanDue(c.Now())
	eventually(t, func() bool { return len(f.queued()) == 1 })
	l.AllowN("k", 3)
	for i := 0; i < 10; i++ {
		l.notify(b)
		l.scanDue(c.Now())
	}
	if len(f.queued()) != 1 {
		t.Fatal("overlapping key attempts")
	}
	call := f.queued()[0]
	if call.command.Delta != 2 {
		t.Fatal(call.command)
	}
	f.deliver(call, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 100e6, SessionTTLMS: 86400000}, nil)
	eventually(t, func() bool { return len(f.queued()) == 2 })
	if f.queued()[1].command.Delta != 3 {
		t.Fatal("pending consumption lost")
	}
}
func TestSchedulerInitAndRetryIdentity(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100})
	l.jitter = func(d time.Duration) time.Duration { return 0 }
	startTestScheduler(t, l)
	l.Allow("k")
	eventually(t, func() bool { return len(f.queued()) == 1 })
	first := f.queued()[0]
	if first.command.Op != opInit || first.command.RegistrationDeadlineMS != c.Now().UnixMilli()+30000 {
		t.Fatal(first.command)
	}
	f.deliver(first, reply{}, &backendError{class: errorTransient, cause: context.DeadlineExceeded})
	eventually(t, func() bool { return l.control.Load().circuit == circuitOpen })
	l.scanDue(c.Now())
	eventually(t, func() bool { l.scanDue(c.Now()); return len(f.queued()) == 2 })
	if f.queued()[1].command != first.command {
		t.Fatal("ambiguous INIT identity changed")
	}
	f.deliver(f.queued()[1], reply{Outcome: outcomeInitExpired}, nil)
	eventually(t, func() bool { l.scanDue(c.Now()); return len(f.queued()) == 3 })
	if f.queued()[2].command.Session == first.command.Session {
		t.Fatal("expired INIT reused")
	}
	if f.timeCalls.Load() != 2 {
		t.Fatalf("TIME calls=%d", f.timeCalls.Load())
	}
}

func TestSchedulerCleanSequenceRotation(t *testing.T) {
	l, _, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 100e6)
	b.mu.Lock()
	old := b.session
	b.ledger.Committed = maxSequence
	b.mu.Unlock()
	startTestScheduler(t, l)
	l.Allow("k")
	eventually(t, func() bool { return len(f.queued()) == 1 })
	c := f.queued()[0].command
	if c.Op != opInit || c.Session == old {
		t.Fatalf("rotation: %+v", c)
	}
}
func TestSchedulerDelayedTimeCannotExtendWindow(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100})
	f.timeFn = func(context.Context) (time.Time, error) {
		sampled := c.Now()
		c.Advance(30 * time.Second)
		return sampled, nil
	}
	b := testBucket(t, l, "k")
	a, ok := l.prepareAttempt(b, c.Now())
	if !ok {
		t.Fatal("claim")
	}
	l.runAttempt(context.Background(), b, a)
	if len(f.queued()) != 0 || b.init != nil || b.busy {
		t.Fatal("delayed TIME extended registration")
	}
}
func TestSchedulerProbeBackoffAndCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, Parent: parent})
	l.jitter = func(d time.Duration) time.Duration { return d - time.Nanosecond }
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	f.probeFn = func(ctx context.Context) error {
		entered <- struct{}{}
		select {
		case <-release:
			return &backendError{class: errorTransient, cause: context.DeadlineExceeded}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	startTestScheduler(t, l)
	l.recordFailure(&backendError{class: errorTransient, cause: context.DeadlineExceeded})
	l.scanDue(c.Now())
	c.Advance(99 * time.Millisecond)
	l.scanDue(c.Now())
	if f.probeCalls.Load() != 0 {
		t.Fatal("probe before jitter deadline")
	}
	c.Advance(time.Millisecond)
	l.scanDue(c.Now())
	<-entered
	for i := 0; i < 20; i++ {
		l.scanDue(c.Now())
	}
	if f.probeCalls.Load() != 1 || f.MaxConcurrent() != 1 {
		t.Fatal("overlapping probes")
	}
	close(release)
	eventually(t, func() bool {
		l.circuitMu.Lock()
		defer l.circuitMu.Unlock()
		return !l.probeQueued && l.probeBackoff == 200*time.Millisecond
	})
	c.Advance(199 * time.Millisecond)
	l.scanDue(c.Now())
	if f.probeCalls.Load() != 1 {
		t.Fatal("exponential backoff bypassed")
	}
	cancel()
	select {
	case <-l.workersDone:
	case <-time.After(time.Second):
		t.Fatal("backoff did not cancel")
	}
}
func TestSchedulerSessionCapacityRetryFloor(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100})
	startTestScheduler(t, l)
	l.Allow("k")
	eventually(t, func() bool { return len(f.queued()) == 1 })
	f.deliver(f.queued()[0], reply{Outcome: outcomeSessionCapacity}, nil)
	b := testBucket(t, l, "k")
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return !b.capacityUntil.IsZero() })
	c.Advance(999 * time.Millisecond)
	l.scanDue(c.Now())
	l.Allow("k")
	if len(f.queued()) != 1 {
		t.Fatal("capacity retry before 1s")
	}
	c.Advance(time.Millisecond)
	l.scanDue(c.Now())
	eventually(t, func() bool { return len(f.queued()) == 2 })
	if f.queued()[0].command != f.queued()[1].command {
		t.Fatal("capacity retry identity changed")
	}
}
func TestSchedulerSaturationFallbackDoesNotReplay(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, LocalBurst: 5, MaxOutstanding: 5, FailurePolicy: LocalOnly, FallbackRate: 1, FallbackBurst: 2})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 100e6)
	l.AllowN("k", 5)
	startTestScheduler(t, l)
	eventually(t, func() bool { return len(f.queued()) == 1 })
	c.Advance(2 * time.Second)
	if ok, r, _ := l.Allow("k"); !ok || r != ReasonLocalFallback {
		t.Fatalf("fallback: %v %s", ok, r)
	}
	b.mu.Lock()
	out := b.ledger.outstanding()
	b.mu.Unlock()
	if out != 5 {
		t.Fatalf("fallback debt=%d", out)
	}
	if l.control.Load().circuit != circuitHealthy {
		t.Fatal("stall fabricated outage")
	}
}

func TestSchedulerApplyTimeoutPreservesBatchAndPending(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, LocalBurst: 5, MaxOutstanding: 5, SyncThreshold: 3, Redis: RedisConfig{Address: "unused:6379", Timeout: 50 * time.Millisecond}})
	l.jitter = func(time.Duration) time.Duration { return 0 }
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 100e6)
	startTestScheduler(t, l)
	l.AllowN("k", 3)
	eventually(t, func() bool { return len(f.queued()) == 1 })
	first := f.queued()[0]
	l.AllowN("k", 2)
	eventually(t, func() bool { return !l.control.Load().permanent && l.control.Load().circuit == circuitOpen })
	eventually(t, func() bool { l.scanDue(c.Now()); return len(f.queued()) >= 2 })
	second := f.queued()[1]
	if second.command != first.command {
		t.Fatal("APPLY timeout changed identity")
	}
	f.deliver(second, reply{Outcome: outcomeOK, CommittedSequence: 1, BalanceU: 100e6, SessionTTLMS: 86400000}, nil)
	eventually(t, func() bool { return len(f.queued()) >= 3 })
	third := f.queued()[2]
	if third.command.Delta != 2 || third.command.Sequence != 2 {
		t.Fatal("pending lost across timeout", third.command)
	}
}
func TestSchedulerConstructorRedis(t *testing.T) {
	client, prefix := redisFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := NewLimiter(Config{GlobalRate: 100, GlobalBurst: 100, SyncThreshold: 1, Parent: ctx, Redis: RedisConfig{Client: client, Prefix: prefix}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-l.workersDone; client.Del(context.Background(), redisKey(prefix, "k")) }()
	if ok, r, _ := l.Allow("k"); ok || r != ReasonSyncPending {
		t.Fatalf("cold %v %s", ok, r)
	}
	eventually(t, func() bool { ok, _, _ := l.Allow("k"); return ok })
	b := testBucket(t, l, "k")
	eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.ledger.outstanding() == 0 && b.ledger.Committed > 0
	})
}

func TestSchedulerCleanSessionExpiryResetsSequence(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, SyncThreshold: 1})
	b := testBucket(t, l, "k")
	initializeTestBucket(t, l, b, 100e6)
	b.ledger.Committed = 7
	c.Advance(l.cfg.syncInterval)
	startTestScheduler(t, l)
	l.scanDue(c.Now())
	eventually(t, func() bool { return len(f.queued()) == 1 })
	f.deliver(f.queued()[0], reply{Outcome: outcomeSessionExpired}, nil)
	eventually(t, func() bool { return len(f.queued()) == 2 })
	f.deliver(f.queued()[1], reply{Outcome: outcomeOK, Slot: 1, BalanceU: 100e6, SessionTTLMS: 86400000}, nil)
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.initialized })
	if ok, _, _ := l.Allow("k"); !ok {
		t.Fatal("admission after clean rotation")
	}
	eventually(t, func() bool { return len(f.queued()) == 3 })
	if got := f.queued()[2].command.Sequence; got != 1 {
		t.Fatalf("new session sequence=%d, want 1", got)
	}
}

func TestSchedulerIdleCleanupPreservesDebtAndLatches(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, IdleTTL: time.Millisecond})
	for _, key := range []string{"clean", "debt", "rejected", "terminal", "capacity"} {
		b, _ := l.store.acquire(key, c.Now())
		initializeTestBucket(t, l, b, 100e6)
		switch key {
		case "debt":
			b.ledger.Pending = 1
		case "rejected":
			b.globalRejected = true
		case "terminal":
			b.terminalReason = ReasonAccountingError
		case "capacity":
			b.capacityUntil = c.Now().Add(time.Second)
		}
		l.store.release(b)
	}
	startTestScheduler(t, l)
	eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.timers) > 0 })
	c.Advance(10 * time.Millisecond)
	eventually(t, func() bool { return l.store.count.Load() == 4 })
	sh := &l.store.shards[shardIndex("clean")]
	sh.mu.Lock()
	found := sh.keys["clean"] != nil
	sh.mu.Unlock()
	if found {
		t.Fatal("idle clean key retained")
	}
}

func TestSchedulerIdleCleanupAndScanHaveIndependentProgress(t *testing.T) {
	l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, MaxKeys: 200, IdleTTL: time.Millisecond})
	for i := 0; i < 130; i++ {
		b, _ := l.store.acquire(fmt.Sprint(i), c.Now())
		initializeTestBucket(t, l, b, 100e6)
		if i%2 == 0 {
			b.ledger.Pending = 1
		}
		l.store.release(b)
	}
	c.Advance(200 * time.Millisecond)
	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		l.scanDue(c.Now())
		drainModelNotifications(l, func(b *bucket) { seen[b.key] = true })
	}
	if l.store.count.Load() != 65 || len(seen) != 65 {
		t.Fatalf("cleanup/scan starvation: retained=%d seen=%d", l.store.count.Load(), len(seen))
	}
}

func TestSchedulerIdleKeyStopsContinuousRefresh(t *testing.T) {
	l, c, f := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, SyncInterval: time.Millisecond, IdleTTL: 5 * time.Millisecond})
	b, _ := l.store.acquire("k", c.Now())
	initializeTestBucket(t, l, b, 100e6)
	l.store.release(b)
	startTestScheduler(t, l)
	c.Advance(time.Millisecond)
	l.scanDue(c.Now())
	eventually(t, func() bool { return len(f.queued()) == 1 })
	c.Advance(10 * time.Millisecond)
	f.deliver(f.queued()[0], reply{Outcome: outcomeOK, BalanceU: 100e6, SessionTTLMS: 86400000}, nil)
	eventually(t, func() bool { l.scanDue(c.Now()); return l.store.count.Load() == 0 })
	if len(f.queued()) != 1 {
		t.Fatal("idle key refreshed continuously")
	}
}

// A negative current-generation observation must keep the global latch while
// returning refresh work to the timer cadence, even when it cannot authorize.
func TestSchedulerNegativeResultsArePaced(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(fmt.Sprint("recovery=", recovery), func(t *testing.T) {
			l, c, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100})
			b := testBucket(t, l, "k")
			var a attempt
			if recovery {
				initializeTestBucket(t, l, b, 100e6)
				l.recordFailure(transientFailure())
				if !l.beginProbe() {
					t.Fatal("probe claim")
				}
				l.finishProbe(nil)
				var ok bool
				a, ok = l.prepareAttempt(b, c.Now())
				if !ok {
					t.Fatal("recovery preparation")
				}
			} else {
				b.init = &command{Op: opInit, Session: "0123456789abcdef0123456789abcdef", Slot: -1, RegistrationDeadlineMS: c.Now().UnixMilli() + 30000}
				var ok bool
				a, ok = l.prepareAttempt(b, c.Now())
				if !ok {
					t.Fatal("init preparation")
				}
			}
			negative := reply{Outcome: outcomeOK, Slot: 0, BalanceU: -1e6, RetryMS: 1, SessionTTLMS: 86400000}
			l.installReply(b, a, negative)
			if l.due(b, c.Now()) {
				t.Fatal("negative result immediately rescheduled")
			}
			c.Advance(time.Millisecond)
			if ok, reason, _ := l.Allow("k"); ok || reason != ReasonGlobalLimit {
				t.Fatalf("retry-after authorized: %v %s", ok, reason)
			}
			for i := 0; i < 8; i++ {
				l.scanDue(c.Now())
				if l.due(b, c.Now()) {
					t.Fatal("request/scan bypassed pacing")
				}
			}
			c.Advance(l.cfg.syncInterval - time.Millisecond)
			if !l.due(b, c.Now()) {
				t.Fatal("negative key never refreshed")
			}
			a, ok := l.prepareAttempt(b, c.Now())
			if !ok || a.command.Op != opRefresh {
				t.Fatal("refresh")
			}
			l.installReply(b, a, negative)
			if l.due(b, c.Now()) {
				t.Fatal("repeated negative refresh immediately due")
			}
			c.Advance(l.cfg.syncInterval)
			a, ok = l.prepareAttempt(b, c.Now())
			if !ok {
				t.Fatal("refill check")
			}
			l.installReply(b, a, reply{Outcome: outcomeOK, BalanceU: 100e6, SessionTTLMS: 86400000})
			if ok, reason, _ := l.Allow("k"); !ok || reason != ReasonAllowed {
				t.Fatalf("refill did not reopen: %v %s", ok, reason)
			}
		})
	}
}

// Capture an already-successful cancellation check, then hold its return until
// the canceled scheduler has had an opportunity to join and drain its queue.
type notificationBarrierContext struct {
	context.Context
	armed            atomic.Bool
	checked, release chan struct{}
}

func (c *notificationBarrierContext) Err() error {
	err := c.Context.Err()
	if c.armed.CompareAndSwap(true, false) {
		close(c.checked)
		<-c.release
	}
	return err
}
func TestSchedulerCancellationJoinsAcceptedNotification(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	barrier := &notificationBarrierContext{Context: parent, checked: make(chan struct{}), release: make(chan struct{})}
	l, _, _ := newTestLimiter(t, Config{GlobalRate: 100, GlobalBurst: 100, Parent: barrier})
	b, ok := l.store.acquire("k", l.clock.Now())
	if !ok {
		t.Fatal("acquire")
	}
	barrier.armed.Store(true)
	notified := make(chan struct{})
	go func() { l.notify(b); close(notified) }()
	<-barrier.checked
	startTestScheduler(t, l)
	cancel()
	// The old implementation closes done here while an accepted sender is
	// paused. The corrected scheduler waits at its short enqueue barrier.
	select {
	case <-l.workersDone:
	case <-time.After(30 * time.Millisecond):
	}
	close(barrier.release)
	<-notified
	<-l.workersDone
	l.store.release(b)
	b.mu.Lock()
	queued := b.queued
	b.mu.Unlock()
	sh := &l.store.shards[shardIndex(b.key)]
	sh.mu.Lock()
	refs := b.refs
	sh.mu.Unlock()
	if len(l.work) != 0 || queued || refs != 0 {
		t.Fatalf("notification survived drain: queue=%d queued=%v refs=%d", len(l.work), queued, refs)
	}
}
