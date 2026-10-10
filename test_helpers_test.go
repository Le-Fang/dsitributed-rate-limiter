package ratelimiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}
type fakeTimer struct {
	owner  *fakeClock
	ch     chan time.Time
	at     time.Time
	active bool
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }
func (c *fakeClock) Now() time.Time           { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if t.active && !c.now.Before(t.at) {
			t.active = false
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
}
func (c *fakeClock) NewTimer(d time.Duration) timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{owner: c, ch: make(chan time.Time, 1), at: c.now.Add(d), active: true}
	c.timers = append(c.timers, t)
	return t
}
func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Reset(d time.Duration) {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	t.at = t.owner.now.Add(d)
	t.active = true
}
func (t *fakeTimer) Stop() { t.owner.mu.Lock(); defer t.owner.mu.Unlock(); t.active = false }

type fakeResult struct {
	reply reply
	err   error
}
type fakeCall struct {
	key     string
	command command
	result  chan fakeResult
}
type fakeBackend struct {
	mu         sync.Mutex
	calls      []*fakeCall
	clock      *fakeClock
	closed     bool
	active     atomic.Int64
	peak       atomic.Int64
	timeCalls  atomic.Int64
	probeCalls atomic.Int64
	timeFn     func(context.Context) (time.Time, error)
	probeFn    func(context.Context) error
}

func (b *fakeBackend) Time(ctx context.Context, key string) (time.Time, error) {
	defer b.enter()()
	b.timeCalls.Add(1)
	if b.timeFn != nil {
		return b.timeFn(ctx)
	}
	return b.clock.Now(), nil
}
func (b *fakeBackend) Probe(ctx context.Context) error {
	defer b.enter()()
	b.probeCalls.Add(1)
	if b.probeFn != nil {
		return b.probeFn(ctx)
	}
	return nil
}
func (b *fakeBackend) Close() error { b.mu.Lock(); defer b.mu.Unlock(); b.closed = true; return nil }
func (b *fakeBackend) Execute(ctx context.Context, key string, c command) (reply, error) {
	defer b.enter()()
	call := &fakeCall{key: key, command: c, result: make(chan fakeResult, 1)}
	b.mu.Lock()
	b.calls = append(b.calls, call)
	b.mu.Unlock()
	select {
	case r := <-call.result:
		return r.reply, r.err
	case <-ctx.Done():
		return reply{}, &backendError{class: errorTransient, cause: ctx.Err()}
	}
}
func (b *fakeBackend) queued() []*fakeCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*fakeCall(nil), b.calls...)
}
func (b *fakeBackend) deliver(c *fakeCall, r reply, err error) { c.result <- fakeResult{r, err} }
func newTestLimiter(t *testing.T, cfg Config) (*Limiter, *fakeClock, *fakeBackend) {
	t.Helper()
	if cfg.Redis.Address == "" && cfg.Redis.Client == nil {
		cfg.Redis.Address = "unused:6379"
	}
	n, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := newFakeClock(time.Unix(1700000000, 0))
	b := &fakeBackend{clock: c}
	l := newLimiterCore(n.core(), n.Parent, c, b, n.Metrics)
	t.Cleanup(func() { b.Close() })
	return l, c, b
}
func testBucket(t *testing.T, l *Limiter, key string) *bucket {
	t.Helper()
	b, ok := l.store.acquire(key, l.clock.Now())
	if !ok {
		t.Fatal("acquire failed")
	}
	t.Cleanup(func() { l.store.release(b) })
	return b
}
func initializeTestBucket(t *testing.T, l *Limiter, b *bucket, balance int64) attempt {
	t.Helper()
	b.mu.Lock()
	b.init = &command{Op: opInit, Session: "0123456789abcdef0123456789abcdef", Slot: -1, Delta: int64(l.cfg.maxOutstanding), RateU: l.cfg.globalRateU, BurstU: int64(l.cfg.globalBurst) * 1e6, RegistrationDeadlineMS: l.clock.Now().UnixMilli() + 30000}
	b.mu.Unlock()
	a, ok := l.prepareAttempt(b, l.clock.Now())
	if !ok {
		t.Fatal("prepare init")
	}
	l.installReply(b, a, reply{Outcome: outcomeOK, Slot: 0, BalanceU: balance, SessionTTLMS: 86400000, FreshInit: true})
	return a
}

func (b *fakeBackend) enter() func() {
	n := b.active.Add(1)
	for old := b.peak.Load(); n > old; old = b.peak.Load() {
		if b.peak.CompareAndSwap(old, n) {
			break
		}
	}
	return func() { b.active.Add(-1) }
}
func (b *fakeBackend) MaxConcurrent() int { return int(b.peak.Load()) }
