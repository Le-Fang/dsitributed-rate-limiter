package ratelimiter

import "time"

// clock is the application boundary for monotonic time and scheduler timers.
type clock interface {
	Now() time.Time
	NewTimer(time.Duration) timer
}

type timer interface {
	C() <-chan time.Time
	Reset(time.Duration)
	Stop()
}

type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) NewTimer(d time.Duration) timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ timer *time.Timer }

func (t realTimer) C() <-chan time.Time   { return t.timer.C }
func (t realTimer) Reset(d time.Duration) { t.timer.Reset(d) }
func (t realTimer) Stop()                 { t.timer.Stop() }
