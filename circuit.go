package ratelimiter

import (
	"context"
	"errors"
)

type circuitState uint8

const (
	circuitHealthy circuitState = iota
	circuitOpen
	circuitProbing
)

// Every publication is a new immutable allocation; admission compares pointers
// at its linearization point. The circuit mutex never nests a bucket lock.
type controlState struct {
	closed     bool
	circuit    circuitState
	generation uint64
	permanent  bool
}

// errorClass is the application boundary. Adapters translate transport details
// into these categories; admission and recovery never inspect error text.
type errorClass uint8

const (
	errorPermanent errorClass = iota
	errorTransient
	errorShutdown
)

type backendError struct {
	class errorClass
	cause error
}

func (e *backendError) Error() string { return e.cause.Error() }
func (e *backendError) Unwrap() error { return e.cause }
func backendErrorClass(err error) errorClass {
	if errors.Is(err, context.Canceled) {
		return errorShutdown
	}
	var classified *backendError
	if errors.As(err, &classified) {
		return classified.class
	}
	return errorPermanent
}

func (l *Limiter) recordFailure(err error) {
	if err == nil {
		return
	}
	l.circuitMu.Lock()
	defer l.circuitMu.Unlock()
	l.recordFailureLocked(err)
}
func (l *Limiter) recordFailureLocked(err error) {
	s := l.control.Load()
	if s.closed {
		return
	}
	next := *s
	class := backendErrorClass(err)
	if l.parent.Err() != nil {
		class = errorShutdown
	}
	switch class {
	case errorShutdown:
		next.closed = true
	case errorPermanent:
		if s.permanent {
			return
		}
		next.permanent = true
	case errorTransient:
		if s.permanent || s.circuit == circuitOpen {
			return
		}
		next.circuit = circuitOpen
		next.generation++
	}
	l.control.Store(&next)
}

// beginProbe claims the sole probe, including ownership of an obsolete probe
// until its completion. A reopened circuit cannot overlap that network call.
func (l *Limiter) beginProbe() bool {
	l.circuitMu.Lock()
	defer l.circuitMu.Unlock()
	s := l.control.Load()
	if l.probe != nil || s.closed || s.permanent || s.circuit != circuitOpen || l.parent.Err() != nil {
		return false
	}
	next := *s
	next.circuit = circuitProbing
	l.probe = &next
	l.control.Store(&next)
	return true
}
func (l *Limiter) finishProbe(err error) {
	l.circuitMu.Lock()
	defer l.circuitMu.Unlock()
	owner := l.probe
	if owner == nil {
		return
	}
	l.probe = nil
	if l.control.Load() != owner {
		// Obsolete successes/transient failures cannot recover or reopen a newer
		// generation. Permanent failures and cancellation still override availability.
		if err != nil && backendErrorClass(err) != errorTransient {
			l.recordFailureLocked(err)
		}
		return
	}
	if err != nil {
		l.recordFailureLocked(err)
		return
	}
	if l.parent.Err() != nil {
		l.recordFailureLocked(context.Canceled)
		return
	}
	next := *owner
	next.circuit = circuitHealthy
	l.control.Store(&next)
}
