package ratelimiter

import (
	"context"
	"time"
)

// These application-owned values express shared accounting operations. Wire
// names, key derivation and serialization belong exclusively to the adapter.
type operation uint8

const (
	opInit operation = iota + 1
	opRefresh
	opApply
)

type command struct {
	Op                                    operation
	Session                               string
	Slot                                  int
	Sequence                              uint64
	Delta                                 int64 // Initialization declares a cap; deduction carries whole tokens.
	RateU, BurstU, RegistrationDeadlineMS int64
}

type outcome uint8

const (
	outcomeOK outcome = iota + 1
	outcomeDuplicate
	outcomeInitExpired
	outcomeInvalidArgument
	outcomeVersionMismatch
	outcomePolicyMismatch
	outcomeStateCorrupt
	outcomeNumericLimit
	outcomeSequenceError
	outcomeSessionExpired
	outcomeSessionCapacity
)

type reply struct {
	Outcome                                    outcome
	Slot                                       int
	CommittedSequence                          uint64
	BalanceU, RetryMS, SessionTTLMS, LogicalMS int64
	FreshInit                                  bool
}

// authorizes checks only the shared balance. Admission also requires freshness,
// a valid session lifetime, local tokens and an unblocked lifecycle state.
func (r reply) authorizes() bool {
	return (r.Outcome == outcomeOK || r.Outcome == outcomeDuplicate) && r.BalanceU >= 1_000_000
}

type backend interface {
	Time(context.Context, string) (time.Time, error)
	Execute(context.Context, string, command) (reply, error)
	Probe(context.Context) error
	Close() error // Only the adapter's owned client is closed.
}

// protocolDisposition is application policy, independent of the Redis adapter.
// Reinitialize is allowed only without unresolved normal consumption. The caller
// retains immutable in-flight batches and applies scheduling/lifecycle changes.
type protocolDisposition struct {
	Reason                             string
	Authorized, Terminal, Reinitialize bool
	RetryDelay                         time.Duration
}

func protocolResult(r reply, unresolved bool) protocolDisposition {
	switch r.Outcome {
	case outcomeOK, outcomeDuplicate:
		if r.authorizes() {
			return protocolDisposition{Reason: ReasonAllowed, Authorized: true}
		}
		return protocolDisposition{Reason: ReasonGlobalLimit}
	case outcomeInitExpired:
		if !unresolved {
			return protocolDisposition{Reason: ReasonSyncPending, Reinitialize: true}
		}
	case outcomeVersionMismatch, outcomePolicyMismatch:
		return protocolDisposition{Reason: ReasonPolicyMismatch, Terminal: true}
	case outcomeSessionExpired:
		if unresolved {
			return protocolDisposition{Reason: ReasonAccountingExpired, Terminal: true}
		}
		return protocolDisposition{Reason: ReasonSyncPending, Reinitialize: true}
	case outcomeSessionCapacity:
		return protocolDisposition{Reason: ReasonCapacityExhausted, RetryDelay: time.Second}
	}
	return protocolDisposition{Reason: ReasonAccountingError, Terminal: true}
}

// backendDiagnostic is a finite, sanitized application value. Only adapters
// inspect transport error text; observation never retains arbitrary strings.
type backendDiagnostic uint8

const (
	diagnosticUnknown backendDiagnostic = iota
	diagnosticTimeout
	diagnosticPoolExhausted
	diagnosticNetwork
	diagnosticOOM
	diagnosticAuthentication
	diagnosticPermission
	diagnosticProtocol
	diagnosticScript
	diagnosticLoading
	diagnosticTryAgain
	diagnosticClusterDown
	diagnosticMasterDown
	diagnosticBusy
	diagnosticReadOnly
	diagnosticCount
)
