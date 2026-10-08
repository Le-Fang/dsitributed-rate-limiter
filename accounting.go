package ratelimiter

// This domain bound ensures that a batch identity stays exactly representable
// by every supported backend. Exhaustion requires a clean session rotation.
const maxSequence uint64 = 1<<53 - 1

type batch struct {
	Sequence uint64
	Delta    int64
}

// ledger tracks only normal consumption, never fallback traffic. The owning
// bucket lock protects every field and method. A flight remains outstanding
// even when its execution succeeded but the acknowledgement was lost.
type ledger struct {
	Pending   int64
	Flight    *batch
	Committed uint64
}

func (l *ledger) outstanding() int64 {
	if l.Flight != nil {
		return l.Pending + l.Flight.Delta
	}
	return l.Pending
}

func (l *ledger) admit(n, cap int64) bool {
	if n <= 0 || cap <= 0 || l.Committed >= maxSequence ||
		(l.Flight != nil && l.Flight.Sequence == maxSequence) || n > cap-l.outstanding() {
		return false
	}
	l.Pending += n
	return true
}

func (l *ledger) begin() (batch, bool) {
	if l.Flight != nil {
		return *l.Flight, true
	}
	if l.Pending == 0 || l.Committed >= maxSequence {
		return batch{}, false
	}
	// Under the bucket lock this move also freezes admission when the new flight
	// is the final sequence, so all remaining consumption fits in its snapshot.
	l.Flight = &batch{Sequence: l.Committed + 1, Delta: l.Pending}
	l.Pending = 0
	return *l.Flight, true
}

func (l *ledger) acknowledge(b batch) bool {
	if l.Flight == nil || *l.Flight != b {
		return false
	}
	l.Committed = b.Sequence
	l.Flight = nil
	return true
}
