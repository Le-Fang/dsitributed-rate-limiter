package ratelimiter

import "testing"

func TestAccountingAckPreservesNewDelta(t *testing.T) {
	var l ledger
	if !l.admit(3, 5) {
		t.Fatal("initial admission")
	}
	b, ok := l.begin()
	if !ok || b.Delta != 3 || l.outstanding() != 3 {
		t.Fatal("begin released cap")
	}
	if !l.admit(2, 5) || l.admit(1, 5) {
		t.Fatal("cap")
	}
	if !l.acknowledge(b) || l.Pending != 2 || l.outstanding() != 2 {
		t.Fatal("ack lost delta")
	}
	if l.acknowledge(b) {
		t.Fatal("duplicate acknowledgement")
	}
	next, ok := l.begin()
	if !ok || next != (batch{Sequence: 2, Delta: 2}) {
		t.Fatalf("next = %+v, %v", next, ok)
	}
}

func TestAccountingImmutableRetry(t *testing.T) {
	var l ledger
	if _, ok := l.begin(); ok {
		t.Fatal("empty batch")
	}
	for _, n := range []int64{-1, 0, 6, 9223372036854775807} {
		if l.admit(n, 5) {
			t.Fatalf("accepted invalid cost %d", n)
		}
	}
	l.admit(3, 5)
	b, _ := l.begin()
	l.admit(2, 5)
	retry, ok := l.begin()
	if !ok || retry != b || l.Pending != 2 {
		t.Fatal("retry mutated payload")
	}
	for _, wrong := range []batch{{Sequence: 2, Delta: 3}, {Sequence: 1, Delta: 4}, {}} {
		if l.acknowledge(wrong) || l.outstanding() != 5 || l.Committed != 0 {
			t.Fatal("mismatched ack changed accounting")
		}
	}
	// Returned batches are values: caller edits cannot mutate the in-flight batch.
	retry.Delta = 1
	again, _ := l.begin()
	if again != b {
		t.Fatal("caller mutated flight")
	}
}

func TestAccountingFinalSequenceFreezesAdmission(t *testing.T) {
	l := ledger{Committed: 9007199254740989}
	l.admit(2, 5)
	penultimate, _ := l.begin()
	if penultimate.Sequence != 9007199254740990 {
		t.Fatal("penultimate sequence")
	}
	l.admit(3, 5)
	l.acknowledge(penultimate)
	final, ok := l.begin()
	if !ok || final != (batch{Sequence: 9007199254740991, Delta: 3}) {
		t.Fatal("final delta not drained")
	}
	if l.admit(1, 5) {
		t.Fatal("admission during final sequence")
	}
	if retry, _ := l.begin(); retry != final {
		t.Fatal("final retry changed")
	}
	if !l.acknowledge(final) || l.outstanding() != 0 || l.admit(1, 5) {
		t.Fatal("sequence wrapped")
	}
	if _, ok := l.begin(); ok {
		t.Fatal("batch after exhaustion")
	}
	// Corrupt exhausted state stays unresolved for the owner to latch an error.
	l.Pending = 1
	if _, ok := l.begin(); ok || l.Pending != 1 {
		t.Fatal("discarded exhausted delta")
	}
}
