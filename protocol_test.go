package ratelimiter

import (
	"reflect"
	"strings"
	"testing"
)

func TestProtocolCommands(t *testing.T) {
	const session = "0123456789abcdef0123456789abcdef"
	tests := []struct {
		c    command
		want []interface{}
	}{
		{command{Op: opInit, Session: session, Slot: -1, Delta: 32, RateU: 1500000, BurstU: 32000000, RegistrationDeadlineMS: 1700000030000}, []interface{}{"INIT", session, "-1", "0", "32", "1500000", "32000000", "1700000030000"}},
		{command{Op: opApply, Session: session, Slot: 255, Sequence: 9007199254740991, Delta: 7, RateU: 1, BurstU: 1000000}, []interface{}{"APPLY", session, "255", "9007199254740991", "7", "1", "1000000", "0"}},
		{command{Op: opRefresh, Session: session, Slot: 0, RateU: 1, BurstU: 1000000}, []interface{}{"REFRESH", session, "0", "0", "0", "1", "1000000", "0"}},
	}
	for _, tt := range tests {
		if got := encodeCommand(tt.c); !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("got %v, want %v", got, tt.want)
		}
	}
}

func TestProtocolSuccessReplies(t *testing.T) {
	for _, code := range []string{"OK", "DUPLICATE"} {
		r, err := decodeReply([]interface{}{code, "255", "9007199254740991", "-1000000000000", "1000001000000000", "86400000", "1700000000000", "0", "1"})
		if err != nil || r.CommittedSequence != 9007199254740991 || r.BalanceU != -1000000000000 || r.Slot != 255 || r.authorizes() {
			t.Fatalf("reply = %+v, %v", r, err)
		}
		want := outcomeOK
		if code == "DUPLICATE" {
			want = outcomeDuplicate
		}
		if r != (reply{Outcome: want, Slot: 255, CommittedSequence: 9007199254740991, BalanceU: -1000000000000, RetryMS: 1000001000000000, SessionTTLMS: 86400000, LogicalMS: 1700000000000}) {
			t.Fatalf("decoded fields = %+v", r)
		}
	}
	for _, tt := range []struct {
		balance, retry string
		allowed        bool
	}{{"0", "1000", false}, {"999999", "1", false}, {"1000000", "0", true}, {"1000000000000", "0", true}} {
		r, err := decodeReply([]interface{}{"OK", "0", "0", tt.balance, tt.retry, "86400000", "1700000000000", "1", "1"})
		if err != nil || r.authorizes() != tt.allowed || !r.FreshInit {
			t.Fatalf("balance %s: %+v, %v", tt.balance, r, err)
		}
	}
}

func TestProtocolErrorReplies(t *testing.T) {
	codes := map[string]outcome{"INIT_EXPIRED": outcomeInitExpired, "INVALID_ARGUMENT": outcomeInvalidArgument, "VERSION_MISMATCH": outcomeVersionMismatch, "POLICY_MISMATCH": outcomePolicyMismatch, "STATE_CORRUPT": outcomeStateCorrupt, "NUMERIC_LIMIT": outcomeNumericLimit, "SEQUENCE_ERROR": outcomeSequenceError, "SESSION_EXPIRED": outcomeSessionExpired, "SESSION_CAPACITY": outcomeSessionCapacity}
	for code, want := range codes {
		for _, balance := range []string{"-1", "1000000"} {
			r, err := decodeReply([]interface{}{code, "-1", "-1", balance, "-1", "-1", "-1", "0", "1"})
			if err != nil || r.Outcome != want || r.authorizes() {
				t.Fatalf("%s: %+v, %v", code, r, err)
			}
		}
	}
}

func TestProtocolRejectsMalformedReplies(t *testing.T) {
	valid := []interface{}{"OK", "0", "0", "1000000", "0", "86400000", "1700000000000", "0", "1"}
	bad := []interface{}{nil, "OK", []string{"OK"}, valid[:8], append(append([]interface{}{}, valid...), "extra")}
	for _, field := range []struct {
		i     int
		value interface{}
	}{
		{0, "UNKNOWN"}, {0, []byte("OK")}, {1, "256"}, {1, "-1"}, {1, "00"}, {1, int64(0)}, {2, "9007199254740992"}, {2, "18446744073709551615"}, {2, "-1"}, {2, "+1"}, {2, "1.0"}, {2, "1e3"}, {2, " 1"}, {2, "01"}, {2, ""}, {2, "-0"},
		{3, "1000000000001"}, {3, "-1000000000001"}, {3, "1.5"}, {3, "-0"}, {4, "-1"}, {4, "1"}, {5, "-1"}, {5, "9007199254740992"}, {6, "-1"}, {6, "9007199254740992"}, {7, "2"}, {8, "2"}, {8, "01"},
	} {
		v := append([]interface{}{}, valid...)
		v[field.i] = field.value
		bad = append(bad, v)
	}
	bad = append(bad, []interface{}{"DUPLICATE", "0", "0", "1000000", "0", "86400000", "0", "1", "1"}, []interface{}{"OK", "0", "1", "1000000", "0", "86400000", "0", "1", "1"}, []interface{}{"OK", "0", "1", "999999", "0", "86400000", "0", "0", "1"})
	for _, v := range bad {
		if r, err := decodeReply(v); err == nil {
			t.Errorf("accepted %v as %+v", v, r)
		}
	}
	for _, i := range []int{1, 2, 3, 4, 5, 6, 7, 8} {
		v := []interface{}{"STATE_CORRUPT", "-1", "-1", "-1", "-1", "-1", "-1", "0", "1"}
		v[i] = "-2"
		if i == 3 {
			v[i] = "-1000000000001"
		}
		if _, err := decodeReply(v); err == nil {
			t.Errorf("accepted error shape %v", v)
		}
	}
}

func TestProtocolRedisKeyFraming(t *testing.T) {
	// SHA-256 vector independently generated from bytes 00 00 00 01 70 6b.
	const want = "p:{29151198d87d8e8881ac0354dcebe5261100791217c069e10f0b3b43bfa7a5f1}:bucket:v1"
	got := redisKey("p", "k")
	if got != want {
		t.Fatalf("key = %s", got)
	}
	if got != redisKey("p", "k") || got == redisKey("q", "k") || redisKey("ab", "c") == redisKey("a", "bc") {
		t.Fatal("unstable or ambiguous key framing")
	}
	opaque := redisKey("p", "{customer}\x00K")
	if strings.Count(opaque, "{") != 1 || strings.Count(opaque, "}") != 1 || strings.Contains(opaque, "customer") || opaque == redisKey("p", "{customer}\x00k") {
		t.Fatal("key bytes not opaque")
	}
}
