package ratelimiter

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
)

// This file is the Redis adapter codec. Application logic must consume command,
// reply and outcome, never the transport values or helpers below.
func redisKey(prefix, key string) string {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(prefix)))
	h := sha256.New()
	h.Write(size[:])
	h.Write([]byte(prefix))
	h.Write([]byte(key))
	return prefix + ":{" + hex.EncodeToString(h.Sum(nil)) + "}:bucket:v1"
}

func encodeCommand(c command) []interface{} {
	var op string
	switch c.Op {
	case opInit:
		op = "INIT"
	case opRefresh:
		op = "REFRESH"
	case opApply:
		op = "APPLY"
	}
	return []interface{}{op, c.Session, strconv.Itoa(c.Slot), strconv.FormatUint(c.Sequence, 10),
		strconv.FormatInt(c.Delta, 10), strconv.FormatInt(c.RateU, 10), strconv.FormatInt(c.BurstU, 10), strconv.FormatInt(c.RegistrationDeadlineMS, 10)}
}

var errMalformedReply = errors.New("malformed accounting reply")

func decodeReply(v interface{}) (reply, error) {
	values, ok := v.([]interface{})
	if !ok || len(values) != 9 {
		return reply{}, errMalformedReply
	}
	var fields [9]string
	for i, value := range values {
		s, ok := value.(string)
		if !ok {
			return reply{}, errMalformedReply
		}
		fields[i] = s
	}
	code := decodeOutcome(fields[0])
	if code == 0 || fields[8] != "1" || (fields[7] != "0" && fields[7] != "1") {
		return reply{}, errMalformedReply
	}
	success := code == outcomeOK || code == outcomeDuplicate
	minField := int64(0)
	if !success {
		minField = -1
	}
	slot, okSlot := parseWireInt(fields[1], minField, 255)
	// Parse sequences as integers throughout; an unavailable error sequence is
	// discarded, never converted through floating point or cast from -1.
	var sequence uint64
	var okSequence bool
	if !success && fields[2] == "-1" {
		okSequence = true
	} else {
		sequence, okSequence = parseWireSequence(fields[2])
	}
	balance, okBalance := parseWireInt(fields[3], -1_000_000_000_000, 1_000_000_000_000)
	retry, okRetry := parseWireInt(fields[4], minField, int64(maxSequence))
	ttl, okTTL := parseWireInt(fields[5], minField, int64(maxSequence))
	logical, okLogical := parseWireInt(fields[6], minField, int64(maxSequence))
	if !okSlot || !okSequence || !okBalance || !okRetry || !okTTL || !okLogical {
		return reply{}, errMalformedReply
	}
	if !success {
		if fields[7] != "0" {
			return reply{}, errMalformedReply
		}
		// Error numeric fields may be unavailable or diagnostic. Neither form can
		// become an admission balance or an accounting acknowledgement.
		return reply{Outcome: code}, nil
	}
	fresh := fields[7] == "1"
	if (fresh && (code != outcomeOK || sequence != 0)) || ((balance >= 1_000_000) != (retry == 0)) {
		return reply{}, errMalformedReply
	}
	return reply{Outcome: code, Slot: int(slot), CommittedSequence: sequence, BalanceU: balance, RetryMS: retry, SessionTTLMS: ttl, LogicalMS: logical, FreshInit: fresh}, nil
}

func parseWireInt(s string, low, high int64) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n >= low && n <= high && strconv.FormatInt(n, 10) == s
}

func parseWireSequence(s string) (uint64, bool) {
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil && n <= maxSequence && strconv.FormatUint(n, 10) == s
}

func decodeOutcome(code string) outcome {
	switch code {
	case "OK":
		return outcomeOK
	case "DUPLICATE":
		return outcomeDuplicate
	case "INIT_EXPIRED":
		return outcomeInitExpired
	case "INVALID_ARGUMENT":
		return outcomeInvalidArgument
	case "VERSION_MISMATCH":
		return outcomeVersionMismatch
	case "POLICY_MISMATCH":
		return outcomePolicyMismatch
	case "STATE_CORRUPT":
		return outcomeStateCorrupt
	case "NUMERIC_LIMIT":
		return outcomeNumericLimit
	case "SEQUENCE_ERROR":
		return outcomeSequenceError
	case "SESSION_EXPIRED":
		return outcomeSessionExpired
	case "SESSION_CAPACITY":
		return outcomeSessionCapacity
	default:
		return 0
	}
}
