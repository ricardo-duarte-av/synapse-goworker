package clientevent

import (
	"strconv"

	"github.com/tidwall/gjson"
)

// StickyUntil is locally_sticky_until_ts (rust/src/events/mod.rs, v1.164.1):
// when an MSC4354 sticky event stops being sticky on this server, in
// milliseconds, or 0 if it is not a sticky event.
//
// Not sticky: no top-level `msc4354_sticky` object, a `duration_ms` that is
// not a strictly positive integer (serde's as_u64: no float, no sign),
// an event a policy server or spam checker marked spammy, or one with no
// received_ts (before Synapse v0.16). The duration is capped at an hour, and
// counted from the earlier of origin_server_ts and received_ts, so a forged
// origin timestamp cannot stretch it (#20324).
//
// eventJSON must be the event as Synapse holds it: a redacted event has lost
// `msc4354_sticky`, so it is not sticky.
func StickyUntil(eventJSON, internalMetadata []byte, receivedTS int64) int64 {
	if receivedTS == 0 {
		return 0
	}
	if gjson.GetBytes(internalMetadata, "policy_server_spammy").Bool() ||
		gjson.GetBytes(internalMetadata, "spam_checker_spammy").Bool() {
		return 0
	}
	sticky := gjson.GetBytes(eventJSON, "msc4354_sticky")
	if !sticky.IsObject() {
		return 0
	}
	raw := sticky.Get("duration_ms")
	if raw.Type != gjson.Number {
		return 0
	}
	ms, err := strconv.ParseUint(raw.Raw, 10, 64)
	if err != nil || ms == 0 {
		return 0
	}
	if ms > uint64(stickyMaxDurationMS) {
		ms = uint64(stickyMaxDurationMS)
	}
	start := gjson.GetBytes(eventJSON, "origin_server_ts").Int()
	if receivedTS < start {
		start = receivedTS
	}
	return start + int64(ms)
}
