package clientevent

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
)

// syntheticJoinPrefix is SYNTHETIC_JOIN_EVENT_ID_PREFIX
// (handlers/server_announcements.py).
const syntheticJoinPrefix = "$fake_announcement_join"

// SyntheticJoin is make_synthetic_join_event: the join a user who is not in
// a server announcement room is shown, so that clients list it as joined.
// Never persisted; the ID is derived from the room and the user, so the
// same user always gets the same event. Federation format, keys sorted as
// serde_json's map (no preserve_order) holds them.
func SyntheticJoin(roomID, userID string, originServerTS int64) []byte {
	localpart := strings.TrimPrefix(userID, "@")
	if i := strings.IndexByte(localpart, ':'); i >= 0 {
		localpart = localpart[:i]
	}
	sum := sha256.Sum256([]byte(roomID + "\x00" + userID))
	eventID := strings.Join([]string{
		syntheticJoinPrefix,
		// room_id[1:9] and localpart[:8]: Python slices code points.
		runeSlice(roomID, 1, 9),
		runeSlice(localpart, 0, 8),
		base64.RawURLEncoding.EncodeToString(sum[:]),
	}, "-")
	body, _ := json.Marshal(map[string]any{
		"room_id":          roomID,
		"type":             "m.room.member",
		"event_id":         eventID,
		"sender":           userID,
		"state_key":        userID,
		"origin_server_ts": originServerTS,
		"content":          map[string]string{"membership": "join"},
	})
	return body
}

// SerializeSynthetic is serialize_synthetic_event: only the event format
// and the field allowlist apply, since there is no stored event to take an
// age, a transaction ID, a redaction or aggregations from. The Rust works
// on a serde_json map, which keeps keys sorted, the allowlist's output
// included.
func SerializeSynthetic(event []byte, cfg Config) ([]byte, error) {
	out, err := applyFormat(event, cfg.Format)
	if err != nil {
		return nil, err
	}
	if len(cfg.EventFields) > 0 {
		if out, err = OnlyFields(out, cfg.EventFields); err != nil {
			return nil, err
		}
	}
	var sorted map[string]json.RawMessage
	if err := json.Unmarshal(out, &sorted); err != nil {
		return nil, err
	}
	return json.Marshal(sorted)
}

// runeSlice is s[from:to] in code points, clamped as Python's slice is.
func runeSlice(s string, from, to int) string {
	r := []rune(s)
	if to > len(r) {
		to = len(r)
	}
	if from > to {
		return ""
	}
	return string(r[from:to])
}
