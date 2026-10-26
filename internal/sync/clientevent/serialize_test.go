package clientevent

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

const pdu = `{
  "auth_events": ["$a"], "prev_events": ["$b"], "depth": 5, "origin": "example.com",
  "hashes": {"sha256": "x"}, "signatures": {"example.com": {"ed25519:a": "y"}},
  "prev_state": [],
  "room_id": "!r:example.com", "sender": "@u:example.com", "type": "m.room.message",
  "origin_server_ts": 1000, "content": {"body": "hi", "msgtype": "m.text"},
  "unsigned": {"age_ts": 1000, "replaces_state": "$old", "prev_content": {"body": "old"}}
}`

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, b)
	}
	return m
}

func serialize(t *testing.T, format Format, ev Stored, nowMS int64) map[string]any {
	t.Helper()
	if ev.JSON == nil {
		ev.JSON = []byte(pdu)
	}
	if ev.EventID == "" {
		ev.EventID = "$e"
	}
	if ev.Type == "" {
		ev.Type = "m.room.message"
	}
	out, err := Serialize(ev, nowMS, Config{Format: format})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	return decode(t, out)
}

// Every client format drops the federation-only fields. Leaving one in would
// leak signature material into client responses and differ from Synapse on
// every single event.
func TestFederationFieldsAreDropped(t *testing.T) {
	for _, format := range []Format{FormatV1, FormatV2, FormatV2NoRoomID} {
		got := serialize(t, format, Stored{}, 3000)
		for _, key := range v2DropKeys {
			if _, ok := got[key]; ok {
				t.Errorf("format %d: %q survived", format, key)
			}
		}
	}
}

func TestEventIDIsAlwaysInserted(t *testing.T) {
	// Room version 3+ PDUs have no event_id of their own.
	got := serialize(t, FormatV2, Stored{EventID: "$abc", JSON: []byte(pdu)}, 3000)
	if got["event_id"] != "$abc" {
		t.Errorf("event_id = %v", got["event_id"])
	}
}

// age_ts is replaced by a wall-clock age, computed before the format transform
// so that v1's copy picks up the computed value.
func TestAgeReplacesAgeTS(t *testing.T) {
	got := serialize(t, FormatV1, Stored{}, 3000)
	unsigned := got["unsigned"].(map[string]any)
	if _, ok := unsigned["age_ts"]; ok {
		t.Error("age_ts survived")
	}
	if unsigned["age"] != float64(2000) {
		t.Errorf("unsigned.age = %v, want 2000", unsigned["age"])
	}
	if got["age"] != float64(2000) {
		t.Errorf("top-level age = %v, want 2000 (v1 copies it up)", got["age"])
	}
}

func TestFormatDifferences(t *testing.T) {
	v1 := serialize(t, FormatV1, Stored{}, 3000)
	v2 := serialize(t, FormatV2, Stored{}, 3000)
	noRoom := serialize(t, FormatV2NoRoomID, Stored{}, 3000)

	// v1 duplicates sender as user_id; v2 does not.
	if v1["user_id"] != "@u:example.com" {
		t.Errorf("v1 user_id = %v", v1["user_id"])
	}
	if _, ok := v2["user_id"]; ok {
		t.Error("v2 must not emit user_id")
	}

	// v1 lifts six fields out of unsigned.
	if v1["replaces_state"] != "$old" {
		t.Errorf("v1 replaces_state = %v", v1["replaces_state"])
	}
	if !reflect.DeepEqual(v1["prev_content"], map[string]any{"body": "old"}) {
		t.Errorf("v1 prev_content = %v", v1["prev_content"])
	}
	if _, ok := v2["replaces_state"]; ok {
		t.Error("v2 must not lift unsigned fields")
	}

	// Only the /sync format strips room_id.
	if v1["room_id"] != "!r:example.com" || v2["room_id"] != "!r:example.com" {
		t.Error("v1 and v2 must keep room_id")
	}
	if _, ok := noRoom["room_id"]; ok {
		t.Error("the /sync format must strip room_id")
	}
}

// An MSC4291 room derives its ID from the create event's hash, so that event
// carries no room_id. Clients still need one.
func TestCreateEventRoomIDIsRestoredForHashRooms(t *testing.T) {
	const createPDU = `{"type":"m.room.create","sender":"@u:e","content":{},"unsigned":{}}`
	ev := Stored{EventID: "$c", RoomID: "!hash", Type: "m.room.create",
		JSON: []byte(createPDU), RoomVersion: "12"}

	// Even the /sync format, which strips room_id, puts it back here.
	got := serialize(t, FormatV2NoRoomID, ev, 0)
	if got["room_id"] != "!hash" {
		t.Errorf("v12 create room_id = %v, want !hash", got["room_id"])
	}

	ev.RoomVersion = "10"
	got = serialize(t, FormatV2NoRoomID, ev, 0)
	if _, ok := got["room_id"]; ok {
		t.Error("a v10 create event must not gain a room_id")
	}
}

// MSC2174 moved `redacts` into content from room version 11, so the canonical
// place depends on the version -- and Synapse copies it to the OTHER place for
// clients written against either. Getting the direction backwards drops the
// field entirely, because the read finds nothing.
func TestRedactsIsMirrored(t *testing.T) {
	t.Run("v11 stores it in content, copies to top level", func(t *testing.T) {
		ev := Stored{EventID: "$r", Type: "m.room.redaction", RoomVersion: "11",
			JSON: []byte(`{"type":"m.room.redaction","content":{"redacts":"$victim"},"unsigned":{}}`)}
		got := serialize(t, FormatV2, ev, 0)
		if got["redacts"] != "$victim" {
			t.Errorf("top-level redacts = %v, want it copied up", got["redacts"])
		}
		if got["content"].(map[string]any)["redacts"] != "$victim" {
			t.Error("content.redacts should survive")
		}
	})
	t.Run("v10 stores it at top level, copies into content", func(t *testing.T) {
		ev := Stored{EventID: "$r", Type: "m.room.redaction", RoomVersion: "10",
			JSON: []byte(`{"type":"m.room.redaction","redacts":"$victim","content":{},"unsigned":{}}`)}
		got := serialize(t, FormatV2, ev, 0)
		if got["content"].(map[string]any)["redacts"] != "$victim" {
			t.Errorf("content.redacts = %v, want it copied down", got["content"])
		}
		if got["redacts"] != "$victim" {
			t.Error("top-level redacts should survive")
		}
	})
}

// MSC4354. The value is the time the event has LEFT to live, not its configured
// duration, so it shrinks as the event ages and vanishes once expired.
func TestStickyTTL(t *testing.T) {
	sticky := func(durationMS int64, originTS int64) Stored {
		return Stored{EventID: "$e", Type: "m.room.message",
			JSON: []byte(fmt.Sprintf(`{"type":"m.room.message","sender":"@u:e","content":{},
				"origin_server_ts":%d,"msc4354_sticky":{"duration_ms":%d},"unsigned":{}}`,
				originTS, durationMS))}
	}
	ttl := func(t *testing.T, ev Stored, nowMS int64, enabled bool) (float64, bool) {
		t.Helper()
		out, err := Serialize(ev, nowMS, Config{Format: FormatV2, MSC4354Enabled: enabled})
		if err != nil {
			t.Fatal(err)
		}
		unsigned := decode(t, out)["unsigned"].(map[string]any)
		v, ok := unsigned["msc4354_sticky_duration_ttl_ms"].(float64)
		return v, ok
	}

	t.Run("remaining lifetime", func(t *testing.T) {
		got, ok := ttl(t, sticky(10000, 1000), 4000, true)
		if !ok || got != 7000 {
			t.Errorf("ttl = %v (present=%v), want 7000", got, ok)
		}
	})
	t.Run("expired events carry nothing", func(t *testing.T) {
		if _, ok := ttl(t, sticky(1000, 1000), 5000, true); ok {
			t.Error("an expired sticky event should carry no ttl")
		}
	})
	t.Run("capped at one hour", func(t *testing.T) {
		got, _ := ttl(t, sticky(99*60*60*1000, 1000), 1000, true)
		if got != float64(stickyMaxDurationMS) {
			t.Errorf("ttl = %v, want the one-hour cap %d", got, stickyMaxDurationMS)
		}
	})
	// A remote server must not be able to claim a future timestamp and make its
	// event stick for longer than the cap allows.
	t.Run("a future origin_server_ts buys nothing", func(t *testing.T) {
		got, _ := ttl(t, sticky(5000, 1_000_000), 1000, true)
		if got != 5000 {
			t.Errorf("ttl = %v, want 5000 (clamped to now, not the claimed future)", got)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		if _, ok := ttl(t, sticky(10000, 1000), 4000, false); ok {
			t.Error("no ttl should appear when the feature is off")
		}
	})
	t.Run("malformed duration means not sticky", func(t *testing.T) {
		ev := Stored{EventID: "$e", Type: "m.room.message",
			JSON: []byte(`{"type":"m.room.message","sender":"@u:e","content":{},
				"origin_server_ts":1000,"msc4354_sticky":{"duration_ms":"soon"},"unsigned":{}}`)}
		if _, ok := ttl(t, ev, 2000, true); ok {
			t.Error("a non-integer duration must be treated as non-sticky")
		}
	})
}

// The delay id uses the MSC-prefixed key, not a bare "delay_id".
func TestDelayIDUsesMSCPrefixedKey(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.message", JSON: []byte(pdu),
		InternalMetadata: []byte(`{"delay_id":"syd_abc"}`)}
	out, err := Serialize(ev, 3000, Config{Format: FormatV2,
		Requester: Requester{UserID: "@u:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	unsigned := decode(t, out)["unsigned"].(map[string]any)
	if unsigned["org.matrix.msc4140.delay_id"] != "syd_abc" {
		t.Errorf("unsigned = %v, want org.matrix.msc4140.delay_id", unsigned)
	}
	if _, ok := unsigned["delay_id"]; ok {
		t.Error("a bare delay_id key must not be emitted")
	}
}

// The transaction ID is the sender's own idempotency key. Revealing it to
// another of the user's sessions would leak which client sent what.
func TestTransactionIDIsRevealedOnlyToTheSendingSession(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.message", JSON: []byte(pdu),
		InternalMetadata: []byte(`{"txn_id":"abc","device_id":"DEV1"}`)}

	cases := []struct {
		name string
		req  Requester
		want bool
	}{
		{"same user and device", Requester{UserID: "@u:example.com", DeviceID: "DEV1"}, true},
		{"same user, other device", Requester{UserID: "@u:example.com", DeviceID: "DEV2"}, false},
		{"other user", Requester{UserID: "@other:example.com", DeviceID: "DEV1"}, false},
		{"no requester", Requester{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Serialize(ev, 3000, Config{Format: FormatV2, Requester: tc.req})
			if err != nil {
				t.Fatal(err)
			}
			unsigned := decode(t, out)["unsigned"].(map[string]any)
			_, got := unsigned["transaction_id"]
			if got != tc.want {
				t.Errorf("transaction_id present = %v, want %v", got, tc.want)
			}
		})
	}
}

// Events stored before device IDs were recorded, and those from appservices,
// guests or admin tokens, fall back to matching the access token.
func TestTransactionIDTokenFallback(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.message", JSON: []byte(pdu),
		InternalMetadata: []byte(`{"txn_id":"abc","token_id":42}`)}

	for _, tc := range []struct {
		name string
		req  Requester
		want bool
	}{
		{"matching token", Requester{UserID: "@u:example.com", TokenID: 42}, true},
		{"other token", Requester{UserID: "@u:example.com", TokenID: 43}, false},
		{"unknown token", Requester{UserID: "@u:example.com"}, false},
		{"guest is assumed same session", Requester{UserID: "@u:example.com", IsGuest: true}, true},
		{"appservice is assumed same session", Requester{UserID: "@u:example.com", AppServiceID: "as"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Serialize(ev, 3000, Config{Format: FormatV2, Requester: tc.req})
			if err != nil {
				t.Fatal(err)
			}
			unsigned := decode(t, out)["unsigned"].(map[string]any)
			_, got := unsigned["transaction_id"]
			if got != tc.want {
				t.Errorf("transaction_id present = %v, want %v", got, tc.want)
			}
		})
	}
}

// Stripped room state is for the invite section of /sync only; everywhere else
// it would leak state of a room the caller has not joined.
func TestStrippedRoomStateIsRemovedByDefault(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.member",
		JSON: []byte(`{"type":"m.room.member","sender":"@u:e","content":{},"unsigned":{"invite_room_state":[{"type":"m.room.name"}]}}`)}

	out, err := Serialize(ev, 0, Config{Format: FormatV2})
	if err != nil {
		t.Fatal(err)
	}
	unsigned := decode(t, out)["unsigned"].(map[string]any)
	if _, ok := unsigned["invite_room_state"]; ok {
		t.Error("invite_room_state should be stripped by default")
	}

	out, err = Serialize(ev, 0, Config{Format: FormatV2, IncludeStrippedRoomState: true})
	if err != nil {
		t.Fatal(err)
	}
	unsigned = decode(t, out)["unsigned"].(map[string]any)
	if _, ok := unsigned["invite_room_state"]; !ok {
		t.Error("invite_room_state should survive when requested")
	}
}

// MSC4115. Synapse attaches it to timeline events but not to the state block,
// which wraps events with FilteredEvent.state(membership=None).
func TestMembershipIsAttachedWhenGiven(t *testing.T) {
	got := serialize(t, FormatV1, Stored{Membership: "join"}, 3000)
	unsigned := got["unsigned"].(map[string]any)
	if unsigned["membership"] != "join" {
		t.Errorf("unsigned.membership = %v", unsigned["membership"])
	}

	got = serialize(t, FormatV1, Stored{}, 3000)
	unsigned = got["unsigned"].(map[string]any)
	if _, ok := unsigned["membership"]; ok {
		t.Error("membership must be omitted when not supplied")
	}
}

// An event with no unsigned block at all must still serialise: the v1 format
// reads unsigned unconditionally.
func TestMissingUnsignedIsCreated(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.message",
		JSON: []byte(`{"type":"m.room.message","sender":"@u:e","content":{}}`)}
	out, err := Serialize(ev, 0, Config{Format: FormatV1})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	got := decode(t, out)
	if _, ok := got["unsigned"]; !ok {
		t.Error("unsigned should have been created")
	}
}

// Synapse models unsigned as a typed struct of six fields, so anything else in
// storage is dropped when the event is loaded. Remote servers send `age` and
// Synapse stores it; passing that through emits a stale age -- baked in at
// receive time, potentially years old -- on events Synapse gives no age at all.
func TestUnsignedIsRebuiltFromAllowlist(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.member", JSON: []byte(`{
		"type":"m.room.member","sender":"@u:e","content":{},
		"unsigned":{"age":310,"replaces_state":"$old","something_else":"x",
		            "prev_sender":"@p:e"}}`)}

	out, err := Serialize(ev, 5000, Config{Format: FormatV1})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	got := decode(t, out)
	unsigned := got["unsigned"].(map[string]any)

	if _, ok := unsigned["age"]; ok {
		t.Error("a stored age must be dropped: it is stale, and Synapse recomputes age from age_ts")
	}
	if _, ok := got["age"]; ok {
		t.Error("a dropped age must not reappear at the top level via the v1 copy")
	}
	if _, ok := unsigned["something_else"]; ok {
		t.Error("unknown unsigned fields must be dropped")
	}
	if unsigned["replaces_state"] != "$old" {
		t.Errorf("replaces_state = %v, want it kept", unsigned["replaces_state"])
	}
	if unsigned["prev_sender"] != "@p:e" {
		t.Errorf("prev_sender = %v, want it kept", unsigned["prev_sender"])
	}
}

// With age_ts present, age is computed rather than carried, so a stored age is
// replaced rather than merely removed.
func TestStoredAgeIsReplacedByComputedAge(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.message", JSON: []byte(`{
		"type":"m.room.message","sender":"@u:e","content":{},
		"unsigned":{"age":999999,"age_ts":1000}}`)}

	out, err := Serialize(ev, 3000, Config{Format: FormatV1})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	got := decode(t, out)
	unsigned := got["unsigned"].(map[string]any)
	if unsigned["age"] != float64(2000) {
		t.Errorf("unsigned.age = %v, want 2000 (computed, not the stored 999999)", unsigned["age"])
	}
}

// unsigned is serialised unconditionally, empty as {}.
func TestUnsignedIsAlwaysPresent(t *testing.T) {
	ev := Stored{EventID: "$e", Type: "m.room.message",
		JSON: []byte(`{"type":"m.room.message","sender":"@u:e","content":{}}`)}
	out, err := Serialize(ev, 0, Config{Format: FormatV2})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	got := decode(t, out)
	unsigned, ok := got["unsigned"].(map[string]any)
	if !ok {
		t.Fatalf("unsigned = %v, want an object", got["unsigned"])
	}
	if len(unsigned) != 0 {
		t.Errorf("unsigned = %v, want empty", unsigned)
	}
}
