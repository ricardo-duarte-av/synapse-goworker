package clientevent

import (
	"encoding/json"
	"testing"
)

func redact(t *testing.T, raw string, version string) map[string]any {
	t.Helper()
	out, err := Redact([]byte(raw), LookupRoomVersion(version))
	if err != nil {
		t.Fatalf("Redact: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out)
	}
	return m
}

// The whole point: a redacted event keeps its original body in storage until a
// background job censors it, so serving stored JSON unchanged publishes exactly
// what the redaction was meant to remove.
func TestRedactStripsMessageContent(t *testing.T) {
	got := redact(t, `{
		"type":"m.room.message","sender":"@u:e","room_id":"!r:e",
		"origin_server_ts":1000,"depth":5,"auth_events":["$a"],"prev_events":["$b"],
		"content":{"body":"something private","msgtype":"m.text"},
		"unsigned":{"age_ts":900,"replaces_state":"$old","transaction_id":"txn"}}`, "10")

	content := got["content"].(map[string]any)
	if len(content) != 0 {
		t.Errorf("content = %v, want empty", content)
	}
	for _, keep := range []string{"type", "sender", "room_id", "origin_server_ts",
		"depth", "auth_events", "prev_events"} {
		if _, ok := got[keep]; !ok {
			t.Errorf("%s should survive redaction", keep)
		}
	}
	// Redaction rebuilds unsigned rather than clearing it: two fields survive.
	unsigned := got["unsigned"].(map[string]any)
	if unsigned["age_ts"] != float64(900) || unsigned["replaces_state"] != "$old" {
		t.Errorf("unsigned = %v, want age_ts and replaces_state kept", unsigned)
	}
	if _, ok := unsigned["transaction_id"]; ok {
		t.Error("transaction_id must not survive redaction")
	}
}

// Room version 11 dropped three top-level fields from the allowlist.
func TestRedactAllowlistDiffersByVersion(t *testing.T) {
	const ev = `{"type":"m.room.message","sender":"@u:e","content":{},
		"prev_state":[],"membership":"join","origin":"e"}`

	old := redact(t, ev, "10")
	for _, k := range []string{"prev_state", "membership", "origin"} {
		if _, ok := old[k]; !ok {
			t.Errorf("v10 should keep %s", k)
		}
	}
	modern := redact(t, ev, "11")
	for _, k := range []string{"prev_state", "membership", "origin"} {
		if _, ok := modern[k]; ok {
			t.Errorf("v11 should drop %s", k)
		}
	}
}

// Aliases survive redaction only in room versions 1 to 5, where they are
// special-cased in auth.
func TestRedactAliasesOnlyInOldVersions(t *testing.T) {
	const ev = `{"type":"m.room.aliases","sender":"@u:e","state_key":"e",
		"content":{"aliases":["#a:e"]}}`
	if got := redact(t, ev, "5")["content"].(map[string]any); got["aliases"] == nil {
		t.Error("v5 should keep aliases")
	}
	if got := redact(t, ev, "6")["content"].(map[string]any); got["aliases"] != nil {
		t.Error("v6 should drop aliases")
	}
}

// MSC2176: from room version 11 a create event's content survives intact.
func TestRedactCreateEvent(t *testing.T) {
	const ev = `{"type":"m.room.create","sender":"@u:e","state_key":"",
		"content":{"creator":"@u:e","room_version":"11","extra":"kept"}}`

	old := redact(t, ev, "10")["content"].(map[string]any)
	if old["creator"] != "@u:e" {
		t.Error("v10 should keep creator")
	}
	if _, ok := old["extra"]; ok {
		t.Error("v10 should drop other create content")
	}

	modern := redact(t, ev, "11")["content"].(map[string]any)
	if modern["extra"] != "kept" {
		t.Errorf("v11 should keep the whole create content, got %v", modern)
	}
	if _, ok := modern["creator"]; !ok {
		t.Error("v11 keeps creator too, as part of the whole content")
	}
}

// A v12 create event has no room_id -- the room ID is its hash -- so redaction
// must not reintroduce one.
func TestRedactCreateEventDropsRoomIDForHashRooms(t *testing.T) {
	const ev = `{"type":"m.room.create","sender":"@u:e","state_key":"",
		"room_id":"!should-not-be-here","content":{}}`
	if _, ok := redact(t, ev, "12")["room_id"]; ok {
		t.Error("a v12 create event must not keep room_id through redaction")
	}
	if _, ok := redact(t, ev, "10")["room_id"]; !ok {
		t.Error("a v10 create event keeps room_id")
	}
}

func TestRedactPowerLevelsAndMembership(t *testing.T) {
	pl := redact(t, `{"type":"m.room.power_levels","sender":"@u:e","state_key":"",
		"content":{"users":{"@a:e":100},"invite":50,"notifications":{"room":50}}}`, "11")
	content := pl["content"].(map[string]any)
	if content["users"] == nil {
		t.Error("users must survive")
	}
	if content["invite"] == nil {
		t.Error("v11 keeps invite")
	}
	if content["notifications"] != nil {
		t.Error("notifications must not survive")
	}

	mem := redact(t, `{"type":"m.room.member","sender":"@u:e","state_key":"@u:e",
		"content":{"membership":"join","displayname":"Bob",
		           "join_authorised_via_users_server":"@a:e"}}`, "10")
	mc := mem["content"].(map[string]any)
	if mc["membership"] != "join" {
		t.Error("membership must survive")
	}
	if mc["displayname"] != nil {
		t.Error("displayname must not survive redaction")
	}
	if mc["join_authorised_via_users_server"] == nil {
		t.Error("v10 keeps join_authorised_via_users_server")
	}
}

// Keys with dots are the norm in Matrix, and sjson treats a dot as nesting, so
// an unescaped key silently builds a nested object instead of the field.
func TestRedactHandlesDottedContentKeys(t *testing.T) {
	got := redact(t, `{"type":"m.room.message","sender":"@u:e",
		"content":{"m.relates_to":{"rel_type":"m.thread","event_id":"$t"},"body":"x"}}`,
		"org.matrix.msc3389.10")
	content := got["content"].(map[string]any)
	rel, ok := content["m.relates_to"].(map[string]any)
	if !ok {
		t.Fatalf("m.relates_to should be a top-level content key, got %v", content)
	}
	if rel["rel_type"] != "m.thread" || rel["event_id"] != "$t" {
		t.Errorf("m.relates_to = %v, want rel_type and event_id kept", rel)
	}
}

// Without MSC3389 the relation is dropped like any other content.
func TestRedactDropsRelationWithoutMSC3389(t *testing.T) {
	got := redact(t, `{"type":"m.room.message","sender":"@u:e",
		"content":{"m.relates_to":{"rel_type":"m.thread"}}}`, "10")
	if len(got["content"].(map[string]any)) != 0 {
		t.Errorf("content = %v, want empty", got["content"])
	}
}

// A redacted event is served with the redaction attached, so a client can show
// who removed it and why.
func TestSerializeAttachesRedactionMetadata(t *testing.T) {
	ev := Stored{
		EventID: "$e", Type: "m.room.message", RoomVersion: "10",
		JSON: []byte(`{"type":"m.room.message","sender":"@u:e","room_id":"!r:e",
			"content":{"body":"secret"},"unsigned":{"age_ts":900}}`),
		RedactedBy:      "$redaction",
		RedactedBecause: []byte(`{"type":"m.room.redaction","event_id":"$redaction"}`),
	}
	out, err := Serialize(ev, 1000, Config{Format: FormatV1})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	got := decode(t, out)

	if len(got["content"].(map[string]any)) != 0 {
		t.Errorf("content = %v, want stripped", got["content"])
	}
	unsigned := got["unsigned"].(map[string]any)
	if unsigned["redacted_by"] != "$redaction" {
		t.Errorf("redacted_by = %v", unsigned["redacted_by"])
	}
	if unsigned["redacted_because"] == nil {
		t.Error("redacted_because should be attached")
	}
	// v1 lifts redacted_because to the top level along with the other copy keys.
	if got["redacted_because"] == nil {
		t.Error("the v1 format should lift redacted_because to the top level")
	}
	// age is still computed from the surviving age_ts.
	if unsigned["age"] != float64(100) {
		t.Errorf("unsigned.age = %v, want 100", unsigned["age"])
	}
}
