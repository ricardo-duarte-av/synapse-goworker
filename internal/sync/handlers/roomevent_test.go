package handlers

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// The C1 fixture, and in !r:
//
//	$m1   alice, while bob was joined (group 3); bob replied in a thread
//	      ($t1) and alice edited it ($e1)
//	$m2   alice, after bob left (group 2)
//	$call a call invite; !r has no join rule
//	$rej  rejected
//
// !pub is public and holds a call invite, $pubcall. !u has a room version
// nobody knows. !w (world_readable) holds $wmsg.
func newEventFixture(t *testing.T, msc2815 bool) (*stateFixture, func(user, path string) (int, []byte)) {
	t.Helper()
	f := newStateFixture(t)
	const r, w = "!r:example.org", "!w:example.org"
	ev, exec := f.ev, f.exec

	// Group 3: the state while bob was joined, in full.
	exec(`INSERT INTO state_groups (id, room_id, event_id) VALUES (3, $1, '$bobjoin')`, r)
	for _, row := range [][3]string{{"m.room.create", "", "$create"}, {"m.room.member", "@alice:example.org", "$alice"},
		{"m.room.history_visibility", "", "$hv"}, {"m.room.topic", "", "$topic1"}, {"m.room.member", "@bob:example.org", "$bobjoin"}} {
		exec(`INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (3, $1, $2, $3, $4)`, r, row[0], row[1], row[2])
	}
	ev(r, "$m1", "m.room.message", "-", "@alice:example.org", `{"msgtype":"m.text","body":"hello"}`)
	ev(r, "$t1", "m.room.message", "-", "@bob:example.org",
		`{"msgtype":"m.text","body":"reply","m.relates_to":{"rel_type":"m.thread","event_id":"$m1"}}`)
	ev(r, "$e1", "m.room.message", "-", "@alice:example.org",
		`{"msgtype":"m.text","body":"* hi","m.new_content":{"msgtype":"m.text","body":"hi"},"m.relates_to":{"rel_type":"m.replace","event_id":"$m1"}}`)
	ev(r, "$m2", "m.room.message", "-", "@alice:example.org", `{"msgtype":"m.text","body":"after"}`)
	ev(r, "$call", "m.call.invite", "-", "@alice:example.org", `{"call_id":"c"}`)
	ev(r, "$rej", "m.room.message", "-", "@alice:example.org", `{"body":"no"}`)
	exec(`INSERT INTO rejections (event_id, reason, last_check) VALUES ('$rej', 'auth_error', '1')`)
	exec(`INSERT INTO event_relations (event_id, relates_to_id, relation_type) VALUES ('$t1', '$m1', 'm.thread'), ('$e1', '$m1', 'm.replace')`)
	exec(`INSERT INTO event_to_state_groups (event_id, state_group) VALUES
		('$m1', 3), ('$t1', 3), ('$e1', 2), ('$m2', 2), ('$call', 2), ('$rej', 2), ('$name', 2)`)

	const pub = "!pub:example.org"
	exec(`INSERT INTO rooms (room_id, room_version) VALUES ($1, '10')`, pub)
	ev(pub, "$pcreate", "m.room.create", "", "@alice:example.org", `{"room_version":"10"}`)
	ev(pub, "$palice", "m.room.member", "@alice:example.org", "@alice:example.org", `{"membership":"join"}`)
	ev(pub, "$pjr", "m.room.join_rules", "", "@alice:example.org", `{"join_rule":"public"}`)
	ev(pub, "$pubcall", "m.call.invite", "-", "@alice:example.org", `{"call_id":"c"}`)
	ev(pub, "$pubmsg", "m.room.message", "-", "@alice:example.org", `{"body":"x"}`)
	exec(`INSERT INTO state_groups (id, room_id, event_id) VALUES (10, $1, '$pjr')`, pub)
	for _, row := range [][3]string{{"m.room.create", "", "$pcreate"}, {"m.room.member", "@alice:example.org", "$palice"},
		{"m.room.join_rules", "", "$pjr"}} {
		exec(`INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (10, $1, $2, $3, $4)`, pub, row[0], row[1], row[2])
	}
	exec(`INSERT INTO event_to_state_groups (event_id, state_group) VALUES ('$pubcall', 10), ('$pubmsg', 10)`)
	exec(`INSERT INTO room_memberships (event_id, user_id, sender, room_id, membership) VALUES ('$palice', '@alice:example.org', '@alice:example.org', $1, 'join')`, pub)
	exec(`INSERT INTO local_current_membership (room_id, user_id, event_id, membership) VALUES ($1, '@alice:example.org', '$palice', 'join')`, pub)

	exec(`INSERT INTO rooms (room_id, room_version) VALUES ('!u:example.org', 'org.example.unknown')`)
	ev("!u:example.org", "$u", "m.room.message", "-", "@alice:example.org", `{"body":"?"}`)

	ev(w, "$wmsg", "m.room.message", "-", "@alice:example.org", `{"body":"public"}`)
	exec(`INSERT INTO state_groups (id, room_id, event_id) VALUES (20, $1, '$whv')`, w)
	exec(`INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES
		(20, $1, 'm.room.create', '', '$wcreate'), (20, $1, 'm.room.history_visibility', '', '$whv')`, w)
	exec(`INSERT INTO event_to_state_groups (event_id, state_group) VALUES ('$wmsg', 20)`)

	d := f.deps
	d.MSC2815Enabled = msc2815
	h := RoomEvent(d)
	return f, func(user, path string) (int, []byte) { return serve(h, user, path) }
}

const notFound = `{"errcode":"M_NOT_FOUND","error":"Event not found."}`

func eventPath(room, id string) string {
	return "/_matrix/client/v3/rooms/" + strings.NewReplacer("!", "%21", ":", "%3A").Replace(room) +
		"/event/" + strings.ReplaceAll(id, "$", "%24")
}

func TestRoomEvent(t *testing.T) {
	f, get := newEventFixture(t, false)
	const r = "!r:example.org"

	t.Run("joined: the original, its edit and thread bundled", func(t *testing.T) {
		code, body := get("alice", eventPath(r, "$m1"))
		b := gjson.ParseBytes(body)
		if code != 200 || b.Get("event_id").Str != "$m1" || b.Get("content.body").Str != "hello" {
			t.Fatalf("%d %s", code, body)
		}
		if b.Get("user_id").Str != "@alice:example.org" || b.Get("room_id").Str != r {
			t.Errorf("not in the v1 format: %s", body)
		}
		if b.Get("unsigned.membership").Str != "join" {
			t.Errorf("unsigned.membership: %s", body)
		}
		rel := b.Get(`unsigned.m\.relations`)
		if rel.Get(`m\.replace.event_id`).Str != "$e1" {
			t.Errorf("no edit bundled: %s", rel.Raw)
		}
		th := rel.Get(`m\.thread`)
		// The root's sender is a participant.
		if th.Get("count").Int() != 1 || th.Get("latest_event.event_id").Str != "$t1" ||
			!th.Get("current_user_participated").Bool() {
			t.Errorf("thread: %s", th.Raw)
		}
	})
	t.Run("left: what was seen while joined, nothing after", func(t *testing.T) {
		code, body := get("bob", eventPath(r, "$m1"))
		if code != 200 || gjson.GetBytes(body, "unsigned.membership").Str != "join" {
			t.Fatalf("%d %s", code, body)
		}
		if gjson.GetBytes(body, `unsigned.m\.relations.m\.thread.current_user_participated`).Bool() != true {
			t.Errorf("bob replied in the thread: %s", body)
		}
		if code, body := get("bob", eventPath(r, "$m2")); code != 404 || string(body) != notFound {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("not a member, not world_readable", func(t *testing.T) {
		if code, body := get("eve", eventPath(r, "$m1")); code != 404 || string(body) != notFound {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("world_readable: peeking", func(t *testing.T) {
		code, body := get("eve", eventPath("!w:example.org", "$wmsg"))
		if code != 200 || gjson.GetBytes(body, "unsigned.membership").Str != "leave" {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("redacted: pruned, with the redaction", func(t *testing.T) {
		code, body := get("alice", eventPath(r, "$name"))
		b := gjson.ParseBytes(body)
		if code != 200 || b.Get("content").Raw != "{}" || b.Get("unsigned.redacted_by").Str != "$redact2" ||
			b.Get("unsigned.redacted_because.event_id").Str != "$redact2" {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("not found", func(t *testing.T) {
		for _, c := range [][2]string{
			{r, "$nope"},                     // not stored
			{r, "$rej"},                      // rejected
			{"!w:example.org", "$m1"},        // another room's
			{"!pub:example.org", "$pubcall"}, // a call invite in a public room
		} {
			if code, body := get("alice", eventPath(c[0], c[1])); code != 404 || string(body) != notFound {
				t.Errorf("%s: %d %s", c[1], code, body)
			}
		}
	})
	t.Run("call invites outside public rooms", func(t *testing.T) {
		if code, body := get("alice", eventPath(r, "$call")); code != 200 {
			t.Fatalf("%d %s", code, body)
		}
		if code, body := get("alice", eventPath("!pub:example.org", "$pubmsg")); code != 200 {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("MSC2815 disabled: the parameter is ignored", func(t *testing.T) {
		if code, body := get("alice", eventPath(r, "$m1")+"?fi.mau.msc2815.include_unredacted_content=true"); code != 200 {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("handed back", func(t *testing.T) {
		f.handedIn = nil
		for _, p := range []string{
			eventPath("!u:example.org", "$u"),                       // unknown room version
			"/_matrix/client/v3/rooms/%21r%3Aexample.org/event/a/b", // not Synapse's path
		} {
			if code, body := get("alice", p); code != 299 {
				t.Errorf("%s: %d %s", p, code, body)
			}
		}
		if len(f.handedIn) != 2 {
			t.Fatalf("handed back %v", f.handedIn)
		}
	})

	_, get2815 := newEventFixture(t, true)
	t.Run("MSC2815 enabled and asked for: Synapse's", func(t *testing.T) {
		if code, _ := get2815("alice", eventPath(r, "$m1")+"?fi.mau.msc2815.include_unredacted_content=false"); code != 299 {
			t.Fatalf("%d", code)
		}
		if code, _ := get2815("alice", eventPath(r, "$m1")); code != 200 {
			t.Fatalf("%d", code)
		}
	})
}
