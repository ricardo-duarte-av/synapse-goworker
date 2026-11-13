package handlers

import (
	"encoding/json"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// The tags of an announcement entry: m.server_notice first, the user's own
// after it, theirs winning on a clash, as {SERVER_NOTICE_ROOM_TAG: {},
// **(tags or {})} builds them; and the stored m.tag never twice.
func TestAnnouncementAccountData(t *testing.T) {
	rest := []store.AccountDataEntry{
		{Type: "m.tag", Content: json.RawMessage(`{"tags":{"stale":{}}}`)},
		{Type: "m.fully_read", Content: json.RawMessage(`{"event_id":"$e"}`)},
	}
	for _, c := range []struct {
		tags string
		want string
	}{
		{"", `{"tags":{"m.server_notice":{}}}`},
		{`{"tags":{}}`, `{"tags":{"m.server_notice":{}}}`},
		{`{"tags":{"m.favourite":{"order":0.5},"u.work":{}}}`,
			`{"tags":{"m.server_notice":{},"m.favourite":{"order":0.5},"u.work":{}}}`},
		{`{"tags":{"m.favourite":{},"m.server_notice":{"order":1}}}`,
			`{"tags":{"m.server_notice":{"order":1},"m.favourite":{}}}`},
	} {
		got := announcementAccountData(json.RawMessage(c.tags), rest)
		if len(got) != 2 || got[0].Type != "m.tag" || got[1].Type != "m.fully_read" {
			t.Fatalf("%s: %+v", c.tags, got)
		}
		if string(got[0].Content) != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.tags, got[0].Content, c.want)
		}
	}
}

// synthetic_join_announcement_room_ids: the filter's rooms, less joined and
// excluded rooms, and less rooms whose last change in the window is not a
// leave or a ban.
func TestSyntheticJoinRooms(t *testing.T) {
	set := announcementSet{
		all:     map[string]bool{"!a": true, "!b": true, "!c": true, "!d": true, "!e": true, "!f": true},
		forUser: map[string]bool{"!a": true, "!b": true, "!c": true, "!d": true, "!e": true, "!f": true},
	}
	f, err := filter.New(json.RawMessage(`{"room":{"not_rooms":["!f"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	d := Deps{ExcludedRooms: map[string]bool{"!e": true}}
	got := syntheticJoinRooms(d, set, f, map[string]bool{"!b": true},
		map[string]string{"!c": "invite", "!d": "ban", "!a": "leave"})
	want := map[string]bool{"!a": true, "!d": true}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// Nothing is read while no room is configured.
func TestAnnouncementRoomsUnconfigured(t *testing.T) {
	d := Deps{Announcements: func() synapsecfg.Announcements { return synapsecfg.Announcements{} }}
	set, err := announcementRooms(t.Context(), d, "@u:x")
	if err != nil || set.all != nil || set.forUser != nil {
		t.Fatalf("%+v %v", set, err)
	}
}

// Taking the user's membership out leaves the memberships aligned.
func TestStripOwnMembership(t *testing.T) {
	v := &announcementView{senders: &senderSet{}}
	msgs := []store.TimelineEvent{
		{Stored: clientevent.Stored{EventID: "$1", Type: "m.room.message"}, Sender: "@x:s"},
		{Stored: clientevent.Stored{EventID: "$2", Type: "m.room.member"}, IsState: true, StateKey: "@u:s", Sender: "@mod:s"},
		{Stored: clientevent.Stored{EventID: "$3", Type: "m.room.member"}, IsState: true, StateKey: "@x:s", Sender: "@x:s"},
	}
	state := map[store.StateKey]string{
		{Type: "m.room.member", StateKey: "@u:s"}: "$0",
		{Type: "m.room.name"}:                     "$n",
	}
	m, ms := v.stripOwnMembership("@u:s", msgs, []string{"join", "leave", "join"}, state)
	if len(m) != 2 || m[0].EventID != "$1" || m[1].EventID != "$3" || len(ms) != 2 || ms[1] != "join" {
		t.Fatalf("%+v %v", m, ms)
	}
	if _, ok := state[store.StateKey{Type: "m.room.member", StateKey: "@u:s"}]; ok || len(state) != 1 {
		t.Fatalf("state %v", state)
	}
	if s := v.senders.list(); len(s) != 1 || s[0] != "@mod:s" {
		t.Fatalf("senders %v", s)
	}
}
