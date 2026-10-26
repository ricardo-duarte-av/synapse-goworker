package handlers

import (
	"testing"
)

func TestStrippedStateEvents(t *testing.T) {
	body := []byte(`{"type":"m.room.member","state_key":"@a:x","content":{"membership":"invite"},` +
		`"unsigned":{"age":5,"invite_room_state":[{"type":"m.room.name","state_key":"","content":{"name":"R"}},` +
		`{"type":"m.room.create","state_key":"","content":{"room_version":"12"}}]}}`)

	events, err := strippedStateEvents(body, "invite_room_state")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"type":"m.room.name","state_key":"","content":{"name":"R"}}`,
		`{"type":"m.room.create","state_key":"","content":{"room_version":"12"}}`,
		`{"type":"m.room.member","state_key":"@a:x","content":{"membership":"invite"},"unsigned":{"age":5}}`,
	}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d: %s", len(events), len(want), events)
	}
	for i := range want {
		if string(events[i]) != want[i] {
			t.Errorf("event %d:\n got %s\nwant %s", i, events[i], want[i])
		}
	}
}

func TestStrippedStateEventsWithoutState(t *testing.T) {
	cases := map[string]string{
		"absent":   `{"type":"m.room.member","unsigned":{"age":5}}`,
		"not list": `{"type":"m.room.member","unsigned":{"age":5,"knock_room_state":{"x":1}}}`,
	}
	for name, body := range cases {
		events, err := strippedStateEvents([]byte(body), "knock_room_state")
		if err != nil {
			t.Fatal(err)
		}
		const want = `{"type":"m.room.member","unsigned":{"age":5}}`
		if len(events) != 1 || string(events[0]) != want {
			t.Errorf("%s: got %s, want [%s]", name, events, want)
		}
	}

	// Synapse writes `unsigned` back even when the pop emptied it.
	events, err := strippedStateEvents([]byte(`{"unsigned":{"invite_room_state":[]}}`), "invite_room_state")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || string(events[0]) != `{"unsigned":{}}` {
		t.Errorf("got %s", events)
	}
}
