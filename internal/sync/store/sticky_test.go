package store

import (
	"context"
	"testing"
)

const stickySchema = `
CREATE TABLE event_json (event_id text NOT NULL, room_id text NOT NULL, internal_metadata text NOT NULL,
	json text NOT NULL, format_version integer);
CREATE TABLE sticky_events (stream_id integer NOT NULL PRIMARY KEY, instance_name text NOT NULL,
	event_id text NOT NULL, room_id text NOT NULL, event_stream_ordering integer NOT NULL UNIQUE,
	sender text NOT NULL, expires_at bigint NOT NULL);
CREATE SEQUENCE sticky_events_sequence;`

// The position reported is Synapse v1.163's (#20186): the last row when the
// limit cut the read short, else the whole range, never behind from.
func TestStickyEventsPosition(t *testing.T) {
	st, exec := throwawayStore(t, stickySchema)
	ctx := context.Background()
	for i, id := range []string{"$a", "$b", "$c"} {
		exec(`INSERT INTO event_json (event_id, room_id, internal_metadata, json) VALUES ($1, '!r', '{}', '{}')`, id)
		exec(`INSERT INTO sticky_events VALUES ($1, 'p', $2, '!r', $1, '@u:x', 9999999999999)`, i+1, id)
	}
	rooms := []string{"!r"}
	for _, c := range []struct {
		name     string
		from, to int64
		limit    int
		want     int64
		n        int
	}{
		{"whole range: to, not the last row", 0, 10, 100, 10, 3},
		{"limited: the last row", 0, 10, 2, 2, 2},
		{"exactly the limit: the last row", 0, 3, 3, 3, 3},
		{"since ahead of now: not rewound", 8, 5, 100, 8, 0},
		{"no limit at all: from", 1, 10, 0, 1, 0},
		{"no rooms", 1, 10, 100, 10, 0},
	} {
		r := rooms
		if c.name == "no rooms" {
			r = nil
		}
		got, byRoom, err := st.StickyEvents(ctx, r, c.from, c.to, 0, c.limit)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want || len(byRoom["!r"]) != c.n {
			t.Errorf("%s: %d %v; want %d and %d events", c.name, got, byRoom, c.want, c.n)
		}
	}

	if got, err := st.StickyMaxAllocated(ctx); err != nil || got != 0 {
		t.Errorf("fresh sequence: %d %v", got, err)
	}
	exec(`SELECT setval('sticky_events_sequence', 42)`)
	if got, err := st.StickyMaxAllocated(ctx); err != nil || got != 42 {
		t.Errorf("after 42: %d %v", got, err)
	}
}
