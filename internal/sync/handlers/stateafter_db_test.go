package handlers

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// The server leaves a room inside the window (its last local user, alice,
// leaves). Synapse then copies the state from BEFORE the leaving batch into
// the delta stream as removals, so the batch's own changes have only a stale
// delta (the topic) or none (the new pinned events), and alice's earlier
// display name change is superseded by her leave. All of them come from the
// state at the leave (Synapse #20081, #20314).
//
//	stream  event                 delta rows
//	   5    alice renames ($aj2)  5: alice $aj -> $aj2
//	  10    topic $t2             10: name, topic, alice removed
//	  11    pinned $pin               (prev: what they held before 10)
//	  12    alice leaves $al
func TestIncrementalStateAfterServerLeft(t *testing.T) {
	admin := os.Getenv("GOSYNC_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip(`GOSYNC_TEST_ADMIN_DSN not set (a throwaway PostgreSQL, e.g. "host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse")`)
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("GOSYNC_TEST_ADMIN_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	ac, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("stateafter_test_%d", time.Now().UnixNano())
	if _, err := ac.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = ac.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		_ = ac.Close(context.Background())
	})
	dsn := admin + " dbname=" + name
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	exec(roomStateSchema)
	exec(`CREATE TABLE instance_map (instance_id INTEGER PRIMARY KEY, instance_name TEXT NOT NULL)`)
	exec(`CREATE TABLE current_state_delta_stream (stream_id BIGINT NOT NULL, room_id TEXT NOT NULL,
		type TEXT NOT NULL, state_key TEXT NOT NULL, event_id TEXT, prev_event_id TEXT, instance_name TEXT)`)
	exec(`INSERT INTO instance_map VALUES (1, 'master')`)

	const room, alice = "!r:example.org", "@alice:example.org"
	for _, e := range []struct {
		id, typ, key string
		so           int64
	}{
		{"$aj2", "m.room.member", alice, 5}, {"$t2", "m.room.topic", "", 10},
		{"$pin", "m.room.pinned_events", "", 11}, {"$al", "m.room.member", alice, 12},
	} {
		exec(`INSERT INTO events (topological_ordering, event_id, type, room_id, processed, outlier,
			stream_ordering, state_key, instance_name, sender) VALUES ($1, $2, $3, $4, true, false, $1, $5, 'master', $6)`,
			e.so, e.id, e.typ, room, e.key, alice)
	}
	// The state after the leave.
	exec(`INSERT INTO state_groups (id, room_id, event_id) VALUES (7, $1, '$al')`, room)
	for _, s := range [][3]string{{"m.room.name", "", "$name"}, {"m.room.topic", "", "$t2"},
		{"m.room.pinned_events", "", "$pin"}, {"m.room.member", alice, "$al"}} {
		exec(`INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (7, $1, $2, $3, $4)`,
			room, s[0], s[1], s[2])
	}
	exec(`INSERT INTO event_to_state_groups (event_id, state_group) VALUES ('$al', 7)`)
	for _, d := range []struct {
		stream      int64
		typ, key    string
		event, prev any
	}{
		{5, "m.room.member", alice, "$aj2", "$aj"},
		{10, "m.room.name", "", nil, "$name"},
		{10, "m.room.topic", "", nil, "$t1"},
		{10, "m.room.member", alice, nil, "$aj2"},
	} {
		exec(`INSERT INTO current_state_delta_stream VALUES ($1, $2, $3, $4, $5, $6, 'master')`,
			d.stream, room, d.typ, d.key, d.event, d.prev)
	}

	st, err := store.Open(ctx, store.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	d := Deps{Store: st}

	timeline := []store.TimelineEvent{
		{Stored: storedOf("$t2", "m.room.topic"), Sender: alice, IsState: true},
		{Stored: storedOf("$pin", "m.room.pinned_events"), Sender: alice, IsState: true},
		{Stored: storedOf("$al", "m.room.member"), Sender: alice, StateKey: alice, IsState: true},
	}
	since := streamtoken.Token{Room: streamtoken.RoomKey{Topological: streamtoken.NoTopological, Stream: 4}}
	end := streamtoken.RoomKey{Topological: streamtoken.NoTopological, Stream: 12}
	ids, changed, err := incrementalStateAfter(ctx, d, room, alice, since, end, timeline, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want := map[store.StateKey]string{
		{Type: "m.room.topic"}:                   "$t2",
		{Type: "m.room.pinned_events"}:           "$pin",
		{Type: "m.room.member", StateKey: alice}: "$al",
	}
	if len(ids) != len(want) {
		t.Errorf("state_after %v, want %v", ids, want)
	}
	for k, id := range want {
		if ids[k] != id || !changed[k] {
			t.Errorf("%v: %q (changed %v), want %q", k, ids[k], changed[k], id)
		}
	}
	if _, ok := ids[store.StateKey{Type: "m.room.name"}]; ok {
		t.Error("the name did not change; a removal outside the timeline is not looked up")
	}
}

func storedOf(id, typ string) clientevent.Stored {
	return clientevent.Stored{EventID: id, Type: typ}
}
