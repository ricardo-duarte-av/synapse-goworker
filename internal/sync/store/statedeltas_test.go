package store

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// throwawayStore is a Store on a fresh database in a throwaway PostgreSQL,
// with the given schema. Never the live one.
func throwawayStore(t *testing.T, schema string) (*Store, func(sql string, args ...any)) {
	t.Helper()
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
	name := fmt.Sprintf("statedeltas_test_%d", time.Now().UnixNano())
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
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	st, err := Open(ctx, Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st, func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
}

const stateDeltasSchema = `
	CREATE TABLE instance_map (instance_id INTEGER PRIMARY KEY, instance_name TEXT NOT NULL);
	CREATE TABLE events (event_id TEXT PRIMARY KEY, instance_name TEXT, stream_ordering BIGINT);
	CREATE TABLE current_state_delta_stream (stream_id BIGINT NOT NULL, room_id TEXT NOT NULL,
		type TEXT NOT NULL, state_key TEXT NOT NULL, event_id TEXT, prev_event_id TEXT, instance_name TEXT);
	CREATE TABLE current_state_events (event_id TEXT NOT NULL, room_id TEXT NOT NULL, type TEXT NOT NULL,
		state_key TEXT NOT NULL, membership TEXT);
`

func roomKey(stream int64) streamtoken.RoomKey {
	return streamtoken.RoomKey{Topological: streamtoken.NoTopological, Stream: stream}
}

// Synapse's example (state_deltas.py, v1.164.1): a persist batch writes all
// its delta rows at its FIRST stream ordering, so a token inside a batch must
// judge that batch's rows by their events.
//
//	stream  event          delta row stream_id
//	  10    message
//	  11    message        <- from = 11
//	  12    state X        X: 10
//	  50    (server left)  R: 50, no event
//	  90    state Y        Y: 90
//	  91    message        <- to = 91
//	  92    state Z        Z: 90
func TestRoomStateDeltasByEventPosition(t *testing.T) {
	st, exec := throwawayStore(t, stateDeltasSchema)
	const room = "!r:x"
	exec(`INSERT INTO instance_map VALUES (1, 'master')`)
	for id, so := range map[string]int64{"$x": 12, "$y": 90, "$z": 92, "$old": 3} {
		exec(`INSERT INTO events VALUES ($1, 'master', $2)`, id, so)
	}
	for _, r := range []struct {
		stream       int64
		key, ev, prev any
	}{
		{2, "old", "$old", nil},
		{10, "x", "$x", "$x0"},
		{50, "r", nil, "$r0"},
		{90, "y", "$y", nil},
		{90, "z", "$z", nil},
	} {
		exec(`INSERT INTO current_state_delta_stream VALUES ($1, $2, 't', $3, $4, $5, 'master')`,
			r.stream, room, r.key, r.ev, r.prev)
	}

	from, to := roomKey(11), roomKey(91)
	got, err := st.RoomStateDeltas(context.Background(), room, &from, &to)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, d := range got {
		keys = append(keys, d.Key.StateKey+"="+d.EventID)
	}
	if want := []string{"x=$x", "r=", "y=$y"}; !slices.Equal(keys, want) {
		t.Errorf("(11, 91]: %v, want %v", keys, want)
	}
	if got[1].PrevEventID != "$r0" {
		t.Errorf("removal's prev %q", got[1].PrevEventID)
	}

	// Unbounded above: Z too. From 92: nothing left but nothing past it.
	got, err = st.RoomStateDeltas(context.Background(), room, &from, nil)
	if err != nil || len(got) != 4 || got[3].Key.StateKey != "z" {
		t.Errorf("(11, ∞): %+v %v", got, err)
	}
	from = roomKey(92)
	if got, err = st.RoomStateDeltas(context.Background(), room, &from, nil); err != nil || len(got) != 0 {
		t.Errorf("(92, ∞): %+v %v", got, err)
	}
}

func TestLazyCurrentStateIDs(t *testing.T) {
	st, exec := throwawayStore(t, stateDeltasSchema)
	const room = "!r:x"
	exec(`INSERT INTO current_state_events VALUES
		('$name', $1, 'm.room.name', '', NULL),
		('$a', $1, 'm.room.member', '@a:x', 'join'),
		('$b', $1, 'm.room.member', '@b:x', 'join')`, room)
	got, err := st.LazyCurrentStateIDs(context.Background(), room, []string{"@a:x", "@nobody:x"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[StateKey]string{{Type: "m.room.name"}: "$name", {Type: "m.room.member", StateKey: "@a:x"}: "$a"}
	if len(got) != len(want) || got[StateKey{Type: "m.room.name"}] != "$name" ||
		got[StateKey{Type: "m.room.member", StateKey: "@a:x"}] != "$a" {
		t.Errorf("%v, want %v", got, want)
	}
}
