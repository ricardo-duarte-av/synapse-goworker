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
)

// The MSC4354 sticky event backlog on a throwaway PostgreSQL, with the
// tables as Synapse v1.164 has them. Never the live database.
//
//	SENDER_TEST_ADMIN_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse" \
//	  GOEXPERIMENT=jsonv2 go test -run Sticky ./internal/sender/store/
func stickyStores(t *testing.T) (*Store, *Writer, func(sql string, args ...any)) {
	t.Helper()
	admin := os.Getenv("SENDER_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip("SENDER_TEST_ADMIN_DSN not set (a throwaway PostgreSQL)")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("SENDER_TEST_ADMIN_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	ac, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("sticky_test_%d", time.Now().UnixNano())
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
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	exec(`
		CREATE TABLE destinations (destination TEXT PRIMARY KEY, retry_last_ts BIGINT, retry_interval BIGINT,
			failure_ts BIGINT, last_successful_stream_ordering BIGINT);
		CREATE TABLE destination_rooms (destination TEXT NOT NULL, room_id TEXT NOT NULL,
			stream_ordering BIGINT NOT NULL, PRIMARY KEY (destination, room_id));
		CREATE TABLE device_federation_outbox (destination TEXT NOT NULL, stream_id BIGINT NOT NULL);
		CREATE TABLE sticky_events (stream_id INTEGER PRIMARY KEY, instance_name TEXT NOT NULL,
			event_id TEXT NOT NULL, room_id TEXT NOT NULL, event_stream_ordering INTEGER NOT NULL UNIQUE,
			sender TEXT NOT NULL, expires_at BIGINT NOT NULL);
		CREATE TABLE destination_rooms_sticky_events_backlog (destination TEXT NOT NULL, room_id TEXT NOT NULL,
			sticky_events_stream_position INTEGER NOT NULL, PRIMARY KEY (destination, room_id));
	`)
	st, err := Open(ctx, Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	w, err := OpenWriter(ctx, Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return st, w, exec
}

func TestStickyBacklog(t *testing.T) {
	st, w, exec := stickyStores(t)
	ctx := context.Background()
	const now, local = int64(1_000_000), "@%:us.example"
	// Room !a: local sticky events at 1, 2, 3 (2 expired), and a remote one.
	// Room !b: only expired events past its backlog position.
	exec(`INSERT INTO sticky_events VALUES
		(1, 'master', '$a1', '!a', 101, '@u:us.example', 2000000),
		(2, 'master', '$a2', '!a', 102, '@u:us.example', 900000),
		(3, 'master', '$a3', '!a', 103, '@u:us.example', 2000000),
		(4, 'master', '$remote', '!a', 104, '@x:them.example', 2000000),
		(5, 'master', '$b5', '!b', 105, '@u:us.example', 900000)`)
	exec(`INSERT INTO destination_rooms_sticky_events_backlog VALUES ('them.example', '!b', 5)`)

	// Only !b is backlogged, and everything owed there expired: it is
	// cleaned out and nothing is owed.
	b, err := w.BackloggedStickyEvents(ctx, "them.example", local, now, 50)
	if err != nil || b != nil {
		t.Fatalf("expired backlog: %+v %v", b, err)
	}

	// A catch-up transaction sends $a1 (stream ordering 101) and moves the
	// cursor from 100 to 110: the gap's local sticky events in rooms the
	// destination is in become a backlog, from the earliest one not sent.
	exec(`INSERT INTO destination_rooms VALUES ('them.example', '!a', 106)`)
	if err := w.MarkStickyBacklogAfterCatchUp(ctx, "them.example", local, 100, 110, []int64{101}); err != nil {
		t.Fatal(err)
	}
	b, err = w.BackloggedStickyEvents(ctx, "them.example", local, now, 50)
	if err != nil || b == nil {
		t.Fatalf("after catch-up: %+v %v", b, err)
	}
	if b.RoomID != "!a" || !slices.Equal(b.EventIDs, []string{"$a3"}) || b.MaxPosition != 3 {
		t.Errorf("backlog %+v: want !a, [$a3] (from position 2, $a2 expired, the remote one not ours), max 3", b)
	}

	// A backlog alone makes the destination one catch-up wakes.
	dests, err := st.GetCatchUpOutstandingDestinations(ctx, "", now, 25)
	if err != nil || !slices.Equal(dests, []string{"them.example"}) {
		t.Errorf("outstanding %v %v", dests, err)
	}

	// Delivered: the position moves past it, and nothing is owed.
	if err := w.MarkBackloggedStickySent(ctx, "them.example", b.RoomID, b.MaxPosition); err != nil {
		t.Fatal(err)
	}
	if b, err = w.BackloggedStickyEvents(ctx, "them.example", local, now, 50); err != nil || b != nil {
		t.Errorf("after delivery: %+v %v", b, err)
	}

	// A position is only ever moved backwards by a catch-up.
	exec(`INSERT INTO destination_rooms_sticky_events_backlog VALUES ('other.example', '!a', 1)`)
	exec(`INSERT INTO destination_rooms VALUES ('other.example', '!a', 106)`)
	if err := w.MarkStickyBacklogAfterCatchUp(ctx, "other.example", local, 100, 110, nil); err != nil {
		t.Fatal(err)
	}
	if b, err = w.BackloggedStickyEvents(ctx, "other.example", local, now, 50); err != nil || b == nil ||
		!slices.Equal(b.EventIDs, []string{"$a1", "$a3"}) {
		t.Errorf("position moved forwards: %+v %v", b, err)
	}
}
