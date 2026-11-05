package receipts

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Against a throwaway PostgreSQL with Synapse's schema, never the live one:
//
//	RECEIPTS_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func openTestDB(t *testing.T) (*DB, func(string, ...any)) {
	t.Helper()
	dsn := os.Getenv("RECEIPTS_TEST_DSN")
	if dsn == "" {
		t.Skip("RECEIPTS_TEST_DSN not set")
	}
	if strings.Contains(dsn, "/var/sockets") {
		t.Fatal("RECEIPTS_TEST_DSN must be a throwaway database, not the live one")
	}
	db, err := Open(context.Background(), dsn, 4, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.pool.Exec(context.Background(), q, args...); err != nil {
			t.Fatal(q, err)
		}
	}
	return db, exec
}

func TestStoreAgainstPostgres(t *testing.T) {
	db, exec := openTestDB(t)
	ctx := context.Background()
	room := fmt.Sprintf("!receipts-%d:hs.test", time.Now().UnixNano())
	ev := func(id string, so int64, rx any) {
		exec(`INSERT INTO events (topological_ordering, event_id, type, room_id, content, unrecognized_keys,
			processed, outlier, depth, origin_server_ts, received_ts, sender, contains_url, instance_name,
			stream_ordering, state_key, rejection_reason)
			VALUES ($1, $2, 'm.room.message', $3, '{}', '', true, false, $1, 0, $4, '@a:hs.test', false, 'master', $5, NULL, NULL)`,
			so, id, room, rx, so)
	}
	base := time.Now().UnixNano() % 1_000_000_000
	ea, eb, ec := fmt.Sprintf("$a%d", base), fmt.Sprintf("$b%d", base), fmt.Sprintf("$c%d", base)
	ev(ea, 900000000+base, 1)
	ev(eb, 900000001+base, 2)
	ev(ec, 900000002+base, nil)
	t.Cleanup(func() {
		for _, q := range []string{`DELETE FROM receipts_linearized WHERE room_id = $1`, `DELETE FROM receipts_graph WHERE room_id = $1`,
			`DELETE FROM events WHERE room_id = $1`, `DELETE FROM event_relations WHERE relates_to_id LIKE '$root%'`} {
			_, _ = db.pool.Exec(context.Background(), q, room)
		}
	})

	r := Receipt{RoomID: room, ReceiptType: "m.read", UserID: "@u:hs.test", EventIDs: []string{eb}, EventID: eb, Data: `{"ts":1}`}
	if stored, isNew, err := db.InsertLinearized(ctx, r, 5001, "gw"); err != nil || !stored || !isNew {
		t.Fatalf("first: %v %v %v", stored, isNew, err)
	}
	if err := db.InsertGraph(ctx, r); err != nil {
		t.Fatal(err)
	}
	// An older event: refused.
	r2 := r
	r2.EventID, r2.EventIDs = ea, []string{ea}
	if stored, _, err := db.InsertLinearized(ctx, r2, 5002, "gw"); err != nil || stored {
		t.Fatalf("older: %v %v", stored, err)
	}
	// A threaded one sits beside the unthreaded one.
	th := "$root"
	r3 := r2
	r3.ThreadID = &th
	if stored, _, err := db.InsertLinearized(ctx, r3, 5003, "gw"); err != nil || !stored {
		t.Fatalf("threaded: %v %v", stored, err)
	}
	// An event with no received_ts: written, but not "new" (rx_ts None).
	r4 := r
	r4.EventID, r4.EventIDs = ec, []string{ec}
	if stored, isNew, err := db.InsertLinearized(ctx, r4, 5004, "gw"); err != nil || !stored || isNew {
		t.Fatalf("no received_ts: %v %v %v", stored, isNew, err)
	}
	// An unknown event: written, with no ordering, and new.
	r5 := r
	r5.ReceiptType, r5.EventID, r5.EventIDs = "m.read.private", "$unknown", []string{"$unknown"}
	if stored, isNew, err := db.InsertLinearized(ctx, r5, 5005, "gw"); err != nil || !stored || !isNew {
		t.Fatalf("unknown: %v %v %v", stored, isNew, err)
	}

	rows, err := db.pool.Query(ctx, `SELECT receipt_type, COALESCE(thread_id, '<null>'), event_id, stream_id, instance_name,
		COALESCE(event_stream_ordering::text, '<null>'), data FROM receipts_linearized WHERE room_id = $1 ORDER BY stream_id`, room)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var a, b, c, e, f, g string
		var d int64
		_ = rows.Scan(&a, &b, &c, &d, &e, &f, &g)
		got = append(got, fmt.Sprintf("%s %s %s %d %s %s %s", a, b, c, d, e, f, g))
	}
	rows.Close()
	want := []string{
		fmt.Sprintf("m.read $root %s 5003 gw %d {\"ts\":1}", ea, 900000000+base),
		fmt.Sprintf("m.read <null> %s 5004 gw %d {\"ts\":1}", ec, 900000002+base),
		"m.read.private <null> $unknown 5005 gw <null> {\"ts\":1}",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	var graph string
	_ = db.pool.QueryRow(ctx, `SELECT event_ids FROM receipts_graph WHERE room_id = $1`, room).Scan(&graph)
	if graph != `["`+eb+`"]` {
		t.Fatalf("graph %s", graph)
	}

	// The stream: our rows in (from, upto], a full page limited.
	ups, upto, limited, err := db.Updates(ctx, "gw", 5002, 5010, 2)
	if err != nil || len(ups) != 2 || !limited || upto != 5004 || ups[0].Token != 5003 {
		t.Fatalf("updates %v %d %v %v", ups, upto, limited, err)
	}
	if want := `["` + room + `","m.read","@u:hs.test","` + ea + `","$root",{"ts":1}]`; string(ups[0].Row) != want {
		t.Fatalf("row %s\nwant %s", ups[0].Row, want)
	}
	if ups, upto, limited, _ := db.Updates(ctx, "other", 5000, 5010, 100); len(ups) != 0 || upto != 5010 || limited {
		t.Fatalf("another instance's rows: %v", ups)
	}

	// Threads, read marker reads.
	exec(`INSERT INTO event_relations (event_id, relates_to_id, relation_type) VALUES ($1, $2, 'm.thread')`, eb, "$root"+room)
	if tid, err := db.ThreadID(ctx, eb); err != nil || tid != "$root"+room {
		t.Fatalf("thread %q %v", tid, err)
	}
	if tid, _ := db.ThreadID(ctx, ea); tid != MainTimeline {
		t.Fatalf("main %q", tid)
	}
	if tid, _ := db.ThreadIDForReceipts(ctx, "$root"+room); tid != "$root"+room {
		t.Fatalf("root for receipts %q", tid)
	}
	if o, err := db.EventOrdering(ctx, eb, room); err != nil || o.Stream != 900000001+base {
		t.Fatalf("ordering %v %v", o, err)
	}
	if _, err := db.EventOrdering(ctx, eb, "!other:x"); err == nil || !strings.Contains(err.Error(), "Could not find event") {
		t.Fatalf("other room: %v", err)
	}
	if c, err := db.RoomAccountData(ctx, "@u:hs.test", room, TypeFullyRead); err != nil || c != nil {
		t.Fatalf("account data %s %v", c, err)
	}
}
