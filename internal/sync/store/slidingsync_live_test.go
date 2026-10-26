package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// These check the materialised tables against the truth they are derived from,
// because everything downstream trusts them completely. A wrong row here is a
// room silently missing from a client's room list, which is the failure nobody
// reports as a bug.
//
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	  go test ./internal/store/ -run LiveSliding -v
func liveSlidingStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("GOSYNC_TEST_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	db, err := Open(ctx, Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)
	return db, ctx
}

func TestLiveSlidingTablesReady(t *testing.T) {
	db, ctx := liveSlidingStore(t)
	ready, why, err := db.SlidingSyncTablesReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatalf("sliding sync tables are not ready: %s -- the endpoint must refuse "+
			"to serve rather than answer from incomplete tables", why)
	}
}

// The snapshot table must agree with local_current_membership, which is what
// classic sync reads. If they disagree, one of the two workers is wrong about
// which rooms a user is in.
func TestLiveSlidingRoomsAgreeWithCurrentMembership(t *testing.T) {
	db, ctx := liveSlidingStore(t)

	var userID string
	if err := db.pool.QueryRow(ctx, `
		SELECT user_id FROM local_current_membership WHERE membership = 'join'
		 GROUP BY user_id ORDER BY count(*) DESC LIMIT 1`).Scan(&userID); err != nil {
		t.Skipf("no local users with joined rooms: %v", err)
	}

	got, err := db.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	joined := map[string]bool{}
	for id, r := range got {
		if r.Membership == "join" {
			joined[id] = true
		}
	}

	rows, err := db.pool.Query(ctx, `
		SELECT lcm.room_id FROM local_current_membership lcm
		  JOIN rooms r USING (room_id)
		 WHERE lcm.user_id = $1 AND lcm.membership = 'join'`, userID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var missing []string
	var total int
	for rows.Next() {
		var roomID string
		if err := rows.Scan(&roomID); err != nil {
			t.Fatal(err)
		}
		total++
		if !joined[roomID] {
			missing = append(missing, roomID)
		}
	}
	t.Logf("%s: %d joined rooms, %d rows in the snapshot table", userID, total, len(got))

	// A room can legitimately be absent only for an unknown room version, which
	// both this query and Synapse's filter out on purpose.
	for _, roomID := range missing {
		var version string
		if err := db.pool.QueryRow(ctx,
			`SELECT room_version FROM rooms WHERE room_id = $1`, roomID).Scan(&version); err != nil {
			t.Fatal(err)
		}
		t.Errorf("%s is joined per local_current_membership but missing from the "+
			"snapshot table (room version %q)", roomID, version)
	}
}

// Self-leaves are excluded from the main query and must come back from the
// sister one, or a sync answering as of a token before the leave loses the room.
func TestLiveSlidingSelfLeavesAreExcludedThenRecovered(t *testing.T) {
	db, ctx := liveSlidingStore(t)

	var userID, roomID string
	var pos int64
	err := db.pool.QueryRow(ctx, `
		SELECT user_id, room_id, event_stream_ordering
		  FROM sliding_sync_membership_snapshots
		 WHERE membership = 'leave' AND user_id = sender AND forgotten = 0
		 ORDER BY event_stream_ordering DESC LIMIT 1`).Scan(&userID, &roomID, &pos)
	if err != nil {
		t.Skipf("no self-leaves on this server: %v", err)
	}

	rooms, err := db.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rooms[roomID]; ok {
		t.Errorf("%s left %s themselves but it is in the main room list", userID, roomID)
	}

	// Asked about a token BEFORE the leave, the room must come back.
	leaves, err := db.SlidingSelfLeavesAfter(ctx, userID, pos-1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := leaves[roomID]; !ok {
		t.Errorf("%s is not returned for a token before the leave at %d; a sync as of "+
			"then would lose a room the user was still in", roomID, pos)
	}

	// Asked about a token after it, it must not.
	leaves, err = db.SlidingSelfLeavesAfter(ctx, userID, pos)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := leaves[roomID]; ok {
		t.Errorf("%s is returned for a token after the leave at %d", roomID, pos)
	}
}

// bump_stamp orders every list, and room_name/is_encrypted/room_type drive the
// filters. All four come from the persister rather than from state resolution,
// so this checks they are actually populated rather than silently null.
func TestLiveSlidingJoinedRoomMetadata(t *testing.T) {
	db, ctx := liveSlidingStore(t)

	rows, err := db.pool.Query(ctx, `SELECT room_id FROM sliding_sync_joined_rooms LIMIT 500`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		t.Skip("no joined rooms")
	}

	meta, err := db.SlidingJoinedRooms(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != len(ids) {
		t.Fatalf("asked for %d rooms, got %d", len(ids), len(meta))
	}

	var named, encrypted, typed, bumped int
	for _, m := range meta {
		if m.RoomName != nil {
			named++
		}
		if m.IsEncrypted {
			encrypted++
		}
		if m.RoomType != nil {
			typed++
		}
		if m.BumpStamp != nil {
			bumped++
		}
		if m.EventStream <= 0 {
			t.Errorf("%s has event_stream_ordering %d", m.RoomID, m.EventStream)
		}
	}
	t.Logf("%d rooms: %d named, %d encrypted, %d typed, %d with a bump_stamp",
		len(meta), named, encrypted, typed, bumped)
	if bumped == 0 {
		t.Error("no room has a bump_stamp; list ordering would be meaningless")
	}
}

// The room name in the joined-rooms table must match the m.room.name state
// event, because the response serves it directly rather than resolving state.
func TestLiveSlidingRoomNameMatchesState(t *testing.T) {
	db, ctx := liveSlidingStore(t)

	rows, err := db.pool.Query(ctx, `
		SELECT j.room_id, j.room_name, ej.json
		  FROM sliding_sync_joined_rooms j
		  JOIN current_state_events cse
		    ON cse.room_id = j.room_id AND cse.type = 'm.room.name' AND cse.state_key = ''
		  JOIN event_json ej ON ej.event_id = cse.event_id
		 WHERE j.room_name IS NOT NULL
		 LIMIT 100`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	checked, mismatched := 0, 0
	for rows.Next() {
		var roomID string
		var name *string
		var raw string
		if err := rows.Scan(&roomID, &name, &raw); err != nil {
			t.Fatal(err)
		}
		checked++
		if name == nil {
			continue
		}
		if want := gjsonName(raw); want != "" && want != *name {
			mismatched++
			if mismatched <= 3 {
				t.Errorf("%s: table says %q, m.room.name says %q", roomID, *name, want)
			}
		}
	}
	if checked == 0 {
		t.Skip("no named rooms")
	}
	t.Logf("checked %d named rooms, %d mismatched", checked, mismatched)
}

func gjsonName(raw string) string {
	return gjson.Get(raw, "content.name").String()
}
