package writer

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestWrites runs every write on a throwaway PostgreSQL holding the live
// schema and no rows. Never point it at the live database.
//
//	WRITER_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse" \
//	  GOEXPERIMENT=jsonv2 go test -run TestWrites ./internal/creator/writer/
func TestWrites(t *testing.T) {
	admin := os.Getenv("WRITER_TEST_DSN")
	if admin == "" {
		t.Skip("WRITER_TEST_DSN not set")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("WRITER_TEST_DSN points at the live database's socket")
	}
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	a, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)
	for _, q := range []string{
		`DELETE FROM event_push_actions_staging`, `DELETE FROM room_memberships`, `DELETE FROM local_current_membership`,
		`DELETE FROM state_groups_pending_deletion`, `DELETE FROM state_group_edges WHERE prev_state_group IN (101, 102)`,
		`DELETE FROM state_groups_state WHERE room_id = '!r:x'`, `DELETE FROM state_groups WHERE room_id = '!r:x'`,
		`INSERT INTO room_memberships (event_id, user_id, sender, room_id, membership, participant) VALUES ('$m', '@u:x', '@u:x', '!r:x', 'join', false)`,
		`INSERT INTO local_current_membership (room_id, user_id, event_id, membership) VALUES ('!r:x', '@u:x', '$m', 'join')`,
		`INSERT INTO state_groups (id, room_id, event_id) VALUES (101, '!r:x', '$a'), (102, '!r:x', '$b')`,
		`INSERT INTO state_groups_pending_deletion (state_group, insertion_ts) VALUES (101, 1)`,
		`DELETE FROM delayed_events WHERE user_localpart = 'd'`,
	} {
		if _, err := a.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	d, err := Open(ctx, admin, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Setup(ctx); err != nil {
		t.Fatal(err)
	}

	if err := d.StagePush(ctx, "$e", []PushRow{
		{UserID: "@a:x", Actions: "", Notif: true, Unread: true, ThreadID: "main"},
		{UserID: "@b:x", Actions: `["notify",{"set_tweak":"sound","value":"ping"}]`, Notif: true, Highlight: true, ThreadID: "$t"},
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := a.QueryRow(ctx, `SELECT count(*) FROM event_push_actions_staging WHERE event_id = '$e' AND inserted_ts > 0`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("staged %d rows (%v)", n, err)
	}
	if err := d.UnstagePush(ctx, "$e"); err != nil {
		t.Fatal(err)
	}

	if err := d.SetRoomParticipation(ctx, "@u:x", "!r:x"); err != nil {
		t.Fatal(err)
	}
	var participant bool
	if err := a.QueryRow(ctx, `SELECT participant FROM room_memberships WHERE event_id = '$m'`).Scan(&participant); err != nil || !participant {
		t.Fatalf("participant %v (%v)", participant, err)
	}

	missing, err := d.BumpDeletion(ctx, []int64{101, 102, 999})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != 999 {
		t.Errorf("missing %v, want [999]", missing)
	}
	var ts int64
	if err := a.QueryRow(ctx, `SELECT insertion_ts FROM state_groups_pending_deletion WHERE state_group = 101`).Scan(&ts); err != nil || ts <= 1 {
		t.Errorf("pending deletion not bumped: %d (%v)", ts, err)
	}

	// A state event's group: a delta on 101.
	g, err := d.StoreStateGroup(ctx, "$s", "!r:x", 101, []StateRow{{"m.room.topic", "", "$s"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var edge int64
	if err := a.QueryRow(ctx, `SELECT prev_state_group FROM state_group_edges WHERE state_group = $1`, g).Scan(&edge); err != nil || edge != 101 {
		t.Errorf("edge %d (%v)", edge, err)
	}
	var row string
	if err := a.QueryRow(ctx, `SELECT type || '|' || state_key || '|' || event_id FROM state_groups_state WHERE state_group = $1`, g).Scan(&row); err != nil || row != "m.room.topic||$s" {
		t.Errorf("row %q (%v)", row, err)
	}
	// A resolved state with no group to build on: whole, and no edge.
	full, err := d.StoreFullStateGroup(ctx, "$f", "!r:x", []StateRow{{"m.room.topic", "", "$s"}, {"m.room.name", "", "$n"}})
	if err != nil {
		t.Fatal(err)
	}
	var rowsIn, edges int
	if err := a.QueryRow(ctx, `SELECT (SELECT count(*) FROM state_groups_state WHERE state_group = $1),
		(SELECT count(*) FROM state_group_edges WHERE state_group = $1)`, full).Scan(&rowsIn, &edges); err != nil || rowsIn != 2 || edges != 0 {
		t.Errorf("full group: %d rows, %d edges (%v)", rowsIn, edges, err)
	}
	if _, err := d.StoreStateGroup(ctx, "$s2", "!r:x", 999, []StateRow{{"m.room.topic", "", "$s2"}}, nil); err == nil {
		t.Error("stored a group on a prev group that does not exist")
	}

	// Delayed events, up to the limit.
	dev := "DEV"
	for i, want := range []error{nil, nil, &DelayedLimitError{}} {
		next, err := d.AddDelayedEvent(ctx, DelayedEvent{DelayID: "syd_" + strconv.Itoa(i), UserLocalpart: "d", DeviceID: &dev,
			DelayMS: 1000, SendTS: int64(5000 + i), RoomID: "!r:x", EventType: "t", Content: "{}"}, 2)
		if want == nil && (err != nil || next != 5000) {
			t.Fatalf("delayed %d: %d %v", i, next, err)
		}
		if want != nil {
			var le *DelayedLimitError
			if !errors.As(err, &le) || le.RetryAfterMS != nil {
				t.Fatalf("delayed over the limit: %v", err)
			}
		}
	}

	// The worker's own table, created by Setup.
	if _, err := a.Exec(ctx, `DELETE FROM goevent_profile_propagations`); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveProfileJob(ctx, ProfileJob{UserID: "@u:x", JobID: "j1", Entity: "@u:x"}); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckpointProfileJob(ctx, "@u:x", "j1", "!r:x"); err != nil {
		t.Fatal(err)
	}
	// A newer change restarts the job; the old one's checkpoint and
	// delete no longer touch it.
	if err := d.SaveProfileJob(ctx, ProfileJob{UserID: "@u:x", JobID: "j2", Entity: "@admin:x"}); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckpointProfileJob(ctx, "@u:x", "j1", "!s:x"); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteProfileJob(ctx, "@u:x", "j1"); err != nil {
		t.Fatal(err)
	}
	jobs, err := d.ProfileJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0] != (ProfileJob{UserID: "@u:x", JobID: "j2", Entity: "@admin:x"}) {
		t.Errorf("jobs %+v", jobs)
	}
	if err := d.DeleteProfileJob(ctx, "@u:x", "j2"); err != nil {
		t.Fatal(err)
	}
	if jobs, _ := d.ProfileJobs(ctx); len(jobs) != 0 {
		t.Errorf("not forgotten: %+v", jobs)
	}
	// Setup is idempotent.
	if err := d.Setup(ctx); err != nil {
		t.Fatal(err)
	}
}
