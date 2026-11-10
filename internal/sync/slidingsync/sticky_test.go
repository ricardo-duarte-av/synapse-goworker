package slidingsync

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

const stickyTestSchema = `
CREATE TABLE event_json (event_id text NOT NULL, room_id text NOT NULL, internal_metadata text NOT NULL,
	json text NOT NULL, format_version integer);
CREATE TABLE sticky_events (stream_id integer NOT NULL PRIMARY KEY, instance_name text NOT NULL,
	event_id text NOT NULL, room_id text NOT NULL, event_stream_ordering integer NOT NULL UNIQUE,
	sender text NOT NULL, expires_at bigint NOT NULL);
CREATE SEQUENCE sticky_events_sequence;`

// stickyStore is a throwaway database with sticky rows: !a at 1, 2, 3; !b
// at 4; !c at 5 (expired, still awaiting cleanup); !d at 6.
func stickyStore(t *testing.T) *store.Store {
	t.Helper()
	admin := os.Getenv("GOSYNC_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip("GOSYNC_TEST_ADMIN_DSN not set")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("GOSYNC_TEST_ADMIN_DSN must be a throwaway database, not the live one")
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
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, stickyTestSchema); err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		pos     int
		room    string
		expires int64
	}{{1, "!a", 1e13}, {2, "!a", 1e13}, {3, "!a", 1e13}, {4, "!b", 1e13}, {5, "!c", 1}, {6, "!d", 1e13}} {
		id := fmt.Sprintf("$%d", r.pos)
		if _, err := conn.Exec(ctx, `INSERT INTO event_json VALUES ($1, $2, '{}', '{}', 3)`, id, r.room); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO sticky_events VALUES ($1, 'p', $2, $3, $1, '@u:x', $4)`,
			r.pos, id, r.room, r.expires); err != nil {
			t.Fatal(err)
		}
	}
	st, err := store.Open(ctx, store.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func stickyInputs(from *int64, rooms []string, prev map[string]slidingstore.HaveSent) extensionInputs {
	in := extensionInputs{
		UserID: "@u:x",
		Now:    streamtoken.Token{StickyEvents: 6},
		NowMS:  1000,
		Lists:  map[string]ListResult{"l": {Ops: []ListOp{{Op: "SYNC", RoomIDs: rooms}}}},
		Previous: &slidingstore.PerConnectionState{
			StickyEvents: slidingstore.NewRoomStatusMap(prev),
		},
		New: &slidingstore.PerConnectionState{
			StickyEvents: slidingstore.NewRoomStatusMap(prev),
		},
	}
	if from != nil {
		in.From = &streamtoken.Token{StickyEvents: *from}
	}
	return in
}

func pos(n int64) *int64 { return &n }

func sticky(limit int) *StickyEventsExtension {
	return &StickyEventsExtension{
		ExtensionScope: ExtensionScope{Enabled: true, Lists: []string{Wildcard}, Rooms: []string{Wildcard}},
		Limit:          limit,
	}
}

func statuses(in extensionInputs) string {
	var parts []string
	for _, id := range sortedKeys(boolSet2(in.New.StickyEvents.All())) {
		hs := in.New.StickyEvents.HaveSentRoom(id)
		parts = append(parts, id+"="+string(hs.Status)+hs.LastToken)
	}
	return strings.Join(parts, " ")
}

func boolSet2(m map[string]slidingstore.HaveSent) map[string]bool {
	out := map[string]bool{}
	for k := range m {
		out[k] = true
	}
	return out
}

func ids(byRoom map[string][]string) string {
	var parts []string
	for _, id := range sortedKeys(boolSet(byRoom)) {
		parts = append(parts, id+":"+strings.Join(byRoom[id], ","))
	}
	return strings.Join(parts, " ")
}

// get_sticky_events_extension_response's selection (Synapse 1.165, #20363).
func TestStickySelection(t *testing.T) {
	d := Deps{Store: stickyStore(t), MSC4354Enabled: true}
	ctx := context.Background()
	rooms := []string{"!a", "!b", "!c", "!d"}
	run := func(t *testing.T, in extensionInputs, req *StickyEventsExtension) (string, string) {
		t.Helper()
		got, err := selectStickyEvents(ctx, d, in, req, relevantRooms(in, &req.ExtensionScope))
		if err != nil {
			t.Fatal(err)
		}
		return ids(got), statuses(in)
	}

	t.Run("initial: every unexpired event, rooms live", func(t *testing.T) {
		got, st := run(t, stickyInputs(nil, rooms, nil), sticky(100))
		if got != "!a:$1,$2,$3 !b:$4 !d:$6" || st != "!a=live !b=live !c=live !d=live" {
			t.Errorf("%s | %s", got, st)
		}
	})
	t.Run("the limit pages: rooms left previously at the last row read", func(t *testing.T) {
		in := stickyInputs(nil, rooms, nil)
		got, st := run(t, in, sticky(2))
		if got != "!a:$1,$2" || st != "!a=previously2 !b=previously2 !c=previously2 !d=previously2" {
			t.Fatalf("%s | %s", got, st)
		}
		// The next request, from any pos, carries on from 2.
		next := stickyInputs(pos(6), rooms, in.New.StickyEvents.All())
		got, st = run(t, next, sticky(2))
		if got != "!a:$3 !b:$4" || st != "!a=previously4 !b=previously4 !c=previously4 !d=previously4" {
			t.Fatalf("second: %s | %s", got, st)
		}
		last := stickyInputs(pos(6), rooms, next.New.StickyEvents.All())
		got, st = run(t, last, sticky(2))
		if got != "!d:$6" || st != "!a=live !b=live !c=live !d=live" {
			t.Errorf("third: %s | %s", got, st)
		}
	})
	t.Run("limit 0 reads nothing; initial rooms resume from the start", func(t *testing.T) {
		got, st := run(t, stickyInputs(nil, rooms, nil), sticky(0))
		if got != "" || st != "!a=previously0 !b=previously0 !c=previously0 !d=previously0" {
			t.Errorf("%s | %s", got, st)
		}
	})
	live := map[string]slidingstore.HaveSent{"!a": slidingstore.Live(), "!b": slidingstore.Live(),
		"!c": slidingstore.Live(), "!d": slidingstore.Live()}
	t.Run("live rooms read from the request's pos", func(t *testing.T) {
		got, st := run(t, stickyInputs(pos(3), rooms, live), sticky(100))
		if got != "!b:$4 !d:$6" || st != "!a=live !b=live !c=live !d=live" {
			t.Errorf("%s | %s", got, st)
		}
	})
	t.Run("a newly joined room starts over", func(t *testing.T) {
		in := stickyInputs(pos(3), rooms, live)
		in.NewlyJoined = map[string]bool{"!a": true}
		if got, _ := run(t, in, sticky(100)); got != "!a:$1,$2,$3 !b:$4 !d:$6" {
			t.Errorf("%s", got)
		}
	})
	t.Run("a room sent from scratch starts over", func(t *testing.T) {
		in := stickyInputs(pos(3), rooms, live)
		in.Rooms = map[string]*RoomResult{"!a": {Initial: true}}
		if got, _ := run(t, in, sticky(100)); got != "!a:$1,$2,$3 !b:$4 !d:$6" {
			t.Errorf("%s", got)
		}
	})
}

// _record_sticky_events_in_out_of_scope_rooms: a live room outside the
// scope that gained sticky rows (expired ones too) goes previously at the
// pos, so it is caught up when it comes back; also with the extension off.
func TestStickyOutOfScope(t *testing.T) {
	d := Deps{Store: stickyStore(t), MSC4354Enabled: true}
	ctx := context.Background()
	live := map[string]slidingstore.HaveSent{"!a": slidingstore.Live(), "!b": slidingstore.Live(),
		"!c": slidingstore.Live(), "!d": slidingstore.Previously("1")}

	in := stickyInputs(pos(3), []string{"!a"}, live)
	if _, err := stickyEventsExtension(ctx, d, in, sticky(100)); err != nil {
		t.Fatal(err)
	}
	// !a in scope (nothing after 3); !b and !c had rows in (3, 6]; !d keeps
	// its own position.
	if st := statuses(in); st != "!a=live !b=previously3 !c=previously3 !d=previously1" {
		t.Errorf("in scope !a: %s", st)
	}

	off := stickyInputs(pos(3), []string{"!a", "!b"}, live)
	v, err := stickyEventsExtension(ctx, d, off, nil)
	if err != nil || v != nil {
		t.Fatal(v, err)
	}
	if st := statuses(off); st != "!a=live !b=previously3 !c=previously3 !d=previously1" {
		t.Errorf("off: %s", st)
	}

	// Back in scope, !b is read from 3.
	back := stickyInputs(pos(6), []string{"!b"}, off.New.StickyEvents.All())
	got, err := selectStickyEvents(ctx, d, back, sticky(100), map[string]bool{"!b": true})
	if err != nil || ids(got) != "!b:$4" {
		t.Errorf("back in scope: %s %v", ids(got), err)
	}

	if _, err := stickyEventsExtension(ctx, d, stickyInputs(nil, nil, nil), nil); err != nil {
		t.Errorf("initial with the extension off: %v", err)
	}
}
