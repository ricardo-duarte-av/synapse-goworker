package slidingsync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Room-list computation is where the 654-room account earns its keep: sorting,
// windowing and the metadata join are all things a 9-room account cannot make
// wrong. Read-only throughout.
//
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	GOSYNC_TEST_USER="@daedric:aguiarvieira.pt" \
//	  go test ./internal/slidingsync/ -run Live -v
func liveDeps(t *testing.T) (Deps, string, streamtoken.Token, context.Context) {
	t.Helper()
	dsn := os.Getenv("GOSYNC_TEST_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	db, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)

	userID := os.Getenv("GOSYNC_TEST_USER")
	if userID == "" {
		if err := db.Pool().QueryRow(ctx, `
			SELECT user_id FROM local_current_membership WHERE membership = 'join'
			 GROUP BY user_id ORDER BY count(*) DESC LIMIT 1`).Scan(&userID); err != nil {
			t.Skipf("no local users: %v", err)
		}
	}

	now, err := db.CurrentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The deployment has MSC4354 on, and a sticky event then carries its
	// remaining lifetime. Leaving it off here made every sticky event differ
	// from the reference for a reason that was ours, not the code's.
	return Deps{Store: db, MSC4354Enabled: true}, userID, now, ctx
}

func allRoomsList(limit int) *Request {
	return &Request{Lists: map[string]List{
		"all": {
			CommonRoomParameters: CommonRoomParameters{
				RequiredState: [][2]string{{"m.room.name", ""}},
				TimelineLimit: 1,
			},
			Ranges: [][2]int{{0, limit - 1}},
		},
	}}
}

func TestLiveComputeRoomLists(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	got, err := ComputeRoomLists(ctx, d, userID, allRoomsList(20), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	list, ok := got.Lists["all"]
	if !ok {
		t.Fatal("no list in the result")
	}
	t.Logf("%s: count=%d, %d rooms in the window, %d relevant, %d in all_rooms",
		userID, list.Count, len(list.Ops[0].RoomIDs), len(got.Relevant), len(got.AllRooms))

	if list.Count == 0 {
		t.Skip("user has no rooms")
	}
	if len(list.Ops) != 1 || list.Ops[0].Op != "SYNC" {
		t.Fatalf("ops = %+v, want exactly one SYNC", list.Ops)
	}

	// The window holds at most what was asked for, and no more than exists.
	want := 20
	if list.Count < want {
		want = list.Count
	}
	if n := len(list.Ops[0].RoomIDs); n != want {
		t.Errorf("window holds %d rooms, want %d (count=%d)", n, want, list.Count)
	}

	// count is the size of the whole filtered list, not of the window: it is
	// how a client knows there is more to scroll to.
	if list.Count < len(list.Ops[0].RoomIDs) {
		t.Errorf("count %d is smaller than the window it describes", list.Count)
	}

	// Every room in the window must be one we will describe.
	for _, roomID := range list.Ops[0].RoomIDs {
		if _, ok := got.Relevant[roomID]; !ok {
			t.Errorf("%s is in the window but has no room config", roomID)
		}
		if !got.AllRooms[roomID] {
			t.Errorf("%s is in the window but not in all_rooms", roomID)
		}
	}
}

// The order is the answer, not a detail: it is what a client's room list looks
// like. Newest activity first, and stable across calls.
func TestLiveOrderingIsByActivityAndStable(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	first, err := ComputeRoomLists(ctx, d, userID, allRoomsList(30), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Lists["all"].Ops) == 0 {
		t.Skip("no rooms")
	}
	ids := first.Lists["all"].Ops[0].RoomIDs
	if len(ids) < 2 {
		t.Skip("not enough rooms to order")
	}

	// Same token, same answer. A list that reshuffles between identical
	// requests makes a client's room list jump.
	second, err := ComputeRoomLists(ctx, d, userID, allRoomsList(30), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	other := second.Lists["all"].Ops[0].RoomIDs
	for i := range ids {
		if ids[i] != other[i] {
			t.Fatalf("position %d differs between identical requests: %s vs %s",
				i, ids[i], other[i])
		}
	}

	// And it really is by descending activity.
	meta, err := d.Store.SlidingJoinedRooms(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	rooms, err := d.Store.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	var prev int64
	for i, roomID := range ids {
		pos := rooms[roomID].EventStream
		if rooms[roomID].Membership == "join" {
			if m, ok := meta[roomID]; ok && m.EventStream <= now.Room.MaxStreamPos() {
				pos = m.EventStream
			}
		}
		if i > 0 && pos > prev {
			t.Errorf("position %d (%s, %d) sorts above position %d (%d)",
				i, roomID, pos, i-1, prev)
		}
		prev = pos
	}
}

// Ranges are inclusive on both sides and index into one sorted list, so a
// window taken in two halves must equal the whole.
func TestLiveRangesArePagesOfOneOrder(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	whole, err := ComputeRoomLists(ctx, d, userID, allRoomsList(10), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(whole.Lists["all"].Ops) == 0 || len(whole.Lists["all"].Ops[0].RoomIDs) < 10 {
		t.Skip("fewer than 10 rooms")
	}

	req := allRoomsList(10)
	list := req.Lists["all"]
	list.Ranges = [][2]int{{0, 4}, {5, 9}}
	req.Lists["all"] = list

	split, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	ops := split.Lists["all"].Ops
	if len(ops) != 2 {
		t.Fatalf("got %d ops, want one per range", len(ops))
	}
	joined := append(append([]string{}, ops[0].RoomIDs...), ops[1].RoomIDs...)
	want := whole.Lists["all"].Ops[0].RoomIDs
	for i := range want {
		if joined[i] != want[i] {
			t.Fatalf("position %d: split gives %s, whole gives %s", i, joined[i], want[i])
		}
	}
	if ops[0].Range != [2]int{0, 4} || ops[1].Range != [2]int{5, 9} {
		t.Errorf("ranges echoed back as %v and %v", ops[0].Range, ops[1].Range)
	}
}

// A range past the end of the list is not an error: it comes back empty, and
// `count` tells the client where the end is.
func TestLiveARangePastTheEndIsEmpty(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	req := allRoomsList(1)
	list := req.Lists["all"]
	list.Ranges = [][2]int{{1000000, 1000010}}
	req.Lists["all"] = list

	got, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	ops := got.Lists["all"].Ops
	if len(ops) != 1 || len(ops[0].RoomIDs) != 0 {
		t.Fatalf("ops = %+v, want one empty SYNC", ops)
	}
	if got.Lists["all"].Count == 0 {
		t.Error("count went to zero for an out-of-range window")
	}
}

// A room named by two lists must be described with the union of what they
// asked for, or one list's presence silently degrades the other.
func TestLiveOverlappingListsCombineTheirConfigs(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	req := &Request{Lists: map[string]List{
		"names": {
			CommonRoomParameters: CommonRoomParameters{
				RequiredState: [][2]string{{"m.room.name", ""}}, TimelineLimit: 1,
			},
			Ranges: [][2]int{{0, 4}},
		},
		"topics": {
			CommonRoomParameters: CommonRoomParameters{
				RequiredState: [][2]string{{"m.room.topic", ""}}, TimelineLimit: 20,
			},
			Ranges: [][2]int{{0, 4}},
		},
	}}
	got, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	ids := got.Lists["names"].Ops[0].RoomIDs
	if len(ids) == 0 {
		t.Skip("no rooms")
	}
	cfg := got.Relevant[ids[0]]
	if cfg.TimelineLimit != 20 {
		t.Errorf("timeline limit = %d, want the higher of the two lists (20)", cfg.TimelineLimit)
	}
	if !cfg.RequiredState["m.room.name"][""] || !cfg.RequiredState["m.room.topic"][""] {
		t.Errorf("required state = %v, want both lists' entries", cfg.RequiredState)
	}
}

// A subscription reaches a room whether or not a window covers it -- that is
// the whole point of the room subscription API.
func TestLiveRoomSubscriptionsBypassTheWindow(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	all, err := ComputeRoomLists(ctx, d, userID, allRoomsList(500), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	ids := all.Lists["all"].Ops[0].RoomIDs
	if len(ids) < 3 {
		t.Skip("not enough rooms")
	}
	// Something outside a one-room window.
	target := ids[len(ids)-1]

	req := allRoomsList(1)
	req.RoomSubscriptions = map[string]RoomSubscribe{
		target: {CommonRoomParameters: CommonRoomParameters{
			RequiredState: [][2]string{{"m.room.topic", ""}}, TimelineLimit: 5,
		}},
	}
	got, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Relevant[target]; !ok {
		t.Fatalf("%s was subscribed to but is not in the relevant set", target)
	}
	if !got.AllRooms[target] {
		t.Errorf("%s was subscribed to but is not in all_rooms", target)
	}
	// It must not appear in the list's window, which asked for one room.
	if n := len(got.Lists["all"].Ops[0].RoomIDs); n != 1 {
		t.Errorf("the window grew to %d rooms because of a subscription", n)
	}
}

// A stale room ID in a client's saved subscriptions must not break every
// subsequent request.
func TestLiveASubscriptionToAnUnknownRoomIsIgnored(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	req := allRoomsList(1)
	req.RoomSubscriptions = map[string]RoomSubscribe{
		"!not-a-room-we-are-in:invalid": {CommonRoomParameters: CommonRoomParameters{TimelineLimit: 1}},
	}
	got, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatalf("a subscription to an unknown room failed the request: %v", err)
	}
	if _, ok := got.Relevant["!not-a-room-we-are-in:invalid"]; ok {
		t.Error("an unknown room was included")
	}
}

func TestLiveFiltersAgainstRealRooms(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	base, err := ComputeRoomLists(ctx, d, userID, allRoomsList(1000), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	total := base.Lists["all"].Count
	if total == 0 {
		t.Skip("no rooms")
	}

	for _, tc := range []struct {
		name string
		f    *Filters
	}{
		{"encrypted", &Filters{IsEncrypted: ptr(true)}},
		{"unencrypted", &Filters{IsEncrypted: ptr(false)}},
		{"dm", &Filters{IsDM: ptr(true)}},
		{"invites", &Filters{IsInvite: ptr(true)}},
		{"spaces", &Filters{RoomTypes: []*string{ptr("m.space")}}},
		{"not spaces", &Filters{NotRoomTypes: []*string{ptr("m.space")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := allRoomsList(1000)
			list := req.Lists["all"]
			list.Filters = tc.f
			req.Lists["all"] = list

			got, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			n := got.Lists["all"].Count
			t.Logf("%d of %d rooms", n, total)
			if n > total {
				t.Errorf("a filter produced MORE rooms (%d) than no filter (%d)", n, total)
			}
		})
	}

	// The two halves of a boolean filter must partition the whole.
	enc := allRoomsList(1000)
	l := enc.Lists["all"]
	l.Filters = &Filters{IsEncrypted: ptr(true)}
	enc.Lists["all"] = l
	gotEnc, err := ComputeRoomLists(ctx, d, userID, enc, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	l.Filters = &Filters{IsEncrypted: ptr(false)}
	enc.Lists["all"] = l
	gotPlain, err := ComputeRoomLists(ctx, d, userID, enc, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if sum := gotEnc.Lists["all"].Count + gotPlain.Lists["all"].Count; sum != total {
		t.Errorf("is_encrypted true (%d) + false (%d) = %d, want %d: the two halves "+
			"must partition the list, and an absent filter must not mean false",
			gotEnc.Lists["all"].Count, gotPlain.Lists["all"].Count, sum, total)
	}
}
