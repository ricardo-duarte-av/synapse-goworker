package slidingstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// The connection store's hard part is entirely in SQL: copy-forward,
// deduplication, and the acknowledge-and-prune that makes a forked request
// safe. None of it is exercised by the unit tests above, and none of it is
// visible in a single response body -- "this room was marked sent but never
// was" only shows up on the request after next.
//
// Run against the live deployment:
//
//	GOSYNC_TEST_SS_DSN="host=/var/sockets user=gosync_ss dbname=synapse-db" \
//	  go test ./internal/slidingstore/ -run Live -v
//
// Every test uses its own (user, device, conn_id) triple under a reserved
// prefix and deletes it afterwards, so a run touches nothing real.
const testUserPrefix = "@gosync-test:invalid"

func liveStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("GOSYNC_TEST_SS_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_SS_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s, ctx
}

// conn returns a triple unique to this test, and removes it afterwards.
func conn(t *testing.T, s *Store, ctx context.Context) (user, device, connID string) {
	t.Helper()
	user, device, connID = testUserPrefix, "DEV-"+t.Name(), "conn-"+t.Name()
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `
			DELETE FROM sliding_sync_connections
			 WHERE user_id = $1 AND effective_device_id = $2 AND conn_id = $3`,
			user, device, connID)
	})
	return user, device, connID
}

func TestLiveRoundTrip(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e", "!b:e"})
	state.Receipts.RecordSentRooms([]string{"!a:e"})
	state.SetRoomConfig("!a:e", RoomSyncConfig{
		TimelineLimit: 10,
		RequiredState: map[string]map[string]bool{"m.room.name": {"": true}},
	})

	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if pos == 0 {
		t.Fatal("persist returned position 0, which is the no-state sentinel")
	}

	got, err := s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if st := got.Rooms.HaveSentRoom("!a:e").Status; st != FlagLive {
		t.Errorf("!a:e rooms = %q, want live", st)
	}
	if st := got.Rooms.HaveSentRoom("!c:e").Status; st != FlagNever {
		t.Errorf("!c:e rooms = %q, want never", st)
	}
	if st := got.Receipts.HaveSentRoom("!a:e").Status; st != FlagLive {
		t.Errorf("!a:e receipts = %q, want live", st)
	}
	if st := got.Receipts.HaveSentRoom("!b:e").Status; st != FlagNever {
		t.Errorf("!b:e receipts = %q, want never -- streams must not bleed", st)
	}
	cfg, ok := got.RoomConfigs["!a:e"]
	if !ok || cfg.TimelineLimit != 10 || !cfg.RequiredState["m.room.name"][""] {
		t.Errorf("room config = %+v, want limit 10 and m.room.name", cfg)
	}
}

func TestLivePreviouslyKeepsItsToken(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	state, err = s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatal(err)
	}
	state.Rooms.RecordUnsentRooms([]string{"!a:e"}, "s12345")
	pos, err = s.Persist(ctx, user, device, connID, pos, state)
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatal(err)
	}
	hs := got.Rooms.HaveSentRoom("!a:e")
	if hs.Status != FlagPreviously || hs.LastToken != "s12345" {
		t.Fatalf("round-tripped %+v, want previously at s12345", hs)
	}
}

// Each position must be a COMPLETE snapshot. Without the copy-forward a
// position holds only what its request touched, and every room mentioned in an
// earlier request silently reverts to NEVER -- meaning its full state is sent
// again on the next request that mentions it.
func TestLiveCopyForwardKeepsEarlierRooms(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!first:e"})
	state.SetRoomConfig("!first:e", RoomSyncConfig{TimelineLimit: 5,
		RequiredState: map[string]map[string]bool{"m.room.name": {"": true}}})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	// Three further rounds, each touching only a new room.
	for _, room := range []string{"!second:e", "!third:e", "!fourth:e"} {
		state, err = s.GetAndClear(ctx, user, device, connID, pos)
		if err != nil {
			t.Fatal(err)
		}
		state.Rooms.RecordSentRooms([]string{room})
		state.SetRoomConfig(room, RoomSyncConfig{TimelineLimit: 5,
			RequiredState: map[string]map[string]bool{"m.room.name": {"": true}}})
		pos, err = s.Persist(ctx, user, device, connID, pos, state)
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatal(err)
	}
	for _, room := range []string{"!first:e", "!second:e", "!third:e", "!fourth:e"} {
		if st := got.Rooms.HaveSentRoom(room).Status; st != FlagLive {
			t.Errorf("%s = %q after later rounds, want live", room, st)
		}
		if _, ok := got.RoomConfigs[room]; !ok {
			t.Errorf("%s lost its room config", room)
		}
	}
}

// A `pos` is user-supplied. Being handed one is not evidence of being entitled
// to it, and the check has to be on the whole triple: Element X runs three
// connections per device, so conn_id alone distinguishes them.
func TestLiveAPositionBelongsToOneTriple(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	for _, bad := range []struct{ name, u, d, c string }{
		{"another user", "@someone-else:invalid", device, connID},
		{"another device", user, device + "-other", connID},
		{"another conn_id", user, device, connID + "-other"},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if _, err := s.GetAndClear(ctx, bad.u, bad.d, bad.c, pos); !errors.Is(err, ErrUnknownPosition) {
				t.Fatalf("err = %v, want ErrUnknownPosition -- one client read another's state", err)
			}
			if _, err := s.Persist(ctx, bad.u, bad.d, bad.c, pos, state); !errors.Is(err, ErrUnknownPosition) {
				t.Fatalf("persist err = %v, want ErrUnknownPosition", err)
			}
		})
	}
}

func TestLiveUnknownPositionIsNotAnError(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	// Position 0 is the sentinel for "I have no state", used by 9.3% of live
	// requests. It must NOT be reported as unknown.
	got, err := s.GetAndClear(ctx, user, device, connID, 0)
	if err != nil {
		t.Fatalf("pos=0 returned %v; it means 'no previous state', not 'unknown'", err)
	}
	if got.Rooms.HaveSentRoom("!a:e").Status != FlagNever {
		t.Fatal("pos=0 returned state")
	}

	// A position that never existed is unknown.
	if _, err := s.GetAndClear(ctx, user, device, connID, 1); !errors.Is(err, ErrUnknownPosition) {
		t.Fatalf("err = %v, want ErrUnknownPosition", err)
	}
}

// The acknowledge-and-prune mechanic. A client that abandons a long poll never
// learns the position we just wrote -- 1,102 of 27,465 live requests in the
// measured window ended in a 499 -- so it comes back with the previous one.
// Until a position is used we cannot know which fork survived; once one IS
// used, every other is unreachable and must go, or these tables grow forever.
//
// Note the shape: the forks are built WITHOUT an intervening read, because
// reading a position is itself what prunes. In the ordinary flow each poll pass
// reads then writes, so at most two positions exist at a time; forks accumulate
// only when responses are computed from one position without it being read
// again, which is what a retry or a concurrent request does.
func TestLiveUsingAPositionPrunesTheForks(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	base := &PerConnectionState{}
	base.Rooms.RecordSentRooms([]string{"!a:e"})
	pos, err := s.Persist(ctx, user, device, connID, 0, base)
	if err != nil {
		t.Fatal(err)
	}

	// Three responses computed from the same position; two will be abandoned.
	var forks []int64
	for _, room := range []string{"!x:e", "!y:e", "!z:e"} {
		state, err := s.GetAndClear(ctx, user, device, connID, pos)
		if err != nil {
			t.Fatal(err)
		}
		state.Rooms.RecordSentRooms([]string{room})
		p, err := s.Persist(ctx, user, device, connID, pos, state)
		if err != nil {
			t.Fatal(err)
		}
		forks = append(forks, p)
	}

	if n := s.countPositions(t, ctx, user, device, connID); n != 2 {
		t.Fatalf("positions = %d after three forks off one read position, want 2: "+
			"each read prunes the previous fork, so only the newest and the read one survive", n)
	}

	// Now the case a read cannot clean up on its own: forks built from a
	// position that is never re-read in between.
	pos = forks[len(forks)-1]
	state, err := s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatal(err)
	}
	forks = nil
	for _, room := range []string{"!p:e", "!q:e", "!r:e"} {
		fresh := &PerConnectionState{}
		fresh.Rooms = NewRoomStatusMap(state.Rooms.All())
		fresh.Rooms.RecordSentRooms([]string{room})
		p, err := s.Persist(ctx, user, device, connID, pos, fresh)
		if err != nil {
			t.Fatal(err)
		}
		forks = append(forks, p)
	}
	if n := s.countPositions(t, ctx, user, device, connID); n != 4 {
		t.Fatalf("positions = %d, want 4 (the read one plus three unacknowledged forks)", n)
	}

	// The client acknowledges one of them. Every other fork is now unreachable.
	survivor := forks[len(forks)-1]
	got, err := s.GetAndClear(ctx, user, device, connID, survivor)
	if err != nil {
		t.Fatal(err)
	}
	if n := s.countPositions(t, ctx, user, device, connID); n != 1 {
		t.Fatalf("positions after acknowledgement = %d, want 1", n)
	}
	for _, fork := range forks[:len(forks)-1] {
		if _, err := s.GetAndClear(ctx, user, device, connID, fork); !errors.Is(err, ErrUnknownPosition) {
			t.Errorf("abandoned fork %d still readable (%v)", fork, err)
		}
	}
	// The surviving fork must carry the earlier rooms, not just its own.
	for _, room := range []string{"!a:e", "!z:e", "!r:e"} {
		if got.Rooms.HaveSentRoom(room).Status != FlagLive {
			t.Errorf("the acknowledged fork lost %s", room)
		}
	}
	if got.Rooms.HaveSentRoom("!p:e").Status != FlagNever {
		t.Error("the acknowledged fork inherited a room from an abandoned sibling")
	}
}

// A response that changed nothing must reuse the client's position and write
// nothing at all. Every new position copies a table's worth of rows forward,
// and Element X holds three connections per device.
func TestLiveAnUnchangedResponseWritesNothing(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		state, err = s.GetAndClear(ctx, user, device, connID, pos)
		if err != nil {
			t.Fatal(err)
		}
		state.Rooms.RecordSentRooms([]string{"!a:e"}) // already live: not a change
		next, err := s.Persist(ctx, user, device, connID, pos, state)
		if err != nil {
			t.Fatal(err)
		}
		if next != pos {
			t.Fatalf("round %d minted position %d from %d for a response that changed nothing",
				i, next, pos)
		}
	}
	if n := s.countPositions(t, ctx, user, device, connID); n != 1 {
		t.Fatalf("positions = %d after 5 unchanged responses, want 1", n)
	}
}

// required_state is deduplicated per connection. Without it, a connection with
// 654 rooms asking for the same state writes 654 copies of it.
func TestLiveRequiredStateIsDeduplicated(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	shared := map[string]map[string]bool{
		"m.room.name":   {"": true},
		"m.room.member": {"$LAZY": true},
	}
	state := &PerConnectionState{}
	for _, room := range []string{"!a:e", "!b:e", "!c:e", "!d:e"} {
		state.Rooms.RecordSentRooms([]string{room})
		state.SetRoomConfig(room, RoomSyncConfig{TimelineLimit: 20, RequiredState: shared})
	}
	// One room wants something else.
	state.Rooms.RecordSentRooms([]string{"!e:e"})
	state.SetRoomConfig("!e:e", RoomSyncConfig{TimelineLimit: 1,
		RequiredState: map[string]map[string]bool{"m.room.topic": {"": true}}})

	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	var rows int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM sliding_sync_connection_required_state
		 WHERE connection_key = (SELECT connection_key FROM sliding_sync_connection_positions
		                          WHERE connection_position = $1)`, pos).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("required_state rows = %d, want 2 (one shared by four rooms, one distinct)", rows)
	}

	var firstStateID int64
	if err := s.pool.QueryRow(ctx, `
		SELECT required_state_id FROM sliding_sync_connection_room_configs
		 WHERE connection_position = $1 AND room_id = '!a:e'`, pos).Scan(&firstStateID); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatal(err)
	}
	if !got.RoomConfigs["!a:e"].RequiredState["m.room.member"]["$LAZY"] {
		t.Error("shared config did not round-trip")
	}
	if !got.RoomConfigs["!e:e"].RequiredState["m.room.topic"][""] {
		t.Error("distinct config did not round-trip")
	}

	// Deduplication has to reach ACROSS requests, not just within one. A client
	// whose list grows by a room at a time -- which is what scrolling a room
	// list does -- would otherwise write a fresh copy of the same required
	// state on every request, and the old rows stay referenced so nothing
	// collects them.
	for i, room := range []string{"!f:e", "!g:e", "!h:e"} {
		got, err = s.GetAndClear(ctx, user, device, connID, pos)
		if err != nil {
			t.Fatal(err)
		}
		got.Rooms.RecordSentRooms([]string{room})
		got.SetRoomConfig(room, RoomSyncConfig{TimelineLimit: 20, RequiredState: shared})
		pos, err = s.Persist(ctx, user, device, connID, pos, got)
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
	if _, err := s.GetAndClear(ctx, user, device, connID, pos); err != nil {
		t.Fatal(err)
	}

	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM sliding_sync_connection_required_state
		 WHERE connection_key = (SELECT connection_key FROM sliding_sync_connection_positions
		                          WHERE connection_position = $1)`, pos).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("required_state rows = %d after three more rooms wanting the SAME state, "+
			"want 2: deduplication must reuse rows across requests, not only within one", rows)
	}

	// The row COUNT alone cannot see a broken cross-request dedup, because the
	// collector in loadState tidies up behind it: each request writes a fresh
	// copy, orphans the previous one, and the next read deletes it. The count
	// stays right and every request pays an extra INSERT and DELETE, on a table
	// that is already the largest of the six. So assert on the identity of the
	// row instead -- reuse means the id does not move.
	var stateID int64
	if err := s.pool.QueryRow(ctx, `
		SELECT required_state_id FROM sliding_sync_connection_room_configs
		 WHERE connection_position = $1 AND room_id = '!a:e'`, pos).Scan(&stateID); err != nil {
		t.Fatal(err)
	}
	if stateID != firstStateID {
		t.Fatalf("required_state_id for !a:e moved from %d to %d across requests; its "+
			"required state never changed, so the row should have been reused", firstStateID, stateID)
	}
}

// A required_state row nothing points at any more must go, or a connection
// whose config keeps changing accumulates one row per version for ever.
func TestLiveUnusedRequiredStateIsCollected(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	state.SetRoomConfig("!a:e", RoomSyncConfig{TimelineLimit: 5,
		RequiredState: map[string]map[string]bool{"m.room.name": {"": true}}})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	for i, typ := range []string{"m.room.topic", "m.room.avatar", "m.room.join_rules"} {
		state, err = s.GetAndClear(ctx, user, device, connID, pos)
		if err != nil {
			t.Fatal(err)
		}
		state.SetRoomConfig("!a:e", RoomSyncConfig{TimelineLimit: 5 + i,
			RequiredState: map[string]map[string]bool{typ: {"": true}}})
		pos, err = s.Persist(ctx, user, device, connID, pos, state)
		if err != nil {
			t.Fatal(err)
		}
	}
	// The collection happens on read, once the forks are pruned.
	if _, err := s.GetAndClear(ctx, user, device, connID, pos); err != nil {
		t.Fatal(err)
	}

	var rows int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM sliding_sync_connection_required_state
		 WHERE connection_key = (SELECT connection_key FROM sliding_sync_connection_positions
		                          WHERE connection_position = $1)`, pos).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("required_state rows = %d after four config changes, want 1", rows)
	}
}

// Restarting a connection must cascade the old one away, or a client that keeps
// starting over leaves a connection behind each time. 2,409 of 25,767 live
// requests carrying a `pos` used the restart sentinel.
func TestLiveRestartingAConnectionClearsTheOldOne(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	first, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	fresh := &PerConnectionState{}
	fresh.Rooms.RecordSentRooms([]string{"!b:e"})
	second, err := s.Persist(ctx, user, device, connID, 0, fresh)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.GetAndClear(ctx, user, device, connID, first); !errors.Is(err, ErrUnknownPosition) {
		t.Errorf("the old position survived a restart (%v)", err)
	}
	var conns int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM sliding_sync_connections
		 WHERE user_id = $1 AND effective_device_id = $2 AND conn_id = $3`,
		user, device, connID).Scan(&conns); err != nil {
		t.Fatal(err)
	}
	if conns != 1 {
		t.Fatalf("connections for the triple = %d, want 1", conns)
	}

	got, err := s.GetAndClear(ctx, user, device, connID, second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rooms.HaveSentRoom("!a:e").Status != FlagNever {
		t.Error("the restarted connection inherited the old one's rooms")
	}
}

// One device, three conn_ids -- what Element X actually does. They must not see
// each other's state.
func TestLiveConnectionsOnOneDeviceAreIndependent(t *testing.T) {
	s, ctx := liveStore(t)
	user := testUserPrefix
	device := "DEV-" + t.Name()
	ids := []string{"room-list", "notifications", ""}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(),
			`DELETE FROM sliding_sync_connections WHERE user_id = $1 AND effective_device_id = $2`,
			user, device)
	})

	positions := map[string]int64{}
	for i, connID := range ids {
		state := &PerConnectionState{}
		state.Rooms.RecordSentRooms([]string{[]string{"!x:e", "!y:e", "!z:e"}[i]})
		p, err := s.Persist(ctx, user, device, connID, 0, state)
		if err != nil {
			t.Fatalf("%q: %v", connID, err)
		}
		positions[connID] = p
	}

	for i, connID := range ids {
		got, err := s.GetAndClear(ctx, user, device, connID, positions[connID])
		if err != nil {
			t.Fatalf("%q: %v", connID, err)
		}
		mine := []string{"!x:e", "!y:e", "!z:e"}[i]
		if got.Rooms.HaveSentRoom(mine).Status != FlagLive {
			t.Errorf("%q lost its own room %s", connID, mine)
		}
		for j, other := range []string{"!x:e", "!y:e", "!z:e"} {
			if i != j && got.Rooms.HaveSentRoom(other).Status != FlagNever {
				t.Errorf("%q sees %s, which belongs to %q", connID, other, ids[j])
			}
		}
	}
}

func TestLiveLazyMembers(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{
		LazyMembership: map[string]*LazyMembers{
			"!a:e": {Returned: map[string]*int64{"@u:e": nil, "@v:e": nil}},
		},
	}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	var key int64
	if err := s.pool.QueryRow(ctx,
		`SELECT connection_key FROM sliding_sync_connection_positions WHERE connection_position = $1`,
		pos).Scan(&key); err != nil {
		t.Fatal(err)
	}

	sent, err := s.LazyMembersSent(ctx, key, "!a:e", []string{"@u:e", "@v:e", "@w:e"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent["@u:e"] == 0 {
		t.Fatalf("sent = %v, want @u:e and @v:e", sent)
	}

	// Invalidating one removes it, so it is sent again next time.
	state, err = s.GetAndClear(ctx, user, device, connID, pos)
	if err != nil {
		t.Fatal(err)
	}
	state.LazyMembership = map[string]*LazyMembers{
		"!a:e": {Invalidated: map[string]bool{"@u:e": true}},
	}
	if _, err := s.Persist(ctx, user, device, connID, pos, state); err != nil {
		t.Fatal(err)
	}

	sent, err = s.LazyMembersSent(ctx, key, "!a:e", []string{"@u:e", "@v:e"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sent["@u:e"]; ok {
		t.Error("@u:e was invalidated but is still recorded as sent")
	}
	if _, ok := sent["@v:e"]; !ok {
		t.Error("@v:e was not invalidated but was dropped")
	}
}

func TestLiveReaperDropsOnlyStaleConnections(t *testing.T) {
	s, ctx := liveStore(t)
	user, device, connID := conn(t, s, ctx)

	state := &PerConnectionState{}
	state.Rooms.RecordSentRooms([]string{"!a:e"})
	pos, err := s.Persist(ctx, user, device, connID, 0, state)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.DeleteOldConnections(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAndClear(ctx, user, device, connID, pos); err != nil {
		t.Fatalf("the reaper took a connection used seconds ago: %v", err)
	}

	// Age it past the cutoff.
	if _, err := s.pool.Exec(ctx, `
		UPDATE sliding_sync_connections SET last_used_ts = $1
		 WHERE user_id = $2 AND effective_device_id = $3 AND conn_id = $4`,
		s.now()-ConnectionExpiryMS-1, user, device, connID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteOldConnections(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAndClear(ctx, user, device, connID, pos); !errors.Is(err, ErrUnknownPosition) {
		t.Fatalf("err = %v, want ErrUnknownPosition after expiry", err)
	}
}

// The write grant's whole justification is that it is narrow, so the process
// checks rather than trusts. This asserts the check would actually refuse the
// roles it exists to refuse.
func TestLiveRoleCannotReachSynapse(t *testing.T) {
	s, ctx := liveStore(t)

	var canRead bool
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT has_table_privilege(c.oid, 'SELECT')
		                   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                  WHERE n.nspname = 'public' AND c.relname = 'events'), false)`).Scan(&canRead); err != nil {
		t.Fatal(err)
	}
	if canRead {
		t.Fatal("the sliding sync role can read public.events; the grant is not narrow")
	}

	if dsn := os.Getenv("GOSYNC_TEST_DSN"); dsn != "" {
		// The read-only role must be refused: pointing sliding_sync.dsn at it
		// is the easy misconfiguration, and it would fail later, at the first
		// write, on a request rather than at startup.
		if _, err := Open(ctx, Config{DSN: dsn, MaxConns: 1}); err == nil {
			t.Fatal("Open accepted the read-only role")
		}
	}
}

func (s *Store) countPositions(t *testing.T, ctx context.Context, user, device, connID string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM sliding_sync_connection_positions
		  JOIN sliding_sync_connections USING (connection_key)
		 WHERE user_id = $1 AND effective_device_id = $2 AND conn_id = $3`,
		user, device, connID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
