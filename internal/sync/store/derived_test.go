package store

import (
	"context"
	"testing"
)

func armed(t *testing.T, positions map[string]int64) *Store {
	t.Helper()
	s := &Store{derived: newDerivedCaches(Config{})}
	s.ArmDerivedCaches(positions)
	return s
}

// The guard's whole purpose: a token ahead of what replication has applied
// must not be answered from cache, because the entry may predate a change the
// token already covers.
func TestGuardRefusesATokenAheadOfReplication(t *testing.T) {
	s := armed(t, map[string]int64{streamEvents: 100})

	ctx := WithHorizon(context.Background(), Horizon{Events: 100})
	if !s.derived.fresh(ctx, streamEvents) {
		t.Fatal("token at the applied position should be answerable")
	}

	ctx = WithHorizon(context.Background(), Horizon{Events: 101})
	if s.derived.fresh(ctx, streamEvents) {
		t.Fatal("token ahead of the applied position must not be answered from cache")
	}

	// Once the row is applied, the same token is answerable.
	s.Applied(streamEvents, 101)
	if !s.derived.fresh(ctx, streamEvents) {
		t.Fatal("applying the position should let the token through")
	}
}

// No horizon means no token to check against, so nothing may be served.
func TestGuardRefusesWithoutAHorizon(t *testing.T) {
	s := armed(t, map[string]int64{streamEvents: 100})
	if s.derived.fresh(context.Background(), streamEvents) {
		t.Fatal("a context with no horizon must not use a guarded cache")
	}
}

// While replication is down every guarded answer is a guess.
func TestGuardRefusesWhileDisarmed(t *testing.T) {
	s := armed(t, map[string]int64{streamEvents: 100})
	s.DisarmDerivedCaches()

	ctx := WithHorizon(context.Background(), Horizon{Events: 100})
	if s.derived.fresh(ctx, streamEvents) {
		t.Fatal("a disarmed cache must not answer")
	}
	// And it must hold nothing, so a path that forgot the guard still misses.
	s.derived.roomSummary.Add("!r", MemberSummary{})
	if _, ok := s.derived.roomSummary.Get("!r"); ok {
		t.Fatal("a disarmed cache must not store or return entries")
	}
}

// Applied must never go backwards: rows can arrive out of order across
// streams, and a horizon that retreats would make a served answer unservable
// and, worse, a refused one servable.
func TestAppliedIsMonotonic(t *testing.T) {
	s := armed(t, map[string]int64{streamEvents: 100})
	s.Applied(streamEvents, 200)
	s.Applied(streamEvents, 150)

	ctx := WithHorizon(context.Background(), Horizon{Events: 200})
	if !s.derived.fresh(ctx, streamEvents) {
		t.Fatal("applied position went backwards")
	}
}

// Invalidation must actually reach the cache it names.
func TestInvalidationDropsTheRightEntry(t *testing.T) {
	s := armed(t, map[string]int64{streamEvents: 1, streamAccountData: 1})

	s.derived.roomSummary.Add("!a", MemberSummary{})
	s.derived.roomSummary.Add("!b", MemberSummary{})
	s.derived.historyVis.Add("!a", "shared")
	s.InvalidateRoom("!a")
	if _, ok := s.derived.roomSummary.Get("!a"); ok {
		t.Error("room summary for !a should have been dropped")
	}
	if _, ok := s.derived.historyVis.Get("!a"); ok {
		t.Error("history visibility for !a should have been dropped")
	}
	if _, ok := s.derived.roomSummary.Get("!b"); !ok {
		t.Error("room summary for !b should have survived")
	}

	s.derived.ignoredUsers.Add("@u", map[string]bool{"@x": true})
	s.InvalidateUserAccountData("@u")
	if _, ok := s.derived.ignoredUsers.Get("@u"); ok {
		t.Error("ignore list should have been dropped")
	}
}

// RoomsForUser is keyed by user with the membership set inside, precisely so
// that a user-keyed invalidation finds it. A composite key would not be found.
func TestRoomsForUserInvalidationFindsEveryMembershipSet(t *testing.T) {
	s := armed(t, map[string]int64{streamEvents: 1})
	s.derived.addRoomsForUser("@u", "join", []RoomForUser{{RoomID: "!a"}})
	s.derived.addRoomsForUser("@u", "invite\x01join", []RoomForUser{{RoomID: "!b"}})

	bySet, ok := s.derived.roomsForUser.Get("@u")
	if !ok || len(bySet) != 2 {
		t.Fatalf("expected both membership sets cached, got %v", bySet)
	}

	s.InvalidateUserMembership("@u")
	if _, ok := s.derived.roomsForUser.Get("@u"); ok {
		t.Fatal("a membership change must drop every membership set for that user")
	}
}

// A cached map handed out must not be the cache's own, or one caller writing
// to it corrupts every later reader.
func TestCachedValuesAreCopiedOnTheWayOut(t *testing.T) {
	original := map[string]bool{"@x": true}
	got := copyStringSet(original)
	got["@y"] = true
	if original["@y"] {
		t.Fatal("copyStringSet returned the caller's map")
	}

	rooms := []RoomForUser{{RoomID: "!a"}}
	copied := copyRoomsForUser(rooms)
	copied[0].RoomID = "!changed"
	if rooms[0].RoomID != "!a" {
		t.Fatal("copyRoomsForUser shared its backing array")
	}

	sum := MemberSummary{Counts: map[string]int{"join": 1}, Members: []SummaryMember{{UserID: "@a"}}}
	cs := copyMemberSummary(sum)
	cs.Counts["join"] = 99
	cs.Members[0].UserID = "@b"
	if sum.Counts["join"] != 1 || sum.Members[0].UserID != "@a" {
		t.Fatal("copyMemberSummary shared state with its source")
	}

	b := []byte("filter")
	cb := copyBytes(b)
	cb[0] = 'X'
	if b[0] != 'f' {
		t.Fatal("copyBytes shared its backing array")
	}
}
