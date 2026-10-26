package slidingstore

import (
	"testing"
)

func TestNeverIsTheDefault(t *testing.T) {
	m := NewRoomStatusMap(nil)
	if got := m.HaveSentRoom("!a:e").Status; got != FlagNever {
		t.Fatalf("unknown room = %q, want %q", got, FlagNever)
	}
}

// NEVER -> LIVE -> PREVIOUSLY -> LIVE are the only transitions. In particular
// there is no route back to NEVER: forgetting that a room was sent is how a
// client stops being told about it.
func TestTheOnlyValidTransitions(t *testing.T) {
	m := NewRoomStatusMap(nil)

	m.RecordSentRooms([]string{"!a:e"})
	if got := m.HaveSentRoom("!a:e"); got.Status != FlagLive {
		t.Fatalf("after sending: %q, want live", got.Status)
	}

	m.RecordUnsentRooms([]string{"!a:e"}, "s100")
	got := m.HaveSentRoom("!a:e")
	if got.Status != FlagPreviously || got.LastToken != "s100" {
		t.Fatalf("after skipping: %+v, want previously at s100", got)
	}

	m.RecordSentRooms([]string{"!a:e"})
	if got := m.HaveSentRoom("!a:e"); got.Status != FlagLive || got.LastToken != "" {
		t.Fatalf("after sending again: %+v, want live with no token", got)
	}
}

// A PREVIOUSLY room already names the point to resume from. Overwriting it with
// a later token would skip everything in between -- events the client has never
// seen and now never will.
func TestPreviouslyKeepsItsOriginalToken(t *testing.T) {
	m := NewRoomStatusMap(nil)
	m.RecordSentRooms([]string{"!a:e"})
	m.RecordUnsentRooms([]string{"!a:e"}, "s100")
	m.RecordUnsentRooms([]string{"!a:e"}, "s200")

	if got := m.HaveSentRoom("!a:e"); got.LastToken != "s100" {
		t.Fatalf("resume token = %q, want the FIRST one (s100)", got.LastToken)
	}
}

// A room never sent has no base for a delta to apply to, so it must stay NEVER
// and be sent in full when it is finally sent at all.
func TestNeverDoesNotBecomePreviously(t *testing.T) {
	m := NewRoomStatusMap(nil)
	m.RecordUnsentRooms([]string{"!a:e"}, "s100")
	if got := m.HaveSentRoom("!a:e").Status; got != FlagNever {
		t.Fatalf("unsent-and-never-sent room = %q, want never", got)
	}
	if len(m.Updates()) != 0 {
		t.Fatalf("recorded an update for a room that did not change: %v", m.Updates())
	}
}

// Re-sending an already-LIVE room must not register an update. Every update
// forces a new connection position, which copies a table's worth of rows
// forward -- so a response that changed nothing must cost nothing.
func TestResendingALiveRoomIsNotAnUpdate(t *testing.T) {
	m := NewRoomStatusMap(map[string]HaveSent{"!a:e": Live()})
	m.RecordSentRooms([]string{"!a:e"})
	if len(m.Updates()) != 0 {
		t.Fatalf("re-sending a live room registered %v", m.Updates())
	}
}

// The base map is read from the database and may be shared; writing through it
// would corrupt whatever else holds it.
func TestUpdatesDoNotMutateTheBase(t *testing.T) {
	base := map[string]HaveSent{"!a:e": Live()}
	m := NewRoomStatusMap(base)
	m.RecordUnsentRooms([]string{"!a:e"}, "s100")

	if base["!a:e"].Status != FlagLive {
		t.Fatal("the base map was written through")
	}
	if got := m.HaveSentRoom("!a:e").Status; got != FlagPreviously {
		t.Fatalf("the update did not take: %q", got)
	}
	all := m.All()
	if all["!a:e"].Status != FlagPreviously {
		t.Fatalf("All() = %+v, want the update to win", all["!a:e"])
	}
	if len(all) != 1 {
		t.Fatalf("All() = %v, want one entry", all)
	}
}

func TestHasUpdates(t *testing.T) {
	const now = 1_700_000_000_000

	t.Run("nothing changed", func(t *testing.T) {
		s := &PerConnectionState{Rooms: NewRoomStatusMap(map[string]HaveSent{"!a:e": Live()})}
		if s.HasUpdates(now) {
			t.Fatal("a response that changed nothing wants a new position")
		}
	})

	t.Run("a room moved", func(t *testing.T) {
		s := &PerConnectionState{}
		s.Rooms.RecordSentRooms([]string{"!a:e"})
		if !s.HasUpdates(now) {
			t.Fatal("a newly sent room is an update")
		}
	})

	t.Run("a room config changed", func(t *testing.T) {
		s := &PerConnectionState{}
		s.SetRoomConfig("!a:e", RoomSyncConfig{TimelineLimit: 10})
		if !s.HasUpdates(now) {
			t.Fatal("a new room config is an update")
		}
	})

	t.Run("a lazy member is new", func(t *testing.T) {
		s := &PerConnectionState{LazyMembership: map[string]*LazyMembers{
			"!a:e": {Returned: map[string]*int64{"@u:e": nil}},
		}}
		if !s.HasUpdates(now) {
			t.Fatal("a member never recorded is an update")
		}
	})

	t.Run("a lazy member was seen recently", func(t *testing.T) {
		recent := int64(now - 60_000)
		s := &PerConnectionState{LazyMembership: map[string]*LazyMembers{
			"!a:e": {Returned: map[string]*int64{"@u:e": &recent}},
		}}
		if s.HasUpdates(now) {
			t.Fatal("re-seeing a member inside the update interval must not force a new position")
		}
	})

	t.Run("a lazy member is stale", func(t *testing.T) {
		old := int64(now - LazyMembersUpdateIntervalMS - 1)
		s := &PerConnectionState{LazyMembership: map[string]*LazyMembers{
			"!a:e": {Returned: map[string]*int64{"@u:e": &old}},
		}}
		if !s.HasUpdates(now) {
			t.Fatal("a member last seen beyond the interval needs its timestamp written")
		}
	})

	t.Run("a lazy member was invalidated", func(t *testing.T) {
		s := &PerConnectionState{LazyMembership: map[string]*LazyMembers{
			"!a:e": {Invalidated: map[string]bool{"@u:e": true}},
		}}
		if !s.HasUpdates(now) {
			t.Fatal("an invalidated member must be removed")
		}
	})
}

// Deduplication is byte-for-byte on the encoding, so two rooms asking for the
// same state in a different order must encode identically. Without that, every
// room gets its own required_state row.
func TestRequiredStateEncodingIsCanonical(t *testing.T) {
	a := map[string]map[string]bool{
		"m.room.name":   {"": true},
		"m.room.member": {"@b:e": true, "@a:e": true},
	}
	b := map[string]map[string]bool{
		"m.room.member": {"@a:e": true, "@b:e": true},
		"m.room.name":   {"": true},
	}
	ea, err := EncodeRequiredState(a)
	if err != nil {
		t.Fatal(err)
	}
	eb, err := EncodeRequiredState(b)
	if err != nil {
		t.Fatal(err)
	}
	if ea != eb {
		t.Fatalf("same state encoded two ways:\n  %s\n  %s", ea, eb)
	}
	const want = `[["m.room.member","@a:e"],["m.room.member","@b:e"],["m.room.name",""]]`
	if ea != want {
		t.Fatalf("encoding = %s, want %s", ea, want)
	}
}

func TestRequiredStateRoundTrips(t *testing.T) {
	in := map[string]map[string]bool{
		"m.room.member":     {"$LAZY": true, "@a:e": true},
		"m.space.child":     {"*": true},
		"m.room.encryption": {"": true},
	}
	encoded, err := EncodeRequiredState(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeRequiredState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("decoded %v, want %v", out, in)
	}
	for typ, keys := range in {
		for key := range keys {
			if !out[typ][key] {
				t.Errorf("lost %s/%s", typ, key)
			}
		}
	}
}

func TestEmptyRequiredStateEncodesAsAnEmptyList(t *testing.T) {
	got, err := EncodeRequiredState(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "[]" {
		t.Fatalf("encoding = %q, want []", got)
	}
}

func TestReturnedToUpdateIsSorted(t *testing.T) {
	l := &LazyMembers{Returned: map[string]*int64{"@c:e": nil, "@a:e": nil, "@b:e": nil}}
	got := l.ReturnedToUpdate(0)
	want := []string{"@a:e", "@b:e", "@c:e"}
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v -- the order decides the write order, and an "+
				"unstable one makes a deadlock intermittent", got, want)
		}
	}
}
