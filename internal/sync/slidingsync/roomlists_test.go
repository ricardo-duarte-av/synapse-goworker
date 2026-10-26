package slidingsync

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

func ptr[T any](v T) *T { return &v }

func room(id, membership string, opts ...func(*store.SlidingRoom)) store.SlidingRoom {
	r := store.SlidingRoom{RoomID: id, Membership: membership, Sender: "@other:e", EventStream: 1}
	for _, o := range opts {
		o(&r)
	}
	return r
}

// A leave is gone; a kick or ban is not. The user's own leave is the last thing
// they should see and they have already seen it, but somebody else removing
// them is news.
func TestMembershipRelevance(t *testing.T) {
	const me = "@me:e"
	cases := []struct {
		name string
		r    store.SlidingRoom
		want bool
	}{
		{"joined", store.SlidingRoom{Membership: "join", Sender: me}, true},
		{"invited", store.SlidingRoom{Membership: "invite", Sender: "@a:e"}, true},
		{"knocked", store.SlidingRoom{Membership: "knock", Sender: me}, true},
		{"banned", store.SlidingRoom{Membership: "ban", Sender: "@a:e"}, true},
		{"left on their own", store.SlidingRoom{Membership: "leave", Sender: me}, false},
		{"kicked", store.SlidingRoom{Membership: "leave", Sender: "@a:e"}, true},
		{"state reset out", store.SlidingRoom{Membership: "leave", Sender: ""}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := membershipIsRelevant(me, tc.r); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilters(t *testing.T) {
	rooms := map[string]store.SlidingRoom{
		"!dm:e":     room("!dm:e", "join"),
		"!enc:e":    room("!enc:e", "join", func(r *store.SlidingRoom) { r.IsEncrypted = true }),
		"!space:e":  room("!space:e", "join", func(r *store.SlidingRoom) { r.RoomType = ptr("m.space") }),
		"!invite:e": room("!invite:e", "invite"),
		"!plain:e":  room("!plain:e", "join"),
	}
	meta := map[string]store.SlidingJoinedRoom{
		"!plain:e": {RoomID: "!plain:e", RoomName: ptr("General Chat")},
		"!dm:e":    {RoomID: "!dm:e", RoomName: ptr("Alice")},
	}
	dm := map[string]bool{"!dm:e": true}
	tags := map[string]map[string]bool{
		"!plain:e": {"m.favourite": true},
		"!enc:e":   {"m.lowpriority": true},
	}

	keys := func(m map[string]store.SlidingRoom) []string {
		out := []string{}
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	has := func(t *testing.T, m map[string]store.SlidingRoom, want ...string) {
		t.Helper()
		if len(m) != len(want) {
			t.Fatalf("got %v, want %v", keys(m), want)
		}
		for _, w := range want {
			if _, ok := m[w]; !ok {
				t.Fatalf("got %v, want %v", keys(m), want)
			}
		}
	}

	t.Run("is_dm true and false are different from absent", func(t *testing.T) {
		got, err := filterRooms(rooms, meta, &Filters{IsDM: ptr(true)}, dm, tags)
		if err != nil {
			t.Fatal(err)
		}
		has(t, got, "!dm:e")

		got, _ = filterRooms(rooms, meta, &Filters{IsDM: ptr(false)}, dm, tags)
		has(t, got, "!enc:e", "!space:e", "!invite:e", "!plain:e")

		got, _ = filterRooms(rooms, meta, &Filters{}, dm, tags)
		if len(got) != len(rooms) {
			t.Fatalf("an absent filter must not filter: got %v", keys(got))
		}
	})

	t.Run("is_encrypted", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{IsEncrypted: ptr(true)}, dm, tags)
		has(t, got, "!enc:e")
	})

	t.Run("is_invite", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{IsInvite: ptr(true)}, dm, tags)
		has(t, got, "!invite:e")
	})

	t.Run("room_types with a null entry means untyped rooms", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{RoomTypes: []*string{ptr("m.space")}}, dm, tags)
		has(t, got, "!space:e")

		got, _ = filterRooms(rooms, meta, &Filters{RoomTypes: []*string{nil}}, dm, tags)
		has(t, got, "!dm:e", "!enc:e", "!invite:e", "!plain:e")
	})

	t.Run("not_room_types wins over room_types", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{
			RoomTypes:    []*string{ptr("m.space"), nil},
			NotRoomTypes: []*string{ptr("m.space")},
		}, dm, tags)
		has(t, got, "!dm:e", "!enc:e", "!invite:e", "!plain:e")
	})

	t.Run("room_name_like is a case-insensitive substring", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{RoomNameLike: ptr("eral ch")}, dm, tags)
		has(t, got, "!plain:e")

		// A room with no name cannot match.
		got, _ = filterRooms(rooms, meta, &Filters{RoomNameLike: ptr("")}, dm, tags)
		has(t, got, "!plain:e", "!dm:e")
	})

	t.Run("not_tags takes priority over tags", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{Tags: []string{"m.favourite"}}, dm, tags)
		has(t, got, "!plain:e")

		got, _ = filterRooms(rooms, meta, &Filters{
			Tags:    []string{"m.favourite", "m.lowpriority"},
			NotTags: []string{"m.lowpriority"},
		}, dm, tags)
		has(t, got, "!plain:e")
	})

	t.Run("filters AND together", func(t *testing.T) {
		got, _ := filterRooms(rooms, meta, &Filters{
			IsEncrypted: ptr(true), IsDM: ptr(true),
		}, dm, tags)
		has(t, got)
	})

	// Synapse raises rather than ignoring it, and so must we: silently
	// returning unfiltered rooms shows a client rooms it asked not to see.
	t.Run("spaces is refused, not ignored", func(t *testing.T) {
		_, err := filterRooms(rooms, meta, &Filters{Spaces: []string{"!s:e"}}, dm, tags)
		if err == nil || !ErrSpacesFilter(err) {
			t.Fatalf("err = %v, want the unimplemented-spaces error", err)
		}
	})
}
