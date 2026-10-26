package slidingsync

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
)

func cfg(limit int, pairs ...[2]string) slidingstore.RoomSyncConfig {
	return NewRoomSyncConfig(CommonRoomParameters{RequiredState: pairs, TimelineLimit: limit})
}

func encode(t *testing.T, c slidingstore.RoomSyncConfig) string {
	t.Helper()
	s, err := slidingstore.EncodeRequiredState(c.RequiredState)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRequiredStateNormalisation(t *testing.T) {
	cases := []struct {
		name string
		in   [][2]string
		want string
	}{
		{
			name: "plain entries are kept",
			in:   [][2]string{{"m.room.name", ""}, {"m.room.topic", ""}},
			want: `[["m.room.name",""],["m.room.topic",""]]`,
		},
		{
			name: "duplicates collapse",
			in:   [][2]string{{"m.room.name", ""}, {"m.room.name", ""}},
			want: `[["m.room.name",""]]`,
		},
		{
			// The whole point of normalising: two clients asking for the same
			// thing must store the same bytes, or deduplication fails and the
			// required-state diff sees a change that is not one.
			name: "a wildcard state key subsumes specific ones",
			in:   [][2]string{{"m.room.member", "@a:e"}, {"m.room.member", "*"}, {"m.room.member", "@b:e"}},
			want: `[["m.room.member","*"]]`,
		},
		{
			name: "order does not matter for that",
			in:   [][2]string{{"m.room.member", "*"}, {"m.room.member", "@a:e"}},
			want: `[["m.room.member","*"]]`,
		},
		{
			name: "a wildcard type subsumes the same key under other types",
			in:   [][2]string{{"m.room.name", ""}, {"m.room.topic", ""}, {"*", ""}},
			want: `[["*",""]]`,
		},
		{
			name: "full wildcard subsumes everything before it",
			in:   [][2]string{{"m.room.name", ""}, {"m.room.member", "@a:e"}, {"*", "*"}},
			want: `[["*","*"]]`,
		},
		{
			// After ["*","*"] the semantics invert -- further entries FILTER
			// rather than add -- so nothing may be folded into the map.
			name: "full wildcard swallows everything after it",
			in:   [][2]string{{"*", "*"}, {"m.room.member", "@a:e"}},
			want: `[["*","*"]]`,
		},
		{
			name: "lazy members alongside everything else",
			in:   [][2]string{{"m.room.member", "$LAZY"}, {"*", "*"}},
			want: `[["*","*"]]`,
		},
		{
			name: "empty is empty",
			in:   nil,
			want: `[]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encode(t, cfg(10, tc.in...)); got != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

// A room in several lists gets the superset. Sending less would let one list's
// presence in the response silently degrade another's.
func TestCombineTakesTheSuperset(t *testing.T) {
	a := cfg(5, [2]string{"m.room.name", ""})
	b := cfg(20, [2]string{"m.room.topic", ""})
	got := CombineRoomSyncConfig(a, b)

	if got.TimelineLimit != 20 {
		t.Errorf("timeline limit = %d, want the higher of 5 and 20", got.TimelineLimit)
	}
	if want := `[["m.room.name",""],["m.room.topic",""]]`; encode(t, got) != want {
		t.Errorf("got %s, want %s", encode(t, got), want)
	}
}

func TestCombineWildcards(t *testing.T) {
	cases := []struct {
		name string
		a, b slidingstore.RoomSyncConfig
		want string
	}{
		{
			name: "a full wildcard on either side wins",
			a:    cfg(1, [2]string{"m.room.name", ""}),
			b:    cfg(1, [2]string{"*", "*"}),
			want: `[["*","*"]]`,
		},
		{
			name: "and from the other direction",
			a:    cfg(1, [2]string{"*", "*"}),
			b:    cfg(1, [2]string{"m.room.name", ""}),
			want: `[["*","*"]]`,
		},
		{
			name: "a wildcard state key absorbs specific keys",
			a:    cfg(1, [2]string{"m.room.member", "@a:e"}),
			b:    cfg(1, [2]string{"m.room.member", "*"}),
			want: `[["m.room.member","*"]]`,
		},
		{
			name: "specific keys do not reappear under a wildcard",
			a:    cfg(1, [2]string{"m.room.member", "*"}),
			b:    cfg(1, [2]string{"m.room.member", "@a:e"}),
			want: `[["m.room.member","*"]]`,
		},
		{
			name: "a wildcard type absorbs the same key elsewhere",
			a:    cfg(1, [2]string{"m.room.name", ""}, [2]string{"m.room.topic", ""}),
			b:    cfg(1, [2]string{"*", ""}),
			want: `[["*",""]]`,
		},
		{
			name: "disjoint keys under different types both survive",
			a:    cfg(1, [2]string{"m.room.member", "@a:e"}),
			b:    cfg(1, [2]string{"m.room.member", "@b:e"}),
			want: `[["m.room.member","@a:e"],["m.room.member","@b:e"]]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encode(t, CombineRoomSyncConfig(tc.a, tc.b)); got != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

// Combining must not write through to either input: both come from the request
// and one of them is reused for every other room in the same list.
func TestCombineDoesNotMutateItsInputs(t *testing.T) {
	a := cfg(5, [2]string{"m.room.name", ""})
	b := cfg(5, [2]string{"m.room.topic", ""})
	before := encode(t, a)

	CombineRoomSyncConfig(a, b)

	if got := encode(t, a); got != before {
		t.Fatalf("the left operand was mutated: %s -> %s", before, got)
	}
	if len(b.RequiredState["m.room.topic"]) != 1 {
		t.Fatal("the right operand was mutated")
	}
}

// An empty set and an absent type must not be distinguishable, or two
// equivalent configs encode differently and deduplication stops working.
func TestSubsumedTypesAreRemovedNotEmptied(t *testing.T) {
	c := cfg(1, [2]string{"m.room.name", ""}, [2]string{"*", ""})
	if _, ok := c.RequiredState["m.room.name"]; ok {
		t.Fatalf("m.room.name left behind as %v", c.RequiredState["m.room.name"])
	}
}
