package streamcache

import (
	"math/rand"
	"sort"
	"testing"
)

func TestUnknownEntityIsUnchangedAboveHorizon(t *testing.T) {
	c := armedCache("events", 100, 10)
	c.EntityHasChanged("!a", 110)

	if c.HasEntityChanged("!b", 105) {
		t.Fatal("an entity absent from a cache that covers (100, 110] must be unchanged")
	}
	if !c.HasEntityChanged("!b", 99) {
		t.Fatal("below the horizon the cache knows nothing and must say changed")
	}
}

func TestChangeAtPositionIsNotChangeAfterIt(t *testing.T) {
	c := armedCache("events", 0, 10)
	c.EntityHasChanged("!a", 10)

	if c.HasEntityChanged("!a", 10) {
		t.Fatal("a change at 10 is not a change after 10")
	}
	if !c.HasEntityChanged("!a", 9) {
		t.Fatal("a change at 10 is a change after 9")
	}
}

func TestPositionsAtOrBelowHorizonAreDropped(t *testing.T) {
	c := armedCache("events", 100, 10)
	c.EntityHasChanged("!a", 100)
	c.EntityHasChanged("!a", 50)

	if _, ok := c.MaxPosOfLastChange("!a"); ok {
		t.Fatal("a change at or below the horizon must not be recorded")
	}
	if got := c.Stats().Positions; got != 0 {
		t.Fatalf("positions = %d, want 0", got)
	}
}

func TestHorizonNeverDecreases(t *testing.T) {
	c := armedCache("events", 500, 10)
	c.AllEntitiesChanged(100)
	if got := c.EarliestKnownPosition(); got != 500 {
		t.Fatalf("horizon moved backwards to %d, want 500", got)
	}
	c.AllEntitiesChanged(900)
	if got := c.EarliestKnownPosition(); got != 900 {
		t.Fatalf("horizon = %d, want 900", got)
	}
}

func TestEvictionRaisesHorizon(t *testing.T) {
	// Two positions fit; the third evicts the oldest. The evicted entity must
	// not silently become "unchanged" -- the horizon has to move up with it.
	c := armedCache("events", 0, 2)
	c.EntityHasChanged("!a", 10)
	c.EntityHasChanged("!b", 20)
	c.EntityHasChanged("!c", 30)

	if got := c.EarliestKnownPosition(); got != 10 {
		t.Fatalf("horizon = %d, want 10 after evicting position 10", got)
	}
	if !c.HasEntityChanged("!a", 5) {
		t.Fatal("!a was evicted; a question below the new horizon must say changed")
	}
	if c.Stats().Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", c.Stats().Evictions)
	}
}

func TestMovingAnEntityForwardCleansUpTheOldPosition(t *testing.T) {
	c := armedCache("events", 0, 10)
	c.EntityHasChanged("!a", 10)
	c.EntityHasChanged("!a", 20)

	if got := c.Stats().Positions; got != 1 {
		t.Fatalf("positions = %d, want 1; position 10 should be gone", got)
	}
	if pos, _ := c.MaxPosOfLastChange("!a"); pos != 20 {
		t.Fatalf("last change = %d, want 20", pos)
	}
	// A stale, lower position must not overwrite a newer one.
	c.EntityHasChanged("!a", 15)
	if pos, _ := c.MaxPosOfLastChange("!a"); pos != 20 {
		t.Fatalf("last change = %d after a stale update, want 20", pos)
	}
}

func TestEntitiesChangedPreservesOrderAndFallsBackBelowHorizon(t *testing.T) {
	c := armedCache("events", 100, 10)
	c.EntityHasChanged("!b", 110)
	c.EntityHasChanged("!d", 120)

	in := []string{"!a", "!b", "!c", "!d"}
	got := c.EntitiesChanged(in, 105)
	want := []string{"!b", "!d"}
	if len(got) != len(want) {
		t.Fatalf("changed = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("changed = %v, want %v (caller order)", got, want)
		}
	}
	if len(c.EntitiesChanged(in, 99)) != len(in) {
		t.Fatal("below the horizon every entity must come back")
	}
}

func TestHasAnyEntityChanged(t *testing.T) {
	c := armedCache("presence", 100, 10)
	if c.HasAnyEntityChanged(150) {
		t.Fatal("an empty cache above the horizon has seen nothing")
	}
	if !c.HasAnyEntityChanged(50) {
		t.Fatal("below the horizon the answer must be changed")
	}
	c.EntityHasChanged("@a", 200)
	if !c.HasAnyEntityChanged(150) {
		t.Fatal("a change at 200 is a change after 150")
	}
	if c.HasAnyEntityChanged(200) {
		t.Fatal("a change at 200 is not a change after 200")
	}
}

// TestDisarmedAnswersChangedToEverything covers the disarmed reads. Note what
// it cannot catch: because Disarm also empties the cache, deleting the
// `c.disarmed` check from EntitiesChanged still passes -- the emptiness check
// behind it produces the same answer. That redundancy is deliberate (it is what
// keeps the reads correct if Disarm ever stops emptying), and it is therefore
// not something a test can distinguish. Verified by mutation, 2026-09-03.
func TestDisarmedAnswersChangedToEverything(t *testing.T) {
	c := armedCache("events", 0, 10)
	c.EntityHasChanged("!a", 10)
	c.Disarm()

	if !c.HasEntityChanged("!a", 5) || !c.HasEntityChanged("!zzz", 5) {
		t.Fatal("a disarmed cache must say changed")
	}
	if !c.HasAnyEntityChanged(5) {
		t.Fatal("a disarmed cache must say changed")
	}
	if _, ok := c.MaxPosOfLastChange("!a"); ok {
		t.Fatal("a disarmed cache must not answer with what it held")
	}
	if got := c.EntitiesChanged([]string{"!a", "!b"}, 5); len(got) != 2 {
		t.Fatalf("changed = %v, want both", got)
	}
	c.EntityHasChanged("!b", 20)
	if c.Stats().Entities != 0 {
		t.Fatal("a disarmed cache must not accept updates")
	}
	if c.Armed() {
		t.Fatal("Armed() disagrees with Disarm()")
	}
}

func TestArmEmptiesWhatPredatesTheOutage(t *testing.T) {
	c := armedCache("events", 0, 10)
	c.EntityHasChanged("!a", 10)
	c.Disarm()
	c.Arm(500)

	if !c.Armed() {
		t.Fatal("Arm did not arm")
	}
	if c.Stats().Entities != 0 {
		t.Fatal("Arm must drop entries that predate the outage")
	}
	if got := c.EarliestKnownPosition(); got != 500 {
		t.Fatalf("horizon = %d, want 500", got)
	}
}

func TestZeroSizedCacheAnswersChanged(t *testing.T) {
	c := New("events", 0, 0)
	c.EntityHasChanged("!a", 10)
	if !c.HasEntityChanged("!a", 100) {
		t.Fatal("a disabled cache must behave as if it knows nothing")
	}
	if !c.HasAnyEntityChanged(100) {
		t.Fatal("a disabled cache must behave as if it knows nothing")
	}
	if got := c.EntitiesChanged([]string{"!a"}, 100); len(got) != 1 {
		t.Fatal("a disabled cache must return every entity")
	}
	// A disabled cache must not read as healthy. It behaves exactly like a
	// disarmed one, and reporting armed=1 would draw a flat, reassuring line
	// for a cache that is answering nothing -- which is how a cache turned off
	// by a config change goes unnoticed for months.
	if c.Armed() || c.Stats().Armed {
		t.Fatal("a cache configured to hold nothing reported itself armed")
	}
}

func TestPrefillSetsTheHorizon(t *testing.T) {
	c := armedCache("events", 0, 100)
	c.Prefill(map[string]int64{"!a": 150, "!b": 200}, 100)

	if got := c.EarliestKnownPosition(); got != 100 {
		t.Fatalf("horizon = %d, want 100", got)
	}
	if c.HasEntityChanged("!c", 120) {
		t.Fatal("above a prefilled horizon an unknown entity is unchanged")
	}
	if !c.HasEntityChanged("!a", 120) {
		t.Fatal("!a changed at 150, after 120")
	}
}

// TestNoFalseNegatives is the property that matters. Over random interleavings
// of changes, evictions and queries, the cache may never claim "unchanged" for
// an entity that did change after the position asked about.
//
// It is deliberately run with a small max so that eviction -- the only path
// that can drop a real change -- runs constantly.
func TestNoFalseNegatives(t *testing.T) {
	const entities = 40
	rng := rand.New(rand.NewSource(20260903))

	for trial := 0; trial < 200; trial++ {
		c := armedCache("events", 0, 1+rng.Intn(8))
		truth := make(map[string]int64)
		var pos int64

		for step := 0; step < 300; step++ {
			if rng.Intn(3) != 0 {
				pos += int64(1 + rng.Intn(3))
				e := names[rng.Intn(entities)]
				c.EntityHasChanged(e, pos)
				truth[e] = pos
			}

			ask := int64(rng.Intn(int(pos) + 2))
			e := names[rng.Intn(entities)]

			if !c.HasEntityChanged(e, ask) {
				if last, ok := truth[e]; ok && last > ask {
					t.Fatalf("false negative: %s changed at %d, cache says unchanged since %d (horizon %d)",
						e, last, ask, c.EarliestKnownPosition())
				}
			}

			if !c.HasAnyEntityChanged(ask) {
				for _, last := range truth {
					if last > ask {
						t.Fatalf("false negative: something changed at %d, cache says nothing since %d (horizon %d)",
							last, ask, c.EarliestKnownPosition())
					}
				}
			}

			all := append([]string(nil), names[:entities]...)
			got := make(map[string]bool, len(all))
			for _, e := range c.EntitiesChanged(all, ask) {
				got[e] = true
			}
			for e, last := range truth {
				if last > ask && !got[e] {
					t.Fatalf("false negative: %s changed at %d, omitted from EntitiesChanged(%d) (horizon %d)",
						e, last, ask, c.EarliestKnownPosition())
				}
			}
		}
	}
}

// TestPositionsStaySorted guards the hand-rolled sorted slice, which is the
// only non-obvious data structure here and the one HasAnyEntityChanged reads
// the last element of.
func TestPositionsStaySorted(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	c := armedCache("events", 0, 1000)
	for i := 0; i < 2000; i++ {
		c.EntityHasChanged(names[rng.Intn(len(names))], int64(1+rng.Intn(5000)))
		if !sort.SliceIsSorted(c.positions, func(a, b int) bool { return c.positions[a] < c.positions[b] }) {
			t.Fatalf("positions unsorted after %d inserts: %v", i, c.positions)
		}
		if len(c.positions) != len(c.byPos) {
			t.Fatalf("positions (%d) and byPos (%d) disagree", len(c.positions), len(c.byPos))
		}
	}
}

// armedCache is New followed by Arm: a cache in service, which is what every
// test below is about. Construction alone leaves a cache disarmed on purpose --
// see TestAFreshCacheAnswersChangedUntilArmed.
func armedCache(name string, currentPos int64, max int) *Cache {
	c := New(name, currentPos, max)
	c.Arm(currentPos)
	return c
}

var names = func() []string {
	out := make([]string, 64)
	for i := range out {
		out[i] = "!room" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	return out
}()

// A cache that has never been armed must gate nothing.
//
// The alternative is the worst failure this package can have: armed at horizon
// 0 with no entries, it claims a complete record of all history and answers
// "unchanged" to everything. Nothing has to go wrong to reach that -- it is what
// construction alone used to produce, so a deployment with replication disabled,
// or a Store built without prefilling, silently stopped serving presence.
func TestAFreshCacheAnswersChangedUntilArmed(t *testing.T) {
	c := New("events", 0, 100)

	if !c.HasEntityChanged("!a", 100) {
		t.Error("an unarmed cache claimed an entity had not changed")
	}
	if !c.HasAnyEntityChanged(100) {
		t.Error("an unarmed cache claimed nothing had changed")
	}
	if got := c.EntitiesChanged([]string{"!a", "!b"}, 100); len(got) != 2 {
		t.Errorf("an unarmed cache narrowed a list to %v", got)
	}
	if c.Armed() {
		t.Error("a fresh cache reports itself armed")
	}

	// And it accepts nothing until armed, so arming cannot inherit junk.
	c.EntityHasChanged("!a", 200)
	if c.Stats().Entities != 0 {
		t.Error("an unarmed cache accepted an update")
	}

	c.Arm(500)
	if !c.Armed() {
		t.Fatal("Arm did not arm")
	}
	if c.HasEntityChanged("!x", 600) {
		t.Error("after arming, an unknown entity above the horizon must be unchanged")
	}
}
