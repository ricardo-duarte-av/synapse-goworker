package streamtoken

import "testing"

func TestRoomKeyForms(t *testing.T) {
	cases := []struct {
		in          string
		historical  bool
		topological int64
		stream      int64
		instances   Instances
	}{
		{in: "s2633508", topological: NoTopological, stream: 2633508},
		{in: "t426-2633508", historical: true, topological: 426, stream: 2633508},
		{in: "m56~2.58~3.59", topological: NoTopological, stream: 56,
			instances: Instances{{ID: 2, Pos: 58}, {ID: 3, Pos: 59}}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			k, err := ParseRoomKey(tc.in)
			if err != nil {
				t.Fatalf("ParseRoomKey: %v", err)
			}
			if k.IsHistorical() != tc.historical {
				t.Errorf("IsHistorical = %v, want %v", k.IsHistorical(), tc.historical)
			}
			if k.Topological != tc.topological {
				t.Errorf("Topological = %d, want %d", k.Topological, tc.topological)
			}
			if k.Stream != tc.stream {
				t.Errorf("Stream = %d, want %d", k.Stream, tc.stream)
			}
			if len(k.Instances) != len(tc.instances) {
				t.Fatalf("Instances = %v, want %v", k.Instances, tc.instances)
			}
			for i := range tc.instances {
				if k.Instances[i] != tc.instances[i] {
					t.Errorf("Instances[%d] = %v, want %v", i, k.Instances[i], tc.instances[i])
				}
			}
			if got := k.String(); got != tc.in {
				t.Errorf("String() = %q, want %q", got, tc.in)
			}
		})
	}
}

// Synapse serialises by iterating a Python dict, which preserves insertion
// order, and on parse that order is the order in the string. Reordering would
// produce a semantically identical token that compares unequal -- a mismatch
// that means nothing and costs a debugging session.
func TestVectorClockPreservesOrder(t *testing.T) {
	const in = "m56~8.61~2.58~5.59"
	k, err := ParseRoomKey(in)
	if err != nil {
		t.Fatalf("ParseRoomKey: %v", err)
	}
	if got := k.String(); got != in {
		t.Errorf("String() = %q, want %q (order must survive)", got, in)
	}
}

// Synapse drops writers at or below the minimum: we may know a writer has
// advanced without having seen a recent write from it, and listing it would
// claim a position a reader cannot rely on. A clock left empty degrades to the
// "s" form, which is what Synapse emits too.
func TestVectorClockDropsWritersNotAhead(t *testing.T) {
	k := RoomKey{Topological: NoTopological, Stream: 56,
		Instances: Instances{{ID: 2, Pos: 56}, {ID: 3, Pos: 59}}}
	if got, want := k.String(), "m56~3.59"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	all := RoomKey{Topological: NoTopological, Stream: 56,
		Instances: Instances{{ID: 2, Pos: 50}, {ID: 3, Pos: 56}}}
	if got, want := all.String(), "s56"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// Synapse emitted tokens of the form "m5~" from a bug and still has to read
// them back, so we must too.
func TestVectorClockTolerlatesTrailingSeparator(t *testing.T) {
	k, err := ParseRoomKey("m5~")
	if err != nil {
		t.Fatalf("ParseRoomKey: %v", err)
	}
	if k.Stream != 5 || len(k.Instances) != 0 {
		t.Errorf("got %+v", k)
	}
	if got, want := k.String(), "s5"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// Paginating backwards from a vector clock has to start from the highest
// position any writer reached, not the agreed minimum: a writer that is ahead
// has already persisted events the token covers.
func TestMaxStreamPos(t *testing.T) {
	k, _ := ParseRoomKey("m56~2.58~3.71")
	if got := k.MaxStreamPos(); got != 71 {
		t.Errorf("MaxStreamPos = %d, want 71", got)
	}
	live, _ := ParseRoomKey("s56")
	if got := live.MaxStreamPos(); got != 56 {
		t.Errorf("MaxStreamPos = %d, want 56", got)
	}
}

// A writer absent from the map is at the minimum: the map lists only writers
// that are ahead.
func TestStreamPosForInstance(t *testing.T) {
	k, _ := ParseRoomKey("m56~2.58")
	if got := k.StreamPosForInstance(2); got != 58 {
		t.Errorf("known instance = %d, want 58", got)
	}
	if got := k.StreamPosForInstance(7); got != 56 {
		t.Errorf("unknown instance = %d, want 56 (the minimum)", got)
	}
}

func TestMultiWriterForms(t *testing.T) {
	for _, in := range []string{"6732159", "m56~2.58~3.59"} {
		m, err := ParseMultiWriter(in)
		if err != nil {
			t.Fatalf("ParseMultiWriter(%q): %v", in, err)
		}
		if got := m.String(); got != in {
			t.Errorf("String() = %q, want %q", got, in)
		}
	}
}

func TestRoomKeyRejects(t *testing.T) {
	for _, in := range []string{"", "x1", "s", "sabc", "t12", "t12-", "tabc-1", "m", "mabc", "m5~2", "m5~a.1", "m5~2.b"} {
		if _, err := ParseRoomKey(in); err == nil {
			t.Errorf("ParseRoomKey(%q) succeeded, want an error", in)
		}
	}
}
