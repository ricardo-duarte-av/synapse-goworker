package streamtoken

import (
	"strings"
	"testing"
)

// Tokens observed on the live deployment (av-sync-worker-2) and in Synapse's
// own docstring. Round-tripping these byte-for-byte is the whole contract: a
// token we hand back must be one Synapse would accept, and one Synapse minted
// must survive a trip through us unchanged.
var realTokens = []string{
	// Synapse's docstring example, types/__init__.py:1148.
	"s2633508_17_338_6732159_1082514_541479_274711_265584_1_379_4242_4141_4343_4444",
	// Captured from /rooms/{id}/initialSync on av-sync-worker-2: the `end` of
	// a message chunk (live) and its `start` (topological).
	"s13908064_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69",
	"t12-13908046_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69",
}

func TestRoundTrip(t *testing.T) {
	for _, want := range realTokens {
		tok, err := Parse(want)
		if err != nil {
			t.Errorf("Parse(%q): %v", want, err)
			continue
		}
		if got := tok.String(); got != want {
			t.Errorf("round trip:\n got %q\nwant %q", got, want)
		}
	}
}

func TestParseFields(t *testing.T) {
	tok, err := Parse("s2633508_17_338_6732159_1082514_541479_274711_265584_1_379_4242_4141_4343_4444")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tok.Room.IsHistorical() {
		t.Error("room key should be live")
	}
	for _, c := range []struct {
		name string
		got  int64
		want int64
	}{
		{"room.stream", tok.Room.Stream, 2633508},
		{"presence", tok.Presence, 17},
		{"typing", tok.Typing, 338},
		{"receipt", tok.Receipt.Stream, 6732159},
		{"account_data", tok.AccountData, 1082514},
		{"push_rules", tok.PushRules, 541479},
		{"to_device", tok.ToDevice, 274711},
		{"device_list", tok.DeviceList.Stream, 265584},
		{"groups", tok.Groups, 1},
		{"un_partial_stated_rooms", tok.UnPartialStatedRooms, 379},
		{"thread_subscriptions", tok.ThreadSubscriptions, 4242},
		{"sticky_events", tok.StickyEvents, 4141},
		{"quarantined_media", tok.QuarantinedMedia.Stream, 4343},
		{"profile_updates", tok.ProfileUpdates, 4444},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// Clients hold tokens across Synapse upgrades, and Synapse has added fields
// over time. from_string right-pads with "0"; so must we, or every client that
// slept through an upgrade gets a 400.
func TestShortTokenIsPadded(t *testing.T) {
	// Thirteen fields, from the docstring's example response.
	tok, err := Parse("s12_4_0_1_1_1_1_4_1_1_1_1_1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tok.Room.Stream != 12 {
		t.Errorf("room.stream = %d", tok.Room.Stream)
	}
	if tok.ProfileUpdates != 0 {
		t.Errorf("padded field = %d, want 0", tok.ProfileUpdates)
	}
	if got, want := tok.String(), "s12_4_0_1_1_1_1_4_1_1_1_1_1_0"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// A token from a *newer* Synapse carries fields we do not know. Truncating it
// would hand back a token that rewinds a stream the client had already
// advanced past, losing events silently. Refusing is the safe failure.
func TestOverlongTokenIsRejected(t *testing.T) {
	long := "s1_2_3_4_5_6_7_8_9_10_11_12_13_14_15"
	_, err := Parse(long)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "more than") {
		t.Errorf("error = %v", err)
	}
}

func TestWithRoomKeyLeavesOtherStreamsAlone(t *testing.T) {
	const orig = "s13908064_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69"
	tok, err := Parse(orig)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := tok.WithRoomKey(Historical(12, 13908046)).String()
	want := "t12-13908046_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69"
	if got != want {
		t.Errorf("WithRoomKey:\n got %q\nwant %q", got, want)
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct{ name, token string }{
		{"empty", ""},
		{"bad room prefix", "x1_0_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"non-numeric room", "sabc_0_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"topological without dash", "t12_0_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"non-numeric presence", "s1_x_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"malformed vector clock", "m5~2_0_0_0_0_0_0_0_0_0_0_0_0_0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse(tc.token); err == nil {
				t.Errorf("Parse(%q) succeeded, want an error", tc.token)
			}
		})
	}
}

// AtLeast raises every stream that is behind the floor and nothing that is
// ahead of it. The tokens are the pair gomuks held across a leave on
// 2026-10-26: a client holding the higher one must never be handed the lower.
func TestAtLeastRaisesOnlyWhatIsBehind(t *testing.T) {
	now, err := Parse("s14980200_289801243_143415_27692474_1561128_1876_325202_41796344_0_1745_2_7271_5_1537")
	if err != nil {
		t.Fatal(err)
	}
	floor, err := Parse("s14980203_289801200_143416_27692500_1561128_1876_325202_41796344_0_1745_2_7271_5_1537")
	if err != nil {
		t.Fatal(err)
	}
	got := now.AtLeast(floor).String()
	want := "s14980203_289801243_143415_27692500_1561128_1876_325202_41796344_0_1745_2_7271_5_1537"
	if got != want {
		t.Errorf("AtLeast:\n got %s\nwant %s", got, want)
	}
}

// Typing is a writer's in-memory serial and restarts at zero. Raising ours to
// a client's pre-restart serial would freeze typing until the new serials
// caught up, so AtLeast must leave it where it is.
func TestAtLeastLeavesTypingAlone(t *testing.T) {
	now := Token{Room: Live(10), Typing: 3}
	floor := Token{Room: Live(10), Typing: 90000}
	if got := now.AtLeast(floor).Typing; got != 3 {
		t.Errorf("typing = %d, want 3: a restarted typing writer's serials must win", got)
	}
}

// Synapse's copy_and_advance, writer by writer: a writer absent from one side
// is at that side's minimum, and entries no longer ahead of the new minimum
// are dropped.
func TestAdvancedToIsWriterByWriter(t *testing.T) {
	a, err := ParseRoomKey("m10~1.15~2.12")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseRoomKey("m11~2.20~3.13")
	if err != nil {
		t.Fatal(err)
	}
	// stream 11; writer 1: max(15, 11)=15; writer 2: max(12, 20)=20;
	// writer 3: max(10, 13)=13.
	if got, want := a.AdvancedTo(b).String(), "m11~1.15~2.20~3.13"; got != want {
		t.Errorf("AdvancedTo = %s, want %s", got, want)
	}

	m := MultiWriter{Stream: 5, Instances: Instances{{ID: 1, Pos: 7}}}
	if got, want := m.AdvancedTo(MultiWriter{Stream: 8}).String(), "8"; got != want {
		t.Errorf("MultiWriter.AdvancedTo = %s, want %s: writer 1 is no longer ahead", got, want)
	}
}

// A historical key has no ordering against a live one to restore.
func TestAdvancedToLeavesHistoricalKeys(t *testing.T) {
	h := Historical(4, 100)
	if got := h.AdvancedTo(Live(200)); got.String() != h.String() {
		t.Errorf("historical key changed to %s", got)
	}
	l := Live(50)
	if got := l.AdvancedTo(h); got.String() != l.String() {
		t.Errorf("live key changed to %s by a historical floor", got)
	}
}
