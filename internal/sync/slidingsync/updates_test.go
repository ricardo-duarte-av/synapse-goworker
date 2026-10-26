package slidingsync

import (
	"encoding/json"
	"testing"
)

// Whether a response counts as NEWS decides whether the long poll waits, and
// getting it wrong is not a subtle bug: it is a hot loop. On 2026-09-03 this
// endpoint treated any present extension as news, so it never waited, and
// SchildiChat was answered about ten times a second per connection until the
// nginx access log gave it away.
//
// Every case below is a field that is ALWAYS present and must therefore never,
// on its own, keep a client from waiting.

func TestAResponseWithNothingNewIsNotNews(t *testing.T) {
	limited := false
	stamp := int64(12345)
	zero := 0

	cases := []struct {
		name string
		room *RoomResult
		want bool
	}{
		{
			// The shape a quiet poll produces for a room already sent: a
			// bump_stamp restating what the client has, and nothing else.
			name: "only a bump_stamp",
			room: &RoomResult{BumpStamp: &stamp, Limited: &limited,
				NotificationCount: 0, HighlightCount: 0},
			want: false,
		},
		{
			// notification_count and highlight_count are hard-coded zeros
			// upstream, so they are on every room entry ever sent.
			name: "only the dummy counts",
			room: &RoomResult{NotificationCount: 0, HighlightCount: 0},
			want: false,
		},
		{
			name: "limited but empty",
			room: &RoomResult{Limited: &limited, NumLive: &zero},
			want: false,
		},
		{
			// A room being sent from scratch is always news, whatever else is
			// in it -- the client has nothing to apply a delta to.
			name: "initial",
			room: &RoomResult{Initial: true},
			want: true,
		},
		{name: "a timeline event", room: &RoomResult{Timeline: rawList(1)}, want: true},
		{name: "required state", room: &RoomResult{RequiredState: rawList(1)}, want: true},
		{name: "invite state", room: &RoomResult{StrippedState: rawList(1)}, want: true},
		{name: "a name", room: &RoomResult{Name: strptr("General")}, want: true},
		{name: "an avatar", room: &RoomResult{Avatar: strptr("mxc://x/y")}, want: true},
		{name: "heroes", room: &RoomResult{Heroes: []Hero{{UserID: "@a:e"}}}, want: true},
		{name: "a joined count", room: &RoomResult{JoinedCount: &zero}, want: true},
		{name: "an invited count", room: &RoomResult{InvitedCount: &zero}, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := roomIsNews(tc.room); got != tc.want {
				t.Errorf("roomIsNews = %v, want %v", got, tc.want)
			}
		})
	}
}

// The extensions each have their own rule, and two of them are why the hot loop
// happened: e2ee always carries one-time-key counts and to_device always
// carries a next_batch. Synapse's comment on the e2ee rule cites
// element-android#3725 for why the counts must be SENT and must not COUNT.
//
// These call the real predicates buildExtensions uses. An earlier version of
// this test restated the rules instead, which tested nothing at all.
func TestAlwaysPresentExtensionFieldsAreNotNews(t *testing.T) {
	t.Run("e2ee key counts alone", func(t *testing.T) {
		if e2eeIsNews(&e2eeJSON{
			DeviceOneTimeKeysCount:   map[string]int{"signed_curve25519": 50},
			DeviceUnusedFallbackKeys: []string{"signed_curve25519"},
		}) {
			t.Error("one-time-key counts counted as news; they are on every response, " +
				"so the long poll would never wait")
		}
	})
	t.Run("e2ee device lists present but empty", func(t *testing.T) {
		if e2eeIsNews(&e2eeJSON{DeviceLists: &deviceListsJSON{
			Changed: []string{}, Left: []string{}}}) {
			t.Error("empty device lists counted as news")
		}
	})
	t.Run("e2ee a changed device", func(t *testing.T) {
		if !e2eeIsNews(&e2eeJSON{DeviceLists: &deviceListsJSON{Changed: []string{"@a:e"}}}) {
			t.Error("a changed device list is news")
		}
	})

	t.Run("to_device next_batch alone", func(t *testing.T) {
		if toDeviceIsNews(&toDeviceJSON{NextBatch: "293695", Events: nil}) {
			t.Error("a moved next_batch counted as news")
		}
	})
	t.Run("to_device a message", func(t *testing.T) {
		if !toDeviceIsNews(&toDeviceJSON{NextBatch: "1", Events: rawList(1)}) {
			t.Error("a to-device message is news")
		}
	})

	t.Run("receipts and typing with an empty rooms object", func(t *testing.T) {
		if receiptsIsNews(&receiptsJSON{Rooms: map[string]json.RawMessage{}}) {
			t.Error("an empty receipts rooms object counted as news")
		}
		if typingIsNews(&typingJSON{Rooms: map[string]json.RawMessage{}}) {
			t.Error("an empty typing rooms object counted as news")
		}
	})
	t.Run("receipts and typing with content", func(t *testing.T) {
		one := map[string]json.RawMessage{"!a:e": json.RawMessage(`{}`)}
		if !receiptsIsNews(&receiptsJSON{Rooms: one}) || !typingIsNews(&typingJSON{Rooms: one}) {
			t.Error("a room entry is news")
		}
	})

	t.Run("sticky next_batch alone", func(t *testing.T) {
		if stickyEventsIsNews(&stickyEventsJSON{NextBatch: "sticky_4637"}) {
			t.Error("a moved sticky next_batch counted as news")
		}
	})

	t.Run("account data", func(t *testing.T) {
		if accountDataIsNews(&accountDataJSON{
			Global: []json.RawMessage{}, Rooms: map[string][]json.RawMessage{}}) {
			t.Error("empty account data counted as news")
		}
		if !accountDataIsNews(&accountDataJSON{Global: rawList(1)}) {
			t.Error("a global account data event is news")
		}
	})

	t.Run("thread subscriptions", func(t *testing.T) {
		if threadSubscriptionsIsNews(&threadSubscriptionsJSON{}) {
			t.Error("an empty thread subscriptions section counted as news")
		}
		prev := "ts5"
		if !threadSubscriptionsIsNews(&threadSubscriptionsJSON{PrevBatch: &prev}) {
			t.Error("a prev_batch means there is a gap to paginate, which is news")
		}
	})
}

func rawList(n int) []json.RawMessage {
	out := make([]json.RawMessage, n)
	for i := range out {
		out[i] = json.RawMessage(`{}`)
	}
	return out
}

func strptr(s string) *string { return &s }
