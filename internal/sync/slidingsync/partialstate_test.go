package slidingsync

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
)

func TestMustAwaitFullState(t *testing.T) {
	isLocal := IsLocalUser("example.com")

	cases := []struct {
		name string
		in   [][2]string
		want bool
	}{
		{"nothing asked for", nil, false},
		{"no memberships at all", [][2]string{{"m.room.name", ""}, {"m.room.topic", ""}}, false},
		{"everything", [][2]string{{"*", "*"}}, true},
		{"a local member", [][2]string{{"m.room.member", "@alice:example.com"}}, false},
		{"a remote member", [][2]string{{"m.room.member", "@bob:other.example"}}, true},
		{"local and remote", [][2]string{
			{"m.room.member", "@alice:example.com"},
			{"m.room.member", "@bob:other.example"},
		}, true},
		// $ME is the caller, who is syncing here and is therefore local.
		{"$ME", [][2]string{{"m.room.member", "$ME"}}, false},
		// Lazy loading only asks for members in the timeline, and those events
		// had to be authorised to be persisted, so their memberships exist even
		// in a partial room.
		{"lazy members", [][2]string{{"m.room.member", "$LAZY"}}, false},
		// Odd but Synapse's: a membership wildcard does NOT force the wait,
		// although ["*","*"] does.
		{"every member", [][2]string{{"m.room.member", "*"}}, false},
		// A wildcard TYPE keyed on a remote user asks for that user's
		// membership under every type, m.room.member included.
		{"wildcard type on a remote user", [][2]string{{"*", "@bob:other.example"}}, true},
		{"wildcard type on a local user", [][2]string{{"*", "@alice:example.com"}}, false},
		{"wildcard type on a non-user key", [][2]string{{"*", ""}}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewRoomSyncConfig(CommonRoomParameters{RequiredState: tc.in, TimelineLimit: 1})
			if got := MustAwaitFullState(cfg, isLocal); got != tc.want {
				t.Errorf("got %v, want %v (required_state %v)", got, tc.want, tc.in)
			}
		})
	}
}

func TestIsLocalUser(t *testing.T) {
	isLocal := IsLocalUser("example.com")
	for _, tc := range []struct {
		userID string
		want   bool
	}{
		{"@a:example.com", true},
		{"@a:other.example", false},
		// A server name that merely ends the same way is not the same server.
		{"@a:notexample.com", false},
		{"", false},
	} {
		if got := isLocal(tc.userID); got != tc.want {
			t.Errorf("%q: got %v, want %v", tc.userID, got, tc.want)
		}
	}
}

// Synapse's must_await_full_state indexes the first character of every
// wildcard-type state key: an empty one fails the request, unless ["*", "*"]
// answered first.
func TestMustAwaitFullStateEmptyWildcardKey(t *testing.T) {
	isLocal := IsLocalUser("example.com")
	bad := slidingstore.RoomSyncConfig{RequiredState: map[string]map[string]bool{Wildcard: {"": true}}}
	if _, err := MustAwaitFullStateChecked(bad, isLocal); err == nil {
		t.Error(`["*", ""] accepted`)
	}
	all := slidingstore.RoomSyncConfig{RequiredState: map[string]map[string]bool{Wildcard: {"": true, Wildcard: true}}}
	if must, err := MustAwaitFullStateChecked(all, isLocal); err != nil || !must {
		t.Errorf(`["*", "*"] with ["*", ""]: %v %v`, must, err)
	}
	if IsLocalUser("example.com")("@a:b:example.com") {
		t.Error("is_mine_id compares everything after the first colon")
	}
}
