package visibility

import "testing"

const user = "@me:example.com"

func msg(sender string) Event {
	return Event{EventID: "$e", Type: "m.room.message", Sender: sender}
}

func at(vis, membership string) StateAtEvent {
	return StateAtEvent{HistoryVisibility: vis, UserMembership: membership, Present: true}
}

func TestHistoryVisibilityRules(t *testing.T) {
	cases := []struct {
		name       string
		visibility string
		membership string
		isPeeking  bool
		want       bool
	}{
		// shared: joined members see all history, including from before they
		// joined. This is why membership must NOT gate the lax path.
		{"shared, was not a member", Shared, "", false, true},
		{"shared, peeking", Shared, "", true, false},
		{"shared, peeking but joined at the time", Shared, "join", true, true},

		{"world_readable, no membership", WorldReadable, "", false, true},
		{"world_readable, peeking", WorldReadable, "", true, true},

		// invited: only those who were joined or invited at the time.
		{"invited, was joined", Invited, "join", false, true},
		{"invited, was invited", Invited, "invite", false, true},
		{"invited, was neither", Invited, "", false, false},
		{"invited, had left", Invited, "leave", false, false},

		// joined: only those who were joined at the time.
		{"joined, was joined", Joined, "join", false, true},
		{"joined, was invited", Joined, "invite", false, false},
		{"joined, was neither", Joined, "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Check(Context{UserID: user, IsPeeking: tc.isPeeking},
				msg("@other:example.com"), at(tc.visibility, tc.membership))
			if got.Visible != tc.want {
				t.Errorf("Visible = %v, want %v", got.Visible, tc.want)
			}
		})
	}
}

// A missing or unrecognised setting falls back to shared.
func TestVisibilityDefaultsToShared(t *testing.T) {
	for _, vis := range []string{"", "nonsense"} {
		got := Check(Context{UserID: user}, msg("@o:e"), at(vis, ""))
		if !got.Visible {
			t.Errorf("visibility %q should default to shared and be visible", vis)
		}
	}
}

// A user must always see their own leave, or they never see the room disappear
// after rejecting an invite.
func TestOwnLeaveIsAlwaysVisible(t *testing.T) {
	ev := Event{EventID: "$m", Type: "m.room.member", StateKey: user, IsState: true,
		Sender: user, Membership: "leave", PrevMembership: "invite"}
	got := Check(Context{UserID: user}, ev, at(Joined, "leave"))
	if !got.Visible {
		t.Error("a user's own leave after an invite must be visible even in a joined-visibility room")
	}
}

// The caller's own membership event is judged by the more permissive end of its
// transition, not by the state after it.
func TestOwnMembershipUsesMostPermissiveEnd(t *testing.T) {
	// join -> ban. The join end is more permissive, so the event is visible.
	ev := Event{EventID: "$m", Type: "m.room.member", StateKey: user, IsState: true,
		Sender: "@admin:e", Membership: "ban", PrevMembership: "join"}
	got := Check(Context{UserID: user}, ev, at(Joined, "ban"))
	if !got.Visible {
		t.Error("a ban following a join should be visible: the join end is more permissive")
	}
	// MSC4115 still reports the membership AFTER the event.
	if got.Membership != "ban" {
		t.Errorf("membership = %q, want ban (the state after the event)", got.Membership)
	}
}

// A history_visibility event is judged by the least restrictive of the settings
// either side of it, so the event announcing a tightening is still visible.
func TestHistoryVisibilityBoundary(t *testing.T) {
	ev := Event{EventID: "$h", Type: "m.room.history_visibility", StateKey: "", IsState: true,
		Sender: "@o:e", PrevHistoryVisibility: Shared}
	got := Check(Context{UserID: user}, ev, at(Joined, ""))
	if !got.Visible {
		t.Error("the event tightening visibility should itself be visible under the old setting")
	}
}

func TestSendToClientFilter(t *testing.T) {
	ctx := Context{UserID: user, IgnoredSenders: map[string]bool{"@spam:e": true}}
	cases := []struct {
		name string
		ev   Event
		want bool
	}{
		{"ordinary message", msg("@o:e"), true},
		{"dummy event", Event{Type: "org.matrix.dummy_event", Sender: "@o:e"}, false},
		{"aliases dropped wholesale", Event{Type: "m.room.aliases", IsState: true, Sender: "@o:e"}, false},
		{"message from an ignored user", msg("@spam:e"), false},
		// Ignoring someone hides their messages, not their membership changes.
		{"state from an ignored user", Event{Type: "m.room.member", StateKey: "@spam:e",
			IsState: true, Sender: "@spam:e"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Check(ctx, tc.ev, at(Shared, "join")); got.Visible != tc.want {
				t.Errorf("Visible = %v, want %v", got.Visible, tc.want)
			}
		})
	}
}

// Soft-failed events never reach a client.
func TestSoftFailedIsInvisible(t *testing.T) {
	ev := msg("@o:e")
	ev.SoftFailed = true
	if Check(Context{UserID: user}, ev, at(WorldReadable, "join")).Visible {
		t.Error("a soft-failed event must not be returned")
	}
}

// MSC1763: retention applies to non-state events only.
func TestRetentionDropsOldMessagesButNotState(t *testing.T) {
	ctx := Context{UserID: user, RetentionMaxLifetimeMS: 1000, NowMS: 10_000}

	old := msg("@o:e")
	old.OriginServerTS = 8_000
	if Check(ctx, old, at(Shared, "join")).Visible {
		t.Error("a message older than the retention window should be dropped")
	}

	recent := msg("@o:e")
	recent.OriginServerTS = 9_500
	if !Check(ctx, recent, at(Shared, "join")).Visible {
		t.Error("a message inside the retention window should survive")
	}

	oldState := Event{Type: "m.room.topic", StateKey: "", IsState: true,
		Sender: "@o:e", OriginServerTS: 8_000}
	if !Check(ctx, oldState, at(Shared, "join")).Visible {
		t.Error("retention must not apply to state events")
	}
}

// An outlier has no state around it. Normally invisible, with one exception:
// the caller's own out-of-band membership, such as a federated invite. Without
// it a user never sees the invite that is their only sign the room exists.
func TestOutliers(t *testing.T) {
	none := StateAtEvent{}

	ownInvite := Event{Type: "m.room.member", StateKey: user, IsState: true,
		Sender: "@inviter:e", Membership: "invite"}
	got := Check(Context{UserID: user}, ownInvite, none)
	if !got.Visible {
		t.Error("the caller's own out-of-band invite must be visible")
	}
	if got.Membership != "invite" {
		t.Errorf("membership = %q, want invite", got.Membership)
	}

	if Check(Context{UserID: user}, msg("@o:e"), none).Visible {
		t.Error("an ordinary outlier must not be visible")
	}
	other := Event{Type: "m.room.member", StateKey: "@someone:e", IsState: true, Sender: "@o:e"}
	if Check(Context{UserID: user}, other, none).Visible {
		t.Error("another user's outlier membership must not be visible")
	}
}

// An erased sender's event is served pruned when the caller was not joined at
// the time, and whole when they were.
func TestErasedSender(t *testing.T) {
	ctx := Context{UserID: user, ErasedSenders: map[string]bool{"@gone:e": true}}

	got := Check(ctx, msg("@gone:e"), at(Shared, ""))
	if !got.Visible || !got.Pruned {
		t.Errorf("want visible and pruned, got %+v", got)
	}

	got = Check(ctx, msg("@gone:e"), at(Shared, "join"))
	if !got.Visible || got.Pruned {
		t.Errorf("a joined member should see the whole event, got %+v", got)
	}
}

// MSC4115 reports the membership AFTER the event, defaulting to leave.
func TestMembershipAnnotation(t *testing.T) {
	if got := Check(Context{UserID: user}, msg("@o:e"), at(Shared, "join")); got.Membership != "join" {
		t.Errorf("membership = %q, want join", got.Membership)
	}
	if got := Check(Context{UserID: user}, msg("@o:e"), at(Shared, "")); got.Membership != "leave" {
		t.Errorf("membership = %q, want leave when the caller had none", got.Membership)
	}
}
