// Package visibility decides which events a user is allowed to see.
//
// This is the one place in the worker where a mistake serves private room
// history to somebody who should not have it, so it is a close port of
// synapse/visibility.py rather than an approximation.
//
// The shape mirrors Synapse's: a cheap pre-filter that needs no room state
// (_check_filter_send_to_client), then the visibility decision proper, which
// needs the state resolved *at each event* -- the history visibility then in
// force, and the caller's membership then. Both are supplied by the caller,
// which is what keeps this package free of database access and testable.
package visibility

// History visibility values, most permissive first: Synapse's
// VISIBILITY_PRIORITY order.
const (
	WorldReadable = "world_readable"
	Shared        = "shared"
	Invited       = "invited"
	Joined        = "joined"
)

// Membership values, most "joined" first: Synapse's MEMBERSHIP_PRIORITY.
// The order matters -- it is how a membership transition is resolved to the
// more permissive of its two ends.
var membershipPriority = []string{"join", "invite", "knock", "leave", "ban"}

func membershipRank(m string) int {
	for i, v := range membershipPriority {
		if v == m {
			return i
		}
	}
	// Synapse maps anything unrecognised to "leave" before ranking it.
	return len(membershipPriority) - 2
}

func normaliseMembership(m string) string {
	for _, v := range membershipPriority {
		if v == m {
			return m
		}
	}
	return "leave"
}

func validVisibility(v string) string {
	switch v {
	case WorldReadable, Shared, Invited, Joined:
		return v
	}
	// Synapse falls back to shared for a missing or invalid setting.
	return Shared
}

// Event is what this package needs to know about an event under test.
type Event struct {
	EventID        string
	Type           string
	StateKey       string
	IsState        bool
	Sender         string
	OriginServerTS int64

	// PolicyServerSpammy comes from internal_metadata, and marks an event
	// soft-failed specifically by a policy server rather than by auth. An admin
	// can ask to see these alone.
	PolicyServerSpammy bool
	// SoftFailed comes from internal_metadata. Soft-failed events are excluded
	// from client responses entirely.
	SoftFailed bool

	// Membership and PrevMembership are read from the event's own content and
	// unsigned.prev_content, and are used only when the event IS the caller's
	// own membership event.
	Membership     string
	PrevMembership string

	// PrevHistoryVisibility is unsigned.prev_content.history_visibility, used
	// only when the event IS an m.room.history_visibility event.
	PrevHistoryVisibility string
}

// StateAtEvent is the resolved state after an event, restricted to what the
// decision needs.
type StateAtEvent struct {
	// HistoryVisibility at the event. Empty means the room has no such event,
	// which Synapse treats as "shared".
	HistoryVisibility string
	// UserMembership is the caller's membership from the state after the event.
	// Empty means they had none.
	UserMembership string
	// Present is false for an outlier, which has no state group.
	Present bool
}

// Context is what is known once per request rather than per event.
type Context struct {
	UserID string
	// IsPeeking is true when the caller is not a member of the room and is
	// reading it as a peek.
	IsPeeking bool
	// IgnoredSenders is the caller's m.ignored_user_list.
	IgnoredSenders map[string]bool
	// ErasedSenders are senders who have been erased (GDPR).
	ErasedSenders map[string]bool
	// RetentionMaxLifetimeMS is the room's retention policy, 0 for none.
	RetentionMaxLifetimeMS int64
	// NowMS is the wall clock the retention check compares against.
	NowMS int64
	// ReturnSoftFailed lets a SERVER ADMIN who has asked for them see
	// soft-failed events. It comes from
	// `io.element.synapse.admin_client_config` and does nothing for anyone
	// else; the store checks the admin flag before setting it.
	ReturnSoftFailed bool
	// ReturnPolicySpammy is the narrower version: only events soft-failed by a
	// policy server.
	ReturnPolicySpammy bool
}

// Verdict is the outcome for one event.
type Verdict struct {
	// Visible is false when the event must not be returned at all.
	Visible bool
	// Pruned is true when the event may be returned only in redacted form,
	// because its sender has been erased and the caller was not joined at the
	// time.
	Pruned bool
	// Membership is the caller's membership *after* this event, for MSC4115's
	// unsigned.membership. Only meaningful when Visible.
	Membership string
}

// Check decides whether one event may be shown.
//
// A close port of _check_client_allowed_to_see_event. state is the resolved
// state after the event; for an outlier it must have Present false.
func Check(ctx Context, ev Event, state StateAtEvent) Verdict {
	// Soft-failed events never reach a client -- unless the caller is a server
	// admin who has explicitly asked for them, which is the only exception
	// Synapse makes and which it makes for both flavours of the request
	// (visibility.py, get_admin_client_config_for_user).
	//
	// The check is here rather than at a call site because it precedes
	// everything else: Synapse drops soft-failed events before any visibility
	// rule runs.
	if ev.SoftFailed {
		switch {
		case ctx.ReturnSoftFailed:
		case ctx.ReturnPolicySpammy && ev.PolicyServerSpammy:
		default:
			return Verdict{}
		}
	}
	if !passesSendToClientFilter(ctx, ev) {
		return Verdict{}
	}

	if !state.Present {
		// Outliers have no state around them. Normally invisible, with one
		// exception: the caller's own out-of-band membership events, such as an
		// invite received over federation or its rejection. Without this a user
		// never sees the invite that is the only reason they know the room
		// exists.
		if ev.Type == "m.room.member" && ev.StateKey == ctx.UserID {
			return Verdict{Visible: true, Membership: MembershipAfter(ctx, ev, state)}
		}
		return Verdict{}
	}

	visibility := validVisibility(state.HistoryVisibility)

	// The lax path: for these settings the answer does not depend on the
	// caller's membership at all. `shared` means joined members see all
	// history, including history from before they joined.
	//
	// Skipped when the sender is erased, because then the membership check
	// decides whether the event is returned whole or pruned.
	if !ctx.ErasedSenders[ev.Sender] && laxAllows(ev, visibility, ctx.IsPeeking) {
		return Verdict{Visible: true, Membership: MembershipAfter(ctx, ev, state)}
	}

	allowed, joined := checkMembership(ctx, ev, visibility, state)
	if !allowed {
		return Verdict{}
	}
	return Verdict{
		Visible:    true,
		Pruned:     ctx.ErasedSenders[ev.Sender] && !joined,
		Membership: MembershipAfter(ctx, ev, state),
	}
}

// passesSendToClientFilter is _check_filter_send_to_client: the checks that
// need no room state.
func passesSendToClientFilter(ctx Context, ev Event) bool {
	// Padding Synapse inserts to advance the DAG. Never shown to anyone.
	if ev.Type == "org.matrix.dummy_event" {
		return false
	}
	// Ignoring a user hides their messages but not their state changes: you
	// still need to see them join, leave or be banned.
	if !ev.IsState && ctx.IgnoredSenders[ev.Sender] {
		return false
	}
	// Until MSC2261 lands a malicious alias event cannot be redacted, so
	// Synapse drops the type wholesale.
	if ev.Type == "m.room.aliases" {
		return false
	}
	// MSC1763: retention applies to non-state events only.
	if !ev.IsState && ctx.RetentionMaxLifetimeMS > 0 {
		if ev.OriginServerTS < ctx.NowMS-ctx.RetentionMaxLifetimeMS {
			return false
		}
	}
	return true
}

// visibilityRank orders VISIBILITY_PRIORITY: lower is more permissive.
func visibilityRank(v string) int {
	for i, x := range []string{WorldReadable, Shared, Invited, Joined} {
		if x == v {
			return i
		}
	}
	return 1 // shared
}

// laxAllows is _check_history_visibility.
func laxAllows(ev Event, visibility string, isPeeking bool) bool {
	// A history visibility event sits on a boundary, and is judged by the
	// *least restrictive* of the settings on either side of it. Otherwise the
	// event announcing a tightening would be hidden by the tightening it
	// announces, and a client could never see why history stopped.
	if ev.Type == "m.room.history_visibility" {
		prev := validVisibility(ev.PrevHistoryVisibility)
		if visibilityRank(prev) < visibilityRank(visibility) {
			visibility = prev
		}
	}
	switch {
	case visibility == Shared && !isPeeking:
		return true
	case visibility == WorldReadable:
		return true
	}
	return false
}

// checkMembership is _check_membership.
func checkMembership(ctx Context, ev Event, visibility string, state StateAtEvent) (allowed, joined bool) {
	membership := ""

	if ev.Type == "m.room.member" && ev.StateKey == ctx.UserID {
		// For the caller's own membership event, judge by the transition
		// rather than by the state after it.
		current := normaliseMembership(ev.Membership)
		prev := normaliseMembership(ev.PrevMembership)

		// Always let a user see their own leave, or they never see the room
		// disappear when they reject an invite.
		if current == "leave" && (prev == "join" || prev == "invite") {
			return true, false
		}
		// Take the more permissive end of the transition.
		if membershipRank(prev) < membershipRank(current) {
			membership = prev
		} else {
			membership = current
		}
	}

	if membership == "" {
		membership = state.UserMembership
	}

	if membership == "join" {
		return true, true
	}

	switch visibility {
	case Joined:
		// Not a member at the time, so no.
		return false, false
	case Invited:
		return membership == "invite", false
	case Shared:
		if ctx.IsPeeking {
			// A peeker cannot see history from before they arrived. Synapse
			// notes it would ideally share history up to the point a user
			// left, but it does not know when that was.
			return false, false
		}
	}
	// shared (not peeking) or world_readable, and not a member at the time.
	return true, false
}

// MembershipAfter is MSC4115's unsigned.membership: the caller's membership
// *after* this event.
//
// For the caller's own membership event that is the event's own membership;
// otherwise it comes from the resolved state, defaulting to leave.
//
// Exported because Synapse annotates the membership AFTER deciding whether the
// event may be seen, and independently of HOW that was decided: an event let
// through by `always_include_ids` is annotated like any other. Check reports
// nothing for an event it rejects, so the caller doing the rescuing has to ask
// for the annotation itself.
func MembershipAfter(ctx Context, ev Event, state StateAtEvent) string {
	if ev.Type == "m.room.member" && ev.StateKey == ctx.UserID {
		if ev.Membership != "" {
			return ev.Membership
		}
		return "leave"
	}
	if state.UserMembership != "" {
		return state.UserMembership
	}
	return "leave"
}
