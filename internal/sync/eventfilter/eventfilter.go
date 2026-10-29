// Package eventfilter applies the visibility rules to a timeline, fetching what
// the decision needs.
//
// Separate from internal/visibility, which holds the rules themselves and says
// in its own doc comment that being free of database access is what keeps it
// testable. This is the other half: the queries, the batching, and the
// translation from a stored event into the shape the rules take. Synapse keeps
// both in synapse/visibility.py; we split them so the rules can be tested
// without a database, and this can be shared by both sync endpoints.
//
// It is shared deliberately. Sliding sync originally did not filter at all,
// which was invisible in testing because every room compared happened to have
// `shared` history visibility. One implementation, used by both, is the only
// arrangement in which that cannot happen again.
package eventfilter

import (
	"context"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/visibility"
)

// Result is one filtered timeline.
type Result struct {
	// Events are the events the caller may see, in the order given.
	Events []store.TimelineEvent
	// Memberships is the caller's membership at each kept event, positionally
	// aligned with Events. This is MSC4115's `unsigned.membership`, and it
	// falls out of the visibility decision -- which is why an endpoint that
	// skips filtering also silently loses the field.
	Memberships []string
	// Pruned names events withheld because their sender has been erased. See
	// the comment at the drop site.
	Pruned []string
	// AdminMetadata is true when the caller is a server admin who asked to see
	// soft-failed events, and so should also be told which events those are.
	// Passed to clientevent.Config.IncludeAdminMetadata.
	AdminMetadata bool
}

// historyVisKey and memberKey are the two state entries a visibility decision
// needs. Resolving the whole state map to get them would mean reading every
// state event in the room.
func visibilityKeys(userID string) []store.StateKey {
	return []store.StateKey{
		{Type: "m.room.history_visibility", StateKey: ""},
		{Type: "m.room.member", StateKey: userID},
	}
}

// ForClient applies visibility to a timeline, returning the events the caller
// may see with their MSC4115 membership attached.
//
// Mirrors filter_events_for_client: the room state is resolved *at each event*
// and the decision is made per event. Everything the decision needs beyond the
// event itself is gathered here in bulk -- one query for the state groups, one
// per distinct group for the state, one each for the ignore list, erased
// senders and the retention policy -- rather than per event.
//
// alwaysInclude is Synapse's `always_include_ids`: events that bypass the
// decision entirely. Pass nil when there are none.
func ForClient(ctx context.Context, db *store.Store, roomID, userID string,
	events []store.TimelineEvent, isPeeking bool, nowMS int64, msc4354 bool,
	alwaysInclude map[string]bool) (Result, error) {

	if len(events) == 0 {
		return Result{Events: events}, nil
	}

	eventIDs := make([]string, len(events))
	senders := make([]string, 0, len(events))
	seen := map[string]bool{}
	for i, ev := range events {
		eventIDs[i] = ev.EventID
		if !seen[ev.Sender] {
			seen[ev.Sender] = true
			senders = append(senders, ev.Sender)
		}
	}

	groups, err := db.StateGroupsForEvents(ctx, eventIDs)
	if err != nil {
		return Result{}, err
	}

	keys := visibilityKeys(userID)
	// Several events usually share a state group -- consecutive messages
	// change no state -- so resolving per group rather than per event is the
	// difference between one walk and a dozen.
	stateByGroup := make(map[int64]map[store.StateKey]store.StateEntry, len(groups))
	for _, group := range groups {
		if _, done := stateByGroup[group]; done {
			continue
		}
		state, err := db.FilteredStateForGroup(ctx, group, keys)
		if err != nil {
			return Result{}, err
		}
		stateByGroup[group] = state
	}

	extra, err := db.VisibilityExtras(ctx, roomID, userID, senders)
	if err != nil {
		return Result{}, err
	}

	vctx := visibility.Context{
		UserID:                 userID,
		IsPeeking:              isPeeking,
		IgnoredSenders:         extra.IgnoredSenders,
		ErasedSenders:          extra.ErasedSenders,
		RetentionMaxLifetimeMS: extra.RetentionMaxLifetimeMS,
		NowMS:                  nowMS,
		ReturnSoftFailed:       extra.ReturnSoftFailed,
		ReturnPolicySpammy:     extra.ReturnPolicySpammy,
		MSC4354:                msc4354,
	}

	// When each still-sticky event stops being sticky. Synapse loads a
	// redacted event pruned, without `msc4354_sticky`, so a redacted event
	// is not sticky; sticky events are few, so only theirs are looked up.
	stickyUntil := map[string]int64{}
	if msc4354 && !isPeeking {
		var candidates []string
		for _, ev := range events {
			if until := clientevent.StickyUntil(ev.JSON, ev.InternalMetadata, ev.ReceivedTS); until > nowMS {
				stickyUntil[ev.EventID] = until
				candidates = append(candidates, ev.EventID)
			}
		}
		if len(candidates) > 0 {
			redacted, err := db.Redactions(ctx, candidates)
			if err != nil {
				return Result{}, err
			}
			for id := range redacted {
				delete(stickyUntil, id)
			}
		}
	}

	kept := make([]store.TimelineEvent, 0, len(events))
	memberships := make([]string, 0, len(events))
	var pruned []string
	for _, ev := range events {
		state := visibility.StateAtEvent{}
		if group, ok := groups[ev.EventID]; ok {
			state.Present = true
			resolved := stateByGroup[group]
			state.HistoryVisibility = resolved[store.StateKey{
				Type: "m.room.history_visibility"}].HistoryVisibility
			state.UserMembership = resolved[store.StateKey{
				Type: "m.room.member", StateKey: userID}].Membership
		}

		vev := timelineToVisibilityEvent(ev)
		vev.StickyUntil = stickyUntil[ev.EventID]
		verdict := visibility.Check(vctx, vev, state)
		if alwaysInclude[ev.EventID] {
			// Synapse's always_include_ids returns early from
			// _check_client_allowed_to_see_event, and the membership is
			// annotated afterwards, in `allowed` -- so a rescued event still
			// carries unsigned.membership. Check has no membership to report
			// for an event it rejected, so it is computed here instead.
			//
			// This is not a corner: a room with history visibility `invited`
			// and a leave whose stored JSON has no prev_content (which is most
			// of them -- Synapse does not persist prev_content) fails the
			// membership check and reaches the client only by this path.
			if !verdict.Visible {
				verdict.Membership = visibility.MembershipAfter(vctx, vev, state)
			}
			verdict.Visible = true
			verdict.Pruned = false
		}
		if !verdict.Visible {
			continue
		}
		if verdict.Pruned {
			// The sender has been erased and the caller was not joined at the
			// time, so Synapse serves a redacted copy. Redaction is per room
			// version and belongs with the event machinery, not here; until it
			// exists, dropping the event is the safe direction -- it withholds
			// content rather than publishing content that should have been
			// stripped.
			pruned = append(pruned, ev.EventID)
			continue
		}
		kept = append(kept, ev)
		memberships = append(memberships, verdict.Membership)
	}
	return Result{
		Events: kept, Memberships: memberships, Pruned: pruned,
		AdminMetadata: extra.ReturnSoftFailed || extra.ReturnPolicySpammy,
	}, nil
}

func timelineToVisibilityEvent(ev store.TimelineEvent) visibility.Event {
	out := visibility.Event{
		EventID:            ev.EventID,
		Type:               ev.Type,
		StateKey:           ev.StateKey,
		IsState:            ev.IsState,
		Sender:             ev.Sender,
		OriginServerTS:     gjson.GetBytes(ev.JSON, "origin_server_ts").Int(),
		SoftFailed:         softFailed(ev.InternalMetadata),
		PolicyServerSpammy: policyServerSpammy(ev.InternalMetadata),
	}
	if ev.Type == "m.room.member" {
		out.Membership = gjson.GetBytes(ev.JSON, "content.membership").String()
		out.PrevMembership = gjson.GetBytes(ev.JSON, "unsigned.prev_content.membership").String()
	}
	if ev.Type == "m.room.history_visibility" {
		out.PrevHistoryVisibility = gjson.GetBytes(ev.JSON,
			"unsigned.prev_content.history_visibility").String()
	}
	return out
}

// softFailed reads internal_metadata.soft_failed.
//
// Synapse drops soft-failed events from client responses before any visibility
// check runs. Only a server admin who has explicitly asked for them sees them.
func softFailed(internalMetadata []byte) bool {
	if len(internalMetadata) == 0 {
		return false
	}
	return gjson.GetBytes(internalMetadata, "soft_failed").Bool()
}

// policyServerSpammy reads internal_metadata.policy_server_spammy.
//
// Distinguishes an event soft-failed by a policy server from one soft-failed by
// auth, which a server admin can ask to see separately.
func policyServerSpammy(internalMetadata []byte) bool {
	if len(internalMetadata) == 0 {
		return false
	}
	return gjson.GetBytes(internalMetadata, "policy_server_spammy").Bool()
}
