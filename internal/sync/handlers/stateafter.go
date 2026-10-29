package handlers

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// incrementalStateAfter is MSC4222's state block on an incremental sync:
// what the room's state BECAME over (since, end], from the state delta
// stream, rather than the state a client must apply before the timeline.
// _compute_state_delta_for_incremental_sync's use_state_after branch
// (handlers/sync.py, v1.164.1).
//
// end is the end of this room's batch: now for a joined room, the user's
// leave for a left one (joined false). members are the memberships a
// lazy-loading client needs for the timeline, nil without lazy loading.
//
// changed is the keys whose state changed in the window, which the
// lazy-loaded members cache must not hold back (#20289); the rest are
// memberships fetched for timeline senders, which the client may have.
func incrementalStateAfter(ctx context.Context, d Deps, roomID, userID string,
	since streamtoken.Token, end streamtoken.RoomKey, messages []store.TimelineEvent,
	members map[string]bool, joined bool) (ids map[store.StateKey]string, changed map[store.StateKey]bool, err error) {

	ids = map[store.StateKey]string{}
	changed = map[store.StateKey]bool{}

	if len(members) > 0 {
		// Lazy loading: the memberships of the timeline's senders, which the
		// delta stream carries only if they changed. A joined user reads
		// them from current state; one who left reads them as of the leave,
		// since current state may hold later memberships (#20169).
		var state map[store.StateKey]string
		if joined {
			state, err = d.Store.LazyCurrentStateIDs(ctx, roomID, memberList(members))
		} else {
			state, err = d.Store.LazyStateIDsAt(ctx, roomID, end, memberList(members))
		}
		if err != nil {
			return nil, nil, err
		}
		for k, id := range keepOnlyMembers(state, members) {
			ids[k] = id
		}
	}

	// Lazy loading does not narrow the deltas: Synapse returns every state
	// change over the window, because a client that missed a membership
	// change cannot ask for it later (riot-web#7211).
	sinceKey := since.Room
	deltas, err := d.Store.RoomStateDeltas(ctx, roomID, &sinceKey, &end)
	if err != nil {
		return nil, nil, err
	}

	// A delta with no event is a key removed from current state, for one of
	// two reasons. A state reset: /sync cannot tell a client a key went, so
	// nothing is sent. Or the server left the room: it then copies the state
	// from BEFORE the batch holding the leave into the delta stream as
	// removals, and no delta refers to that batch's own events. So a key that
	// batch changed has only a stale delta, or none if it is new; the batch
	// ends the timeline, so those keys are read from the state at end (#20081,
	// #20314).
	timeline := timelineState(messages)
	own := store.StateKey{Type: memberType, StateKey: userID}
	lookUp := map[store.StateKey]bool{}
	for _, delta := range deltas {
		if delta.EventID == "" {
			// The user's own membership too, in case a filter took their
			// leave out of the timeline.
			if _, inTimeline := timeline[delta.Key]; inTimeline || delta.Key == own {
				lookUp[delta.Key] = true
			}
			continue
		}
		// Deltas are in stream order, so the last one for a key wins.
		ids[delta.Key] = delta.EventID
		changed[delta.Key] = true
		delete(lookUp, delta.Key)
	}
	if lookUp[own] {
		// The server left in this window. It wrote deltas only for keys that
		// existed before the leaving batch, so the keys that batch added have
		// none: look up every key in the timeline.
		for k := range timeline {
			lookUp[k] = true
		}
	}
	if len(lookUp) > 0 {
		atEnd, err := d.Store.StateIDsAt(ctx, roomID, end)
		if err != nil {
			return nil, nil, err
		}
		// This replaces any earlier delta for the key, a display name change
		// before the leave say.
		for k := range lookUp {
			if id, ok := atEnd[k]; ok {
				ids[k] = id
				changed[k] = true
			}
		}
	}
	return ids, changed, nil
}

// initialStateAfter is MSC4222's state block on a full sync
// (_compute_state_delta_for_full_sync, v1.164.1). For a joined room it is
// the current state wound back to end by the deltas after it, positioned at
// their events; for a left room, the state at end.
func initialStateAfter(ctx context.Context, d Deps, roomID string, end streamtoken.RoomKey,
	members map[string]bool, joined bool) (map[store.StateKey]string, error) {

	if !joined {
		if members != nil {
			state, err := d.Store.LazyStateIDsAt(ctx, roomID, end, memberList(members))
			if err != nil {
				return nil, err
			}
			restrictToMembers(state, members)
			return state, nil
		}
		return d.Store.StateIDsAt(ctx, roomID, end)
	}

	var state map[store.StateKey]string
	var err error
	if members != nil {
		state, err = d.Store.LazyCurrentStateIDs(ctx, roomID, memberList(members))
	} else {
		state, err = d.Store.CurrentStateIDs(ctx, roomID)
	}
	if err != nil {
		return nil, err
	}
	deltas, err := d.Store.RoomStateDeltas(ctx, roomID, &end, nil)
	if err != nil {
		return nil, err
	}
	// Undo what came after end, newest first. Synapse applies the deltas to
	// every key, lazy-loaded or not, and so does this.
	for i := len(deltas) - 1; i >= 0; i-- {
		delta := deltas[i]
		if delta.PrevEventID != "" {
			state[delta.Key] = delta.PrevEventID
		} else {
			delete(state, delta.Key)
		}
	}
	return state, nil
}

// timelineState is timeline_state: the last event for each state key among
// the timeline's state events.
func timelineState(messages []store.TimelineEvent) map[store.StateKey]string {
	out := map[store.StateKey]string{}
	for _, ev := range messages {
		if ev.IsState {
			out[store.StateKey{Type: ev.Type, StateKey: ev.StateKey}] = ev.EventID
		}
	}
	return out
}
