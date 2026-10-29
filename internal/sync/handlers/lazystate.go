package handlers

import (
	"context"
	"sort"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lazyload"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// memberType is the state event type lazy loading is about.
const memberType = "m.room.member"

// timelineMembers is Synapse's `members_to_fetch`: the memberships a client
// needs in order to render the timeline it is being sent.
//
// Not every member of the room -- that is the whole point of lazy loading --
// but the sender of each timeline event, so the client can show a name and an
// avatar beside it.
//
// The two branches are not equivalent. Under MSC4222 the state block describes
// where the room ENDED UP, so a membership in the timeline says nothing about
// what the client should end on and the sender's membership is fetched
// regardless. Under the classic `state` block the client reconstructs state by
// layering the timeline over it, so a sender whose membership is already in the
// timeline will arrive there anyway and need not be sent twice.
func timelineMembers(messages []store.TimelineEvent, useStateAfter bool) map[string]bool {
	members := map[string]bool{}
	seenState := map[store.StateKey]bool{}
	for _, ev := range messages {
		if useStateAfter {
			members[ev.Sender] = true
		} else if !seenState[store.StateKey{Type: memberType, StateKey: ev.Sender}] {
			members[ev.Sender] = true
		}
		if ev.IsState {
			seenState[store.StateKey{Type: ev.Type, StateKey: ev.StateKey}] = true
		}
	}
	return members
}

// restrictToMembers applies StateFilter.from_lazy_load_member_list in place:
// every non-member state entry survives, and member entries only for the named
// users.
//
// Synapse pushes this down into the state query. Doing it after the fact is
// equivalent -- the filter selects rows, it does not change which state group
// is resolved -- and keeps the state resolver, which is the delicate part of
// this worker, untouched.
func restrictToMembers(state map[store.StateKey]string, members map[string]bool) {
	if members == nil {
		return
	}
	for k := range state {
		if k.Type == memberType && !members[k.StateKey] {
			delete(state, k)
		}
	}
}

// keepOnlyMembers is the opposite restriction: member entries for the named
// users and nothing else.
//
// Used on the non-gappy incremental path, where Synapse asks for
// `StateFilter.from_types((Member, m) for m in members_to_fetch)` -- just the
// memberships, because everything else the client needs is in the timeline.
func keepOnlyMembers(state map[store.StateKey]string, members map[string]bool) map[store.StateKey]string {
	out := map[store.StateKey]string{}
	for k, id := range state {
		if k.Type == memberType && members[k.StateKey] {
			out[k] = id
		}
	}
	return out
}

// dedupeLazyMembers removes members the client has already been sent, and
// records the ones it is about to be sent.
//
// Synapse's block at the end of compute_state_delta, and the reason a
// lazy-loading incremental sync is not reproducible from the database alone:
// the answer depends on what this process happened to send earlier. An initial
// sync clears the cache first, which is what makes that case deterministic --
// the client has "had amnesia", in Synapse's words, so the server must forget
// too.
//
// With `include_redundant_members` the whole step is skipped, cache population
// included. That is Synapse's behaviour and not an oversight to tidy up: a
// client asking for redundant members is asking not to be deduplicated, and
// recording what it was sent would start deduplicating it as soon as it
// stopped asking.
//
// A key in changed -- its state changed since the client's last sync -- is
// sent even if the cache says it was sent already: the client may be retrying
// a sync whose answer it never received (#20289).
func dedupeLazyMembers(d Deps, f *filter.Collection, verdictUserID, deviceID string,
	stateIDs map[store.StateKey]string, changed map[store.StateKey]bool,
	messages []store.TimelineEvent, initial bool) {

	if !f.LazyLoadMembers() || f.IncludeRedundantMembers() {
		return
	}
	cache := d.LazyLoad.For(lazyload.Key{UserID: verdictUserID, DeviceID: deviceID})
	if initial {
		cache.Clear()
	} else {
		for k, id := range stateIDs {
			if k.Type == memberType && !changed[k] && cache.Sent(k.StateKey) == id {
				delete(stateIDs, k)
			}
		}
	}
	for k, id := range stateIDs {
		if k.Type == memberType {
			cache.Record(k.StateKey, id)
		}
	}
	// The timeline's own member events count as sent: the client will see them
	// there, so repeating them in a later state block would be waste.
	for _, ev := range messages {
		if ev.IsState && ev.Type == memberType {
			cache.Record(ev.StateKey, ev.EventID)
		}
	}
}

// roomSummary builds the `summary` block: member counts, and the heroes a
// client uses to name a room that has no name of its own.
//
// Returns nil when the room has no events at all up to the sync point, which
// Synapse reports by omitting the key rather than sending an empty object.
//
// The last argument is the state block, which this may ADD TO: a hero whose
// membership the client has not been sent is useless, since the client would
// have a user ID to display and no profile to display it with.
func roomSummary(ctx context.Context, d Deps, roomID, userID, deviceID string,
	f *filter.Collection, stateIDs map[store.StateKey]string,
	messages []store.TimelineEvent, endKey streamtoken.RoomKey) (map[string]any, error) {

	// Synapse asks for the last event in the room up to the sync point and
	// gives up if there is none, so a room with no visible history has no
	// summary key at all rather than an empty one.
	last, err := d.Store.LastEventBefore(ctx, roomID, endKey)
	if err != nil {
		return nil, err
	}
	if last == "" {
		return nil, nil
	}

	details, err := d.Store.RoomSummary(ctx, roomID)
	if err != nil {
		return nil, err
	}
	summary := map[string]any{
		"m.joined_member_count":  details.Counts["join"],
		"m.invited_member_count": details.Counts["invite"],
	}

	// A room that can name itself needs no heroes, and Synapse does not even
	// compute them. An empty name or alias does not count: the check is on the
	// content being truthy, not on the event existing.
	named, err := roomHasName(ctx, d, roomID, endKey)
	if err != nil {
		return nil, err
	}
	if named {
		return summary, nil
	}

	heroes := details.Heroes(userID)
	summary["m.heroes"] = heroes

	if !f.LazyLoadMembers() {
		return summary, nil
	}

	memberIDs := map[string]string{}
	for _, m := range details.Members {
		memberIDs[m.UserID] = m.EventID
	}

	// A hero the client already knows about needs nothing added: either their
	// membership is in the state block we just built, or it is in the
	// timeline, or we have sent it before and the cache remembers.
	existing := map[string]bool{}
	for k := range stateIDs {
		if k.Type == memberType {
			existing[k.StateKey] = true
		}
	}
	for _, ev := range messages {
		if ev.IsState && ev.Type == memberType {
			existing[ev.StateKey] = true
		}
	}

	cache := d.LazyLoad.For(lazyload.Key{UserID: userID, DeviceID: deviceID})
	for _, hero := range heroes {
		id := memberIDs[hero]
		if id == "" || existing[hero] || cache.Sent(hero) == id {
			continue
		}
		stateIDs[store.StateKey{Type: memberType, StateKey: hero}] = id
		cache.Record(hero, id)
	}
	return summary, nil
}

// wantSummary decides whether to compute a summary at all.
//
// Synapse only sends one when lazy loading is on -- without it the client has
// every membership and can name the room itself -- and then only when something
// might have changed the answer: a membership in the timeline, a membership in
// the state block of a gappy sync, or an initial sync.
func wantSummary(f *filter.Collection, messages []store.TimelineEvent,
	stateIDs map[store.StateKey]string, limited, initial bool) bool {

	if !f.LazyLoadMembers() {
		return false
	}
	if initial {
		return true
	}
	for _, ev := range messages {
		if ev.Type == memberType {
			return true
		}
	}
	if limited {
		for k := range stateIDs {
			if k.Type == memberType {
				return true
			}
		}
	}
	return false
}

// roomHasName reports whether a room names itself, by m.room.name or by
// m.room.canonical_alias.
func roomHasName(ctx context.Context, d Deps, roomID string,
	endKey streamtoken.RoomKey) (bool, error) {

	// The state at the sync point, not the room's current state: a room
	// renamed after the token was minted must still be named as the client's
	// window saw it.
	//
	// Two state keys, asked for by name. This used to resolve the room's
	// ENTIRE state map to answer a yes/no question about two of its entries,
	// which on a large room is six figures of rows for one boolean -- and a
	// summary is computed for every lazy-loading room in an initial sync.
	state, err := d.Store.FilteredStateAt(ctx, roomID, endKey, []store.StateKey{
		{Type: "m.room.name"},
		{Type: "m.room.canonical_alias"},
	})
	if err != nil {
		return false, err
	}
	var want []string
	nameID := state[store.StateKey{Type: "m.room.name"}]
	aliasID := state[store.StateKey{Type: "m.room.canonical_alias"}]
	for _, id := range []string{nameID, aliasID} {
		if id != "" {
			want = append(want, id)
		}
	}
	if len(want) == 0 {
		return false, nil
	}
	events, err := d.Store.EventsByID(ctx, want, "")
	if err != nil {
		return false, err
	}
	if ev, ok := events[nameID]; ok && gjson.GetBytes(ev.JSON, "content.name").String() != "" {
		return true, nil
	}
	if ev, ok := events[aliasID]; ok && gjson.GetBytes(ev.JSON, "content.alias").String() != "" {
		return true, nil
	}
	return false, nil
}

// memberList turns the lazy-load member set into the sorted list the state
// queries take.
//
// Sorted so the same set produces the same query text and the same plan, and
// so a cache keyed on it would be stable. Nil in, empty out: a room whose
// timeline names nobody still wants all of the non-member state.
func memberList(members map[string]bool) []string {
	out := make([]string, 0, len(members))
	for user := range members {
		out = append(out, user)
	}
	sort.Strings(out)
	return out
}
