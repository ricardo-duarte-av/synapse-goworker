package handlers

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// maxTimelineRepeats is Synapse's `max_repeat`: how many times a room's
// timeline may be re-paginated to make up for events the filters removed.
//
// The cap matters. A filter matching nothing in a large room would otherwise
// walk the room's entire history looking for a tenth event that does not
// exist, once per room, on every sync.
const maxTimelineRepeats = 5

// loadFilteredRecents builds a room's timeline.
//
// Port of Synapse's _load_filtered_recents, and it serves every path: an
// initial sync (no `since`, no events supplied, walk backwards from now), an
// incremental one (a window of events already loaded, a `since` to stop at),
// and an archived room (walk back from the leave event).
//
// The arguments mirror Synapse's, because the differences between the paths
// are entirely in what is passed:
//
//   - `upto` is Synapse's `upto_token`: the token a client should paginate
//     from when the timeline is NOT trimmed. For a joined room in an
//     incremental sync it is the start of the chunk that was loaded, which is
//     the `since` token itself for a room whose events all fit; for an initial
//     sync it is the now token; for an archived room, the leave token.
//   - `sinceKey` is the lower bound of the walk, nil on an initial sync. Its
//     presence also chooses the ordering: stream for updates, topological for
//     history.
//   - `potential` is the window of events the caller already loaded, with
//     `hasPotential` distinguishing "loaded, and there were none" from "not
//     loaded at all". That distinction decides `limited` and cannot be
//     recovered from an empty slice.
//
// Two things here are easy to leave out and wrong to.
//
// The first is the loop. Events are thinned twice -- once by the client's
// filter, once by history visibility -- and either pass can leave the timeline
// short of the limit, so Synapse goes back for more, up to five times. Without
// it, a filter selecting one event type returns a nearly empty timeline for a
// busy room and calls it limited, sending the client round the same filter
// again.
//
// The second is that `limited` is decided BEFORE any filtering, from how many
// events the window held against the TIMELINE limit -- not from how many
// survived, and not against the load limit. A timeline that ends up short
// because the filter removed things is not limited; the client has everything
// there was.
func loadFilteredRecents(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	now, upto streamtoken.Token, sinceKey *streamtoken.RoomKey,
	potential []store.TimelineEvent, hasPotential, newlyJoined bool,
	timeNow int64, f *filter.Collection, gaps *gapSet) (
	[]store.TimelineEvent, []string, streamtoken.Token, bool, error) {

	timelineLimit := f.TimelineLimit()

	// Not loaded at all means there is nothing to be sure about; a newly
	// joined room is limited by definition, because the client has none of its
	// history. Otherwise: did the window hold more than we may return?
	limited := !hasPotential || newlyJoined || timelineLimit < len(potential)

	// A hole in the room's history changes the question. The events after the
	// gap are not a continuation of the ones before it, so Synapse throws away
	// the window it was given, walks back from the sync point to the gap
	// instead, and marks the timeline limited -- leaving the client to
	// paginate across the hole rather than handing it two disjoint runs of
	// events as though they were one.
	//
	// The set is prefetched for every room at once where the caller can do
	// that -- an incremental sync asks about all of them with the same bounds
	// -- and looked up per room otherwise.
	gapStream, gap, err := gaps.lookup(ctx, d, room.RoomID, sinceKey, now.Room)
	if err != nil {
		return nil, nil, upto, false, err
	}
	if gap {
		potential, hasPotential = nil, false
		upto = now
		limited = true
		if sinceKey != nil {
			gapKey := streamtoken.Live(gapStream)
			sinceKey = &gapKey
		}
	}

	// A filter that can match nothing gets an empty, explicitly unlimited
	// timeline: telling the client to paginate would be pointless, because the
	// same filter applies to /messages. Synapse reaches this by way of
	// block_all_timeline taking the not-limited exit with everything filtered
	// out.
	if f.BlocksAllRoomTimeline() || f.BlocksAllRooms() || timelineLimit <= 0 {
		return nil, nil, upto, false, nil
	}

	messages, memberships, err := filterAndVisible(ctx, d, room.RoomID, userID,
		potential, timeNow, f)
	if err != nil {
		return nil, nil, upto, false, err
	}

	if !limited {
		// Nothing was withheld, so the client can paginate from wherever the
		// window began -- or from just before the first event it is being
		// given, when there is one.
		prev := upto
		if len(messages) > 0 {
			prev = upto.WithRoomKey(streamtoken.Live(messages[0].StreamOrdering - 1))
		}
		return messages, memberships, prev, false, nil
	}

	loadLimit := timelineLimit * 2
	if loadLimit < 10 {
		loadLimit = 10
	}

	// roomKey is only ever moved by the trim below. Synapse keeps it at the
	// token it was given and assigns the pagination cursor to a DIFFERENT
	// variable, so an untrimmed timeline reports a prev_batch equal to where
	// the window began even though pagination walked past it. Following the
	// cursor instead looks more correct and is measurably not what Synapse
	// returns.
	roomKey := upto.Room
	from := upto.Room

	for repeat := 0; repeat < maxTimelineRepeats && limited && len(messages) < timelineLimit; repeat++ {
		var (
			loaded []store.TimelineEvent
			next   streamtoken.RoomKey
			more   bool
			err    error
		)
		if sinceKey != nil {
			loaded, next, more, err = d.Store.PaginateBackwardsStream(ctx, room.RoomID,
				room.RoomVersion, loadLimit, from, *sinceKey)
		} else {
			loaded, next, more, err = d.Store.PaginateBackwards(ctx, room.RoomID,
				room.RoomVersion, loadLimit, from)
		}
		if err != nil {
			return nil, nil, upto, false, err
		}
		from, limited = next, more

		visible, ms, err := filterAndVisible(ctx, d, room.RoomID, userID, loaded, timeNow, f)
		if err != nil {
			return nil, nil, upto, false, err
		}

		// Each pass walks further back, so its events are OLDER than what we
		// already have and belong in front of them.
		messages = append(visible, messages...)
		memberships = append(ms, memberships...)
	}

	// Trim to the requested limit, keeping the NEWEST events. This is the only
	// thing that moves the prev_batch token: what was cut off is exactly what
	// the client must paginate for.
	if len(messages) > timelineLimit {
		limited = true
		messages = messages[len(messages)-timelineLimit:]
		memberships = memberships[len(memberships)-timelineLimit:]
		roomKey = streamtoken.Live(messages[0].StreamOrdering - 1)
	}

	// A gap and a newly joined room are limited whatever the pagination said.
	return messages, memberships, upto.WithRoomKey(roomKey), limited || newlyJoined || gap, nil
}

// filterAndVisible applies the client's timeline filter and then history
// visibility, in that order.
//
// The order is load-bearing for the always-include set: it is computed from
// what SURVIVED the client's filter, not from what was loaded. An event the
// client filtered out is gone, and re-admitting it because it happens to be
// current state would override the client's own request.
func filterAndVisible(ctx context.Context, d Deps, roomID, userID string,
	events []store.TimelineEvent, timeNow int64, f *filter.Collection) (
	[]store.TimelineEvent, []string, error) {

	if len(events) == 0 {
		return nil, nil, nil
	}
	events = filterTimeline(f, roomID, events)
	events, err := applyRelationFilter(ctx, d, f.TimelineFilter(), events)
	if err != nil {
		return nil, nil, err
	}
	alwaysInclude, err := stateEventIDsInCurrentState(ctx, d, events)
	if err != nil {
		return nil, nil, err
	}
	return filterVisibleAlways(ctx, d, roomID, userID, events, false, timeNow, alwaysInclude)
}

// filterTimeline applies the client's timeline filter, room filter included.
func filterTimeline(f *filter.Collection, roomID string, events []store.TimelineEvent) []store.TimelineEvent {
	out := make([]store.TimelineEvent, 0, len(events))
	for _, ev := range events {
		if f.CheckRoomTimeline(timelineFilterEvent(roomID, ev)) {
			out = append(out, ev)
		}
	}
	return out
}

// filterAccountDataEntries applies the room account data filter.
func filterAccountDataEntries(f *filter.Collection, roomID string,
	entries []store.AccountDataEntry) []store.AccountDataEntry {

	out := make([]store.AccountDataEntry, 0, len(entries))
	for _, e := range entries {
		if f.CheckRoomAccountData(accountDataFilterEvent(roomID, e)) {
			out = append(out, e)
		}
	}
	return out
}

// filterAccountDataEvents applies the GLOBAL account data filter to
// already-rendered events.
//
// The global section is filtered after rendering because m.push_rules is
// synthesised rather than stored: there is no AccountDataEntry to filter.
func filterAccountDataEvents(f *filter.Collection, roomID string,
	events []json.RawMessage) []json.RawMessage {

	out := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		if f.CheckGlobalAccountData(ephemeralFilterEvent(roomID, e)) {
			out = append(out, e)
		}
	}
	return out
}

// filterEphemeral applies the ephemeral filter to rendered receipts and typing.
func filterEphemeral(f *filter.Collection, roomID string, events []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		if f.CheckRoomEphemeral(ephemeralFilterEvent(roomID, e)) {
			out = append(out, e)
		}
	}
	return out
}

// filterPresence applies the presence filter.
func filterPresence(f *filter.Collection, states []store.PresenceState) []store.PresenceState {
	out := make([]store.PresenceState, 0, len(states))
	for _, p := range states {
		if f.CheckPresence(presenceFilterEvent(p.UserID)) {
			out = append(out, p)
		}
	}
	return out
}

// stripRoomIDs removes room_id from every ephemeral event.
//
// Applied after filtering, not before: the spec says a client must not receive
// room_id on a receipt or typing notification nested under its room, but the
// filter is entitled to see it -- Synapse builds the dict with room_id, filters
// it, and strips the key on the way into the response.
func stripRoomIDs(events []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		out = append(out, stripRoomID(e))
	}
	return out
}

// filterStateBlock applies the state filter to an already-loaded state block,
// returning the event IDs that survive in their original order.
func filterStateBlock(f *filter.Collection, roomID string, ids []string,
	loaded map[string]store.StateEvent) []string {

	out := make([]string, 0, len(ids))
	for _, id := range ids {
		ev, ok := loaded[id]
		if !ok {
			// Not loaded, so it cannot be rendered either. Dropping it here
			// keeps the two loops over this slice in agreement.
			continue
		}
		if f.CheckRoomState(stateFilterEvent(roomID, ev)) {
			out = append(out, id)
		}
	}
	return out
}

// gapSet is a prefetched answer to "does this room have a history gap in the
// window", for a set of rooms sharing one pair of bounds.
//
// A nil gapSet is valid and means "ask the database per room", which is what
// the paths that build a single room's entry do.
type gapSet struct {
	byRoom map[string]int64
	// Bounds are held as their token strings because a RoomKey carries a
	// slice of per-writer positions and so cannot be compared with ==. The
	// string form is canonical for a parsed key, which is exactly the
	// property needed here.
	hasFrom bool
	from    string
	to      string
}

func newGapSet(byRoom map[string]int64, from *streamtoken.RoomKey, to streamtoken.RoomKey) *gapSet {
	g := &gapSet{byRoom: byRoom, to: to.String()}
	if from != nil {
		g.hasFrom, g.from = true, from.String()
	}
	return g
}

// lookup answers from the prefetch when the bounds match the ones it was built
// with, and falls back to a query otherwise.
//
// The bounds check is not defensive noise: the prefetch is built with the
// caller's `since`, and a newly joined room is loaded with different bounds in
// the same loop. Serving one from the other's answer would report a gap in the
// wrong place, which a client sees as history it never gets told to paginate
// for.
func (g *gapSet) lookup(ctx context.Context, d Deps, roomID string,
	from *streamtoken.RoomKey, to streamtoken.RoomKey) (int64, bool, error) {

	if g == nil || !g.covers(from, to) {
		return d.Store.TimelineGap(ctx, roomID, from, to)
	}
	stream, ok := g.byRoom[roomID]
	return stream, ok, nil
}

func (g *gapSet) covers(from *streamtoken.RoomKey, to streamtoken.RoomKey) bool {
	if g.to != to.String() {
		return false
	}
	if from == nil {
		return !g.hasFrom
	}
	return g.hasFrom && g.from == from.String()
}
