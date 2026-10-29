package handlers

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// stickyByRoom loads the MSC4354 sticky events for a set of joined rooms and
// winds the now token's sticky field to what it actually returned.
//
// Mirrors sticky_events_by_room, including where it happens: BEFORE any room
// entry is built. Synapse reassigns `sync_result_builder.now_token` there, so
// the wound-back position reaches every prev_batch in the response as well as
// next_batch. Doing it afterwards would leave the two disagreeing.
//
// Returns nil when MSC4354 is off, in which case no room carries the section.
func stickyByRoom(ctx context.Context, d Deps, joinedIDs []string, from int64,
	now *streamtoken.Token, timeNow int64) (map[string][]string, error) {

	if !d.MSC4354Enabled || len(joinedIDs) == 0 {
		return nil, nil
	}
	to, byRoom, err := d.Store.StickyEvents(ctx, joinedIDs, from, now.StickyEvents,
		timeNow, store.StickyMaxEventsInSync)
	if err != nil {
		return nil, err
	}
	now.StickyEvents = to
	return byRoom, nil
}

// stickySection builds a room's `msc4354_sticky` block, or nil if there is
// nothing to say, and the senders of the events in it: a lazy-loading client
// is sent their memberships too (#20362). It runs before the state block is
// computed, as in Synapse.
//
// Two rules from MSC4354, and both are the opposite of what the rest of a room
// entry does:
//
//   - An event already in the timeline is REMOVED from this section. The client
//     learns about it either way, and sticky events are spammable, so sending
//     it twice is the one thing worth avoiding. This is why the section is
//     invisible until an event ages out of the timeline -- or until a filter
//     excludes it, which is how the omission was noticed at all.
//   - History visibility is NOT applied to a still-sticky event: "Any joined
//     user is authorised to see sticky events for the duration they remain
//     sticky". Since Synapse v1.164 the visibility check itself grants that,
//     on every endpoint (#20295), so the section takes the ordinary pass,
//     which is also what stamps `unsigned.membership` (MSC4115) onto each
//     event.
func stickySection(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	stickyIDs []string, timeline []store.TimelineEvent, timeNow int64,
	cfg clientevent.Config) (map[string]any, []string, error) {

	if len(stickyIDs) == 0 {
		return nil, nil, nil
	}
	inTimeline := make(map[string]bool, len(timeline))
	for _, ev := range timeline {
		inTimeline[ev.EventID] = true
	}
	// Stream order is preserved: the client is meant to apply these in the
	// order the server saw them.
	wanted := make([]string, 0, len(stickyIDs))
	for _, id := range stickyIDs {
		if !inTimeline[id] {
			wanted = append(wanted, id)
		}
	}
	if len(wanted) == 0 {
		return nil, nil, nil
	}

	events, err := d.Store.EventsByID(ctx, wanted, room.RoomVersion)
	if err != nil {
		return nil, nil, err
	}
	loaded := make([]store.TimelineEvent, 0, len(wanted))
	for _, id := range wanted {
		ev, ok := events[id]
		if !ok {
			// The sticky row outlived its event, which a purge can do.
			continue
		}
		loaded = append(loaded, store.TimelineEvent{
			Stored: ev.Stored, Sender: ev.Sender, StateKey: ev.StateKey,
		})
	}
	// isPeeking is false: only a joined room reaches here.
	visible, memberships, err := filterVisible(ctx, d, room.RoomID, userID,
		loaded, false, timeNow)
	if err != nil {
		return nil, nil, err
	}
	senders := make([]string, 0, len(visible))
	for _, ev := range visible {
		senders = append(senders, ev.Sender)
	}

	redactions, err := d.Store.Redactions(ctx, wanted)
	if err != nil {
		return nil, nil, err
	}

	out := make([]json.RawMessage, 0, len(visible))
	for i, ev := range visible {
		stored := ev.Stored
		stored.Membership = memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, body)
	}
	if len(out) == 0 {
		return nil, senders, nil
	}
	return map[string]any{"events": out}, senders, nil
}
