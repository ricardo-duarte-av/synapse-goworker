package app

import (
	"context"
	"slices"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// EventAccess is what EventHandler.get_event (handlers/events.py) says of
// one event for one user.
type EventAccess int

const (
	// EventMissing: no such event in that room (store.get_event with
	// check_room_id and allow_none), or of a room version Synapse does not
	// know, which it drops. get_event returns None.
	EventMissing EventAccess = iota
	// EventVisible: filter_and_transform_events_for_client keeps it
	// (pruned for an erased sender counts: it is still returned).
	EventVisible
	// EventForbidden: it filters the event out, and get_event raises
	// AuthError(403, "You don't have permission to access that event.").
	EventForbidden
)

// EventAccessFor is get_event(user, room_id, event_id) without building the
// event: whether the user may see it, peeking unless joined now. The
// receipts writer asks it of the event a receipt names, as
// received_client_receipt does.
func (s *Sync) EventAccessFor(ctx context.Context, userID, roomID, eventID string) (EventAccess, error) {
	ctx = store.WithRequestCache(ctx)
	ev, found, err := s.db.EventForClient(ctx, eventID)
	if err != nil {
		return 0, err
	}
	if !found || ev.RoomID != roomID || !clientevent.IsKnownRoomVersion(ev.RoomVersion) {
		return EventMissing, nil
	}
	m, err := s.db.LocalMembership(ctx, roomID, userID)
	if err != nil {
		return 0, err
	}
	msc4354 := s.cfg.Synapse.Current().Flag("msc4354_enabled", false)
	res, err := eventfilter.ForClient(ctx, s.db, roomID, userID,
		[]store.TimelineEvent{ev}, m.Membership != "join", time.Now().UnixMilli(), msc4354, nil)
	if err != nil {
		return 0, err
	}
	if len(res.Events) == 0 && !slices.Contains(res.Pruned, eventID) {
		return EventForbidden, nil
	}
	return EventVisible, nil
}
