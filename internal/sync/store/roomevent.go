package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// What GET /rooms/{id}/event/{id} reads (handlers/events.py get_event).

// EventForClient is get_event(event_id, allow_rejected=False,
// allow_none=True): the event, unless it is not stored or was rejected.
// Its RoomVersion is rooms.room_version, empty when the room has no row.
// _fetch_event_rows joins events, event_json, rooms and rejections the
// same way; the room_id check is the caller's.
func (s *Store) EventForClient(ctx context.Context, eventID string) (TimelineEvent, bool, error) {
	const q = `
		SELECT e.event_id, e.room_id, e.type, e.sender, e.stream_ordering,
		       e.topological_ordering, COALESCE(e.instance_name, ''),
		       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
		       ej.json, ej.internal_metadata, COALESCE(e.received_ts, 0),
		       COALESCE(r.room_version, '')
		  FROM events e JOIN event_json ej USING (event_id)
		  LEFT JOIN rooms r ON r.room_id = e.room_id
		  LEFT JOIN rejections rej USING (event_id)
		 WHERE e.event_id = $1 AND rej.event_id IS NULL`
	var ev TimelineEvent
	err := s.queryRow(ctx, "EventForClient", q, eventID).Scan(
		&ev.EventID, &ev.RoomID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
		&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
		&ev.JSON, &ev.InternalMetadata, &ev.ReceivedTS, &ev.RoomVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return TimelineEvent{}, false, nil
	}
	if err != nil {
		return TimelineEvent{}, false, fmt.Errorf("store: event for client: %w", err)
	}
	return ev, true, nil
}
