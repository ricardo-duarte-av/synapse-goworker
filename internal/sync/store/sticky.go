package store

import (
	"context"
	"fmt"
)

// StickyMaxEventsInSync is how many sticky events one sync may carry, across
// every room. Synapse's StickyEvent.MAX_EVENTS_IN_SYNC.
//
// The cap exists because anyone may send a sticky event, so the section is
// spammable in a way the timeline is not.
const StickyMaxEventsInSync = 100

// StickyEvents returns the MSC4354 sticky events in the given rooms, in sticky
// stream order, and the stream position the response should report.
//
// Bounds are (from, to], as everywhere else. The returned position is the
// subtle part, and it works like the to-device one: when the limit truncates
// the result, the caller's now token must be wound back to the LAST ROW
// RETURNED so the next sync resumes there. Reporting `to` instead silently
// skips whatever the limit cut off.
//
// Two filters come from Synapse's query and neither is optional. Expired
// events are skipped -- `expires_at` is what makes a sticky event stop being
// sticky, so the answer depends on the wall clock as well as the stream. And
// soft-failed events are skipped: they are in the room's tables but were never
// accepted, and no client should be told about them.
func (s *Store) StickyEvents(ctx context.Context, roomIDs []string,
	from, to, nowMS int64, limit int) (int64, map[string][]string, error) {

	if len(roomIDs) == 0 || from >= to || limit <= 0 {
		return to, nil, nil
	}

	// internal_metadata is cast to jsonb here, which event_json.json never can
	// be: the NUL-byte events that rule out casting the event body do not
	// affect this column, and Synapse's own query does the same cast.
	const q = `
		SELECT se.stream_id, se.room_id, se.event_id
		  FROM sticky_events se
		  JOIN event_json ej USING (event_id)
		 WHERE NOT COALESCE(((ej.internal_metadata::jsonb)->>'soft_failed')::boolean, FALSE)
		   AND $1 < se.expires_at
		   AND $2 < se.stream_id AND se.stream_id <= $3
		   AND se.room_id = ANY($4)
		 ORDER BY se.stream_id ASC
		 LIMIT $5`

	rows, err := s.query(ctx, "StickyEvents", q, nowMS, from, to, roomIDs, limit)
	if err != nil {
		return 0, nil, fmt.Errorf("store: sticky events: %w", err)
	}
	defer rows.Close()

	var (
		byRoom = map[string][]string{}
		last   int64
	)
	for rows.Next() {
		var (
			streamID        int64
			roomID, eventID string
		)
		if err := rows.Scan(&streamID, &roomID, &eventID); err != nil {
			return 0, nil, fmt.Errorf("store: sticky events: %w", err)
		}
		last = streamID
		byRoom[roomID] = append(byRoom[roomID], eventID)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("store: sticky events: %w", err)
	}

	if len(byRoom) == 0 {
		return to, nil, nil
	}
	// Synapse takes the last row's position unconditionally when there were
	// any rows, not only when the limit was hit -- the rows are ordered
	// ascending, so the last one is the highest either way.
	return last, byRoom, nil
}
