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
// subtle part, and it is Synapse v1.163's (#20186): when the limit truncates
// the result, the caller's now token must be wound back to the LAST ROW
// RETURNED so the next sync resumes there (reporting `to` instead silently
// skips whatever the limit cut off); otherwise the whole range was read and
// it is `to`, never behind `from` (copy_and_advance); with no limit at all,
// `from`.
//
// One integer, not Synapse's sharded MultiWriterStreamToken: exact while
// sticky_events has one writer (the one events writer each host has). A
// second events writer needs sharded tokens throughout sync, the room key
// first, not here alone.
//
// Two filters come from Synapse's query and neither is optional. Expired
// events are skipped -- `expires_at` is what makes a sticky event stop being
// sticky, so the answer depends on the wall clock as well as the stream. And
// soft-failed events are skipped: they are in the room's tables but were never
// accepted, and no client should be told about them.
func (s *Store) StickyEvents(ctx context.Context, roomIDs []string,
	from, to, nowMS int64, limit int) (int64, map[string][]string, error) {

	if limit <= 0 {
		return from, nil, nil
	}
	if len(roomIDs) == 0 || to <= from {
		return max(from, to), nil, nil
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
		n      int
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
		n++
		byRoom[roomID] = append(byRoom[roomID], eventID)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("store: sticky events: %w", err)
	}

	if n == limit {
		// advance_multiwriter_sharded_token_after_partial_read, one writer.
		return last, byRoom, nil
	}
	if len(byRoom) == 0 {
		byRoom = nil
	}
	return max(from, to), byRoom, nil
}

// StickyRoomsBetween is get_rooms_with_sticky_events_between (Synapse
// 1.165): those of roomIDs with a row in sticky_events -- expired ones
// awaiting cleanup included -- whose stream position is after from and at
// or before to.
func (s *Store) StickyRoomsBetween(ctx context.Context, roomIDs []string, from, to int64) (map[string]bool, error) {
	if len(roomIDs) == 0 || to <= from {
		return nil, nil
	}
	rows, err := s.query(ctx, "StickyRoomsBetween", `
		SELECT DISTINCT room_id
		  FROM sticky_events
		 WHERE $1 < stream_id AND stream_id <= $2
		   AND room_id = ANY($3)`, from, to, roomIDs)
	if err != nil {
		return nil, fmt.Errorf("store: sticky rooms: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var roomID string
		if err := rows.Scan(&roomID); err != nil {
			return nil, fmt.Errorf("store: sticky rooms: %w", err)
		}
		out[roomID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: sticky rooms: %w", err)
	}
	return out, nil
}

// StickyMaxAllocated is the sticky events id generator's
// get_max_allocated_token: the sequence's last value, less one if none was
// ever taken.
func (s *Store) StickyMaxAllocated(ctx context.Context) (int64, error) {
	var last int64
	var called bool
	if err := s.queryRow(ctx, "StickyMaxAllocated",
		`SELECT last_value, is_called FROM sticky_events_sequence`).Scan(&last, &called); err != nil {
		return 0, fmt.Errorf("store: sticky events sequence: %w", err)
	}
	if !called {
		last--
	}
	return last, nil
}
