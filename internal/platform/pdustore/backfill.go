package pdustore

import (
	"context"
	"fmt"
)

// What _maybe_backfill_inner reads (handlers/federation.py,
// storage/databases/main/event_federation.py, roommember.py).

// BackfillPoint is a backward extremity and the depth of an event that
// points at it.
type BackfillPoint struct {
	EventID string
	Depth   int64
}

// The backoff on a failed backfill point: an hour, doubled up to eight
// times (event_federation.py).
const (
	backfillBackoffMaxDoublings = 8
	backfillBackoffStepMS       = 60 * 60 * 1000
)

const qBackfillPoints = `
	SELECT backward_extrem.event_id, event.depth FROM events AS event
	 INNER JOIN event_edges AS edge ON edge.event_id = event.event_id
	 INNER JOIN event_backward_extremities AS backward_extrem
	    ON edge.prev_event_id = backward_extrem.event_id
	  LEFT JOIN event_failed_pull_attempts AS failed_backfill_attempt_info
	    ON failed_backfill_attempt_info.room_id = backward_extrem.room_id
	   AND failed_backfill_attempt_info.event_id = backward_extrem.event_id
	 WHERE backward_extrem.room_id = $1
	   AND event.depth <= $2
	   AND (failed_backfill_attempt_info.event_id IS NULL
	        OR $3 >= failed_backfill_attempt_info.last_attempt_ts + (
	             (1 << LEAST(failed_backfill_attempt_info.num_attempts, $4)) * $5))
	 ORDER BY event.depth DESC, backward_extrem.event_id DESC
	 LIMIT $6`

// BackfillPoints is get_backfill_points_in_room: backward extremities
// referenced by events at depth at most nearbyDepth, not in their failed
// pull backoff at nowMS, deepest first.
func (s *Store) BackfillPoints(ctx context.Context, roomID string, nearbyDepth, nowMS int64, limit int) ([]BackfillPoint, error) {
	rows, err := s.pool.Query(ctx, qBackfillPoints, roomID, nearbyDepth, nowMS,
		backfillBackoffMaxDoublings, backfillBackoffStepMS, limit)
	if err != nil {
		return nil, fmt.Errorf("pdustore: backfill points: %w", err)
	}
	defer rows.Close()
	var out []BackfillPoint
	for rows.Next() {
		var p BackfillPoint
		if err := rows.Scan(&p.EventID, &p.Depth); err != nil {
			return nil, fmt.Errorf("pdustore: backfill points: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// get_successor_events, then get_events_as_list: the events naming
// eventID as a prev event that are held and not rejected.
const qSuccessors = `
	SELECT e.event_id FROM event_edges AS edge
	  JOIN events AS e ON e.event_id = edge.event_id
	 WHERE edge.prev_event_id = $1 AND e.rejection_reason IS NULL`

// Successors is get_successor_events loaded as get_events_as_list loads
// them: held, not rejected.
func (s *Store) Successors(ctx context.Context, eventID string) ([]string, error) {
	ids, err := s.idSet(ctx, qSuccessors, eventID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	return out, nil
}

const qHostsOrdered = `
	SELECT substring(c.state_key FROM '@[^:]*:(.*)$') AS server_domain
	  FROM current_state_events c
	 INNER JOIN events AS e USING (event_id)
	 WHERE c.type = 'm.room.member' AND c.membership = 'join' AND c.room_id = $1
	 GROUP BY server_domain
	 ORDER BY min(e.depth) ASC`

// HostsOrdered is get_current_hosts_in_room_ordered: the servers with a
// joined member, the longest in the room first.
func (s *Store) HostsOrdered(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, qHostsOrdered, roomID)
	if err != nil {
		return nil, fmt.Errorf("pdustore: hosts in room: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d *string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("pdustore: hosts in room: %w", err)
		}
		if d != nil {
			out = append(out, *d)
		}
	}
	return out, rows.Err()
}
