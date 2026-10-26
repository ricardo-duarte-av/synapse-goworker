package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// RoomEventsForward returns each room's events in (from, to], oldest first,
// capped at limit per room.
//
// The forward twin of RoomTimelineSince, and the difference is which end of the
// window the limit cuts. A sync reports the NEWEST events in the window and
// tells the client the timeline is limited; /events streams forward from where
// the client stopped, so a capped page must be the OLDEST events, and the next
// request resumes after them. Cutting the wrong end silently skips everything
// in between.
//
// Ordered by stream position throughout, because that is the order in which
// the server received things -- which is the only order /events claims.
func (s *Store) RoomEventsForward(ctx context.Context, roomIDs []string,
	roomVersions map[string]string, from, to int64, limit int) ([]TimelineEvent, error) {

	if len(roomIDs) == 0 || limit <= 0 || from >= to {
		return nil, nil
	}
	const q = `
		SELECT * FROM (
			SELECT e.room_id, e.event_id, e.type, e.sender, e.stream_ordering,
			       e.topological_ordering, COALESCE(e.instance_name, ''),
			       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
			       ej.json, ej.internal_metadata,
			       e.rejection_reason IS NULL,
			       ROW_NUMBER() OVER (PARTITION BY e.room_id
			                          ORDER BY e.stream_ordering ASC) AS rn
			  FROM events e JOIN event_json ej USING (event_id)
			 WHERE e.outlier = FALSE AND e.room_id = ANY($1)
			   AND e.stream_ordering > $2 AND e.stream_ordering <= $3
		) x WHERE rn <= $4
		ORDER BY stream_ordering ASC`

	rows, err := s.query(ctx, "RoomEventsForward", q, roomIDs, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("store: room events forward: %w", err)
	}
	defer rows.Close()

	var out []TimelineEvent
	for rows.Next() {
		var (
			ev TimelineEvent
			ok bool
			rn int64
		)
		if err := rows.Scan(&ev.RoomID, &ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ok, &rn); err != nil {
			return nil, fmt.Errorf("store: room events forward: %w", err)
		}
		// Rejected events consume a row number and are dropped after it.
		if !ok {
			continue
		}
		ev.RoomVersion = roomVersions[ev.RoomID]
		out = append(out, ev)
	}
	return out, rows.Err()
}

// MembershipEventsForUser returns the caller's own membership events in
// (from, to], oldest first.
//
// Separate from RoomEventsForward and not redundant with it: an invite or a
// leave lands in a room the caller is NOT joined to, so it is invisible to a
// query over the joined set. This is what lets /events tell a client it has
// been invited somewhere.
//
// The room version comes from the room rather than a caller-supplied map,
// because these events span rooms the caller may know nothing about.
func (s *Store) MembershipEventsForUser(ctx context.Context, userID string,
	from, to int64) ([]TimelineEvent, error) {

	if from >= to {
		return nil, nil
	}
	const q = `
		SELECT e.room_id, e.event_id, e.type, e.sender, e.stream_ordering,
		       e.topological_ordering, COALESCE(e.instance_name, ''),
		       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
		       ej.json, ej.internal_metadata, COALESCE(r.room_version, '1')
		  FROM events e
		  JOIN event_json ej USING (event_id)
		  LEFT JOIN rooms r ON r.room_id = e.room_id
		 WHERE e.type = 'm.room.member' AND e.state_key = $1
		   AND e.outlier = FALSE
		   AND e.stream_ordering > $2 AND e.stream_ordering <= $3
		 ORDER BY e.stream_ordering ASC`

	rows, err := s.query(ctx, "MembershipEventsForUser", q, userID, from, to)
	if err != nil {
		return nil, fmt.Errorf("store: membership events for user: %w", err)
	}
	defer rows.Close()

	var out []TimelineEvent
	for rows.Next() {
		var ev TimelineEvent
		if err := rows.Scan(&ev.RoomID, &ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ev.RoomVersion); err != nil {
			return nil, fmt.Errorf("store: membership events for user: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// UpdatedTags returns the FULL tag set of every room whose tags changed after
// the given account-data position.
//
// The stream records that a room's tags changed, not what changed, so the
// answer is always the room's whole current tag set -- and a room whose last
// tag was removed is still reported, with an empty set. Dropping those would
// leave a client showing a tag that no longer exists.
func (s *Store) UpdatedTags(ctx context.Context, userID string, since int64) (map[string]json.RawMessage, error) {
	const q = `
		SELECT rev.room_id, t.tag, t.content
		  FROM room_tags_revisions rev
		  LEFT JOIN room_tags t ON t.user_id = rev.user_id AND t.room_id = rev.room_id
		 WHERE rev.user_id = $1 AND rev.stream_id > $2
		 ORDER BY rev.room_id, t.tag`

	rows, err := s.query(ctx, "UpdatedTags", q, userID, since)
	if err != nil {
		return nil, fmt.Errorf("store: updated tags: %w", err)
	}
	defer rows.Close()

	byRoom := map[string]map[string]json.RawMessage{}
	for rows.Next() {
		var (
			roomID  string
			tag     *string
			content *string
		)
		if err := rows.Scan(&roomID, &tag, &content); err != nil {
			return nil, fmt.Errorf("store: updated tags: %w", err)
		}
		if _, ok := byRoom[roomID]; !ok {
			byRoom[roomID] = map[string]json.RawMessage{}
		}
		if tag == nil {
			continue
		}
		body := json.RawMessage("{}")
		if content != nil {
			body = json.RawMessage(*content)
		}
		byRoom[roomID][*tag] = body
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: updated tags: %w", err)
	}

	out := make(map[string]json.RawMessage, len(byRoom))
	for roomID, tags := range byRoom {
		body, err := json.Marshal(map[string]any{"tags": tags})
		if err != nil {
			return nil, fmt.Errorf("store: updated tags: %w", err)
		}
		out[roomID] = body
	}
	return out, nil
}
