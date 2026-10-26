package store

import (
	"context"
	"fmt"
)

// ThreadSummary is the aggregate a threaded root event carries.
type ThreadSummary struct {
	// Count is the number of replies in the thread.
	Count int
	// LatestEventID is the most recent reply, by the room's own ordering.
	LatestEventID string
}

// RelationTypesOf reports which of the given events are themselves a relation,
// and of what type.
//
// An event that is itself an edit or an annotation gets no aggregations of its
// own; one that is a thread reply does.
func (s *Store) RelationTypesOf(ctx context.Context, eventIDs []string) (map[string]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `SELECT event_id, relation_type FROM event_relations WHERE event_id = ANY($1)`
	rows, err := s.query(ctx, "RelationTypesOf", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: relation types: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, relType string
		if err := rows.Scan(&id, &relType); err != nil {
			return nil, fmt.Errorf("store: relation types: %w", err)
		}
		out[id] = relType
	}
	return out, rows.Err()
}

// ThreadSummaries returns the reply count and latest reply for each event that
// roots a thread.
//
// The latest reply is chosen by (topological_ordering, stream_ordering)
// descending -- the room's own order, not the server's insertion order, for the
// same reason pagination uses it.
func (s *Store) ThreadSummaries(ctx context.Context, eventIDs []string) (map[string]ThreadSummary, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const latestQ = `
		SELECT DISTINCT ON (parent.event_id) parent.event_id, child.event_id
		  FROM events AS child
		  INNER JOIN event_relations USING (event_id)
		  INNER JOIN events AS parent
		     ON parent.event_id = relates_to_id AND parent.room_id = child.room_id
		 WHERE relates_to_id = ANY($1) AND relation_type = 'm.thread'
		 ORDER BY parent.event_id, child.topological_ordering DESC, child.stream_ordering DESC`
	rows, err := s.query(ctx, "ThreadSummaries", latestQ, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: thread latest: %w", err)
	}
	out := map[string]ThreadSummary{}
	for rows.Next() {
		var parent, child string
		if err := rows.Scan(&parent, &child); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: thread latest: %w", err)
		}
		out[parent] = ThreadSummary{LatestEventID: child}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: thread latest: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}

	roots := make([]string, 0, len(out))
	for id := range out {
		roots = append(roots, id)
	}
	const countQ = `
		SELECT parent.event_id, COUNT(child.event_id)
		  FROM events AS child
		  INNER JOIN event_relations USING (event_id)
		  INNER JOIN events AS parent
		     ON parent.event_id = relates_to_id AND parent.room_id = child.room_id
		 WHERE relates_to_id = ANY($1) AND relation_type = 'm.thread'
		 GROUP BY parent.event_id`
	crows, err := s.query(ctx, "ThreadSummaries", countQ, roots)
	if err != nil {
		return nil, fmt.Errorf("store: thread counts: %w", err)
	}
	defer crows.Close()
	for crows.Next() {
		var parent string
		var count int
		if err := crows.Scan(&parent, &count); err != nil {
			return nil, fmt.Errorf("store: thread counts: %w", err)
		}
		summary := out[parent]
		summary.Count = count
		out[parent] = summary
	}
	return out, crows.Err()
}

// ThreadsParticipated reports which of the given thread roots the user has
// replied in.
func (s *Store) ThreadsParticipated(ctx context.Context, eventIDs []string, userID string) (map[string]bool, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT DISTINCT relates_to_id
		  FROM events AS child INNER JOIN event_relations USING (event_id)
		 WHERE relates_to_id = ANY($1) AND relation_type = 'm.thread'
		   AND child.sender = $2`
	rows, err := s.query(ctx, "ThreadsParticipated", q, eventIDs, userID)
	if err != nil {
		return nil, fmt.Errorf("store: threads participated: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: threads participated: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}

// ThreadRepliesBySender counts each sender's replies in the given threads,
// so an ignored user's replies can be subtracted from the count.
func (s *Store) ThreadRepliesBySender(ctx context.Context, eventIDs, senders []string) (map[string]int, error) {
	if len(eventIDs) == 0 || len(senders) == 0 {
		return nil, nil
	}
	const q = `
		SELECT relates_to_id, COUNT(*)
		  FROM events AS child INNER JOIN event_relations USING (event_id)
		 WHERE relates_to_id = ANY($1) AND relation_type = 'm.thread'
		   AND child.sender = ANY($2)
		 GROUP BY relates_to_id`
	rows, err := s.query(ctx, "ThreadRepliesBySender", q, eventIDs, senders)
	if err != nil {
		return nil, fmt.Errorf("store: ignored thread replies: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("store: ignored thread replies: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

// ApplicableEdits returns the latest edit for each event, keyed by the original.
//
// An edit only counts when it has the same sender, type and room as what it
// edits: otherwise anyone could rewrite anyone's message. "Latest" is by
// origin_server_ts then event id, so two edits with the same timestamp still
// resolve the same way for every reader.
func (s *Store) ApplicableEdits(ctx context.Context, eventIDs []string) (map[string]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT DISTINCT ON (original.event_id) original.event_id, edit.event_id
		  FROM events AS edit
		  INNER JOIN event_relations USING (event_id)
		  INNER JOIN events AS original
		     ON original.event_id = relates_to_id
		    AND edit.type = original.type
		    AND edit.sender = original.sender
		    AND edit.room_id = original.room_id
		 WHERE relates_to_id = ANY($1) AND relation_type = 'm.replace'
		 ORDER BY original.event_id DESC, edit.origin_server_ts DESC, edit.event_id DESC`
	rows, err := s.query(ctx, "ApplicableEdits", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: applicable edits: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var original, edit string
		if err := rows.Scan(&original, &edit); err != nil {
			return nil, fmt.Errorf("store: applicable edits: %w", err)
		}
		out[original] = edit
	}
	return out, rows.Err()
}

// References returns the events referencing each of the given events, in the
// room's own order.
func (s *Store) References(ctx context.Context, eventIDs []string, ignored map[string]bool) (map[string][]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT relates_to_id, ref.event_id, ref.sender
		  FROM events AS ref
		  INNER JOIN event_relations USING (event_id)
		  INNER JOIN events AS parent
		     ON parent.event_id = relates_to_id AND parent.room_id = ref.room_id
		 WHERE relates_to_id = ANY($1) AND relation_type = 'm.reference'
		 ORDER BY ref.topological_ordering, ref.stream_ordering`
	rows, err := s.query(ctx, "References", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: references: %w", err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var parent, ref, sender string
		if err := rows.Scan(&parent, &ref, &sender); err != nil {
			return nil, fmt.Errorf("store: references: %w", err)
		}
		if ignored[sender] {
			continue
		}
		out[parent] = append(out[parent], ref)
	}
	return out, rows.Err()
}
