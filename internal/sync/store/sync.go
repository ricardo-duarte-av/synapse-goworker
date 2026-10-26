package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// fullStateGroupQuery resolves the entire state map at a group.
//
// Same recursive walk as the filtered version, without the type restriction.
// Only used where the whole map is genuinely needed -- the `state` block of an
// initial /sync -- because a large room's state runs to six figures.
const fullStateGroupQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (type, state_key) type, state_key, event_id
	  FROM state_groups_state
	  INNER JOIN sgs USING (state_group)
	 ORDER BY type, state_key, state_group DESC`

// lazyStateGroupQuery resolves the state at a group, keeping every type except
// m.room.member and only the member events for the named users.
//
// This is StateFilter.from_lazy_load_member_list in SQL. It matters far more
// than it looks: the full query walks every state event in the room, and in a
// large public room that is six figures of members, nearly all of which a
// lazy-loading client is about to discard. Resolving 500 rooms that way cost
// 209 seconds and 1.5GB on a real account; the members a timeline actually
// mentions are a handful.
const lazyStateGroupQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (type, state_key) type, state_key, event_id
	  FROM state_groups_state
	  INNER JOIN sgs USING (state_group)
	 WHERE type <> 'm.room.member' OR state_key = ANY($2)
	 ORDER BY type, state_key, state_group DESC`

// LazyStateForGroup resolves the state at a group for a lazy-loading client:
// everything except member events, plus the members named.
//
// A nil or empty member list still returns all the non-member state, which is
// what a lazy-loading sync of a room whose timeline mentions nobody needs.
func (s *Store) LazyStateForGroup(ctx context.Context, group int64, members []string) (
	map[StateKey]string, error) {

	if members == nil {
		members = []string{}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}
	rows, err := txQuery(ctx, tx, "LazyStateForGroup", lazyStateGroupQuery, group, members)
	if err != nil {
		return nil, fmt.Errorf("store: lazy state for group: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: lazy state for group: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: lazy state for group: %w", err)
	}
	return state, nil
}

// FullStateForGroup resolves the complete state map at a state group.
func (s *Store) FullStateForGroup(ctx context.Context, group int64) (map[StateKey]string, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}
	rows, err := txQuery(ctx, tx, "FullStateForGroup", fullStateGroupQuery, group)
	if err != nil {
		return nil, fmt.Errorf("store: full state for group: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: full state for group: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: full state for group: %w", err)
	}
	return state, nil
}

// CurrentStateIDs is the room's current state as a map, for state calculations
// that work in event IDs rather than whole events.
func (s *Store) CurrentStateIDs(ctx context.Context, roomID string) (map[StateKey]string, error) {
	const q = `
		SELECT type, state_key, event_id FROM current_state_events WHERE room_id = $1`
	rows, err := s.query(ctx, "CurrentStateIDs", q, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: current state ids: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: current state ids: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: current state ids: %w", err)
	}
	return state, nil
}

// MemberSummary is the membership breakdown a room summary is built from.
type MemberSummary struct {
	// Counts by membership.
	Counts map[string]int
	// Members lists up to six members, ordered join, invite, leave, then
	// anything else, and by stream ordering within each -- so the earliest
	// members come first and the heroes are stable between requests.
	Members []SummaryMember
}

// SummaryMember is one member in a room summary.
type SummaryMember struct {
	UserID     string
	Membership string
	// EventID is the member event itself, needed by the lazy-loading hero
	// fixup: a hero whose membership the client has not been sent must have it
	// added to the state block, or the client cannot render the name it was
	// just told to use.
	EventID string
}

// RoomSummary loads the member counts and the first few members of a room.
//
// Mirrors get_room_summary. The ordering is load-bearing: heroes are taken from
// this list in order, so an unstable sort would rename a room between two
// otherwise identical syncs.
// Cached per room, invalidated by any event in that room, and served only
// when replication has caught up to the token (see derived.go).
//
// A summary is a snapshot rather than a delta, so it is the safest of the
// guarded caches: a stale one is corrected the next time the room appears in
// a sync. The guard is still applied, because "corrected next time" is not the
// same as "correct", and this is the cache that runs on every room of an
// initial sync.
func (s *Store) RoomSummary(ctx context.Context, roomID string) (MemberSummary, error) {
	use := s.derived.fresh(ctx, streamEvents)
	if use {
		if sum, ok := s.derived.roomSummary.Get(roomID); ok {
			return copyMemberSummary(sum), nil
		}
	}
	sum, err := s.roomSummary(ctx, roomID)
	if err != nil {
		return MemberSummary{}, err
	}
	if use {
		s.derived.roomSummary.Add(roomID, sum)
	}
	return copyMemberSummary(sum), nil
}

func (s *Store) roomSummary(ctx context.Context, roomID string) (MemberSummary, error) {
	out := MemberSummary{Counts: map[string]int{}}

	const countQ = `
		SELECT membership, COUNT(*) FROM current_state_events
		 WHERE type = 'm.room.member' AND room_id = $1 AND membership IS NOT NULL
		 GROUP BY membership`
	rows, err := s.query(ctx, "RoomSummary", countQ, roomID)
	if err != nil {
		return out, fmt.Errorf("store: room summary counts: %w", err)
	}
	for rows.Next() {
		var membership string
		var count int
		if err := rows.Scan(&membership, &count); err != nil {
			rows.Close()
			return out, fmt.Errorf("store: room summary counts: %w", err)
		}
		out.Counts[membership] = count
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("store: room summary counts: %w", err)
	}

	// Six rather than five: one of them may be the calling user, who is never
	// their own hero.
	const memberQ = `
		SELECT state_key, membership, event_id FROM current_state_events
		 WHERE type = 'm.room.member' AND room_id = $1 AND membership IS NOT NULL
		 ORDER BY CASE membership
		            WHEN 'join' THEN 1 WHEN 'invite' THEN 2 WHEN 'leave' THEN 3
		            ELSE 4 END ASC,
		          event_stream_ordering ASC
		 LIMIT 6`
	mrows, err := s.query(ctx, "RoomSummary", memberQ, roomID)
	if err != nil {
		return out, fmt.Errorf("store: room summary members: %w", err)
	}
	defer mrows.Close()
	for mrows.Next() {
		var m SummaryMember
		if err := mrows.Scan(&m.UserID, &m.Membership, &m.EventID); err != nil {
			return out, fmt.Errorf("store: room summary members: %w", err)
		}
		out.Members = append(out.Members, m)
	}
	if err := mrows.Err(); err != nil {
		return out, fmt.Errorf("store: room summary members: %w", err)
	}
	return out, nil
}

// Heroes picks the users that represent a room without a name, from the
// perspective of `me`.
//
// Joined and invited members first, in stream order; only if there are none
// does it fall back to those who have left or been banned. Spec: "Room
// Summary" in the client-server API.
func (ms MemberSummary) Heroes(me string) []string {
	var joined, invited, gone []string
	for _, m := range ms.Members {
		if m.UserID == me {
			continue
		}
		switch m.Membership {
		case "join":
			joined = append(joined, m.UserID)
		case "invite":
			invited = append(invited, m.UserID)
		case "leave", "ban":
			gone = append(gone, m.UserID)
		}
	}
	if len(joined) > 0 || len(invited) > 0 {
		return firstN(append(joined, invited...), 5)
	}
	return firstN(gone, 5)
}

func firstN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// UnreadCounts is a room's notification counts for one user.
type UnreadCounts struct {
	NotifyCount    int
	HighlightCount int
	// UnreadCount is MSC2654's count, a different number from NotifyCount: it
	// counts events marked `unread` rather than events that would notify. A
	// muted room accumulates unread events and no notifications.
	UnreadCount int
}

// MainTimeline is the thread id Synapse gives events that are not in a thread.
//
// A real value rather than a NULL, in event_push_actions and
// event_push_summary alike, so "the main timeline" is just another thread as
// far as the counting is concerned -- and is separated out again only when the
// response is built. See synapse/api/constants.py MAIN_TIMELINE.
const MainTimeline = "main"

// RoomNotifCounts is a room's unread counts, split the way Synapse splits them.
//
// Whether the split is visible to the client is the client's choice: a filter
// asking for `unread_thread_notifications` gets Threads reported separately and
// Main on its own, and any other filter gets them added together. Both need the
// same query, so the split is made here and folded later.
type RoomNotifCounts struct {
	Main UnreadCounts
	// Threads is keyed by thread root event id. Never contains MainTimeline.
	Threads map[string]UnreadCounts
}

// Total returns the counts as a client that did not ask for the thread split
// sees them: everything added together.
func (r RoomNotifCounts) Total() UnreadCounts {
	out := r.Main
	for _, t := range r.Threads {
		out.NotifyCount += t.NotifyCount
		out.HighlightCount += t.HighlightCount
		out.UnreadCount += t.UnreadCount
	}
	return out
}

// UnreadNotifications reads a user's unread counts for a room, per thread.
//
// A port of _get_unread_counts_by_receipt_txn. Four things make this more than
// a SELECT SUM:
//
//   - Counts are relative to the user's latest READ RECEIPT, not the start of
//     the room. Without that bound every count is the room's whole history --
//     one room here reported 3,950 against Synapse's 39.
//   - The bound is PER THREAD: a threaded receipt ahead of the unthreaded one
//     moves that thread's starting point on its own.
//   - event_push_summary is a rollup, usable only when its
//     last_receipt_stream_ordering matches the receipt in force. An all-zero
//     row does not count as a summary at all; treating it as one makes the
//     thread look already-counted and undercounts everything between the
//     receipt and the last rotation.
//   - A thread is reported even when every one of its counts is zero. Synapse
//     creates the entry as a side effect of seeing any row for that thread, so
//     a thread whose only events neither notify nor count as unread still
//     appears in the response as a pair of zeroes.
//
// Highlights are never summarised, so they are always counted from
// event_push_actions and never bounded by the rotation point.
func (s *Store) UnreadNotifications(ctx context.Context, roomID, userID string) (RoomNotifCounts, error) {
	const q = `
		WITH receipt AS (
			-- The user's latest unthreaded read receipt, or failing that their
			-- own membership event.
			SELECT COALESCE(
				(SELECT MAX(event_stream_ordering) FROM receipts_linearized
				  WHERE user_id = $2 AND room_id = $1 AND thread_id IS NULL
				    AND receipt_type IN ('m.read', 'm.read.private')),
				(SELECT e.stream_ordering FROM local_current_membership c
				   JOIN events e ON e.event_id = c.event_id
				  WHERE c.room_id = $1 AND c.user_id = $2),
				0) AS pos
		),
		rotated AS (
			SELECT COALESCE((SELECT stream_ordering FROM event_push_summary_stream_ordering), 0) AS pos
		),
		thread_receipts AS (
			-- Only receipts AHEAD of the unthreaded one move a thread's bound.
			SELECT r.thread_id, MAX(r.event_stream_ordering) AS pos
			  FROM receipts_linearized r, receipt
			 WHERE r.user_id = $2 AND r.room_id = $1
			   AND r.event_stream_ordering > receipt.pos
			   AND r.receipt_type IN ('m.read', 'm.read.private')
			 GROUP BY r.thread_id
		),
		summary AS (
			SELECT COALESCE(s.thread_id, 'main') AS thread_id,
			       SUM(s.notif_count) AS notif_count,
			       SUM(COALESCE(s.unread_count, 0)) AS unread_count
			  FROM event_push_summary s
			  LEFT JOIN thread_receipts tr ON tr.thread_id = s.thread_id
			  CROSS JOIN receipt
			 WHERE s.room_id = $1 AND s.user_id = $2
			   AND ((s.last_receipt_stream_ordering IS NULL
			         AND s.stream_ordering > COALESCE(tr.pos, receipt.pos))
			        OR s.last_receipt_stream_ordering = COALESCE(tr.pos, receipt.pos))
			   AND (s.notif_count != 0 OR COALESCE(s.unread_count, 0) != 0)
			 GROUP BY COALESCE(s.thread_id, 'main')
		),
		actions AS (
			-- A summarised thread is already counted up to the last rotation,
			-- so only what came after it is added. An unsummarised one is
			-- counted from its receipt.
			SELECT COALESCE(a.thread_id, 'main') AS thread_id,
			       COUNT(*) FILTER (WHERE a.notif = 1) AS notif,
			       COUNT(*) FILTER (WHERE a.unread = 1) AS unread
			  FROM event_push_actions a
			  LEFT JOIN thread_receipts tr ON tr.thread_id = a.thread_id
			  LEFT JOIN summary su ON su.thread_id = COALESCE(a.thread_id, 'main')
			  CROSS JOIN receipt CROSS JOIN rotated
			 WHERE a.room_id = $1 AND a.user_id = $2
			   AND a.stream_ordering > COALESCE(tr.pos, receipt.pos)
			   AND (su.thread_id IS NULL OR a.stream_ordering > rotated.pos)
			 GROUP BY COALESCE(a.thread_id, 'main')
		),
		highlights AS (
			SELECT COALESCE(a.thread_id, 'main') AS thread_id, COUNT(*) AS n
			  FROM event_push_actions a
			  LEFT JOIN thread_receipts tr ON tr.thread_id = a.thread_id
			  CROSS JOIN receipt
			 WHERE a.room_id = $1 AND a.user_id = $2 AND a.highlight = 1
			   AND a.stream_ordering > COALESCE(tr.pos, receipt.pos)
			 GROUP BY COALESCE(a.thread_id, 'main')
		),
		threads AS (
			SELECT thread_id FROM summary
			UNION SELECT thread_id FROM actions
			UNION SELECT thread_id FROM highlights
		)
		SELECT t.thread_id,
		       COALESCE(s.notif_count, 0) + COALESCE(ac.notif, 0),
		       COALESCE(h.n, 0),
		       COALESCE(s.unread_count, 0) + COALESCE(ac.unread, 0)
		  FROM threads t
		  LEFT JOIN summary s ON s.thread_id = t.thread_id
		  LEFT JOIN actions ac ON ac.thread_id = t.thread_id
		  LEFT JOIN highlights h ON h.thread_id = t.thread_id`

	rows, err := s.query(ctx, "UnreadNotifications", q, roomID, userID)
	if err != nil {
		return RoomNotifCounts{}, fmt.Errorf("store: unread notifications: %w", err)
	}
	defer rows.Close()

	out := RoomNotifCounts{}
	for rows.Next() {
		var (
			threadID string
			c        UnreadCounts
		)
		if err := rows.Scan(&threadID, &c.NotifyCount, &c.HighlightCount, &c.UnreadCount); err != nil {
			return RoomNotifCounts{}, fmt.Errorf("store: unread notifications: %w", err)
		}
		if threadID == MainTimeline {
			out.Main = c
			continue
		}
		if out.Threads == nil {
			out.Threads = map[string]UnreadCounts{}
		}
		out.Threads[threadID] = c
	}
	if err := rows.Err(); err != nil {
		return RoomNotifCounts{}, fmt.Errorf("store: unread notifications: %w", err)
	}
	return out, nil
}

// DeviceKeyCounts is the end-to-end key inventory a sync reports.
type DeviceKeyCounts struct {
	// OneTimeKeys counts unclaimed one-time keys by algorithm.
	OneTimeKeys map[string]int
	// UnusedFallbackKeyTypes lists algorithms with an unused fallback key.
	UnusedFallbackKeyTypes []string
}

// DeviceKeyCounts loads the one-time and fallback key inventory for a device.
//
// Synapse always emits both fields, even when empty: a client cannot otherwise
// tell "no keys" from "nothing changed".
func (s *Store) DeviceKeyCounts(ctx context.Context, userID, deviceID string) (DeviceKeyCounts, error) {
	out := DeviceKeyCounts{
		// signed_curve25519 is always reported, even at zero. Synapse does the
		// same: a client cannot otherwise tell "no keys left" from "this
		// server does not report key counts", and Element Android got that
		// wrong (element-hq/element-android#3725).
		OneTimeKeys:            map[string]int{"signed_curve25519": 0},
		UnusedFallbackKeyTypes: []string{},
	}
	if deviceID == "" {
		return out, nil
	}

	rows, err := s.query(ctx, "DeviceKeyCounts", `
		SELECT algorithm, COUNT(*) FROM e2e_one_time_keys_json
		 WHERE user_id = $1 AND device_id = $2 GROUP BY algorithm`, userID, deviceID)
	if err != nil {
		return out, fmt.Errorf("store: one time keys: %w", err)
	}
	for rows.Next() {
		var alg string
		var n int
		if err := rows.Scan(&alg, &n); err != nil {
			rows.Close()
			return out, fmt.Errorf("store: one time keys: %w", err)
		}
		out.OneTimeKeys[alg] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("store: one time keys: %w", err)
	}

	frows, err := s.query(ctx, "DeviceKeyCounts", `
		SELECT algorithm FROM e2e_fallback_keys_json
		 WHERE user_id = $1 AND device_id = $2 AND used IS FALSE`, userID, deviceID)
	if err != nil {
		return out, fmt.Errorf("store: fallback keys: %w", err)
	}
	defer frows.Close()
	for frows.Next() {
		var alg string
		if err := frows.Scan(&alg); err != nil {
			return out, fmt.Errorf("store: fallback keys: %w", err)
		}
		out.UnusedFallbackKeyTypes = append(out.UnusedFallbackKeyTypes, alg)
	}
	if err := frows.Err(); err != nil {
		return out, fmt.Errorf("store: fallback keys: %w", err)
	}
	return out, nil
}

// EventsByID loads stored events for serialisation, keyed by event id.
func (s *Store) EventsByID(ctx context.Context, eventIDs []string, roomVersion string) (map[string]StateEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT e.event_id, e.room_id, e.type, COALESCE(e.state_key, ''), e.sender,
		       ej.json, ej.internal_metadata
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.event_id = ANY($1)`
	rows, err := s.query(ctx, "EventsByID", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: events by id: %w", err)
	}
	defer rows.Close()

	out := make(map[string]StateEvent, len(eventIDs))
	for rows.Next() {
		var ev StateEvent
		if err := rows.Scan(&ev.EventID, &ev.RoomID, &ev.Type, &ev.StateKey, &ev.Sender,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, fmt.Errorf("store: events by id: %w", err)
		}
		ev.RoomVersion = roomVersion
		out[ev.EventID] = ev
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: events by id: %w", err)
	}
	return out, nil
}

// PaginateBackwards loads a page of a room's history, newest first, in the
// room's topological order.
//
// This is Synapse's _paginate_room_events_by_topological_ordering_txn, and an
// initial /sync uses it -- NOT the stream-ordering variant. `pagination_method`
// picks topological when there is no `since` and stream ordering only for
// updates (handlers/sync.py:852). The difference is visible in any room with
// backfilled history, where stream ordering is negative and bears no relation
// to the room's own order.
//
// Returns the page in ascending order, a token just before its earliest event,
// and whether more events were available than were returned.
func (s *Store) PaginateBackwards(ctx context.Context, roomID, roomVersion string,
	limit int, from streamtoken.RoomKey) ([]TimelineEvent, streamtoken.RoomKey, bool, error) {

	if limit <= 0 {
		return nil, from, false, nil
	}

	// Synapse fetches twice what it was asked for, because the result set is
	// filtered afterwards and would otherwise come up short.
	requested := limit * 2

	var (
		sql  string
		args []any
	)
	// rejection_reason is SELECTED and not filtered in SQL, which is the whole
	// subtlety. Synapse pages the events table without a rejection predicate
	// and drops rejected events afterwards, in get_events_as_list -- so a
	// rejected event CONSUMES a slot in the limit and is then not returned.
	//
	// Filtering it in the WHERE clause instead looks equivalent and is not: the
	// page then reaches further back, and a room whose recent history is all
	// auth_error events returns old messages that Synapse never sends. One
	// matrix.org room here did exactly that, and its state block moved with it.
	const cols = `e.event_id, e.type, e.sender, e.stream_ordering,
	       e.topological_ordering, COALESCE(e.instance_name, ''),
	       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
	       ej.json, ej.internal_metadata, e.rejection_reason IS NULL`
	if from.IsHistorical() {
		sql = `SELECT ` + cols + `
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.outlier = FALSE AND e.room_id = $1
		   AND (e.topological_ordering < $2
		        OR (e.topological_ordering = $2 AND e.stream_ordering <= $3))
		 ORDER BY e.topological_ordering DESC, e.stream_ordering DESC
		 LIMIT $4`
		args = []any{roomID, from.Topological, from.Stream, requested}
	} else {
		sql = `SELECT ` + cols + `
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.outlier = FALSE AND e.room_id = $1
		   AND e.stream_ordering <= $2
		 ORDER BY e.topological_ordering DESC, e.stream_ordering DESC
		 LIMIT $3`
		args = []any{roomID, from.MaxStreamPos(), requested}
	}

	rows, err := s.query(ctx, "PaginateBackwards", sql, args...)
	if err != nil {
		return nil, from, false, fmt.Errorf("store: paginate backwards: %w", err)
	}
	defer rows.Close()

	var (
		descending []TimelineEvent
		accepted   []bool
	)
	for rows.Next() {
		var (
			ev TimelineEvent
			ok bool
		)
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ok); err != nil {
			return nil, from, false, fmt.Errorf("store: paginate backwards: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		descending = append(descending, ev)
		accepted = append(accepted, ok)
	}
	if err := rows.Err(); err != nil {
		return nil, from, false, fmt.Errorf("store: paginate backwards: %w", err)
	}

	limited := len(descending) >= requested
	if len(descending) > limit {
		limited = true
		descending = descending[:limit]
		accepted = accepted[:limit]
	}
	if len(descending) == 0 {
		return nil, from, limited, nil
	}

	// A token names a position between events, so the page starts one before
	// its oldest event -- the oldest row PAGED, rejected or not, because that
	// is how far the page reached. Historical form, because that is what the
	// topological walk produced.
	oldest := descending[len(descending)-1]
	next := streamtoken.Historical(oldest.TopologicalOrder, oldest.StreamOrdering-1)

	ascending := make([]TimelineEvent, 0, len(descending))
	for i := len(descending) - 1; i >= 0; i-- {
		if accepted[i] {
			ascending = append(ascending, descending[i])
		}
	}
	return ascending, next, limited, nil
}

// PaginateBackwardsStream walks a room's events backwards by STREAM ordering,
// from `from` down to but not including `to`.
//
// The companion to PaginateBackwards, and the difference is not cosmetic.
// Synapse picks between the two by whether the sync has a `since`
// (`_load_filtered_recents`: "use topological_ordering for historical events,
// stream_ordering for updates"). An incremental sync is asking what arrived
// since the client last looked, which is a question about the order the server
// received things in; an initial sync is asking for a historical slice of the
// room, which is a question about the DAG. An event backfilled today sits in a
// different place in each order, so using the wrong one silently returns a
// different set of events.
//
// The lower bound is what keeps an incremental timeline honest: without it, a
// filter that thins the page sends the re-pagination loop straight past the
// client's `since` and into events it already has.
func (s *Store) PaginateBackwardsStream(ctx context.Context, roomID, roomVersion string,
	limit int, from, to streamtoken.RoomKey) ([]TimelineEvent, streamtoken.RoomKey, bool, error) {

	if limit <= 0 {
		return nil, from, false, nil
	}
	if from.MaxStreamPos() <= to.MaxStreamPos() {
		// Already at or below the lower bound. Synapse returns the bound
		// itself here rather than the starting point.
		return nil, to, false, nil
	}

	// Twice what was asked for, as Synapse does: the result set is filtered
	// afterwards and would otherwise come up short.
	requested := limit * 2

	const q = `
		SELECT e.event_id, e.type, e.sender, e.stream_ordering,
		       e.topological_ordering, COALESCE(e.instance_name, ''),
		       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
		       ej.json, ej.internal_metadata, e.rejection_reason IS NULL
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.outlier = FALSE AND e.room_id = $1
		   AND e.stream_ordering <= $2 AND e.stream_ordering > $3
		 ORDER BY e.stream_ordering DESC
		 LIMIT $4`

	rows, err := s.query(ctx, "PaginateBackwardsStream", q, roomID, from.MaxStreamPos(), to.MaxStreamPos(), requested)
	if err != nil {
		return nil, from, false, fmt.Errorf("store: paginate backwards by stream: %w", err)
	}
	defer rows.Close()

	var (
		descending []TimelineEvent
		accepted   []bool
	)
	for rows.Next() {
		var (
			ev TimelineEvent
			ok bool
		)
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ok); err != nil {
			return nil, from, false, fmt.Errorf("store: paginate backwards by stream: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		descending = append(descending, ev)
		accepted = append(accepted, ok)
	}
	if err := rows.Err(); err != nil {
		return nil, from, false, fmt.Errorf("store: paginate backwards by stream: %w", err)
	}

	limited := len(descending) >= requested
	if len(descending) > limit {
		limited = true
		descending = descending[:limit]
		accepted = accepted[:limit]
	}
	if len(descending) == 0 {
		// No rows means the bound was reached, and the bound is what the
		// caller should resume from.
		return nil, to, limited, nil
	}

	// A token names a position between events. Live form, not historical:
	// this walk was ordered by stream position, so that is what the token
	// must mean (generate_next_token with last_topo_ordering=None).
	oldest := descending[len(descending)-1]
	next := streamtoken.Live(oldest.StreamOrdering - 1)

	ascending := make([]TimelineEvent, 0, len(descending))
	for i := len(descending) - 1; i >= 0; i-- {
		if accepted[i] {
			ascending = append(ascending, descending[i])
		}
	}
	return ascending, next, limited, nil
}

// EventsInCurrentState reports which of the given events are part of the
// room's current state.
//
// A state event that is still current is shown in the timeline regardless of
// history visibility (`always_include_ids` in filter_events_for_client): a
// client that can see the room's current state gains nothing from having the
// event that established it withheld.
func (s *Store) EventsInCurrentState(ctx context.Context, eventIDs []string) (map[string]bool, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `SELECT event_id FROM current_state_events WHERE event_id = ANY($1)`
	rows, err := s.query(ctx, "EventsInCurrentState", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: events in current state: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: events in current state: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}

// PushRules loads a user's stored push rules and their enabled flags.
//
// Synapse stores only a user's *deviations* from the built-in ruleset, so this
// is usually empty and the reported ruleset is entirely the base one.
//
// Sorted by (priority_class DESC, priority DESC), which is the order
// get_push_rules_for_user applies before handing the rows to the Rust. Rule
// order decides which notification a user gets, so this is not cosmetic.
func (s *Store) PushRules(ctx context.Context, userID string) ([]pushrules.UserRule, map[string]bool, error) {
	const q = `
		SELECT rule_id, priority_class, priority, conditions, actions
		  FROM push_rules WHERE user_name = $1
		 ORDER BY priority_class DESC, priority DESC`
	rows, err := s.query(ctx, "PushRules", q, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("store: push rules: %w", err)
	}
	var rules []pushrules.UserRule
	for rows.Next() {
		var r pushrules.UserRule
		if err := rows.Scan(&r.RuleID, &r.PriorityClass, &r.Priority, &r.Conditions, &r.Actions); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("store: push rules: %w", err)
		}
		rules = append(rules, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: push rules: %w", err)
	}

	erows, err := s.query(ctx, "PushRules",
		`SELECT rule_id, enabled FROM push_rules_enable WHERE user_name = $1`, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("store: push rules enabled: %w", err)
	}
	defer erows.Close()
	enabled := map[string]bool{}
	for erows.Next() {
		var ruleID string
		var on *int
		if err := erows.Scan(&ruleID, &on); err != nil {
			return nil, nil, fmt.Errorf("store: push rules enabled: %w", err)
		}
		enabled[ruleID] = on != nil && *on != 0
	}
	if err := erows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: push rules enabled: %w", err)
	}
	return rules, enabled, nil
}

// StateIDsAt resolves the room state at a stream position.
//
// This is NOT the same as the room's current state, and using current state in
// its place is wrong in exactly the case that matters: a busy room, where state
// changes between the token being minted and the query running. The sync
// response would then describe state the client's token does not cover.
//
// Mirrors get_state_ids_at: find the last event at or before the position, then
// take the state after it. Synapse notes this returns an arbitrary one of
// several forward extremities when the room had a fork at that moment; we
// inherit that, because we resolve the same event it does.
// FilteredStateAt resolves a few named state keys at a stream position.
//
// For callers that need two or three entries rather than a room's whole state:
// resolving the full map to read one key is the difference between a handful
// of rows and six figures of them. The underlying filtered lookup is cached,
// because a state group's contents never change.
func (s *Store) FilteredStateAt(ctx context.Context, roomID string, key streamtoken.RoomKey,
	keys []StateKey) (map[StateKey]string, error) {

	group, ok, err := s.stateGroupAt(ctx, roomID, key)
	if err != nil || !ok {
		return map[StateKey]string{}, err
	}
	entries, err := s.FilteredStateForGroup(ctx, group, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[StateKey]string, len(entries))
	for k, e := range entries {
		out[k] = e.EventID
	}
	return out, nil
}

// LazyStateIDsAt is StateIDsAt for a lazy-loading client: it resolves the same
// point in the room but asks for only the state such a client keeps.
func (s *Store) LazyStateIDsAt(ctx context.Context, roomID string, key streamtoken.RoomKey,
	members []string) (map[StateKey]string, error) {

	group, ok, err := s.stateGroupAt(ctx, roomID, key)
	if err != nil || !ok {
		return map[StateKey]string{}, err
	}
	return s.LazyStateForGroup(ctx, group, members)
}

// stateGroupAt finds the state group in force at a stream position.
func (s *Store) stateGroupAt(ctx context.Context, roomID string, key streamtoken.RoomKey) (
	int64, bool, error) {

	const lastQ = `
		SELECT event_id FROM events
		 WHERE room_id = $1 AND stream_ordering <= $2 AND outlier = FALSE
		   AND rejection_reason IS NULL
		 ORDER BY stream_ordering DESC LIMIT 1`
	var eventID string
	err := s.queryRow(ctx, "stateGroupAt", lastQ, roomID, key.MaxStreamPos()).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: last event before position: %w", err)
	}
	groups, err := s.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return 0, false, err
	}
	group, ok := groups[eventID]
	return group, ok, nil
}

func (s *Store) StateIDsAt(ctx context.Context, roomID string, key streamtoken.RoomKey) (map[StateKey]string, error) {
	const lastQ = `
		SELECT event_id FROM events
		 WHERE room_id = $1 AND stream_ordering <= $2 AND outlier = FALSE
		   AND rejection_reason IS NULL
		 ORDER BY stream_ordering DESC LIMIT 1`
	var eventID string
	err := s.queryRow(ctx, "StateIDsAt", lastQ, roomID, key.MaxStreamPos()).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[StateKey]string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: last event before position: %w", err)
	}

	groups, err := s.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return nil, err
	}
	group, ok := groups[eventID]
	if !ok {
		return map[StateKey]string{}, nil
	}
	return s.FullStateForGroup(ctx, group)
}
