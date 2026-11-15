package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// What GET /rooms/{id}/messages reads (handlers/pagination.py get_messages,
// storage/databases/main/stream.py), and what decides whether Synapse would
// backfill (handlers/federation.py _maybe_backfill_inner).

// PageFilter is the part of a /messages filter that filter_to_clause turns
// into SQL. Each field is applied only when Python finds it truthy: an empty
// list adds no clause (the filter's own check then rejects everything), and
// contains_url adds one only when true.
type PageFilter struct {
	Types, NotTypes       []string
	Senders, NotSenders   []string
	Rooms, NotRooms       []string
	ContainsURL           bool
	Labels                []string
	RelatedBySenders      []string
	RelatedByRelTypes     []string
	RelTypes, NotRelTypes []string
}

// PageEvent is a paginated event with the depth its PDU carries, which the
// gap check reads (event.depth, not topological_ordering).
type PageEvent struct {
	TimelineEvent
	Depth int64
}

// sqlBound is _make_generic_sql_bound for PostgreSQL: the values on the
// left, the columns on the right. A nil topological bounds the stream alone.
func sqlBound(op string, topo *int64, stream int64) string {
	if topo == nil {
		return "(" + strconv.FormatInt(stream, 10) + " " + op + " event.stream_ordering)"
	}
	return "((" + strconv.FormatInt(*topo, 10) + "," + strconv.FormatInt(stream, 10) + ") " + op +
		" (event.topological_ordering,event.stream_ordering))"
}

// pageBound is generate_pagination_bounds for one token: the historical
// tuple, or for a live token its stream (its greatest position when it is
// the upper end).
func pageBound(k streamtoken.RoomKey, upper bool) (*int64, int64) {
	if k.IsHistorical() {
		t := k.Topological
		return &t, k.Stream
	}
	if upper {
		return nil, k.MaxStreamPos()
	}
	return nil, k.Stream
}

// filterClause is filter_to_clause, with PostgreSQL placeholders numbered
// from next.
func filterClause(f *PageFilter, next int) (string, []any) {
	if f == nil {
		return "", nil
	}
	var clauses []string
	var args []any
	ph := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(next+len(args)-1)
	}
	ors := func(col string, vals []string) string {
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = col + " = " + ph(v)
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	}
	if len(f.Types) > 0 {
		parts := make([]string, len(f.Types))
		for i, typ := range f.Types {
			if strings.Contains(typ, "*") {
				parts[i] = "event.type LIKE " + ph(strings.ReplaceAll(typ, "*", "%"))
			} else {
				parts[i] = "event.type = " + ph(typ)
			}
		}
		clauses = append(clauses, "("+strings.Join(parts, " OR ")+")")
	}
	for _, typ := range f.NotTypes {
		if strings.Contains(typ, "*") {
			clauses = append(clauses, "event.type NOT LIKE "+ph(strings.ReplaceAll(typ, "*", "%")))
		} else {
			clauses = append(clauses, "event.type != "+ph(typ))
		}
	}
	if len(f.Senders) > 0 {
		clauses = append(clauses, ors("event.sender", f.Senders))
	}
	for _, s := range f.NotSenders {
		clauses = append(clauses, "event.sender != "+ph(s))
	}
	if len(f.Rooms) > 0 {
		clauses = append(clauses, ors("event.room_id", f.Rooms))
	}
	for _, r := range f.NotRooms {
		clauses = append(clauses, "event.room_id != "+ph(r))
	}
	if f.ContainsURL {
		clauses = append(clauses, "event.contains_url = "+ph(true))
	}
	if len(f.Labels) > 0 {
		clauses = append(clauses, ors("label", f.Labels))
	}
	if len(f.RelatedBySenders) > 0 {
		clauses = append(clauses, ors("related_event.sender", f.RelatedBySenders))
	}
	if len(f.RelatedByRelTypes) > 0 {
		clauses = append(clauses, ors("relation_type", f.RelatedByRelTypes))
	}
	if len(f.RelTypes) > 0 {
		clauses = append(clauses, ors("event_relation.relation_type", f.RelTypes))
	}
	if len(f.NotRelTypes) > 0 {
		parts := make([]string, len(f.NotRelTypes))
		for i, t := range f.NotRelTypes {
			parts[i] = "event_relation.relation_type != " + ph(t)
		}
		clauses = append(clauses, "(("+strings.Join(parts, " AND ")+") OR event_relation.relation_type IS NULL)")
	}
	return strings.Join(clauses, " AND "), args
}

// RoomKeyBeforeOrEq is RoomStreamToken.is_before_or_eq for tokens without
// instance maps: the stream positions alone, topological parts ignored.
func RoomKeyBeforeOrEq(a, b streamtoken.RoomKey) bool { return a.Stream <= b.Stream }

// inPage is _filter_results for tokens without instance maps.
func inPage(lower, upper *streamtoken.RoomKey, topo, stream int64) bool {
	less := func(t1, s1, t2, s2 int64) bool { return t1 < t2 || (t1 == t2 && s1 < s2) }
	if lower != nil {
		if lower.IsHistorical() {
			if !less(lower.Topological, lower.Stream, topo, stream) {
				return false
			}
		} else if stream <= lower.Stream {
			return false
		}
	}
	if upper != nil {
		if upper.IsHistorical() {
			if less(upper.Topological, upper.Stream, topo, stream) {
				return false
			}
		} else if upper.Stream < stream {
			return false
		}
	}
	return true
}

// PaginateTopological is paginate_room_events_by_topological_ordering:
// events of the room between from and to, backwards or forwards, at most
// limit, the filter applied in SQL. The events come back in the order
// paginated (newest first when backwards), loaded as get_events_as_list
// loads them: rejected events and rows without a stored event dropped,
// after they took their place in the page and in the token. next is the
// token after the page (to, else from, when it is empty); limited is
// whether there may be more.
//
// Tokens carrying an instance map are the caller's to refuse: they need
// instance names, and this compares stream positions alone.
func (s *Store) PaginateTopological(ctx context.Context, roomID, roomVersion string,
	from streamtoken.RoomKey, to *streamtoken.RoomKey, backwards bool, limit int, f *PageFilter) (
	[]PageEvent, streamtoken.RoomKey, bool, error) {

	empty := from
	if to != nil {
		empty = *to
	}
	// The bail-outs before the transaction.
	if to != nil {
		if !backwards && RoomKeyBeforeOrEq(*to, from) {
			return nil, empty, false, nil
		}
		if backwards && RoomKeyBeforeOrEq(from, *to) {
			return nil, empty, false, nil
		}
	}

	order := "ASC"
	fromOp, toOp := "<", ">="
	if backwards {
		order = "DESC"
		fromOp, toOp = ">=", "<"
	}
	var bounds []string
	ft, fs := pageBound(from, backwards)
	bounds = append(bounds, sqlBound(fromOp, ft, fs))
	if to != nil {
		tt, ts := pageBound(*to, !backwards)
		bounds = append(bounds, sqlBound(toOp, tt, ts))
	}
	where := strings.Join(bounds, " AND ")

	args := []any{roomID}
	clause, fargs := filterClause(f, 2)
	if clause != "" {
		where += " AND " + clause
		args = append(args, fargs...)
	}
	requested := limit * 2
	args = append(args, requested)
	limitPH := "$" + strconv.Itoa(len(args))

	selectKW := "SELECT"
	join := ""
	distinct := false
	if f != nil && len(f.Labels) > 0 {
		join += ` LEFT JOIN event_labels USING (event_id, room_id, topological_ordering)`
		if len(f.Labels) > 1 {
			distinct = true
		}
	}
	if f != nil && (len(f.RelatedBySenders) > 0 || len(f.RelatedByRelTypes) > 0) {
		distinct = true
		join += ` LEFT JOIN event_relations AS relation ON (event.event_id = relation.relates_to_id)`
		if len(f.RelatedBySenders) > 0 {
			join += ` LEFT JOIN events AS related_event ON (relation.event_id = related_event.event_id)`
		}
	}
	if f != nil && (len(f.RelTypes) > 0 || len(f.NotRelTypes) > 0) {
		join += ` LEFT JOIN event_relations AS event_relation USING (event_id)`
	}
	if distinct {
		selectKW += " DISTINCT"
	}
	q := selectKW + ` event.event_id, event.topological_ordering, event.stream_ordering
		  FROM events AS event` + join + `
		 WHERE event.outlier = FALSE AND event.room_id = $1 AND ` + where + `
		 ORDER BY event.topological_ordering ` + order + `, event.stream_ordering ` + order + `
		 LIMIT ` + limitPH

	type row struct {
		id           string
		topo, stream int64
	}
	rows, err := s.query(ctx, "PaginateTopological", q, args...)
	if err != nil {
		return nil, from, false, fmt.Errorf("store: paginate: %w", err)
	}
	var fetched []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.topo, &r.stream); err != nil {
			rows.Close()
			return nil, from, false, fmt.Errorf("store: paginate: %w", err)
		}
		fetched = append(fetched, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, from, false, fmt.Errorf("store: paginate: %w", err)
	}

	limited := len(fetched) >= requested
	lower, upper := to, &from
	if !backwards {
		lower, upper = &from, to
	}
	kept := fetched[:0:0]
	for _, r := range fetched {
		if inPage(lower, upper, r.topo, r.stream) {
			kept = append(kept, r)
		}
	}
	if len(kept) > limit {
		limited = true
	}
	if len(kept) > limit {
		kept = kept[:limit]
	}
	if len(kept) == 0 {
		return nil, empty, limited, nil
	}
	last := kept[len(kept)-1]
	nextStream := last.stream
	if backwards {
		// generate_next_token: the token points after the last event, so
		// going backwards it steps one before it.
		nextStream--
	}
	next := streamtoken.Historical(last.topo, nextStream)

	ids := make([]string, len(kept))
	for i, r := range kept {
		ids[i] = r.id
	}
	loaded, err := s.pageEvents(ctx, roomVersion, ids)
	if err != nil {
		return nil, from, false, err
	}
	out := make([]PageEvent, 0, len(ids))
	for _, id := range ids {
		if ev, ok := loaded[id]; ok {
			out = append(out, ev)
		}
	}
	return out, next, limited, nil
}

// pageEvents is get_events_as_list for a page: the events not rejected,
// with an event_json row, by id.
func (s *Store) pageEvents(ctx context.Context, roomVersion string, ids []string) (map[string]PageEvent, error) {
	const q = `
		SELECT e.event_id, e.room_id, e.type, e.sender, e.stream_ordering,
		       e.topological_ordering, COALESCE(e.instance_name, ''),
		       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
		       ej.json, ej.internal_metadata, COALESCE(e.received_ts, 0)
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.event_id = ANY($1) AND e.rejection_reason IS NULL`
	rows, err := s.query(ctx, "PageEvents", q, ids)
	if err != nil {
		return nil, fmt.Errorf("store: page events: %w", err)
	}
	defer rows.Close()
	out := make(map[string]PageEvent, len(ids))
	for rows.Next() {
		var ev PageEvent
		if err := rows.Scan(&ev.EventID, &ev.RoomID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ev.ReceivedTS); err != nil {
			return nil, fmt.Errorf("store: page events: %w", err)
		}
		ev.RoomVersion = roomVersion
		ev.Depth = gjson.GetBytes(ev.JSON, "depth").Int()
		out[ev.EventID] = ev
	}
	return out, rows.Err()
}

// MaxTopological is _get_max_topological_txn: the room's greatest
// topological ordering, 0 for none.
func (s *Store) MaxTopological(ctx context.Context, roomID string) (int64, error) {
	var topo *int64
	err := s.queryRow(ctx, "MaxTopological",
		`SELECT MAX(topological_ordering) FROM events WHERE room_id = $1`, roomID).Scan(&topo)
	if err != nil {
		return 0, fmt.Errorf("store: max topological: %w", err)
	}
	if topo == nil {
		return 0, nil
	}
	return *topo, nil
}

// CurrentTopological is get_current_topological_token: the least
// topological ordering at or after the stream position, the position
// rolled back to the room's last event; 0 for none.
func (s *Store) CurrentTopological(ctx context.Context, roomID string, stream int64) (int64, error) {
	const q = `
		WITH fallback(max_stream_ordering) AS (
			SELECT MAX(stream_ordering) FROM events WHERE room_id = $1
		)
		SELECT COALESCE(MIN(topological_ordering), 0) FROM events
		 WHERE room_id = $1
		   AND events.stream_ordering >= LEAST($2::bigint, (SELECT max_stream_ordering FROM fallback))`
	var topo int64
	if err := s.queryRow(ctx, "CurrentTopological", q, roomID, stream).Scan(&topo); err != nil {
		return 0, fmt.Errorf("store: current topological: %w", err)
	}
	return topo, nil
}

// ErrNoEvent is an event the store does not have.
var ErrNoEvent = errors.New("store: no such event")

// TopologicalTokenForEvent is get_topological_token_for_event.
func (s *Store) TopologicalTokenForEvent(ctx context.Context, eventID string) (streamtoken.RoomKey, error) {
	var stream int64
	var topo *int64
	err := s.queryRow(ctx, "TopologicalTokenForEvent",
		`SELECT stream_ordering, topological_ordering FROM events WHERE event_id = $1`, eventID).
		Scan(&stream, &topo)
	if errors.Is(err, pgx.ErrNoRows) {
		return streamtoken.RoomKey{}, ErrNoEvent
	}
	if err != nil {
		return streamtoken.RoomKey{}, fmt.Errorf("store: topological token: %w", err)
	}
	if topo == nil {
		return streamtoken.RoomKey{}, ErrNoEvent
	}
	return streamtoken.Historical(*topo, stream), nil
}

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

// BackfillPoints is get_backfill_points_in_room: backward extremities
// referenced by events at depth at most nearbyDepth, not in their failed
// pull backoff at nowMS, closest first.
func (s *Store) BackfillPoints(ctx context.Context, roomID string, nearbyDepth, nowMS int64, limit int) (
	[]BackfillPoint, error) {
	const q = `
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
	rows, err := s.query(ctx, "BackfillPoints", q, roomID, nearbyDepth, nowMS,
		backfillBackoffMaxDoublings, backfillBackoffStepMS, limit)
	if err != nil {
		return nil, fmt.Errorf("store: backfill points: %w", err)
	}
	defer rows.Close()
	var out []BackfillPoint
	for rows.Next() {
		var p BackfillPoint
		if err := rows.Scan(&p.EventID, &p.Depth); err != nil {
			return nil, fmt.Errorf("store: backfill points: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// purgePaginationLock is PURGE_PAGINATION_LOCK_NAME.
const purgePaginationLock = "purge_pagination_lock"

// lockTimeoutMS is _LOCK_TIMEOUT: a lock not renewed for this long is
// stale, and reaped.
const lockTimeoutMS = 2 * 60 * 1000

// PurgeLocked reports a live write lock on the room's purge/pagination
// lock: a purge in progress, which maybe_backfill waits for.
func (s *Store) PurgeLocked(ctx context.Context, roomID string, nowMS int64) (bool, error) {
	var one int
	err := s.queryRow(ctx, "PurgeLocked", `
		SELECT 1 FROM worker_read_write_locks
		 WHERE lock_name = $1 AND lock_key = $2 AND write_lock AND last_renewed_ts >= $3
		 LIMIT 1`, purgePaginationLock, roomID, nowMS-lockTimeoutMS).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: purge lock: %w", err)
	}
	return true, nil
}

// MemberStateAtEvent is get_state_ids_for_event with a filter of the
// members given, then get_events: their member events in the state after
// eventID, sorted by state key. Synapse's order comes from a frozenset and
// differs between its processes. Rejected and missing events are left out;
// found is whether the state named any, since Synapse then answers with a
// state block even if none loads.
func (s *Store) MemberStateAtEvent(ctx context.Context, eventID, roomVersion string, members []string) (
	events []StateEvent, found bool, err error) {
	groups, err := s.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return nil, false, err
	}
	group, ok := groups[eventID]
	if !ok {
		return nil, false, ErrNoStateGroup
	}
	full, err := s.FullStateForGroup(ctx, group)
	if err != nil {
		return nil, false, err
	}
	keys := append([]string(nil), members...)
	sort.Strings(keys)
	var ids []string
	for _, m := range keys {
		if id, ok := full[StateKey{Type: "m.room.member", StateKey: m}]; ok {
			ids = append(ids, id)
		}
	}
	loaded, err := s.eventsNotRejected(ctx, ids, roomVersion)
	if err != nil {
		return nil, false, err
	}
	out := make([]StateEvent, 0, len(ids))
	for _, id := range ids {
		if ev, ok := loaded[id]; ok {
			out = append(out, ev)
		}
	}
	return out, len(ids) > 0, nil
}
