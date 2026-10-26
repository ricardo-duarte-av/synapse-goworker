package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// ErrNoSuchFilter is returned when the user has no filter with that ID.
//
// Synapse surfaces the same condition as a 404 from the storage layer and the
// REST layer rewrites it to a 400 "No such filter"; the split is kept here so
// the handler owns the HTTP shape.
var ErrNoSuchFilter = errors.New("store: no such filter")

// UserFilter loads a filter the client previously uploaded.
//
// Keyed on `full_user_id`, not `user_id`: the latter is a legacy localpart
// column that a background migration is still backfilling the other way round,
// and two users on different servers could collide on it.
// Cached without invalidation: `user_filters` is append-only. A client
// uploads a filter, gets an id back and reuses that id for the life of the
// login; the row for an id is never rewritten. Only deletion could make this
// wrong, which is what the `caches` stream and the drop purge cover.
func (s *Store) UserFilter(ctx context.Context, userID string, filterID int64) ([]byte, error) {
	key := userID + "\x00" + strconv.FormatInt(filterID, 10)
	if raw, ok := s.derived.userFilter.Get(key); ok {
		return copyBytes(raw), nil
	}
	raw, err := s.userFilter(ctx, userID, filterID)
	if err != nil {
		return nil, err
	}
	s.derived.userFilter.Add(key, raw)
	return copyBytes(raw), nil
}

func (s *Store) userFilter(ctx context.Context, userID string, filterID int64) ([]byte, error) {
	const q = `
		SELECT filter_json
		  FROM user_filters
		 WHERE full_user_id = $1 AND filter_id = $2`
	var raw []byte
	err := s.queryRow(ctx, "UserFilter", q, userID, filterID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchFilter
	}
	if err != nil {
		return nil, fmt.Errorf("store: user filter: %w", err)
	}
	return raw, nil
}

// EventsWithRelations returns which of the given events have a relation from
// one of the given senders, of one of the given types.
//
// This backs a filter's `related_by_senders` / `related_by_rel_types`. With
// neither constraint set every event qualifies, which Synapse short-circuits
// before touching the database and so do we.
func (s *Store) EventsWithRelations(ctx context.Context, parentIDs, senders, relTypes []string) (map[string]bool, error) {
	out := make(map[string]bool, len(parentIDs))
	if len(senders) == 0 && len(relTypes) == 0 {
		for _, id := range parentIDs {
			out[id] = true
		}
		return out, nil
	}
	if len(parentIDs) == 0 {
		return out, nil
	}

	q := `
		SELECT relates_to_id
		  FROM event_relations
		  JOIN events USING (event_id)
		 WHERE relates_to_id = ANY($1)`
	args := []any{parentIDs}
	if len(senders) > 0 {
		args = append(args, senders)
		q += fmt.Sprintf(" AND sender = ANY($%d)", len(args))
	}
	if len(relTypes) > 0 {
		args = append(args, relTypes)
		q += fmt.Sprintf(" AND relation_type = ANY($%d)", len(args))
	}

	rows, err := s.query(ctx, "EventsWithRelations", q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: events with relations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: events with relations: %w", err)
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: events with relations: %w", err)
	}
	return out, nil
}

// InstanceIDs maps a writer's name to the id a stream token records it under.
//
// A token carries integer instance ids; the event tables carry names. Comparing
// a row against a vector-clock token needs the mapping, and `instance_map` is
// append-only, so it is read once and kept.
func (s *Store) InstanceIDs(ctx context.Context) (map[string]int, error) {
	s.instanceMu.Lock()
	defer s.instanceMu.Unlock()
	if s.instanceIDs != nil {
		return s.instanceIDs, nil
	}
	rows, err := s.query(ctx, "InstanceIDs", `SELECT instance_name, instance_id FROM instance_map`)
	if err != nil {
		return nil, fmt.Errorf("store: instance map: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var name string
		var id int
		if err := rows.Scan(&name, &id); err != nil {
			return nil, fmt.Errorf("store: instance map: %w", err)
		}
		out[name] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: instance map: %w", err)
	}
	s.instanceIDs = out
	return out, nil
}

// TimelineGap reports whether the room's timeline has a hole in (from, to], and
// where.
//
// A gap is recorded when Synapse persists an event whose predecessors it did
// not already have -- typically a federated room going quiet and then coming
// back. The events either side are contiguous in stream ordering but not in the
// room's own history, so a client handed both would silently miss whatever fell
// in between.
//
// Synapse's answer is a token positioned so the event AT the gap is included,
// since the gap is *before* that event. It has two effects that are easy to
// miss: the room's timeline is reported `limited` even when nothing was
// trimmed, and an incremental sync stops paginating at the gap rather than at
// the client's `since`.
//
// This deployment has 99,053 gap rows across 1,392 rooms, so it is not an edge
// case -- it is merely invisible whenever the timeline was long enough to be
// trimmed, which with the default filter is nearly always.
// TimelineGap reports whether a room has a hole in its history inside the
// window, and where.
//
// A thin wrapper over TimelineGaps: the single-room form is what the legacy
// endpoints and the archived-room path want, and there is no reason for two
// copies of the per-writer arithmetic below.
func (s *Store) TimelineGap(ctx context.Context, roomID string,
	from *streamtoken.RoomKey, to streamtoken.RoomKey) (int64, bool, error) {

	gaps, err := s.TimelineGaps(ctx, []string{roomID}, from, to)
	if err != nil {
		return 0, false, err
	}
	stream, ok := gaps[roomID]
	return stream, ok, nil
}

// TimelineGaps reports history holes for several rooms in one query.
//
// The map holds only rooms that have a gap; absent means none. The value is
// already the token position a caller should rewind to, not the raw gap.
//
// One query rather than one per room. This is the most-called query on the
// worker -- an incremental sync asks it about every room the user is in,
// whether or not anything happened there, which on a 654-room account was 653
// round trips for a response that mentioned two rooms. Nothing about the
// answer changes: same rows, same arithmetic, same bounds.
func (s *Store) TimelineGaps(ctx context.Context, roomIDs []string,
	from *streamtoken.RoomKey, to streamtoken.RoomKey) (map[string]int64, error) {

	if len(roomIDs) == 0 {
		return nil, nil
	}
	fromStream := int64(0)
	if from != nil {
		fromStream = from.Stream
	}

	// A timeline_gaps row is written in the same transaction as an event in the
	// same room (`storage/databases/main/events.py`), so a room with no event
	// in the window has no gap in it either. Narrowing the room list here turns
	// the common quiet sync -- nothing happened anywhere -- into no query at
	// all.
	//
	// from.Stream rather than MaxStreamPos: for a vector-clock key it is the
	// lower bound, so the narrowing errs towards keeping rooms in. Below the
	// horizon the cache returns the list unchanged and this is a no-op.
	//
	// Do NOT try to infer this from an empty timeline instead: RoomTimelineSince
	// drops rejected events in Go, after the window function, so an empty slice
	// does not prove nothing was persisted.
	if roomIDs = s.RoomsWithEventsSince(roomIDs, fromStream); len(roomIDs) == 0 {
		return nil, nil
	}

	const q = `
		SELECT room_id, COALESCE(instance_name, ''), stream_ordering
		  FROM timeline_gaps
		 WHERE room_id = ANY($1) AND stream_ordering > $2 AND stream_ordering <= $3
		 ORDER BY stream_ordering`
	rows, err := s.query(ctx, "TimelineGaps", q, roomIDs, fromStream, to.MaxStreamPos())
	if err != nil {
		return nil, fmt.Errorf("store: timeline gaps: %w", err)
	}
	defer rows.Close()

	type gap struct {
		instance string
		stream   int64
	}
	byRoom := map[string][]gap{}
	for rows.Next() {
		var roomID string
		var g gap
		if err := rows.Scan(&roomID, &g.instance, &g.stream); err != nil {
			return nil, fmt.Errorf("store: timeline gaps: %w", err)
		}
		byRoom[roomID] = append(byRoom[roomID], g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: timeline gaps: %w", err)
	}
	if len(byRoom) == 0 {
		return nil, nil
	}

	// Only fetched when a gap actually exists, which is the rare case. The
	// instance map is read once per process, so this is not a round trip after
	// the first, but it is still work worth not doing 653 times.
	ids, err := s.InstanceIDs(ctx)
	if err != nil {
		return nil, err
	}

	out := map[string]int64{}
	for roomID, gaps := range byRoom {
		// persisted_after(token) is per writer: a gap counts only if the
		// token's position FOR THE WRITER THAT RECORDED IT is behind it. With
		// a sharded event persister the plain stream comparison in the query
		// above is only an approximation, and both bounds have to be
		// re-checked here.
		last := int64(0)
		found := false
		for _, g := range gaps {
			id, ok := ids[g.instance]
			if !ok {
				// An unknown writer cannot be placed per instance; fall back
				// to the agreed minimum, which is what a token without that
				// writer means.
				id = -1
			}
			if from != nil && !(from.StreamPosForInstance(id) < g.stream) {
				continue
			}
			if to.StreamPosForInstance(id) < g.stream {
				continue
			}
			last, found = g.stream, true
		}
		if found {
			// The gap sits *before* the event at this position, so the token
			// has to be one below it for that event to be included.
			out[roomID] = last - 1
		}
	}
	return out, nil
}
