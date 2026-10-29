package store

import (
	"context"
	"fmt"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// StateDelta is one current_state_delta_stream row.
type StateDelta struct {
	Key StateKey
	// EventID is empty when the key was removed from current state: the
	// server left the room (its last local user left), or a state reset.
	EventID string
	// PrevEventID is what the key held before, empty if nothing.
	PrevEventID string
}

// RoomStateDeltas is get_current_state_deltas_for_room_by_event_position
// (storage/databases/main/state_deltas.py, v1.164.1): the room's state
// deltas in (from, to], a nil bound being unbounded, in stream_id order.
//
// A delta row's stream_id is the FIRST stream ordering of its persist batch,
// so a state event persisted mid-batch has stream_id < stream_ordering, and a
// token -- which advances one event at a time on replication -- routinely
// falls inside a batch. Bounding on stream_id alone misses a delta whose event
// is in the window and includes one whose event is past it (Synapse #20171).
// So a delta is positioned at the later of its stream_id and its event's
// stream_ordering. Only the two batches a token can cut need the event's
// position: the latest stream_id at or before each token, per writer.
func (s *Store) RoomStateDeltas(ctx context.Context, roomID string, from, to *streamtoken.RoomKey) ([]StateDelta, error) {
	args := []any{roomID}
	q := `SELECT instance_name, stream_id, type, state_key, event_id, prev_event_id
		    FROM current_state_delta_stream
		   WHERE room_id = $1`
	if from != nil {
		// The from-token batch's stream_id, the latest at or before the
		// token's base position: fetch from it inclusive, so its rows are
		// judged on their events below.
		var batch *int64
		if err := s.queryRow(ctx, "RoomStateDeltas", `
			SELECT MAX(stream_id) FROM current_state_delta_stream
			 WHERE room_id = $1 AND stream_id <= $2`, roomID, from.Stream).Scan(&batch); err != nil {
			return nil, fmt.Errorf("store: room state deltas: %w", err)
		}
		if batch != nil {
			args = append(args, *batch)
			q += fmt.Sprintf(" AND $%d <= stream_id", len(args))
		} else {
			args = append(args, from.Stream)
			q += fmt.Sprintf(" AND $%d < stream_id", len(args))
		}
	}
	if to != nil {
		args = append(args, to.MaxStreamPos())
		q += fmt.Sprintf(" AND stream_id <= $%d", len(args))
	}
	q += " ORDER BY stream_id ASC"

	type row struct {
		instance       string
		streamID       int64
		delta          StateDelta
		hasEvent, keep bool
	}
	rs, err := s.query(ctx, "RoomStateDeltas", q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: room state deltas: %w", err)
	}
	var rows []row
	for rs.Next() {
		var r row
		var instance, eventID, prevEventID *string
		if err := rs.Scan(&instance, &r.streamID, &r.delta.Key.Type, &r.delta.Key.StateKey,
			&eventID, &prevEventID); err != nil {
			rs.Close()
			return nil, fmt.Errorf("store: room state deltas: %w", err)
		}
		// Historic rows have no instance name and count as master.
		r.instance = "master"
		if instance != nil && *instance != "" {
			r.instance = *instance
		}
		if eventID != nil {
			r.delta.EventID, r.hasEvent = *eventID, true
		}
		if prevEventID != nil {
			r.delta.PrevEventID = *prevEventID
		}
		rows = append(rows, r)
	}
	rs.Close()
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("store: room state deltas: %w", err)
	}

	instances, err := s.InstanceIDs(ctx)
	if err != nil {
		return nil, err
	}
	posFor := func(k streamtoken.RoomKey, instance string) int64 {
		id, ok := instances[instance]
		if !ok {
			id = -1
		}
		return k.StreamPosForInstance(id)
	}

	// The token batches, per writer: the latest stream_id at or before the
	// writer's position in each token.
	type batchKey struct {
		instance string
		streamID int64
	}
	tokenBatches := map[batchKey]bool{}
	for _, k := range []*streamtoken.RoomKey{from, to} {
		if k == nil {
			continue
		}
		latest := map[string]int64{}
		for _, r := range rows {
			if r.streamID <= posFor(*k, r.instance) {
				if cur, ok := latest[r.instance]; !ok || r.streamID > cur {
					latest[r.instance] = r.streamID
				}
			}
		}
		for inst, id := range latest {
			tokenBatches[batchKey{inst, id}] = true
		}
	}
	var lookup []string
	for _, r := range rows {
		if r.hasEvent && tokenBatches[batchKey{r.instance, r.streamID}] {
			lookup = append(lookup, r.delta.EventID)
		}
	}
	type position struct {
		instance string
		stream   int64
	}
	positions := map[string]position{}
	if len(lookup) > 0 {
		ps, err := s.query(ctx, "RoomStateDeltas", `
			SELECT event_id, COALESCE(instance_name, ''), stream_ordering
			  FROM events WHERE event_id = ANY($1)`, lookup)
		if err != nil {
			return nil, fmt.Errorf("store: room state delta positions: %w", err)
		}
		for ps.Next() {
			var id string
			var p position
			var stream *int64
			if err := ps.Scan(&id, &p.instance, &stream); err != nil {
				ps.Close()
				return nil, fmt.Errorf("store: room state delta positions: %w", err)
			}
			if stream != nil {
				p.stream = *stream
				positions[id] = p
			}
		}
		ps.Close()
		if err := ps.Err(); err != nil {
			return nil, fmt.Errorf("store: room state delta positions: %w", err)
		}
	}

	var out []StateDelta
	for _, r := range rows {
		instance, stream := r.instance, r.streamID
		if r.hasEvent && tokenBatches[batchKey{r.instance, r.streamID}] {
			if p, ok := positions[r.delta.EventID]; ok && p.stream > r.streamID {
				instance, stream = p.instance, p.stream
			}
		}
		if from != nil && stream <= posFor(*from, nonEmpty(instance)) {
			continue
		}
		if to != nil && stream > posFor(*to, nonEmpty(instance)) {
			continue
		}
		out = append(out, r.delta)
	}
	return out, nil
}

func nonEmpty(instance string) string {
	if instance == "" {
		return "master"
	}
	return instance
}

// LazyCurrentStateIDs is the room's current state restricted as
// StateFilter.from_lazy_load_member_list restricts it: every non-member
// entry, and member entries for the named users only. A big room's state is
// mostly members, so this is what keeps a lazy-loading sync cheap.
func (s *Store) LazyCurrentStateIDs(ctx context.Context, roomID string, members []string) (map[StateKey]string, error) {
	const q = `
		SELECT type, state_key, event_id FROM current_state_events
		 WHERE room_id = $1 AND (type <> 'm.room.member' OR state_key = ANY($2))`
	rows, err := s.query(ctx, "LazyCurrentStateIDs", q, roomID, members)
	if err != nil {
		return nil, fmt.Errorf("store: lazy current state ids: %w", err)
	}
	defer rows.Close()
	state := map[StateKey]string{}
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: lazy current state ids: %w", err)
		}
		state[k] = eventID
	}
	return state, rows.Err()
}
