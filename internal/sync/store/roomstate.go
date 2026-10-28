package store

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
)

// What GET /rooms/{id}/state and /rooms/{id}/state/{type}/{key} read
// (handlers/message.py get_state_events, get_room_data).

// LocalMembership is get_local_current_membership_for_user_in_room: the
// user's membership from local_current_membership, which is what
// check_user_in_room reads -- not current_state_events.
func (s *Store) LocalMembership(ctx context.Context, roomID, userID string) (Membership, error) {
	const q = `
		SELECT membership, event_id FROM local_current_membership
		 WHERE room_id = $1 AND user_id = $2`
	var m Membership
	err := s.queryRow(ctx, "LocalMembership", q, roomID, userID).Scan(&m.Membership, &m.EventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, nil
	}
	if err != nil {
		return Membership{}, fmt.Errorf("store: local membership: %w", err)
	}
	return m, nil
}

// DidForget is did_forget: no membership of the user's in the room is still
// unforgotten.
func (s *Store) DidForget(ctx context.Context, userID, roomID string) (bool, error) {
	const q = `
		SELECT COUNT(*) FROM room_memberships
		 WHERE user_id = $1 AND room_id = $2 AND forgotten = 0`
	var n int64
	if err := s.queryRow(ctx, "DidForget", q, userID, roomID).Scan(&n); err != nil {
		return false, fmt.Errorf("store: did forget: %w", err)
	}
	return n == 0, nil
}

// IsPartialStateRoom reports a room joined over faster joins whose state is
// not yet complete. Synapse makes a read of its current state wait for the
// rest (await_full_state); this worker leaves such a room to Synapse.
func (s *Store) IsPartialStateRoom(ctx context.Context, roomID string) (bool, error) {
	var one int
	err := s.queryRow(ctx, "IsPartialStateRoom",
		`SELECT 1 FROM partial_state_rooms WHERE room_id = $1`, roomID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: partial state room: %w", err)
	}
	return true, nil
}

// IsServerAdmin is is_server_admin: users.admin, false for no such user.
func (s *Store) IsServerAdmin(ctx context.Context, userID string) (bool, error) {
	var admin int
	err := s.queryRow(ctx, "IsServerAdmin",
		`SELECT admin FROM users WHERE name = $1`, userID).Scan(&admin)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: server admin: %w", err)
	}
	return admin != 0, nil
}

// CurrentStateInRowOrder is get_current_state_ids then get_events: the
// room's current state, or the one key asked for, in the order the rows
// come -- the order of the dict Synapse builds from the same query, which
// has no ORDER BY. A row whose event is not stored is left out, as
// get_events leaves it out.
func (s *Store) CurrentStateInRowOrder(ctx context.Context, roomID, roomVersion string, key *StateKey) ([]StateEvent, error) {
	q := `
		SELECT cse.event_id, cse.type, cse.state_key, e.sender, ej.json, ej.internal_metadata
		  FROM current_state_events cse
		  JOIN events e USING (event_id)
		  JOIN event_json ej USING (event_id)
		 WHERE cse.room_id = $1`
	args := []any{roomID}
	if key != nil {
		q += ` AND cse.type = $2 AND cse.state_key = $3`
		args = append(args, key.Type, key.StateKey)
	}
	rows, err := s.query(ctx, "CurrentStateInRowOrder", q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: current state: %w", err)
	}
	defer rows.Close()
	var out []StateEvent
	for rows.Next() {
		var ev StateEvent
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.StateKey, &ev.Sender,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, fmt.Errorf("store: current state: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: current state: %w", err)
	}
	return out, nil
}

// ErrNoStateGroup is an event without a state group: an outlier, such as
// the leave from rejecting a remote invite. Synapse fails the request
// ("Unable to find state groups for events").
var ErrNoStateGroup = errors.New("no state group for event")

// StateAtEvent is get_state_for_events for one event: the state after it,
// whole, or the one key asked for, then get_events -- in (type, state_key)
// order, as _get_state_groups_from_groups_txn returns it. Rejected and
// missing events are left out.
func (s *Store) StateAtEvent(ctx context.Context, eventID, roomID, roomVersion string, key *StateKey) ([]StateEvent, error) {
	groups, err := s.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return nil, err
	}
	group, ok := groups[eventID]
	if !ok {
		return nil, ErrNoStateGroup
	}
	full, _, err := s.caches.states.Load(ctx, group, stateLoader{s})
	if err != nil {
		return nil, err
	}

	keys := make([]statecache.Key, 0, len(full))
	if key != nil {
		k := statecache.Key{Type: key.Type, StateKey: key.StateKey}
		if _, ok := full[k]; ok {
			keys = append(keys, k)
		}
	} else {
		for k := range full {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].Type != keys[j].Type {
				return keys[i].Type < keys[j].Type
			}
			return keys[i].StateKey < keys[j].StateKey
		})
	}
	ids := make([]string, len(keys))
	for i, k := range keys {
		ids[i] = full[k]
	}
	events, err := s.eventsNotRejected(ctx, ids, roomVersion)
	if err != nil {
		return nil, err
	}
	out := make([]StateEvent, 0, len(ids))
	for _, id := range ids {
		if ev, ok := events[id]; ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

// eventsNotRejected is get_events with allow_rejected=False.
func (s *Store) eventsNotRejected(ctx context.Context, eventIDs []string, roomVersion string) (map[string]StateEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT e.event_id, e.room_id, e.type, COALESCE(e.state_key, ''), e.sender,
		       ej.json, ej.internal_metadata
		  FROM events e JOIN event_json ej USING (event_id)
		  LEFT JOIN rejections r USING (event_id)
		 WHERE e.event_id = ANY($1) AND r.event_id IS NULL`
	rows, err := s.query(ctx, "EventsNotRejected", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: events: %w", err)
	}
	defer rows.Close()
	out := make(map[string]StateEvent, len(eventIDs))
	for rows.Next() {
		var ev StateEvent
		if err := rows.Scan(&ev.EventID, &ev.RoomID, &ev.Type, &ev.StateKey, &ev.Sender,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, fmt.Errorf("store: events: %w", err)
		}
		ev.RoomVersion = roomVersion
		out[ev.EventID] = ev
	}
	return out, rows.Err()
}

// stateLoader reads state groups for the worker's one state cache, as the
// federation reads and inbound do.
type stateLoader struct{ s *Store }

func (l stateLoader) Delta(ctx context.Context, group int64) (int64, bool, statecache.Map, error) {
	var prev int64
	err := l.s.queryRow(ctx, "StateGroupEdge",
		`SELECT prev_state_group FROM state_group_edges WHERE state_group = $1`, group).Scan(&prev)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil, nil
	case err != nil:
		return 0, false, nil, fmt.Errorf("store: state edge: %w", err)
	}
	rows, err := l.s.query(ctx, "StateGroupDelta",
		`SELECT type, state_key, event_id FROM state_groups_state WHERE state_group = $1`, group)
	if err != nil {
		return 0, false, nil, fmt.Errorf("store: state delta: %w", err)
	}
	defer rows.Close()
	delta := statecache.Map{}
	for rows.Next() {
		var k statecache.Key
		var id string
		if err := rows.Scan(&k.Type, &k.StateKey, &id); err != nil {
			return 0, false, nil, fmt.Errorf("store: state delta: %w", err)
		}
		delta[k] = id
	}
	return prev, true, delta, rows.Err()
}

// Full is _get_state_groups_from_groups_txn for one group. Synapse turns
// sequential scans off for it: state_groups_state is the largest table,
// and the planner otherwise reads it end to end.
func (l stateLoader) Full(ctx context.Context, group int64) (statecache.Map, error) {
	tx, err := l.s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}
	rows, err := txQuery(ctx, tx, "StateGroupFull", `
		WITH RECURSIVE sgs(state_group) AS (
			VALUES($1::bigint)
		  UNION ALL
			SELECT prev_state_group FROM state_group_edges e, sgs s
			WHERE s.state_group = e.state_group
		)
		SELECT DISTINCT ON (type, state_key) type, state_key, event_id
		  FROM state_groups_state
		 INNER JOIN sgs USING (state_group)
		 ORDER BY type, state_key, state_group DESC`, group)
	if err != nil {
		return nil, fmt.Errorf("store: state for group: %w", err)
	}
	defer rows.Close()
	out := statecache.Map{}
	for rows.Next() {
		var k statecache.Key
		var id string
		if err := rows.Scan(&k.Type, &k.StateKey, &id); err != nil {
			return nil, fmt.Errorf("store: state for group: %w", err)
		}
		out[k] = id
	}
	return out, rows.Err()
}
