package store

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// StateKey identifies a piece of room state.
type StateKey struct {
	Type     string
	StateKey string
}

// StateEntry is a resolved state event, with the two fields visibility and
// membership annotation actually need.
//
// Resolving state yields event IDs; loading each event to read one field would
// be a second round trip per entry. These two are pulled alongside.
type StateEntry struct {
	EventID string
	// Membership is set for m.room.member events, from room_memberships.
	Membership string
	// HistoryVisibility is set for m.room.history_visibility events.
	HistoryVisibility string
}

// StateGroupsForEvents maps event IDs to their state groups.
//
// A state group is the state *after* the event, so a state event's own group
// includes it. An outlier has a row with a NULL state group -- we hold the
// event but not the state around it -- and is omitted from the result rather
// than reported as an error, matching Synapse, which excludes outliers from the
// state fetch and handles them separately.
func (s *Store) StateGroupsForEvents(ctx context.Context, eventIDs []string) (map[string]int64, error) {
	out := make(map[string]int64, len(eventIDs))
	missing := make([]string, 0, len(eventIDs))
	for _, id := range eventIDs {
		if g, ok := s.caches.eventStateGroup.Get(id); ok {
			out[id] = g
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) == 0 {
		return out, nil
	}

	const q = `
		SELECT event_id, state_group FROM event_to_state_groups
		 WHERE event_id = ANY($1) AND state_group IS NOT NULL`
	rows, err := s.query(ctx, "StateGroupsForEvents", q, missing)
	if err != nil {
		return nil, fmt.Errorf("store: state groups for events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var group int64
		if err := rows.Scan(&id, &group); err != nil {
			return nil, fmt.Errorf("store: state groups for events: %w", err)
		}
		// An event's state group never changes once assigned.
		s.caches.eventStateGroup.Add(id, group)
		out[id] = group
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: state groups for events: %w", err)
	}
	return out, nil
}

// stateGroupFilteredQuery walks the state group delta chain and takes the
// newest row per (type, state_key), restricted to the wanted keys.
//
// State groups form a chain of deltas: a group either holds full state or a
// delta against a previous one, linked by state_group_edges. Resolving means
// collecting the whole chain and letting the highest-numbered group win for
// each key, which DISTINCT ON with a descending sort does in one pass. Ported
// from Synapse's _get_state_groups_from_groups_txn.
//
// The filter is not an optimisation. Visibility needs two keys -- the history
// visibility and one user's membership -- and resolving the whole map to get
// them would mean reading every state event in the room, which is over a
// hundred thousand in the largest room on this server, for a single event.
const stateGroupFilteredQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (sgs2.type, sgs2.state_key)
	       sgs2.type, sgs2.state_key, sgs2.event_id,
	       COALESCE(m.membership, ''),
	       COALESCE(ej.json::jsonb -> 'content' ->> 'history_visibility', '')
	  FROM state_groups_state sgs2
	  INNER JOIN sgs USING (state_group)
	  LEFT JOIN room_memberships m ON m.event_id = sgs2.event_id
	  LEFT JOIN event_json ej ON ej.event_id = sgs2.event_id
	 WHERE (sgs2.type, sgs2.state_key) IN (SELECT * FROM unnest($2::text[], $3::text[]))
	 ORDER BY sgs2.type, sgs2.state_key, sgs2.state_group DESC`

// FilteredStateForGroup resolves the state at a group for the given keys only.
//
// state_groups_state is by a wide margin the largest table in a Synapse
// database -- 17GB here -- and the planner will choose a sequential scan over
// it without help. Synapse disables seqscan for this transaction and so must
// we: the query is otherwise pathological on a large room. SET LOCAL scopes the
// change to the transaction, which also keeps it safe behind a
// transaction-mode pooler.
func (s *Store) FilteredStateForGroup(ctx context.Context, group int64, keys []StateKey) (map[StateKey]StateEntry, error) {
	cacheKey := filteredStateKey(group, keys)
	if state, ok := s.caches.filteredState.Get(cacheKey); ok {
		return state, nil
	}

	types := make([]string, len(keys))
	stateKeys := make([]string, len(keys))
	for i, k := range keys {
		types[i], stateKeys[i] = k.Type, k.StateKey
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}

	rows, err := txQuery(ctx, tx, "FilteredStateForGroup", stateGroupFilteredQuery, group, types, stateKeys)
	if err != nil {
		return nil, fmt.Errorf("store: filtered state for group: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]StateEntry, len(keys))
	for rows.Next() {
		var k StateKey
		var e StateEntry
		if err := rows.Scan(&k.Type, &k.StateKey, &e.EventID, &e.Membership, &e.HistoryVisibility); err != nil {
			return nil, fmt.Errorf("store: filtered state for group: %w", err)
		}
		state[k] = e
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: filtered state for group: %w", err)
	}

	// Safe to cache without invalidation: a state group's contents are fixed
	// once written, so a filtered view of one is fixed too.
	s.caches.filteredState.Add(cacheKey, state)
	return state, nil
}

// filteredStateKey builds a cache key that is stable across caller orderings.
func filteredStateKey(group int64, keys []StateKey) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k.Type + "\x00" + k.StateKey
	}
	sort.Strings(parts)
	return fmt.Sprintf("%d\x01%s", group, strings.Join(parts, "\x02"))
}

// PurgeCaches empties the state caches.
//
// Everything cached is immutable, so this is not needed on the write path. It
// is here for the replication subscriber to call when it loses its connection
// and can no longer see rooms being purged underneath us.
func (s *Store) PurgeCaches() {
	s.caches.eventStateGroup.Purge()
	s.caches.filteredState.Purge()
}

// CacheLen reports the cached entry counts, for metrics.
func (s *Store) CacheLen() (eventGroups, filteredState int) {
	return s.caches.eventStateGroup.Len(), s.caches.filteredState.Len()
}

// stateGroupTypeFilteredQuery is stateGroupFilteredQuery with an additional
// wildcard clause: whole event types, not just named (type, state_key) pairs.
//
// Sliding sync's `required_state` can ask for every state key of a type --
// `["m.room.member", "*"]` is the common one -- and resolving the room's whole
// state to satisfy that would read the largest table in the database for a room
// that may have a hundred thousand state events. Synapse pushes the same
// wildcard down to SQL (`StateFilter` with a `None` state key); this is the
// same idea in one more clause.
const stateGroupTypeFilteredQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (sgs2.type, sgs2.state_key)
	       sgs2.type, sgs2.state_key, sgs2.event_id,
	       COALESCE(m.membership, ''),
	       COALESCE(ej.json::jsonb -> 'content' ->> 'history_visibility', '')
	  FROM state_groups_state sgs2
	  INNER JOIN sgs USING (state_group)
	  LEFT JOIN room_memberships m ON m.event_id = sgs2.event_id
	  LEFT JOIN event_json ej ON ej.event_id = sgs2.event_id
	 WHERE (sgs2.type, sgs2.state_key) IN (SELECT * FROM unnest($2::text[], $3::text[]))
	    OR sgs2.type = ANY($4::text[])
	 ORDER BY sgs2.type, sgs2.state_key, sgs2.state_group DESC`

// StateForGroupWithWildcards resolves state at a group for named keys plus
// every key of the given types.
func (s *Store) StateForGroupWithWildcards(
	ctx context.Context, group int64, keys []StateKey, wildcardTypes []string,
) (map[StateKey]StateEntry, error) {

	if len(wildcardTypes) == 0 {
		return s.FilteredStateForGroup(ctx, group, keys)
	}

	cacheKey := filteredStateKey(group, keys) + "\x03" + strings.Join(sortedCopy(wildcardTypes), "\x02")
	if state, ok := s.caches.filteredState.Get(cacheKey); ok {
		return state, nil
	}

	types := make([]string, len(keys))
	stateKeys := make([]string, len(keys))
	for i, k := range keys {
		types[i], stateKeys[i] = k.Type, k.StateKey
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}

	rows, err := txQuery(ctx, tx, "StateForGroupWithWildcards",
		stateGroupTypeFilteredQuery, group, types, stateKeys, wildcardTypes)
	if err != nil {
		return nil, fmt.Errorf("store: state for group with wildcards: %w", err)
	}
	defer rows.Close()

	state := map[StateKey]StateEntry{}
	for rows.Next() {
		var k StateKey
		var e StateEntry
		if err := rows.Scan(&k.Type, &k.StateKey, &e.EventID, &e.Membership, &e.HistoryVisibility); err != nil {
			return nil, fmt.Errorf("store: state for group with wildcards: %w", err)
		}
		state[k] = e
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: state for group with wildcards: %w", err)
	}
	s.caches.filteredState.Add(cacheKey, state)
	return state, nil
}

// StateAtWithWildcards resolves state at a stream position for named keys plus
// every key of the given types.
func (s *Store) StateAtWithWildcards(
	ctx context.Context, roomID string, key streamtoken.RoomKey,
	keys []StateKey, wildcardTypes []string,
) (map[StateKey]string, error) {

	group, ok, err := s.stateGroupAt(ctx, roomID, key)
	if err != nil || !ok {
		return map[StateKey]string{}, err
	}
	entries, err := s.StateForGroupWithWildcards(ctx, group, keys, wildcardTypes)
	if err != nil {
		return nil, err
	}
	out := make(map[StateKey]string, len(entries))
	for k, e := range entries {
		out[k] = e.EventID
	}
	return out, nil
}

func sortedCopy(xs []string) []string {
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}
