package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// MembershipChange is one of the caller's membership events in the sync window.
type MembershipChange struct {
	RoomID         string
	EventID        string
	Membership     string
	Sender         string
	StreamOrdering int64
	// OutOfBand marks a membership event that arrived without the room's
	// state -- an invite or its rejection over federation. Such a room has no
	// timeline to paginate, so its single event is the whole response.
	OutOfBand bool
}

// MembershipChangesForUser lists the caller's membership events in
// (since, now].
//
// This is what decides which rooms appear in the response at all, and in which
// section. A room the caller neither joined nor left in the window is reported
// only if something else happened in it.
func (s *Store) MembershipChangesForUser(ctx context.Context, userID string,
	since, now int64) ([]MembershipChange, error) {

	const q = `
		SELECT e.room_id, e.event_id, COALESCE(m.membership, ''), e.sender,
		       e.stream_ordering,
		       COALESCE(ej.internal_metadata::jsonb ->> 'out_of_band_membership', 'false')
		  FROM events e
		  LEFT JOIN room_memberships m ON m.event_id = e.event_id
		  JOIN event_json ej ON ej.event_id = e.event_id
		 WHERE e.type = 'm.room.member' AND e.state_key = $1
		   AND e.stream_ordering > $2 AND e.stream_ordering <= $3
		 ORDER BY e.stream_ordering`
	rows, err := s.query(ctx, "MembershipChangesForUser", q, userID, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: membership changes: %w", err)
	}
	defer rows.Close()

	var out []MembershipChange
	for rows.Next() {
		var c MembershipChange
		var oob string
		if err := rows.Scan(&c.RoomID, &c.EventID, &c.Membership, &c.Sender,
			&c.StreamOrdering, &oob); err != nil {
			return nil, fmt.Errorf("store: membership changes: %w", err)
		}
		c.OutOfBand = oob == "true"
		out = append(out, c)
	}
	return out, rows.Err()
}

// RoomTimelineSince returns each room's events in (since, now], newest first,
// capped at limit per room.
//
// One query for every joined room rather than one per room: an incremental sync
// touches every room the caller is in, and most of them will have nothing.
// Synapse uses an in-memory stream cache to skip those before reaching the
// database at all; without one, a single query over all of them is the next
// best thing.
//
// Ordered by stream, not topologically: an incremental sync reports *updates*,
// and updates arrive in stream order. This is the one place where sync's
// pagination genuinely is stream-ordered, unlike the initial sync.
func (s *Store) RoomTimelineSince(ctx context.Context, roomIDs []string, roomVersions map[string]string,
	since, now int64, limit int) (map[string][]TimelineEvent, error) {

	if len(roomIDs) == 0 {
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
			                          ORDER BY e.stream_ordering DESC) AS rn
			  FROM events e JOIN event_json ej USING (event_id)
			 WHERE e.outlier = FALSE AND e.room_id = ANY($1)
			   AND e.stream_ordering > $2 AND e.stream_ordering <= $3
		) x WHERE rn <= $4`
	rows, err := s.query(ctx, "RoomTimelineSince", q, roomIDs, since, now, limit)
	if err != nil {
		return nil, fmt.Errorf("store: room timeline since: %w", err)
	}
	defer rows.Close()

	out := map[string][]TimelineEvent{}
	for rows.Next() {
		var (
			ev TimelineEvent
			ok bool
			rn int64
		)
		if err := rows.Scan(&ev.RoomID, &ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata, &ok, &rn); err != nil {
			return nil, fmt.Errorf("store: room timeline since: %w", err)
		}
		// A rejected event took a row number, so it counted towards the limit
		// -- as it does in Synapse, which pages without a rejection predicate
		// and drops them afterwards -- and is dropped here rather than in SQL.
		if !ok {
			continue
		}
		ev.RoomVersion = roomVersions[ev.RoomID]
		out[ev.RoomID] = append(out[ev.RoomID], ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: room timeline since: %w", err)
	}
	// The window function gives newest first; a timeline is reported oldest
	// first.
	for roomID, events := range out {
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
		out[roomID] = events
	}
	return out, nil
}

// LastEventBefore returns the most recent event in a room at or before a
// stream position.
//
// Used to anchor the linearity check: an incremental timeline whose events form
// an unbroken chain from the caller's last sync needs no `state` block at all,
// because the timeline itself carries every change.
func (s *Store) LastEventBefore(ctx context.Context, roomID string, key streamtoken.RoomKey) (string, error) {
	// Memoised: asked once for prev_batch, once for the lazy-loaded state
	// lookup and once by the newly-joined probe, per room, with the same
	// arguments each time. It is the most-called query on the worker.
	return memo(ctx, "LastEventBefore\x00"+roomID+"\x00"+key.String(), func() (string, error) {
		return s.lastEventBefore(ctx, roomID, key)
	})
}

func (s *Store) lastEventBefore(ctx context.Context, roomID string, key streamtoken.RoomKey) (string, error) {
	const q = `
		SELECT event_id FROM events
		 WHERE room_id = $1 AND stream_ordering <= $2 AND outlier = FALSE
		   AND rejection_reason IS NULL
		 ORDER BY stream_ordering DESC LIMIT 1`
	var id string
	err := s.queryRow(ctx, "LastEventBefore", q, roomID, key.MaxStreamPos()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: last event before: %w", err)
	}
	return id, nil
}

// GlobalAccountDataSince loads global account data changed in (since, now].
func (s *Store) GlobalAccountDataSince(ctx context.Context, userID string,
	since, now int64, msc3391 bool) ([]AccountDataEntry, error) {

	q := `
		SELECT account_data_type, content FROM account_data
		 WHERE user_id = $1 AND stream_id > $2 AND stream_id <= $3`
	if msc3391 {
		q += ` AND content != '{}'`
	}
	q += ` ORDER BY account_data_type`
	rows, err := s.query(ctx, "GlobalAccountDataSince", q, userID, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: global account data since: %w", err)
	}
	defer rows.Close()
	var out []AccountDataEntry
	for rows.Next() {
		var e AccountDataEntry
		var content string
		if err := rows.Scan(&e.Type, &content); err != nil {
			return nil, fmt.Errorf("store: global account data since: %w", err)
		}
		e.Content = json.RawMessage(content)
		out = append(out, e)
	}
	return out, rows.Err()
}

// PushRulesChangedSince reports whether a user's push rules changed in
// (since, now] -- have_push_rules_changed_for_user.
//
// m.push_rules is not in account_data at all, so GlobalAccountDataSince cannot
// see a rule change; without this an incremental sync drops it, and because
// next_batch has already moved past it the client never hears of it.
//
// The table's own maximum is not a usable bound on the token: push_rules ids
// drift above it (docs/tokens.md). A row in the window is what counts.
func (s *Store) PushRulesChangedSince(ctx context.Context, userID string, since, now int64) (bool, error) {
	const q = `
		SELECT EXISTS (SELECT 1 FROM push_rules_stream
		                WHERE user_id = $1 AND stream_id > $2 AND stream_id <= $3)`
	var changed bool
	if err := s.queryRow(ctx, "PushRulesChangedSince", q, userID, since, now).Scan(&changed); err != nil {
		return false, fmt.Errorf("store: push rules changed since: %w", err)
	}
	return changed, nil
}

// RoomAccountDataSince loads per-room account data changed in (since, now],
// keyed by room.
//
// Tags are excluded: they live in their own table with their own stream, and
// Synapse reports them through a separate path.
func (s *Store) RoomAccountDataSince(ctx context.Context, userID string,
	since, now int64, msc3391 bool) (map[string][]AccountDataEntry, error) {

	// Memoised: this returns every room's account data in one query, and the
	// archived-room path then calls it once per archived room and reads a
	// single key out of the result. Readers index the map and hand the slice
	// to filterAccountDataEntries, which builds a new slice, so nothing
	// written here is written through.
	key := fmt.Sprintf("RoomAccountDataSince\x00%s\x00%d\x00%d\x00%t", userID, since, now, msc3391)
	return memo(ctx, key, func() (map[string][]AccountDataEntry, error) {
		return s.roomAccountDataSince(ctx, userID, since, now, msc3391)
	})
}

func (s *Store) roomAccountDataSince(ctx context.Context, userID string,
	since, now int64, msc3391 bool) (map[string][]AccountDataEntry, error) {

	q := `
		SELECT room_id, account_data_type, content FROM room_account_data
		 WHERE user_id = $1 AND stream_id > $2 AND stream_id <= $3`
	if msc3391 {
		q += ` AND content != '{}'`
	}
	q += ` ORDER BY room_id, account_data_type`
	rows, err := s.query(ctx, "RoomAccountDataSince", q, userID, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: room account data since: %w", err)
	}
	defer rows.Close()
	out := map[string][]AccountDataEntry{}
	for rows.Next() {
		var roomID string
		var e AccountDataEntry
		var content string
		if err := rows.Scan(&roomID, &e.Type, &content); err != nil {
			return nil, fmt.Errorf("store: room account data since: %w", err)
		}
		e.Content = json.RawMessage(content)
		out[roomID] = append(out[roomID], e)
	}
	return out, rows.Err()
}

// ReceiptsSince loads receipts in (since, now] for the given rooms.
func (s *Store) ReceiptsSince(ctx context.Context, roomIDs []string, since, now int64) (map[string][]ReceiptRow, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT room_id, stream_id, COALESCE(instance_name, ''), receipt_type,
		       user_id, event_id, COALESCE(thread_id, ''), data
		  FROM receipts_linearized
		 WHERE room_id = ANY($1) AND stream_id > $2 AND stream_id <= $3`
	rows, err := s.query(ctx, "ReceiptsSince", q, roomIDs, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: receipts since: %w", err)
	}
	defer rows.Close()
	out := map[string][]ReceiptRow{}
	for rows.Next() {
		var roomID string
		var r ReceiptRow
		var data string
		if err := rows.Scan(&roomID, &r.StreamID, &r.InstanceName, &r.ReceiptType,
			&r.UserID, &r.EventID, &r.ThreadID, &data); err != nil {
			return nil, fmt.Errorf("store: receipts since: %w", err)
		}
		r.Data = json.RawMessage(data)
		out[roomID] = append(out[roomID], r)
	}
	return out, rows.Err()
}

// PresenceSince loads presence changed in (since, now] for users the caller
// shares a room with.
//
// Unlike the initial-sync presence, this one IS bounded by the token, so it can
// be pinned and compared exactly.
func (s *Store) PresenceSince(ctx context.Context, userID string, since, now int64) ([]PresenceState, error) {
	// The gate, not the answer. This query is the most expensive thing on the
	// sync path -- a correlated subquery over presence_stream and
	// current_state_events, 25ms warm and 430ms cold, run once per sync whether
	// or not anybody's presence moved -- and on a quiet server nobody's did.
	//
	// Asking "did ANY presence move after `since`?" is enough: if none did,
	// this query returns nothing, so skipping it and returning nothing is the
	// same answer for less. Below the cache's horizon, or with replication
	// down, the gate says "changed" and the query runs exactly as before.
	//
	// Note what is NOT cached here: the presence rows themselves. last_active_ago
	// is recomputed from the wall clock per request, so a cached row is wrong
	// the moment it is stored. Cache the gate, never the rows.
	if !s.AnyPresenceSince(since) {
		return nil, nil
	}
	const q = `
		SELECT p.user_id, p.state, p.last_active_ts, p.status_msg, p.currently_active
		  FROM presence_stream p
		 WHERE p.stream_id > $2 AND p.stream_id <= $3
		   AND (p.user_id = $1 OR p.user_id IN (
				SELECT cse.state_key FROM current_state_events cse
				 WHERE cse.type = 'm.room.member' AND cse.membership = 'join'
				   AND cse.room_id IN (
						SELECT room_id FROM local_current_membership
						 WHERE user_id = $1 AND membership = 'join')))`
	rows, err := s.query(ctx, "PresenceSince", q, userID, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: presence since: %w", err)
	}
	defer rows.Close()
	return scanPresence(rows)
}

// MembershipAtPosition returns the caller's membership in a room at a stream
// position, or "" if they had none.
//
// This is what decides whether a room is "newly joined" for an incremental
// sync: joined now but not at `since`. Synapse resolves it from the state at
// the token rather than from the membership events in the window, because a
// join that happened before `since` and a join inside it are the same event
// type and only the state tells them apart.
func (s *Store) MembershipAtPosition(ctx context.Context, roomID, userID string,
	key streamtoken.RoomKey) (string, error) {

	eventID, err := s.LastEventBefore(ctx, roomID, key)
	if err != nil {
		return "", err
	}
	if eventID == "" {
		return "", nil
	}
	groups, err := s.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return "", err
	}
	group, ok := groups[eventID]
	if !ok {
		return "", nil
	}
	state, err := s.FilteredStateForGroup(ctx, group,
		[]StateKey{{Type: "m.room.member", StateKey: userID}})
	if err != nil {
		return "", err
	}
	return state[StateKey{Type: "m.room.member", StateKey: userID}].Membership, nil
}

// JoinedMembersOf lists the joined members of the given rooms.
//
// Used for the presence and device-list sections: joining a room entitles the
// caller to the presence and device lists of everyone already in it, whether or
// not those changed recently.
func (s *Store) JoinedMembersOf(ctx context.Context, roomIDs []string) ([]string, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}
	// Memoised: the presence and device-list sections ask this the same way,
	// for the same newly-joined rooms, in the same pass. Callers only ever
	// append the result elsewhere, never write into it.
	return memo(ctx, "JoinedMembersOf\x00"+strings.Join(roomIDs, "\x01"), func() ([]string, error) {
		return s.joinedMembersOf(ctx, roomIDs)
	})
}

func (s *Store) joinedMembersOf(ctx context.Context, roomIDs []string) ([]string, error) {
	const q = `
		SELECT DISTINCT state_key FROM current_state_events
		 WHERE room_id = ANY($1) AND type = 'm.room.member' AND membership = 'join'`
	rows, err := s.query(ctx, "JoinedMembersOf", q, roomIDs)
	if err != nil {
		return nil, fmt.Errorf("store: joined members: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: joined members: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PresenceForUsers loads the current presence of specific users, offline
// included.
func (s *Store) PresenceForUsers(ctx context.Context, userIDs []string) ([]PresenceState, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT user_id, state, last_active_ts, status_msg, currently_active
		  FROM presence_stream WHERE user_id = ANY($1)`
	rows, err := s.query(ctx, "PresenceForUsers", q, userIDs)
	if err != nil {
		return nil, fmt.Errorf("store: presence for users: %w", err)
	}
	defer rows.Close()
	return scanPresence(rows)
}

// DeviceListChanges returns the users whose device lists changed in
// (since, now] and whose changes the caller is entitled to see.
//
// Four sources, and missing any of them leaves an end-to-end encrypted client
// unable to decrypt:
//
//   - device_lists_changes_in_room, for users sharing one of the caller's
//     rooms;
//   - the caller's own devices, which are not in that table for their own
//     rooms;
//   - users whose cross-signing signatures changed, which is a separate
//     stream;
//   - every member of a newly joined room, added by the caller.
func (s *Store) DeviceListChanges(ctx context.Context, userID string, roomIDs []string,
	since, now int64) ([]string, error) {

	changed := map[string]bool{}

	// device_lists_changes_in_room is pruned, and a `since` older than the
	// pruning cannot be answered from it: the rows that would name who changed
	// are gone. Synapse then falls back to asking device_lists_stream about
	// everyone who shares a room with the caller -- a client that has been
	// away for a while is exactly the one that needs this list complete.
	pruned := false
	if len(roomIDs) > 0 {
		var maxPruned int64
		if err := s.queryRow(ctx, "DeviceListChanges", `
			SELECT stream_id FROM device_lists_changes_in_room_max_pruned_stream_id`,
		).Scan(&maxPruned); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store: device list pruning: %w", err)
		}
		pruned = maxPruned > since
	}

	if pruned {
		sharing, err := s.UsersSharingAnyRoom(ctx, userID)
		if err != nil {
			return nil, err
		}
		tracked := make([]string, 0, len(sharing)+1)
		for u := range sharing {
			tracked = append(tracked, u)
		}
		tracked = append(tracked, userID)
		rows, err := s.query(ctx, "DeviceListChanges", `
			SELECT DISTINCT user_id FROM device_lists_stream
			 WHERE user_id = ANY($1) AND stream_id > $2 AND stream_id <= $3`,
			tracked, since, now)
		if err != nil {
			return nil, fmt.Errorf("store: device list changes: %w", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: device list changes: %w", err)
			}
			changed[id] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("store: device list changes: %w", err)
		}
	} else if len(roomIDs) > 0 {
		const q = `
			SELECT DISTINCT user_id FROM device_lists_changes_in_room
			 WHERE room_id = ANY($1) AND stream_id > $2 AND stream_id <= $3`
		rows, err := s.query(ctx, "DeviceListChanges", q, roomIDs, since, now)
		if err != nil {
			return nil, fmt.Errorf("store: device list changes: %w", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("store: device list changes: %w", err)
			}
			changed[id] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("store: device list changes: %w", err)
		}
	}

	// The caller's own devices.
	var own int
	if err := s.queryRow(ctx, "DeviceListChanges", `
		SELECT COUNT(*) FROM device_lists_stream
		 WHERE user_id = $1 AND stream_id > $2 AND stream_id <= $3`,
		userID, since, now).Scan(&own); err != nil {
		return nil, fmt.Errorf("store: own device changes: %w", err)
	}
	if own > 0 {
		changed[userID] = true
	}

	// Cross-signing signatures the caller made on other users.
	sigRows, err := s.query(ctx, "DeviceListChanges", `
		SELECT DISTINCT user_ids FROM user_signature_stream
		 WHERE from_user_id = $1 AND stream_id > $2 AND stream_id <= $3`,
		userID, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: signature changes: %w", err)
	}
	defer sigRows.Close()
	for sigRows.Next() {
		var raw string
		if err := sigRows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("store: signature changes: %w", err)
		}
		var users []string
		if err := json.Unmarshal([]byte(raw), &users); err != nil {
			continue
		}
		for _, u := range users {
			changed[u] = true
		}
	}
	if err := sigRows.Err(); err != nil {
		return nil, fmt.Errorf("store: signature changes: %w", err)
	}

	out := make([]string, 0, len(changed))
	for id := range changed {
		out = append(out, id)
	}
	return out, nil
}

// UsersSharingAnyRoom returns the users who share a joined room with the caller.
//
// Used to filter device_lists.left: someone who left one room but is still in
// another with us has not left our view, and telling a client otherwise makes
// it drop keys it still needs.
func (s *Store) UsersSharingAnyRoom(ctx context.Context, userID string) (map[string]bool, error) {
	const q = `
		SELECT DISTINCT cse.state_key FROM current_state_events cse
		 WHERE cse.type = 'm.room.member' AND cse.membership = 'join'
		   AND cse.room_id IN (
				SELECT room_id FROM local_current_membership
				 WHERE user_id = $1 AND membership = 'join')`
	rows, err := s.query(ctx, "UsersSharingAnyRoom", q, userID)
	if err != nil {
		return nil, fmt.Errorf("store: users sharing rooms: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: users sharing rooms: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}

// MembershipOfEvents returns the membership recorded by each of the given
// membership events.
//
// Used to resolve a leave's previous membership from `unsigned.replaces_state`.
// Synapse reads `unsigned.prev_content` off its in-memory event, which it may
// have populated for another reason entirely; we never emit prev_content on
// state events, so the link has to be followed explicitly rather than read off
// our own output.
func (s *Store) MembershipOfEvents(ctx context.Context, eventIDs []string) (map[string]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `SELECT event_id, membership FROM room_memberships WHERE event_id = ANY($1)`
	rows, err := s.query(ctx, "MembershipOfEvents", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: membership of events: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, membership string
		if err := rows.Scan(&id, &membership); err != nil {
			return nil, fmt.Errorf("store: membership of events: %w", err)
		}
		out[id] = membership
	}
	return out, rows.Err()
}

// CurrentStateDeltas returns the room's current-state changes in (since, now],
// as (type, state_key) -> event_id.
//
// This is MSC4222's `state_after`: rather than the state block a client must
// apply *before* the timeline, it reports what current state became. A delta
// whose event_id is NULL is a state key being removed, which the MSC has no way
// to express, so Synapse skips it and so do we.
//
// Ordered ascending so a key changed twice in the window ends on its last
// value.
func (s *Store) CurrentStateDeltas(ctx context.Context, roomID string,
	since, now int64) (map[StateKey]string, error) {

	const q = `
		SELECT type, state_key, event_id FROM current_state_delta_stream
		 WHERE room_id = $1 AND stream_id > $2 AND stream_id <= $3
		 ORDER BY stream_id ASC`
	rows, err := s.query(ctx, "CurrentStateDeltas", q, roomID, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: current state deltas: %w", err)
	}
	defer rows.Close()
	out := map[StateKey]string{}
	for rows.Next() {
		var k StateKey
		var eventID *string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: current state deltas: %w", err)
		}
		if eventID == nil {
			continue
		}
		out[k] = *eventID
	}
	return out, rows.Err()
}
