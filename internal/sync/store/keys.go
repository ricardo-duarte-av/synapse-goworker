package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// What client POST /keys/query reads (storage/databases/main/
// end_to_end_keys.py, devices.py, roommember.py, transactions.py, v1.164.1).

// UsersWithCachedDevices is get_users_whose_devices_are_cached: users with
// a device_lists_remote_extremeties stream ID (a non-empty one: Synapse
// tests it for truth) and no pending resync.
func (s *Store) UsersWithCachedDevices(ctx context.Context, users []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(users) == 0 {
		return out, nil
	}
	rows, err := s.query(ctx, "UsersWithCachedDevices", `
		SELECT user_id FROM device_lists_remote_extremeties x
		 WHERE user_id = ANY($1) AND stream_id IS NOT NULL AND stream_id <> ''
		   AND NOT EXISTS (SELECT 1 FROM device_lists_remote_resync r WHERE r.user_id = x.user_id)`, users)
	if err != nil {
		return nil, fmt.Errorf("store: cached device lists: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("store: cached device lists: %w", err)
		}
		out[u] = true
	}
	return out, rows.Err()
}

// CachedDevices is get_cached_devices_for_user: each cached device's
// content, as stored.
func (s *Store) CachedDevices(ctx context.Context, userID string) (map[string][]byte, error) {
	rows, err := s.query(ctx, "CachedDevices",
		`SELECT device_id, content FROM device_lists_remote_cache WHERE user_id = $1`, userID)
	if err != nil {
		return nil, fmt.Errorf("store: cached devices: %w", err)
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var d, content string
		if err := rows.Scan(&d, &content); err != nil {
			return nil, fmt.Errorf("store: cached devices: %w", err)
		}
		out[d] = []byte(content)
	}
	return out, rows.Err()
}

// CachedDevice is _get_cached_user_device: one cached device's content, or
// false when there is no row (where Synapse's lookup raises).
func (s *Store) CachedDevice(ctx context.Context, userID, deviceID string) ([]byte, bool, error) {
	var content string
	err := s.queryRow(ctx, "CachedDevice",
		`SELECT content FROM device_lists_remote_cache WHERE user_id = $1 AND device_id = $2`,
		userID, deviceID).Scan(&content)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: cached device: %w", err)
	}
	return []byte(content), true, nil
}

// UsersServerSharesRoomWith is get_users_server_still_shares_room_with:
// the users joined to some room in current state.
func (s *Store) UsersServerSharesRoomWith(ctx context.Context, users []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(users) == 0 {
		return out, nil
	}
	rows, err := s.query(ctx, "UsersServerSharesRoomWith", `
		SELECT state_key FROM current_state_events
		 WHERE type = 'm.room.member' AND membership = 'join' AND state_key = ANY($1)
		 GROUP BY state_key`, users)
	if err != nil {
		return nil, fmt.Errorf("store: users sharing a room: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("store: users sharing a room: %w", err)
		}
		out[u] = true
	}
	return out, rows.Err()
}

// DestinationsNotReady is filter_destinations_by_retry_limiter's complement:
// the destinations whose backoff (get_destination_retry_timings, a row with
// failure_ts, retry_last_ts and retry_interval all set) is not due within
// dueWithinMS of nowMS.
func (s *Store) DestinationsNotReady(ctx context.Context, destinations []string, nowMS, dueWithinMS int64) (map[string]bool, error) {
	out := map[string]bool{}
	if len(destinations) == 0 {
		return out, nil
	}
	rows, err := s.query(ctx, "DestinationsNotReady", `
		SELECT destination FROM destinations
		 WHERE destination = ANY($1)
		   AND COALESCE(failure_ts, 0) <> 0 AND COALESCE(retry_last_ts, 0) <> 0
		   AND COALESCE(retry_interval, 0) <> 0
		   AND retry_last_ts + retry_interval > $2`, destinations, nowMS+dueWithinMS)
	if err != nil {
		return nil, fmt.Errorf("store: destination retry timings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("store: destination retry timings: %w", err)
		}
		out[d] = true
	}
	return out, rows.Err()
}

// UsersSharingARoomJoinedOrInvited is do_users_share_a_room_joined_or_invited:
// which of others share a room with userID, each joined or invited to it.
func (s *Store) UsersSharingARoomJoinedOrInvited(ctx context.Context, userID string, others []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(others) == 0 {
		return out, nil
	}
	rows, err := s.query(ctx, "UsersSharingARoomJoinedOrInvited", `
		SELECT DISTINCT b.state_key
		  FROM (SELECT room_id FROM current_state_events
		         WHERE type = 'm.room.member' AND membership IN ('join', 'invite') AND state_key = $1) AS a
		 INNER JOIN (SELECT room_id, state_key FROM current_state_events
		         WHERE type = 'm.room.member' AND membership IN ('join', 'invite') AND state_key = ANY($2)) AS b
		 USING (room_id)`, userID, others)
	if err != nil {
		return nil, fmt.Errorf("store: users sharing a room: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("store: users sharing a room: %w", err)
		}
		out[u] = true
	}
	return out, rows.Err()
}
