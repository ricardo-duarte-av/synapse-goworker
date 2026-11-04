package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// What client POST /keys/query reads (storage/databases/main/
// end_to_end_keys.py, devices.py, roommember.py, transactions.py, v1.164.1).

// UserDevice is a (user ID, device ID) pair.
type UserDevice struct{ UserID, DeviceID string }

// DeviceKey is one device's row from _get_e2e_device_keys.
type DeviceKey struct {
	DisplayName *string
	// KeyJSON is key_json as stored; nil when empty.
	KeyJSON []byte
}

// DeviceKeys is _get_e2e_device_keys (include_all_devices=False): every
// device of each user in users, and each pair in pairs, that has a
// key_json row and is not hidden.
func (s *Store) DeviceKeys(ctx context.Context, users []string, pairs []UserDevice) (map[string]map[string]DeviceKey, error) {
	out := map[string]map[string]DeviceKey{}
	if len(users) == 0 && len(pairs) == 0 {
		return out, nil
	}
	pu, pd := splitPairs(pairs)
	rows, err := s.query(ctx, "DeviceKeys", `
		SELECT user_id, device_id, d.display_name, k.key_json
		  FROM e2e_device_keys_json k
		 INNER JOIN devices d USING (user_id, device_id)
		 WHERE (user_id = ANY($1)
		        OR (user_id, device_id) IN (SELECT * FROM unnest($2::text[], $3::text[])))
		   AND NOT d.hidden`, users, pu, pd)
	if err != nil {
		return nil, fmt.Errorf("store: device keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u, d string
		var name, key *string
		if err := rows.Scan(&u, &d, &name, &key); err != nil {
			return nil, fmt.Errorf("store: device keys: %w", err)
		}
		k := DeviceKey{DisplayName: name}
		if key != nil && *key != "" {
			k.KeyJSON = []byte(*key)
		}
		if out[u] == nil {
			out[u] = map[string]DeviceKey{}
		}
		out[u][d] = k
	}
	return out, rows.Err()
}

// Signature is one (key ID, signature).
type Signature struct{ KeyID, Signature string }

// DeviceSelfCrossSignatures is _get_e2e_cross_signing_signatures_for_devices:
// the signatures each device's own user made on it with a cross-signing key.
func (s *Store) DeviceSelfCrossSignatures(ctx context.Context, pairs []UserDevice) (map[UserDevice][]Signature, error) {
	out := map[UserDevice][]Signature{}
	if len(pairs) == 0 {
		return out, nil
	}
	pu, pd := splitPairs(pairs)
	rows, err := s.query(ctx, "DeviceSelfCrossSignatures", `
		SELECT user_id, key_id, target_device_id, signature
		  FROM e2e_cross_signing_signatures
		 WHERE (target_user_id, target_device_id, user_id)
		       IN (SELECT u, d, u FROM unnest($1::text[], $2::text[]) AS t(u, d))`, pu, pd)
	if err != nil {
		return nil, fmt.Errorf("store: device signatures: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u, keyID, d, sig string
		if err := rows.Scan(&u, &keyID, &d, &sig); err != nil {
			return nil, fmt.Errorf("store: device signatures: %w", err)
		}
		k := UserDevice{u, d}
		out[k] = append(out[k], Signature{keyID, sig})
	}
	return out, rows.Err()
}

// CrossSigningKeys is _get_bare_e2e_cross_signing_keys_bulk: each user's
// newest key of each type, as stored.
func (s *Store) CrossSigningKeys(ctx context.Context, users []string) (map[string]map[string][]byte, error) {
	out := map[string]map[string][]byte{}
	if len(users) == 0 {
		return out, nil
	}
	rows, err := s.query(ctx, "CrossSigningKeys", `
		SELECT DISTINCT ON (user_id, keytype) user_id, keytype, keydata
		  FROM e2e_cross_signing_keys
		 WHERE user_id = ANY($1)
		 ORDER BY user_id, keytype, stream_id DESC`, users)
	if err != nil {
		return nil, fmt.Errorf("store: cross-signing keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u, typ, data string
		if err := rows.Scan(&u, &typ, &data); err != nil {
			return nil, fmt.Errorf("store: cross-signing keys: %w", err)
		}
		if out[u] == nil {
			out[u] = map[string][]byte{}
		}
		out[u][typ] = []byte(data)
	}
	return out, rows.Err()
}

// CrossSignature is a signature from _get_e2e_cross_signing_signatures_txn.
type CrossSignature struct {
	Target UserDevice
	Signature
}

// CrossSignaturesBy is _get_e2e_cross_signing_signatures_txn's query: the
// signatures from made on each target, in the order the rows come.
func (s *Store) CrossSignaturesBy(ctx context.Context, from string, targets []UserDevice) ([]CrossSignature, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	tu, td := splitPairs(targets)
	rows, err := s.query(ctx, "CrossSignaturesBy", `
		SELECT target_user_id, target_device_id, key_id, signature
		  FROM e2e_cross_signing_signatures
		 WHERE user_id = $1
		   AND (target_user_id, target_device_id) IN (SELECT * FROM unnest($2::text[], $3::text[]))`,
		from, tu, td)
	if err != nil {
		return nil, fmt.Errorf("store: cross signatures: %w", err)
	}
	defer rows.Close()
	var out []CrossSignature
	for rows.Next() {
		var c CrossSignature
		if err := rows.Scan(&c.Target.UserID, &c.Target.DeviceID, &c.KeyID, &c.Signature.Signature); err != nil {
			return nil, fmt.Errorf("store: cross signatures: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

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

func splitPairs(pairs []UserDevice) ([]string, []string) {
	u := make([]string, len(pairs))
	d := make([]string, len(pairs))
	for i, p := range pairs {
		u[i], d[i] = p.UserID, p.DeviceID
	}
	return u, d
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
