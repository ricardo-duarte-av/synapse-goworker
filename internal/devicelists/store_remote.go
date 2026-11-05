package devicelists

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
)

// AnyPartialRooms is get_partial_state_room_resync_info being non-empty.
func (d *DB) AnyPartialRooms(ctx context.Context) (bool, error) {
	var any bool
	err := d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM partial_state_rooms)`).Scan(&any)
	return any, err
}

// AddRemotePending is add_remote_device_list_to_pending's upsert.
func (d *DB) AddRemotePending(ctx context.Context, instance string, id int64, userID, deviceID string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO device_lists_remote_pending (user_id, device_id, stream_id, instance_name)
			VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, device_id) DO UPDATE SET
			stream_id = EXCLUDED.stream_id, instance_name = EXCLUDED.instance_name`, userID, deviceID, id, instance)
		return err
	})
}

// Extremity is get_device_list_last_stream_id_for_remote: nil for none.
func (d *DB) Extremity(ctx context.Context, userID string) (*string, error) {
	var s *string
	err := d.pool.QueryRow(ctx, `SELECT stream_id FROM device_lists_remote_extremeties WHERE user_id = $1`, userID).Scan(&s)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return s, err
}

// NeedsResync is the user being in device_lists_remote_resync.
func (d *DB) NeedsResync(ctx context.Context, userID string) (bool, error) {
	var any bool
	err := d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM device_lists_remote_resync WHERE user_id = $1)`, userID).Scan(&any)
	return any, err
}

// AllNeedingResync is get_user_ids_requiring_device_list_resync().
func (d *DB) AllNeedingResync(ctx context.Context) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id FROM device_lists_remote_resync`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// MarkStale is mark_remote_users_device_caches_as_stale: an upsert that
// keeps the first added_ts.
func (d *DB) MarkStale(ctx context.Context, userIDs []string, nowMS int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		for _, u := range userIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO device_lists_remote_resync (user_id, added_ts) VALUES ($1, $2)
				ON CONFLICT (user_id) DO NOTHING`, u, nowMS); err != nil {
				return err
			}
		}
		return nil
	})
}

// MarkValid is mark_remote_user_device_cache_as_valid.
func (d *DB) MarkValid(ctx context.Context, userID string) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM device_lists_remote_resync WHERE user_id = $1`, userID)
	return err
}

// CachedDevices is get_cached_devices_for_user: device ID to its stored
// content.
func (d *DB) CachedDevices(ctx context.Context, userID string) (map[string]json.RawMessage, error) {
	rows, err := d.pool.Query(ctx, `SELECT device_id, content FROM device_lists_remote_cache WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var id, c string
		if err := rows.Scan(&id, &c); err != nil {
			rows.Close()
			return nil, err
		}
		out[id] = json.RawMessage(c)
	}
	rows.Close()
	return out, rows.Err()
}

func upsertExtremity(ctx context.Context, tx pgx.Tx, userID, streamID string) error {
	_, err := tx.Exec(ctx, `INSERT INTO device_lists_remote_extremeties (user_id, stream_id) VALUES ($1, $2)
		ON CONFLICT (user_id) DO UPDATE SET stream_id = EXCLUDED.stream_id`, userID, streamID)
	return err
}

// UpdateRemoteEntry is _update_remote_device_list_cache_entry_txn: the
// device deleted (content["deleted"] truthy) or upserted as json_encoder
// writes it, and the user's extremity set.
func (d *DB) UpdateRemoteEntry(ctx context.Context, userID, deviceID, content string, deleted bool, streamID string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		var err error
		if deleted {
			_, err = tx.Exec(ctx, `DELETE FROM device_lists_remote_cache WHERE user_id = $1 AND device_id = $2`, userID, deviceID)
		} else {
			_, err = tx.Exec(ctx, `INSERT INTO device_lists_remote_cache (user_id, device_id, content) VALUES ($1, $2, $3)
				ON CONFLICT (user_id, device_id) DO UPDATE SET content = EXCLUDED.content`, userID, deviceID, content)
		}
		if err != nil {
			return err
		}
		return upsertExtremity(ctx, tx, userID, streamID)
	})
}

// RemoteDevice is one device of a resync, its content as json_encoder
// writes it.
type RemoteDevice struct{ ID, Content string }

// ReplaceRemoteCache is _update_remote_device_list_cache_txn: the user's
// cache replaced, the extremity set.
func (d *DB) ReplaceRemoteCache(ctx context.Context, userID string, devices []RemoteDevice, streamID string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM device_lists_remote_cache WHERE user_id = $1`, userID); err != nil {
			return err
		}
		for _, dv := range devices {
			if _, err := tx.Exec(ctx, `INSERT INTO device_lists_remote_cache (user_id, device_id, content) VALUES ($1, $2, $3)`,
				userID, dv.ID, dv.Content); err != nil {
				return err
			}
		}
		return upsertExtremity(ctx, tx, userID, streamID)
	})
}

// CrossSigningKeys is get_e2e_cross_signing_keys_bulk([user]) without
// signatures: the newest key of each type, as stored.
func (d *DB) CrossSigningKeys(ctx context.Context, userID string) (map[string]json.RawMessage, error) {
	rows, err := d.pool.Query(ctx, `SELECT keytype, keydata FROM e2e_cross_signing_keys WHERE user_id = $1 ORDER BY stream_id ASC`, userID)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var t, k string
		if err := rows.Scan(&t, &k); err != nil {
			rows.Close()
			return nil, err
		}
		out[t] = json.RawMessage(k)
	}
	rows.Close()
	return out, rows.Err()
}

// SetCrossSigningKey is _set_e2e_cross_signing_key_txn for a remote user
// (no `devices` row: that is only for our own users): the key, and the
// caches-stream row of _get_bare_e2e_cross_signing_keys. id is from
// e2e_cross_signing_keys_sequence (writers=[]: nobody reads its positions).
func (d *DB) SetCrossSigningKey(ctx context.Context, instance string, userID, keyType, key string, nowMS int64) ([]cachestream.Row, error) {
	var rows []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		var id int64
		if err := tx.QueryRow(ctx, `SELECT nextval('e2e_cross_signing_keys_sequence')`).Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO e2e_cross_signing_keys (user_id, keytype, keydata, stream_id, instance_name)
			VALUES ($1, $2, $3, $4, $5)`, userID, keyType, key, id, instance); err != nil {
			return err
		}
		var err error
		rows, err = cachestream.Insert(ctx, tx, instance, "_get_bare_e2e_cross_signing_keys", [][]string{{userID}}, nowMS)
		return err
	})
	return rows, err
}

// RetryDue is filter_destinations_by_retry_limiter: of hosts, those with
// no retry timings or whose retry is due.
func (d *DB) RetryDue(ctx context.Context, hosts []string, nowMS int64) (map[string]bool, error) {
	out := map[string]bool{}
	for _, h := range hosts {
		out[h] = true
	}
	rows, err := d.pool.Query(ctx, `SELECT destination, retry_last_ts, retry_interval FROM destinations WHERE destination = ANY($1)`, hosts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		var last, interval *int64
		if err := rows.Scan(&h, &last, &interval); err != nil {
			return nil, err
		}
		// get_destination_retry_timings_batch: a row with no timings (NULL
		// or a zero interval) is None, which passes.
		if last != nil && interval != nil && *interval != 0 && *last+*interval > nowMS {
			delete(out, h)
		}
	}
	return out, rows.Err()
}

// Partial-state rooms (DeviceWriterHandler.handle_room_un_partial_stated).

// PendingForRoom is get_pending_remote_device_list_updates_for_room.
func (d *DB) PendingForRoom(ctx context.Context, roomID string) ([][2]string, error) {
	var min int64
	if err := d.pool.QueryRow(ctx, `SELECT device_lists_stream_id FROM partial_state_rooms WHERE room_id = $1`, roomID).Scan(&min); err != nil {
		return nil, err
	}
	rows, err := d.pool.Query(ctx, `SELECT user_id, device_id FROM device_lists_remote_pending AS d
		INNER JOIN current_state_events AS c ON type = 'm.room.member' AND state_key = user_id AND membership = 'join'
		WHERE room_id = $1 AND stream_id > $2`, roomID, min)
	if err != nil {
		return nil, err
	}
	return pairs(rows)
}

func pairs(rows pgx.Rows) ([][2]string, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]string, error) {
		var p [2]string
		return p, r.Scan(&p[0], &p[1])
	})
}

// PartialJoin is get_join_event_id_and_device_lists_stream_id_for_partial_state.
func (d *DB) PartialJoin(ctx context.Context, roomID string) (string, int64, error) {
	var ev string
	var s int64
	err := d.pool.QueryRow(ctx, `SELECT join_event_id, device_lists_stream_id FROM partial_state_rooms WHERE room_id = $1`, roomID).Scan(&ev, &s)
	return ev, s, err
}

// ChangesInRoom is get_device_list_changes_in_room: nil (ok false) when
// min is below what pruning has kept.
func (d *DB) ChangesInRoom(ctx context.Context, roomID string, min int64) ([][2]string, bool, error) {
	var pruned int64
	if err := d.pool.QueryRow(ctx, `SELECT stream_id FROM device_lists_changes_in_room_max_pruned_stream_id`).Scan(&pruned); err != nil {
		return nil, false, err
	}
	if pruned > min {
		return nil, false, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT DISTINCT user_id, device_id FROM device_lists_changes_in_room WHERE room_id = $1 AND stream_id > $2`, roomID, min)
	if err != nil {
		return nil, false, err
	}
	p, err := pairs(rows)
	return p, true, err
}

// DeviceChangesForUsers is get_device_changes_for_users(from, users) up
// to the current token.
func (d *DB) DeviceChangesForUsers(ctx context.Context, from, upto int64, userIDs []string) ([][2]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT DISTINCT user_id, device_id FROM device_lists_stream
		WHERE $1 < stream_id AND stream_id <= $2 AND user_id = ANY($3)`, from, upto, userIDs)
	if err != nil {
		return nil, err
	}
	return pairs(rows)
}

// LocalUsersInRoom is get_local_users_in_room.
func (d *DB) LocalUsersInRoom(ctx context.Context, roomID string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id FROM local_current_membership WHERE room_id = $1 AND membership = 'join'`, roomID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// PartialServersAtJoin is get_partial_state_servers_at_join.
func (d *DB) PartialServersAtJoin(ctx context.Context, roomID string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT server_name FROM partial_state_rooms_servers WHERE room_id = $1`, roomID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// CurrentMembers is get_partial_current_state_ids's member entries:
// state_key to event ID.
func (d *DB) CurrentMembers(ctx context.Context, roomID string) (map[string]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT state_key, event_id FROM current_state_events WHERE room_id = $1 AND type = 'm.room.member'`, roomID)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for rows.Next() {
		var k, e string
		if err := rows.Scan(&k, &e); err != nil {
			rows.Close()
			return nil, err
		}
		out[k] = e
	}
	rows.Close()
	return out, rows.Err()
}

// CurrentHosts is get_current_hosts_in_room.
func (d *DB) CurrentHosts(ctx context.Context, roomID string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT DISTINCT substring(state_key FROM '@[^:]*:(.*)$') FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND room_id = $1`, roomID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
