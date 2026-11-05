package todevice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is what the to-device writer reads and writes in Synapse's database,
// through its own pool, in Exec mode (pgcat).
type DB struct{ pool *pgxpool.Pool }

// Open connects.
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("todevice: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 2)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("todevice: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("todevice: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Pool is the pool, for the stream ID generator.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

const maxAttempts = 5

// runTxn is runInteraction: REPEATABLE READ (Synapse's default on
// Postgres), run again on a serialization failure or deadlock, at most
// maxAttempts times in all.
func (d *DB) runTxn(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for range maxAttempts {
		err = pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, fn)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || (pg.Code != "40001" && pg.Code != "40P01") {
			return err
		}
	}
	return err
}

// Message is one message for one device, as json_encoder writes
// {"content", "type", "sender"}. NotObject is a content that is not an
// object: _get_msgid_for_message's msg["content"].get raises on it, which
// fails the transaction once the message is stored for a device.
type Message struct {
	JSON      string
	NotObject bool
}

// UserMessages are one recipient's messages: device ID to message, in the
// order received. A lone "*" is every visible device.
type UserMessages struct {
	UserID  string
	Devices []string
	ByID    map[string]Message
}

// errNotObject is that AttributeError.
var errNotObject = errors.New(`todevice: 'msg["content"]' has no attribute 'get': a to-device content is not an object`)

// addLocalTxn is _add_messages_to_local_device_inbox_txn.
func addLocalTxn(ctx context.Context, tx pgx.Tx, instance string, streamID int64, users []UserMessages) error {
	type row struct{ user, device, json string }
	var rows []row
	for _, u := range users {
		if len(u.Devices) == 0 {
			continue
		}
		if len(u.Devices) == 1 && u.Devices[0] == "*" {
			// Every device but the hidden ones (cross-signing keys).
			devs, err := column(ctx, tx, `SELECT device_id FROM devices WHERE user_id = $1 AND hidden = false`, u.UserID)
			if err != nil {
				return err
			}
			if len(devs) == 0 {
				continue
			}
			m := u.ByID["*"]
			if m.NotObject {
				return errNotObject
			}
			for _, dev := range devs {
				rows = append(rows, row{u.UserID, dev, m.JSON})
			}
			continue
		}
		devs, err := column(ctx, tx, `SELECT device_id FROM devices WHERE user_id = $1 AND hidden = false AND device_id = ANY($2)`,
			u.UserID, u.Devices)
		if err != nil {
			return err
		}
		for _, dev := range devs {
			m := u.ByID[dev]
			if m.NotObject {
				return errNotObject
			}
			rows = append(rows, row{u.UserID, dev, m.JSON})
		}
	}
	for _, r := range rows {
		if _, err := tx.Exec(ctx, `INSERT INTO device_inbox (user_id, device_id, stream_id, message_json, instance_name)
			VALUES ($1, $2, $3, $4, $5)`, r.user, r.device, streamID, r.json, instance); err != nil {
			return err
		}
	}
	return nil
}

func column(ctx context.Context, tx pgx.Tx, q string, args ...any) ([]string, error) {
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// AddLocal is add_local_messages_from_client_to_device_inbox's transaction.
func (d *DB) AddLocal(ctx context.Context, instance string, streamID int64, users []UserMessages) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error { return addLocalTxn(ctx, tx, instance, streamID, users) })
}

// AddRemote is add_remote_messages_from_client_to_device_inbox's
// transaction, for one destination's EDU (json_encoder's).
func (d *DB) AddRemote(ctx context.Context, instance string, streamID int64, destination, edu string, nowMS int64) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO device_federation_outbox (destination, stream_id, queued_ts, messages_json, instance_name)
			VALUES ($1, $2, $3, $4, $5)`, destination, streamID, nowMS, edu, instance)
		return err
	})
}

// AddFromRemote is add_messages_from_remote_to_device_inbox's transaction:
// a message_id already seen from origin is not stored again; otherwise
// it is recorded (without instance_name, as Synapse inserts it) and the
// messages delivered. stored is false for a repeat.
func (d *DB) AddFromRemote(ctx context.Context, instance string, streamID int64, origin, messageID string, users []UserMessages, nowMS int64) (bool, error) {
	var stored bool
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		stored = false
		var one int
		err := tx.QueryRow(ctx, `SELECT 1 FROM device_federation_inbox WHERE origin = $1 AND message_id = $2 LIMIT 1`, origin, messageID).Scan(&one)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO device_federation_inbox (origin, message_id, received_ts) VALUES ($1, $2, $3)`,
			origin, messageID, nowMS); err != nil {
			return err
		}
		stored = true
		return addLocalTxn(ctx, tx, instance, streamID, users)
	})
	return stored, err
}

// SharesRoom is get_rooms_for_user(user) being non-empty: a join in
// current_state_events.
func (d *DB) SharesRoom(ctx context.Context, userID string) (bool, error) {
	var one int
	err := d.pool.QueryRow(ctx, `SELECT 1 FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND state_key = $1 LIMIT 1`, userID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// CachedDevices is get_cached_devices_for_user's device IDs.
func (d *DB) CachedDevices(ctx context.Context, userID string) (map[string]bool, error) {
	rows, err := d.pool.Query(ctx, `SELECT device_id FROM device_lists_remote_cache WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out, nil
}

// MarkStale is mark_remote_users_device_caches_as_stale for one user: an
// upsert that changes nothing on conflict.
func (d *DB) MarkStale(ctx context.Context, userID string, nowMS int64) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO device_lists_remote_resync (user_id, added_ts) VALUES ($1, $2)
			ON CONFLICT (user_id) DO NOTHING`, userID, nowMS)
		return err
	})
}

// RatelimitExempt is get_ratelimit_for_user having a row whose
// messages_per_second is null or zero: every ratelimiter lets the user by.
func (d *DB) RatelimitExempt(ctx context.Context, userID string) (bool, error) {
	var mps *float64
	err := d.pool.QueryRow(ctx, `SELECT messages_per_second FROM ratelimit_override WHERE user_id = $1`, userID).Scan(&mps)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return mps == nil || *mps == 0, nil
}

// Update is one row of the stream.
type Update struct {
	Token int64
	Row   json.RawMessage
}

// Updates is get_all_new_device_messages: the range is cut at from+limit
// IDs (not rows, so a stream ID is never split), each user and destination
// once at its greatest ID, sorted by (ID, entity).
func (d *DB) Updates(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error) {
	if from == upto {
		return nil, upto, false, nil
	}
	to := min(upto, from+int64(limit))
	type ent struct {
		id     int64
		entity string
	}
	var all []ent
	for _, q := range []string{
		`SELECT max(stream_id), user_id FROM device_inbox WHERE $1 < stream_id AND stream_id <= $2 GROUP BY user_id`,
		`SELECT max(stream_id), destination FROM device_federation_outbox WHERE $1 < stream_id AND stream_id <= $2 GROUP BY destination`,
	} {
		rows, err := d.pool.Query(ctx, q, from, to)
		if err != nil {
			return nil, 0, false, err
		}
		got, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (ent, error) {
			var e ent
			return e, r.Scan(&e.id, &e.entity)
		})
		if err != nil {
			return nil, 0, false, err
		}
		all = append(all, got...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].id != all[j].id {
			return all[i].id < all[j].id
		}
		return all[i].entity < all[j].entity
	})
	out := make([]Update, len(all))
	for i, e := range all {
		row, _ := json.Marshal([]string{e.entity})
		out[i] = Update{Token: e.id, Row: row}
	}
	return out, to, to < upto, nil
}
