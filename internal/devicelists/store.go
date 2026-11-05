package devicelists

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/roomhosts"
)

// DB is what the device-lists writer reads and writes in Synapse's
// database, through its own pool, in Exec mode (pgcat).
type DB struct{ pool *pgxpool.Pool }

// Open connects.
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("devicelists: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 2)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("devicelists: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("devicelists: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// NewDB uses pool.
func NewDB(pool *pgxpool.Pool) *DB { return &DB{pool: pool} }

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Pool is the pool, for the stream ID generator.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

const maxAttempts = 5

// runTxn is runInteraction: REPEATABLE READ, run again on a serialization
// failure or deadlock, at most maxAttempts times in all.
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

func nowMS() int64 { return time.Now().UnixMilli() }

// emptyContext is json_encoder.encode(get_active_span_text_map()) with
// tracing off, which every opentracing_context column holds here.
const emptyContext = "{}"

// RoomsForUser is get_rooms_for_user: the rooms the user is joined to, by
// current_state_events (for a remote user, the rooms we share).
func (d *DB) RoomsForUser(ctx context.Context, userID string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT room_id FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND state_key = $1`, userID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// AddDeviceChange is add_device_changes_txn, for one batch (at most 1000
// devices, a stream ID each): the user's older rows for these devices
// dropped from device_lists_stream, the new ones inserted, and a row per
// (room, device) in device_lists_changes_in_room, already converted for a
// remote user.
func (d *DB) AddDeviceChange(ctx context.Context, instance string, ids []int64, userID string, deviceIDs, roomIDs []string, isMine bool, nowMS int64) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM device_lists_stream WHERE user_id = $1 AND stream_id < $2 AND device_id = ANY($3)`,
			userID, ids[0], deviceIDs); err != nil {
			return err
		}
		for i, dev := range deviceIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO device_lists_stream (instance_name, stream_id, user_id, device_id) VALUES ($1, $2, $3, $4)`,
				instance, ids[i], userID, dev); err != nil {
				return err
			}
		}
		for _, room := range roomIDs {
			for i, dev := range deviceIDs {
				if _, err := tx.Exec(ctx, `INSERT INTO device_lists_changes_in_room
					(user_id, device_id, room_id, stream_id, instance_name, converted_to_destinations, opentracing_context, inserted_ts)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
					userID, dev, room, ids[i], instance, !isMine, emptyContext, nowMS); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// AddUserSignatureChange is _add_user_signature_change_txn; userIDs is the
// list as json_encoder writes it.
func (d *DB) AddUserSignatureChange(ctx context.Context, instance string, id int64, fromUserID, userIDs string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO user_signature_stream (stream_id, from_user_id, user_ids, instance_name) VALUES ($1, $2, $3, $4)`,
			id, fromUserID, userIDs, instance)
		return err
	})
}

// Change is one row of device_lists_changes_in_room to convert.
type Change struct {
	UserID, DeviceID, RoomID string
	StreamID                 int64
}

// Unconverted is get_uncoverted_outbound_room_pokes: past (stream, room),
// up to the stream's current token, not converted, at most limit.
func (d *DB) Unconverted(ctx context.Context, stream int64, room string, upto int64, limit int) ([]Change, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id, device_id, room_id, stream_id FROM device_lists_changes_in_room
		WHERE (stream_id, room_id) > ($1, $2) AND stream_id <= $3 AND NOT converted_to_destinations
		ORDER BY stream_id ASC, room_id ASC LIMIT $4`, stream, room, upto, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Change, error) {
		var c Change
		return c, r.Scan(&c.UserID, &c.DeviceID, &c.RoomID, &c.StreamID)
	})
}

// AddOutboundPokes is _add_device_outbound_poke_to_stream_txn: a row per
// host under one stream ID; sent already for a remote user's device.
func (d *DB) AddOutboundPokes(ctx context.Context, instance string, id int64, userID, deviceID string, hosts []string, isMine bool, nowMS int64) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		for _, h := range hosts {
			if _, err := tx.Exec(ctx, `INSERT INTO device_lists_outbound_pokes
				(destination, instance_name, stream_id, user_id, device_id, sent, ts, opentracing_context)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				h, instance, id, userID, deviceID, !isMine, nowMS, emptyContext); err != nil {
				return err
			}
		}
		return nil
	})
}

// bgUnconvertedIdx is BG_UPDATE_ADD_UNCONVERTED_IDX.
const bgUnconvertedIdx = "device_lists_changes_in_room_unconverted_idx"

// MarkRedundant is mark_redundant_device_lists_pokes: once its index's
// background update is done, later unconverted changes of the same (user,
// device, room) are marked converted.
func (d *DB) MarkRedundant(ctx context.Context, userID, deviceID, roomID string, upto int64) error {
	var pending bool
	if err := d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM background_updates WHERE update_name = $1)`, bgUnconvertedIdx).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return nil
	}
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE device_lists_changes_in_room SET converted_to_destinations = true
			WHERE stream_id > $1 AND user_id = $2 AND device_id = $3 AND room_id = $4 AND NOT converted_to_destinations`,
			upto, userID, deviceID, roomID)
		return err
	})
}

// ConvertedPos is get_device_change_last_converted_pos: the least
// (stream_id, room_id) of the table.
func (d *DB) ConvertedPos(ctx context.Context) (int64, string, error) {
	var s int64
	var r string
	err := d.pool.QueryRow(ctx, `SELECT stream_id, room_id FROM device_lists_changes_converted_stream_position
		ORDER BY stream_id ASC, room_id ASC LIMIT 1`).Scan(&s, &r)
	return s, r, err
}

// SetConvertedPos is set_device_change_last_converted_pos (simple_update_one
// with no key: exactly one row).
func (d *DB) SetConvertedPos(ctx context.Context, instance string, stream int64, room string) error {
	tag, err := d.pool.Exec(ctx, `UPDATE device_lists_changes_converted_stream_position SET stream_id = $1, instance_name = $2, room_id = $3`,
		stream, instance, room)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("devicelists: converted position: %d rows", tag.RowsAffected())
	}
	return nil
}

// ConversionLag is get_device_list_conversion_lag: the inserted_ts of the
// oldest unconverted change (nil: none, or older than the column), and the
// converted position.
func (d *DB) ConversionLag(ctx context.Context) (*int64, int64, error) {
	s, r, err := d.ConvertedPos(ctx)
	if err != nil {
		return nil, 0, err
	}
	var ts *int64
	err = d.pool.QueryRow(ctx, `SELECT inserted_ts FROM device_lists_changes_in_room
		WHERE (stream_id, room_id) > ($1, $2) AND NOT converted_to_destinations ORDER BY stream_id ASC LIMIT 1`, s, r).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, s, nil
	}
	return ts, s, err
}

// Hosts is get_current_hosts_in_room_or_partial_state_approximation.
func (d *DB) Hosts(ctx context.Context, roomID string) (map[string]struct{}, error) {
	return roomhosts.Approx(ctx, d.pool, roomID)
}

// Update is one row of the stream.
type Update struct {
	Token int64
	Row   json.RawMessage
}

type idRow struct {
	id   int64
	user string
	sig  bool
	host bool
}

// Updates is DeviceListsStream._update_function: device changes for remotes
// (device_lists_stream, and each stream ID of device_lists_outbound_pokes
// once with hosts_calculated) and signature changes, each by stream_id and
// limited; the lower limit bounds both, merged by stream ID (devices first
// on a tie, heapq.merge's order).
func (d *DB) Updates(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error) {
	if from == upto {
		return nil, upto, false, nil
	}
	devRows, err := d.pool.Query(ctx, `SELECT stream_id, user_id, hosts FROM (
			SELECT stream_id, user_id, false AS hosts FROM device_lists_stream
			UNION ALL
			SELECT DISTINCT stream_id, user_id, true AS hosts FROM device_lists_outbound_pokes
		) AS e
		WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	devs, err := pgx.CollectRows(devRows, func(r pgx.CollectableRow) (idRow, error) {
		var x idRow
		return x, r.Scan(&x.id, &x.user, &x.host)
	})
	if err != nil {
		return nil, 0, false, err
	}
	sigRows, err := d.pool.Query(ctx, `SELECT stream_id, from_user_id FROM user_signature_stream
		WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	sigs, err := pgx.CollectRows(sigRows, func(r pgx.CollectableRow) (idRow, error) {
		x := idRow{sig: true}
		return x, r.Scan(&x.id, &x.user)
	})
	if err != nil {
		return nil, 0, false, err
	}
	top := upto
	devLimited, sigLimited := len(devs) >= limit, len(sigs) >= limit
	if devLimited {
		top = min(top, devs[len(devs)-1].id)
	}
	if sigLimited {
		top = min(top, sigs[len(sigs)-1].id)
	}
	var out []Update
	i, j := 0, 0
	for i < len(devs) || j < len(sigs) {
		var x idRow
		if j >= len(sigs) || (i < len(devs) && devs[i].id <= sigs[j].id) {
			x, i = devs[i], i+1
		} else {
			x, j = sigs[j], j+1
		}
		if x.id > top {
			continue
		}
		row, _ := json.Marshal([]any{x.user, x.sig, x.host})
		out = append(out, Update{Token: x.id, Row: row})
	}
	return out, top, devLimited || sigLimited, nil
}
