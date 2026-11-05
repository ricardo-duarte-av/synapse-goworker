package receipts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/roomhosts"
)

// MainTimeline is the thread ID of the main timeline (MAIN_TIMELINE).
const MainTimeline = "main"

// DB is what the receipts writer reads and writes in Synapse's database,
// through its own pool, in Exec mode (pgcat).
type DB struct{ pool *pgxpool.Pool }

// Open connects.
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("receipts: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 2)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("receipts: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("receipts: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Pool is the pool, for the stream ID generator.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

// Receipt is one receipt to store (ReadReceipt), with one event: the
// linearized one.
type Receipt struct {
	RoomID      string
	ReceiptType string
	UserID      string
	EventID     string
	// ThreadID is nil for none (NULL). A remote server's "" is a string,
	// and Synapse stores it as one.
	ThreadID *string
	// EventIDs is what the receipt named, for receipts_graph.
	EventIDs []string
	// Data is the data as json_encoder writes it (pyjson.Encode).
	Data string
}

// InsertLinearized is _insert_linearized_receipt_txn, at READ COMMITTED as
// insert_receipt runs it. stored is whether a row was written; new is what
// Synapse's return value says: false for a receipt older than the stored
// one (nothing written) and, as Synapse's code has it, also for a row
// written for an event with no received_ts (rx_ts None).
func (d *DB) InsertLinearized(ctx context.Context, r Receipt, streamID int64, instance string) (stored, isNew bool, err error) {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		ordering *int64
		rxTS     *int64
		known    bool
	)
	err = tx.QueryRow(ctx, `SELECT stream_ordering, received_ts FROM events WHERE event_id = $1`, r.EventID).Scan(&ordering, &rxTS)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return false, false, err
	default:
		known = true
	}
	if known && ordering != nil {
		// Don't clobber a receipt for a later event. One with no ordering
		// is a remote receipt for an event we lack: a new one replaces it.
		q := `SELECT r.event_stream_ordering FROM receipts_linearized AS r
			WHERE r.room_id = $1 AND r.receipt_type = $2 AND r.user_id = $3
			AND r.event_stream_ordering IS NOT NULL AND `
		args := []any{r.RoomID, r.ReceiptType, r.UserID}
		if r.ThreadID == nil {
			q += `r.thread_id IS NULL`
		} else {
			q += `r.thread_id = $4`
			args = append(args, *r.ThreadID)
		}
		rows, err := tx.Query(ctx, q, args...)
		if err != nil {
			return false, false, err
		}
		later := false
		for rows.Next() {
			var so int64
			if err := rows.Scan(&so); err != nil {
				rows.Close()
				return false, false, err
			}
			if so >= *ordering {
				later = true
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return false, false, err
		}
		if later {
			return false, false, nil
		}
	}

	// simple_upsert_txn_native_upsert: keyvalues, then values, in Synapse's
	// order; the conflict target is the partial unique index.
	cols := []string{"room_id", "receipt_type", "user_id"}
	args := []any{r.RoomID, r.ReceiptType, r.UserID}
	where := "WHERE thread_id IS NULL "
	if r.ThreadID != nil {
		cols = append(cols, "thread_id")
		args = append(args, *r.ThreadID)
		where = ""
	}
	conflict := strings.Join(cols, ", ")
	var eso any
	if ordering != nil {
		eso = *ordering
	}
	cols = append(cols, "stream_id", "instance_name", "event_id", "event_stream_ordering", "data")
	args = append(args, streamID, instance, r.EventID, eso, r.Data)
	if _, err := tx.Exec(ctx, upsertSQL("receipts_linearized", cols, conflict, where,
		[]string{"stream_id", "instance_name", "event_id", "event_stream_ordering", "data"}), args...); err != nil {
		return false, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, false, err
	}
	return true, rxTS != nil || !known, nil
}

// InsertGraph is _insert_graph_receipt: receipts_graph, in a transaction of
// its own after the linearized row.
func (d *DB) InsertGraph(ctx context.Context, r Receipt) error {
	ids := make([]any, len(r.EventIDs))
	for i, e := range r.EventIDs {
		ids[i] = e
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	eventIDs, err := pyjson.Encode(raw)
	if err != nil {
		return err
	}
	cols := []string{"room_id", "receipt_type", "user_id"}
	args := []any{r.RoomID, r.ReceiptType, r.UserID}
	where := "WHERE thread_id IS NULL "
	if r.ThreadID != nil {
		cols = append(cols, "thread_id")
		args = append(args, *r.ThreadID)
		where = ""
	}
	conflict := strings.Join(cols, ", ")
	cols = append(cols, "event_ids", "data")
	args = append(args, eventIDs, r.Data)
	_, err = d.pool.Exec(ctx, upsertSQL("receipts_graph", cols, conflict, where, []string{"event_ids", "data"}), args...)
	return err
}

func upsertSQL(table string, cols []string, conflict, where string, update []string) string {
	ph := make([]string, len(cols))
	for i := range cols {
		ph[i] = fmt.Sprintf("$%d", i+1)
	}
	set := make([]string, len(update))
	for i, c := range update {
		set[i] = c + "=EXCLUDED." + c
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) %sDO UPDATE SET %s",
		table, strings.Join(cols, ", "), strings.Join(ph, ", "), conflict, where, strings.Join(set, ", "))
}

// Updates is get_all_updated_receipts: our rows in (from, upto], by
// stream_id, at most limit; a full page is limited and ends at its last
// row. Each row is ReceiptsStream's [room_id, receipt_type, user_id,
// event_id, thread_id, data].
func (d *DB) Updates(ctx context.Context, instance string, from, upto int64, limit int) ([]Update, int64, bool, error) {
	if from == upto {
		return nil, upto, false, nil
	}
	rows, err := d.pool.Query(ctx, `
		SELECT stream_id, room_id, receipt_type, user_id, event_id, thread_id, data
		FROM receipts_linearized
		WHERE $1 < stream_id AND stream_id <= $2
		AND instance_name = $3
		ORDER BY stream_id ASC
		LIMIT $4`, from, upto, instance, limit)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()
	var out []Update
	for rows.Next() {
		var (
			id                       int64
			room, rtype, user, event string
			thread                   *string
			data                     string
		)
		if err := rows.Scan(&id, &room, &rtype, &user, &event, &thread, &data); err != nil {
			return nil, 0, false, err
		}
		row, err := encodeRow(room, rtype, user, event, thread, data)
		if err != nil {
			return nil, 0, false, err
		}
		out = append(out, Update{Token: id, Row: row})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, err
	}
	if len(out) == limit {
		return out, out[len(out)-1].Token, true, nil
	}
	return out, upto, false, nil
}

// Update is one row of the stream.
type Update struct {
	Token int64
	Row   json.RawMessage
}

// encodeRow is the row as json_encoder writes attr.astuple(row): strings
// ensure_ascii, the data (stored by json_encoder) as it is.
func encodeRow(room, rtype, user, event string, thread *string, data string) (json.RawMessage, error) {
	parts := []any{room, rtype, user, event, nil}
	if thread != nil {
		parts[4] = *thread
	}
	head, err := json.Marshal(parts)
	if err != nil {
		return nil, err
	}
	enc, err := pyjson.Encode(head)
	if err != nil {
		return nil, err
	}
	if !gjson.Valid(data) {
		return nil, fmt.Errorf("receipts: stored data is not JSON: %.40q", data)
	}
	// db_to_json then json_encoder: the stored text, re-encoded as Synapse
	// re-encodes it, which for text it wrote itself is the text.
	d, err := pyjson.Encode([]byte(data))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(enc[:len(enc)-1] + "," + d + "]"), nil
}

// ThreadID is get_thread_id: the thread the event is in, following its
// relations up (at most four levels), or MainTimeline.
func (d *DB) ThreadID(ctx context.Context, eventID string) (string, error) {
	var t string
	err := d.pool.QueryRow(ctx, `
		WITH RECURSIVE related_events AS (
			SELECT event_id, relates_to_id, relation_type, 0 AS depth
			FROM event_relations
			WHERE event_id = $1
			UNION SELECT e.event_id, e.relates_to_id, e.relation_type, depth + 1
			FROM event_relations e
			INNER JOIN related_events r ON r.relates_to_id = e.event_id
			WHERE depth <= 3
		)
		SELECT relates_to_id FROM related_events
		WHERE relation_type = 'm.thread'
		ORDER BY depth DESC
		LIMIT 1`, eventID).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) {
		return MainTimeline, nil
	}
	return t, err
}

// ThreadIDForReceipts is get_thread_id_for_receipts: the top-most related
// event, if some event has a thread relation to it, or MainTimeline.
func (d *DB) ThreadIDForReceipts(ctx context.Context, eventID string) (string, error) {
	var t string
	err := d.pool.QueryRow(ctx, `
		SELECT relates_to_id FROM event_relations WHERE relates_to_id = COALESCE((
			WITH RECURSIVE related_events AS (
				SELECT event_id, relates_to_id, relation_type, 0 AS depth
				FROM event_relations
				WHERE event_id = $1
				UNION SELECT e.event_id, e.relates_to_id, e.relation_type, depth + 1
				FROM event_relations e
				INNER JOIN related_events r ON r.relates_to_id = e.event_id
				WHERE depth <= 3
			)
			SELECT relates_to_id FROM related_events
			ORDER BY depth DESC
			LIMIT 1
		), $2) AND relation_type = 'm.thread' LIMIT 1`, eventID, eventID).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) {
		return MainTimeline, nil
	}
	return t, err
}

// RoomAccountData is get_account_data_for_room_and_type: the content, nil
// for none.
func (d *DB) RoomAccountData(ctx context.Context, userID, roomID, dataType string) (json.RawMessage, error) {
	var content string
	err := d.pool.QueryRow(ctx, `
		SELECT content FROM room_account_data
		WHERE user_id = $1 AND room_id = $2 AND account_data_type = $3`, userID, roomID, dataType).Scan(&content)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(content), nil
}

// ErrNoEvent is get_event_ordering's SynapseError(404, "Could not find
// event %s in room %s").
type ErrNoEvent struct{ EventID, RoomID string }

func (e *ErrNoEvent) Error() string {
	return fmt.Sprintf("Could not find event %s in room %s", e.EventID, e.RoomID)
}

// Ordering is an event's (topological_ordering, stream_ordering).
type Ordering struct{ Topological, Stream int64 }

// After is Python's tuple comparison o > p.
func (o Ordering) After(p Ordering) bool {
	if o.Topological != p.Topological {
		return o.Topological > p.Topological
	}
	return o.Stream > p.Stream
}

// EventOrdering is get_event_ordering.
func (d *DB) EventOrdering(ctx context.Context, eventID, roomID string) (Ordering, error) {
	var o Ordering
	err := d.pool.QueryRow(ctx, `
		SELECT topological_ordering, stream_ordering FROM events
		WHERE event_id = $1 AND room_id = $2`, eventID, roomID).Scan(&o.Topological, &o.Stream)
	if errors.Is(err, pgx.ErrNoRows) {
		return Ordering{}, &ErrNoEvent{eventID, roomID}
	}
	return o, err
}

// HostJoined and HostsApprox are the room-host checks a remote receipt
// passes.
func (d *DB) HostJoined(ctx context.Context, roomID, host string) (bool, error) {
	return roomhosts.HostJoined(ctx, d.pool, roomID, host)
}

func (d *DB) HostsApprox(ctx context.Context, roomID string) (map[string]struct{}, error) {
	return roomhosts.Approx(ctx, d.pool, roomID)
}

// ShadowBanned is unused by receipts (Synapse checks none), kept off.
