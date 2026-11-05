package accountdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
)

// DB is what the account-data writer reads and writes in Synapse's
// database, through its own pool, in Exec mode (pgcat).
type DB struct {
	pool  *pgxpool.Pool
	nowMS func() int64
}

// Open connects.
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("accountdata: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 2)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("accountdata: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("accountdata: ping: %w", err)
	}
	return NewDB(pool), nil
}

// NewDB uses pool.
func NewDB(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool, nowMS: func() int64 { return time.Now().UnixMilli() }}
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Pool is the pool, for the stream ID generator.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

// maxAttempts is new_transaction's MAX_NUMBER_OF_ATTEMPTS.
const maxAttempts = 5

// runTxn is runInteraction: Synapse's default isolation on Postgres
// (REPEATABLE READ), and the transaction run again on a serialization
// failure or deadlock (psycopg2's TransactionRollbackError is an
// OperationalError), at most maxAttempts times in all.
func (d *DB) runTxn(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for range maxAttempts {
		err = pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, fn)
		if !retryable(err) {
			return err
		}
	}
	return err
}

func retryable(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40001" || pg.Code == "40P01")
}

func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

// Caches whose rows the ignored-user list streams.
const (
	cacheIgnoredBy    = "ignored_by"
	cacheIgnoredUsers = "ignored_users"
)

// TypeIgnoredUserList is AccountDataTypes.IGNORED_USER_LIST.
const TypeIgnoredUserList = "m.ignored_user_list"

// ErrIgnoreSelf is the user in their own ignored list.
var ErrIgnoreSelf = errors.New("You cannot ignore yourself")

const qUpsertUser = `
	INSERT INTO account_data (user_id, account_data_type, stream_id, content)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (user_id, account_data_type) DO UPDATE SET
		stream_id = EXCLUDED.stream_id, content = EXCLUDED.content`

// AddUser is _add_account_data_for_user, in one transaction: the upsert
// and, for m.ignored_user_list, the ignored_users rows and their caches
// rows (returned, for the caller to publish). content is as json_encoder
// writes it.
func (d *DB) AddUser(ctx context.Context, instance string, streamID int64, userID, dataType, content string) ([]cachestream.Row, error) {
	var out []cachestream.Row
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		out = nil
		if _, err := tx.Exec(ctx, qUpsertUser, userID, dataType, streamID, content); err != nil {
			return err
		}
		if dataType != TypeIgnoredUserList {
			return nil
		}
		previous, err := ignoring(ctx, tx, userID)
		if err != nil {
			return err
		}
		current, err := ignoredIn(content)
		if err != nil {
			return err
		}
		if current[userID] {
			return ErrIgnoreSelf
		}
		if sameSet(previous, current) {
			return nil
		}
		var gone, added []string
		for u := range previous {
			if !current[u] {
				gone = append(gone, u)
			}
		}
		for u := range current {
			if !previous[u] {
				added = append(added, u)
			}
		}
		if len(gone) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM ignored_users WHERE ignored_user_id = ANY($1) AND ignorer_user_id = $2`, gone, userID); err != nil {
				return err
			}
		}
		for _, u := range added {
			if _, err := tx.Exec(ctx, `INSERT INTO ignored_users (ignorer_user_id, ignored_user_id) VALUES ($1, $2)`, userID, u); err != nil {
				return err
			}
		}
		changed := make([][]string, 0, len(gone)+len(added))
		for _, u := range append(gone, added...) {
			changed = append(changed, []string{u})
		}
		rows, err := cachestream.Insert(ctx, tx, instance, cacheIgnoredBy, changed, d.nowMS())
		if err != nil {
			return err
		}
		self, err := cachestream.Insert(ctx, tx, instance, cacheIgnoredUsers, [][]string{{userID}}, d.nowMS())
		if err != nil {
			return err
		}
		out = append(rows, self...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ignoring is the users userID ignores, as ignored_users has them.
func ignoring(ctx context.Context, tx pgx.Tx, userID string) (map[string]bool, error) {
	rows, err := tx.Query(ctx, `SELECT ignored_user_id FROM ignored_users WHERE ignorer_user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(ids))
	for _, u := range ids {
		out[u] = true
	}
	return out, nil
}

// ignoredIn is content.get("ignored_users", {}): its keys if it is an
// object, else nobody. content is json_encoder's (no repeated keys).
func ignoredIn(content string) (map[string]bool, error) {
	root := gjson.Parse(content)
	if !root.IsObject() {
		// content.get on a non-dict: AttributeError.
		return nil, errors.New("accountdata: m.ignored_user_list content is not an object")
	}
	out := map[string]bool{}
	v := root.Get(gjsonKey("ignored_users"))
	if v.IsObject() {
		v.ForEach(func(k, _ gjson.Result) bool {
			out[k.Str] = true
			return true
		})
	}
	return out, nil
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

const qRemoveUser = `
	UPDATE account_data SET stream_id = $1, content = '{}'
	WHERE user_id = $2 AND account_data_type = $3 AND content != '{}'`

// RemoveUser is _remove_account_data_for_user_txn: whether a row was
// emptied, and the caches rows of the ignored list it cleared.
func (d *DB) RemoveUser(ctx context.Context, instance string, streamID int64, userID, dataType string) (bool, []cachestream.Row, error) {
	var (
		updated bool
		out     []cachestream.Row
	)
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		updated, out = false, nil
		tag, err := tx.Exec(ctx, qRemoveUser, streamID, userID, dataType)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		updated = true
		if dataType != TypeIgnoredUserList {
			return nil
		}
		previous, err := ignoring(ctx, tx, userID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM ignored_users WHERE ignorer_user_id = $1`, userID); err != nil {
			return err
		}
		keys := make([][]string, 0, len(previous))
		for u := range previous {
			keys = append(keys, []string{u})
		}
		rows, err := cachestream.Insert(ctx, tx, instance, cacheIgnoredBy, keys, d.nowMS())
		if err != nil {
			return err
		}
		self, err := cachestream.Insert(ctx, tx, instance, cacheIgnoredUsers, [][]string{{userID}}, d.nowMS())
		if err != nil {
			return err
		}
		out = append(rows, self...)
		return nil
	})
	if err != nil {
		return false, nil, err
	}
	return updated, out, nil
}

const qUpsertRoom = `
	INSERT INTO room_account_data (user_id, room_id, account_data_type, stream_id, content)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (user_id, room_id, account_data_type) DO UPDATE SET
		stream_id = EXCLUDED.stream_id, content = EXCLUDED.content`

// AddRoom is add_account_data_to_room's simple_upsert.
func (d *DB) AddRoom(ctx context.Context, streamID int64, userID, roomID, dataType, content string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, qUpsertRoom, userID, roomID, dataType, streamID, content)
		return err
	})
}

const qRemoveRoom = `
	UPDATE room_account_data SET stream_id = $1, content = '{}'
	WHERE user_id = $2 AND room_id = $3 AND account_data_type = $4 AND content != '{}'`

// RemoveRoom is _remove_account_data_for_room_txn: whether a row was
// emptied.
func (d *DB) RemoveRoom(ctx context.Context, streamID int64, userID, roomID, dataType string) (bool, error) {
	var updated bool
	err := d.runTxn(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, qRemoveRoom, streamID, userID, roomID, dataType)
		updated = err == nil && tag.RowsAffected() > 0
		return err
	})
	return updated, err
}

const qUpsertTag = `
	INSERT INTO room_tags (user_id, room_id, tag, content) VALUES ($1, $2, $3, $4)
	ON CONFLICT (user_id, room_id, tag) DO UPDATE SET content = EXCLUDED.content`

// errLostRevision ends a tag transaction the way psycopg2 ends it in
// Synapse; see updateRevision.
var errLostRevision = errors.New("accountdata: room_tags_revisions insert lost a race")

// AddTag is add_tag_txn: the tag's upsert, then the revision.
func (d *DB) AddTag(ctx context.Context, streamID int64, userID, roomID, tag, content string) error {
	return d.tagTxn(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, qUpsertTag, userID, roomID, tag, content); err != nil {
			return err
		}
		return updateRevision(ctx, tx, userID, roomID, streamID)
	})
}

// RemoveTag is remove_tag_txn.
func (d *DB) RemoveTag(ctx context.Context, streamID int64, userID, roomID, tag string) error {
	return d.tagTxn(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM room_tags WHERE user_id = $1 AND room_id = $2 AND tag = $3`, userID, roomID, tag); err != nil {
			return err
		}
		return updateRevision(ctx, tx, userID, roomID, streamID)
	})
}

func (d *DB) tagTxn(ctx context.Context, fn func(pgx.Tx) error) error {
	err := d.runTxn(ctx, fn)
	if errors.Is(err, errLostRevision) {
		return nil
	}
	return err
}

// updateRevision is _update_revision_txn: UPDATE, else INSERT, with the
// INSERT's IntegrityError caught "because if two updates happened
// concurrently the one with the higher stream_id will not be reported".
// On Postgres the catch does not save the transaction: the failed INSERT
// has aborted it, and psycopg2's commit() of an aborted transaction is a
// silent ROLLBACK. So in Synapse the whole tag write is lost and the
// request answers 200. Reproduced, not fixed: errLostRevision rolls back,
// and tagTxn reports success.
func updateRevision(ctx context.Context, tx pgx.Tx, userID, roomID string, streamID int64) error {
	tag, err := tx.Exec(ctx, `UPDATE room_tags_revisions SET stream_id = $1 WHERE user_id = $2 AND room_id = $3`, streamID, userID, roomID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	_, err = tx.Exec(ctx, `INSERT INTO room_tags_revisions (user_id, room_id, stream_id) VALUES ($1, $2, $3)`, userID, roomID, streamID)
	if isUniqueViolation(err) {
		return errLostRevision
	}
	return err
}

// Global is get_global_account_data_by_type_for_user: the stored content,
// or nil.
func (d *DB) Global(ctx context.Context, userID, dataType string) (json.RawMessage, error) {
	return d.one(ctx, `SELECT content FROM account_data WHERE user_id = $1 AND account_data_type = $2`, userID, dataType)
}

// Room is get_account_data_for_room_and_type.
func (d *DB) Room(ctx context.Context, userID, roomID, dataType string) (json.RawMessage, error) {
	return d.one(ctx, `SELECT content FROM room_account_data WHERE user_id = $1 AND room_id = $2 AND account_data_type = $3`, userID, roomID, dataType)
}

func (d *DB) one(ctx context.Context, q string, args ...any) (json.RawMessage, error) {
	var s string
	err := d.pool.QueryRow(ctx, q, args...).Scan(&s)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(s), nil
}

// Tag is one of a room's tags, in the order the table gives them.
type Tag struct {
	Name    string
	Content json.RawMessage
}

// Tags is get_tags_for_room (simple_select_list, no ORDER BY, as Synapse's).
func (d *DB) Tags(ctx context.Context, userID, roomID string) ([]Tag, error) {
	rows, err := d.pool.Query(ctx, `SELECT tag, content FROM room_tags WHERE user_id = $1 AND room_id = $2`, userID, roomID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Tag, error) {
		var t Tag
		var c string
		err := r.Scan(&t.Name, &c)
		t.Content = json.RawMessage(c)
		return t, err
	})
}

// HasMembership is get_local_current_membership_for_user_in_room not
// being None.
func (d *DB) HasMembership(ctx context.Context, userID, roomID string) (bool, error) {
	var one int
	err := d.pool.QueryRow(ctx, `SELECT 1 FROM local_current_membership WHERE user_id = $1 AND room_id = $2`, userID, roomID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Update is one row of the stream.
type Update struct {
	Token int64
	Row   json.RawMessage
}

type idRow struct {
	id                   int64
	user, room, dataType string
	hasRoom              bool
}

// Updates is AccountDataStream._update_function: the three tables, each
// (from, upto] by stream_id and at most limit; one that fills its limit
// lowers upto to its last row for the queries after it, and every row past
// the final upto is dropped. The tables are read in Synapse's order
// (global, room, tags), and merged by stream ID only, rooms first on a tie
// (heapq.merge of room, global, tag rows).
func (d *DB) Updates(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error) {
	if from == upto {
		return nil, upto, false, nil
	}
	limited := false
	global, err := d.idRows(ctx, `SELECT stream_id, user_id, account_data_type FROM account_data
		WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`, false, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	if len(global) >= limit {
		upto, limited = global[len(global)-1].id, true
	}
	room, err := d.idRows(ctx, `SELECT stream_id, user_id, room_id, account_data_type FROM room_account_data
		WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`, true, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	if len(room) >= limit {
		upto, limited = room[len(room)-1].id, true
	}
	var tags []idRow
	if from != upto {
		rows, err := d.pool.Query(ctx, `SELECT stream_id, user_id, room_id FROM room_tags_revisions
			WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`, from, upto, limit)
		if err != nil {
			return nil, 0, false, err
		}
		tags, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (idRow, error) {
			x := idRow{dataType: "m.tag", hasRoom: true}
			return x, r.Scan(&x.id, &x.user, &x.room)
		})
		if err != nil {
			return nil, 0, false, err
		}
		if len(tags) >= limit {
			upto, limited = tags[len(tags)-1].id, true
		}
	}
	merged := mergeRows(upto, room, global, tags)
	out := make([]Update, 0, len(merged))
	for _, r := range merged {
		var room any
		if r.hasRoom {
			room = r.room
		}
		row, err := json.Marshal([]any{r.user, room, r.dataType})
		if err != nil {
			return nil, 0, false, err
		}
		out = append(out, Update{Token: r.id, Row: row})
	}
	return out, upto, limited, nil
}

func (d *DB) idRows(ctx context.Context, q string, withRoom bool, args ...any) ([]idRow, error) {
	rows, err := d.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (idRow, error) {
		x := idRow{hasRoom: withRoom}
		if withRoom {
			return x, r.Scan(&x.id, &x.user, &x.room, &x.dataType)
		}
		return x, r.Scan(&x.id, &x.user, &x.dataType)
	})
}

// mergeRows is heapq.merge(room_rows, global_rows, tag_rows, key=id) of
// the rows at or below upto: stable, so on equal IDs the earlier list's row
// comes first.
func mergeRows(upto int64, lists ...[]idRow) []idRow {
	var out []idRow
	idx := make([]int, len(lists))
	for {
		best := -1
		for i, l := range lists {
			for idx[i] < len(l) && l[idx[i]].id > upto {
				idx[i] = len(l) // ordered: the rest are past upto too
			}
			if idx[i] >= len(l) {
				continue
			}
			if best == -1 || l[idx[i]].id < lists[best][idx[best]].id {
				best = i
			}
		}
		if best == -1 {
			return out
		}
		out = append(out, lists[best][idx[best]])
		idx[best]++
	}
}

// gjsonKey escapes a key for gjson's path syntax.
func gjsonKey(k string) string {
	out := make([]byte, 0, len(k))
	for i := 0; i < len(k); i++ {
		switch c := k[i]; c {
		case '\\', '.', '*', '?', '|', '#', '@', '!', '=', '<', '>', '%':
			out = append(out, '\\', c)
		default:
			out = append(out, c)
		}
	}
	return string(out)
}
