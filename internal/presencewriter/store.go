package presencewriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
)

// State is a UserPresenceState as presence_stream holds it. Every column
// but the user and the stream ID is nullable (synapse#16467), and Synapse
// writes back what it read.
type State struct {
	UserID                 string
	State                  string
	LastActiveTS           *int64
	LastFederationUpdateTS *int64
	LastUserSyncTS         *int64
	StatusMsg              *string
	CurrentlyActive        bool
}

// DB is the writer's database work.
type DB struct{ pool *pgxpool.Pool }

// Open connects a pool of its own (database.pools.presence).
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("presencewriter: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 2)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("presencewriter: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("presencewriter: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// NewDB uses pool.
func NewDB(pool *pgxpool.Pool) *DB { return &DB{pool: pool} }

// Pool is the pool, for the stream ID generator.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Active is _get_active_presence: every state that is not offline, which
// the writer loads at start.
func (d *DB) Active(ctx context.Context) ([]State, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id, state, last_active_ts, last_federation_update_ts,
		last_user_sync_ts, status_msg, currently_active FROM presence_stream WHERE state != 'offline'`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (State, error) {
		var s State
		var active *bool
		err := r.Scan(&s.UserID, &s.State, &s.LastActiveTS, &s.LastFederationUpdateTS,
			&s.LastUserSyncTS, &s.StatusMsg, &active)
		// bool(currently_active): NULL is False.
		s.CurrentlyActive = active != nil && *active
		return s, err
	})
}

// Persist is _update_presence_txn at READ COMMITTED: each user's older
// rows deleted (in batches of 50 users, below the last of the IDs, as
// Synapse's loop variable leaves it), then a row per state at its ID.
func (d *DB) Persist(ctx context.Context, instance string, ids []int64, states []State) error {
	if len(ids) != len(states) || len(ids) == 0 {
		return fmt.Errorf("presencewriter: %d ids for %d states", len(ids), len(states))
	}
	last := ids[len(ids)-1]
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for start := 0; start < len(states); start += 50 {
		end := min(start+50, len(states))
		users := make([]string, 0, end-start)
		for _, s := range states[start:end] {
			users = append(users, s.UserID)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM presence_stream WHERE stream_id < $1 AND user_id = ANY($2)`,
			last, users); err != nil {
			return err
		}
	}
	var (
		users, stateCol, instances    []string
		msgs                          []*string
		lastActive, lastFed, lastSync []*int64
		active                        []bool
	)
	for _, s := range states {
		users = append(users, s.UserID)
		stateCol = append(stateCol, s.State)
		lastActive = append(lastActive, s.LastActiveTS)
		lastFed = append(lastFed, s.LastFederationUpdateTS)
		lastSync = append(lastSync, s.LastUserSyncTS)
		msgs = append(msgs, s.StatusMsg)
		active = append(active, s.CurrentlyActive)
		instances = append(instances, instance)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO presence_stream (stream_id, user_id, state, last_active_ts,
			last_federation_update_ts, last_user_sync_ts, status_msg, currently_active, instance_name)
		SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[],
			$7::text[], $8::boolean[], $9::text[])`,
		ids, users, stateCol, lastActive, lastFed, lastSync, msgs, active, instances); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Updates is get_all_presence_updates: rows with from < stream_id <= upto,
// at most limit, as PresenceStreamRow (user_id, state, last_active_ts,
// last_federation_update_ts, last_user_sync_ts, status_msg,
// currently_active) with the columns' values as read.
func (d *DB) Updates(ctx context.Context, from, upto int64, limit int) ([]streamwriter.Update, int64, bool, error) {
	if from == upto {
		return nil, upto, false, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT stream_id, user_id, state, last_active_ts,
			last_federation_update_ts, last_user_sync_ts, status_msg, currently_active
		FROM presence_stream WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`,
		from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (streamwriter.Update, error) {
		var (
			u                             streamwriter.Update
			user                          string
			state, msg                    *string
			lastActive, lastFed, lastSync *int64
			active                        *bool
		)
		if err := r.Scan(&u.Token, &user, &state, &lastActive, &lastFed, &lastSync, &msg, &active); err != nil {
			return u, err
		}
		row, err := encodeRow(user, state, lastActive, lastFed, lastSync, msg, active)
		u.Row = row
		return u, err
	})
	if err != nil {
		return nil, 0, false, err
	}
	if len(out) >= limit {
		return out, out[len(out)-1].Token, true, nil
	}
	return out, upto, false, nil
}

// encodeRow is the row as json_encoder writes it (ensure_ascii).
func encodeRow(user string, state *string, lastActive, lastFed, lastSync *int64, msg *string, active *bool) (json.RawMessage, error) {
	raw, err := json.Marshal([]any{user, state, lastActive, lastFed, lastSync, msg, active})
	if err != nil {
		return nil, err
	}
	enc, err := pyjson.Encode(raw)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(enc), nil
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
