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

// PresenceForUsers is get_presence_for_users: the rows of those users that
// have one, NULLs read as 0 (Synapse's would be None, and fail its
// arithmetic; a row written by Synapse has none).
func (d *DB) PresenceForUsers(ctx context.Context, userIDs []string) (map[string]Presence, error) {
	rows, err := d.pool.Query(ctx, `SELECT user_id, state, last_active_ts, last_federation_update_ts,
		last_user_sync_ts, status_msg, currently_active FROM presence_stream WHERE user_id = ANY($1)`, userIDs)
	if err != nil {
		return nil, err
	}
	out := map[string]Presence{}
	for rows.Next() {
		var (
			p                             Presence
			state                         *string
			lastActive, lastFed, lastSync *int64
			active                        *bool
		)
		if err := rows.Scan(&p.UserID, &state, &lastActive, &lastFed, &lastSync, &p.StatusMsg, &active); err != nil {
			rows.Close()
			return nil, err
		}
		p.State = deref(state)
		p.LastActiveTS, p.LastFederationUpdateTS, p.LastUserSyncTS = deref(lastActive), deref(lastFed), deref(lastSync)
		p.CurrentlyActive = deref(active)
		out[p.UserID] = p
	}
	return out, rows.Err()
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// RoomsForUser is get_rooms_for_user: the rooms the user is joined to.
func (d *DB) RoomsForUser(ctx context.Context, userID string) ([]string, error) {
	return d.strings(ctx, `SELECT room_id FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND state_key = $1`, userID)
}

// UsersInRoom is get_users_in_room: the joined members.
func (d *DB) UsersInRoom(ctx context.Context, roomID string) ([]string, error) {
	return d.strings(ctx, `SELECT state_key FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND room_id = $1`, roomID)
}

// HostsInRoom is get_current_hosts_in_room's PostgreSQL query; a state key
// the regex does not match (NULL) is left out.
func (d *DB) HostsInRoom(ctx context.Context, roomID string) ([]string, error) {
	return d.strings(ctx, `SELECT DISTINCT substring(state_key FROM '@[^:]*:(.*)$') FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND room_id = $1`, roomID)
}

func (d *DB) strings(ctx context.Context, q string, arg any) ([]string, error) {
	rows, err := d.pool.Query(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		var s *string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return nil, err
		}
		if s != nil {
			out = append(out, *s)
		}
	}
	return out, rows.Err()
}

// StateDelta is a current_state_delta_stream row.
type StateDelta struct {
	StreamID    int64
	RoomID      string
	Type        string
	StateKey    string
	EventID     *string
	PrevEventID *string
}

// StateDeltas is get_partial_current_state_deltas: the rows after prev up
// to max, whole stream_id groups of them up to limit rows (at least the
// first group), and the position they go up to.
func (d *DB) StateDeltas(ctx context.Context, prev, maxID int64, limit int) (int64, []StateDelta, error) {
	rows, err := d.pool.Query(ctx, `SELECT stream_id, COUNT(*) FROM current_state_delta_stream
		WHERE stream_id > $1 AND stream_id <= $2 GROUP BY stream_id ORDER BY stream_id LIMIT $3`, prev, maxID, limit+1)
	if err != nil {
		return 0, nil, err
	}
	type group struct{ id, n int64 }
	groups, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (group, error) {
		var g group
		return g, r.Scan(&g.id, &g.n)
	})
	if err != nil {
		return 0, nil, err
	}
	if len(groups) == 0 {
		return maxID, nil, nil
	}
	upto, included, all := groups[0].id, groups[0].n, true
	for _, g := range groups[1:] {
		if included+g.n > int64(limit) {
			all = false
			break
		}
		included += g.n
		upto = g.id
	}
	caughtUp := all && len(groups) < limit+1
	rows, err = d.pool.Query(ctx, `SELECT stream_id, room_id, type, state_key, event_id, prev_event_id
		FROM current_state_delta_stream WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC`, prev, upto)
	if err != nil {
		return 0, nil, err
	}
	deltas, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (StateDelta, error) {
		var s StateDelta
		return s, r.Scan(&s.StreamID, &s.RoomID, &s.Type, &s.StateKey, &s.EventID, &s.PrevEventID)
	})
	if err != nil {
		return 0, nil, err
	}
	if caughtUp {
		upto = maxID
	}
	return upto, deltas, nil
}

// Membership is get_event(event_id, allow_none=True)'s content.membership:
// "" for an event not found or rejected.
func (d *DB) Membership(ctx context.Context, eventID string) (string, error) {
	var m string
	err := d.pool.QueryRow(ctx, `SELECT rm.membership FROM room_memberships rm
		WHERE rm.event_id = $1 AND NOT EXISTS (SELECT 1 FROM rejections r WHERE r.event_id = rm.event_id)`, eventID).Scan(&m)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return m, err
}
