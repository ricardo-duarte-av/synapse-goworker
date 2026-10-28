// Package writer is what the event creator writes to Synapse's database
// itself, written as Synapse writes it. Everything else an event needs is
// done by the events writer, over replication (send_events).
//
// It connects as Synapse does, through pgcat with homeserver.yaml's user
// (goworker's rule), so nothing in the database limits it to Tables:
// TestQueriesUseOnlyTables keeps the queries and Tables in step, and
// TestTablesAreNarrow keeps Tables to what Synapse's event creator writes.
package writer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Table is a table the creator touches, and how.
type Table struct {
	Table, Privileges string
}

// Tables is every table the creator touches through this connection, and how.
var Tables = []Table{
	// add_push_actions_to_staging, and removal when persisting fails.
	{Table: "event_push_actions_staging", Privileges: "SELECT, INSERT, DELETE"},
	// set_room_participation.
	{Table: "room_memberships", Privileges: "SELECT, UPDATE (participant)"},
	{Table: "local_current_membership", Privileges: "SELECT"},
	// store_state_group, for a state event's new group. UPDATE (id) only so
	// the prev group can be locked FOR KEY SHARE, as
	// _get_existing_groups_with_lock does with lock=True; nothing updates it.
	{Table: "state_groups", Privileges: "SELECT, INSERT, UPDATE (id)"},
	{Table: "state_group_edges", Privileges: "SELECT, INSERT"},
	{Table: "state_groups_state", Privileges: "SELECT, INSERT"},
	// check_state_groups_and_bump_deletion.
	{Table: "state_groups_pending_deletion", Privileges: "SELECT, UPDATE"},
	// add_delayed_event: a delayed event's row, sent later by main.
	{Table: "delayed_events", Privileges: "SELECT, INSERT"},
	// The worker's own: profile changes whose join events are not all
	// sent yet (Setup).
	{Table: "goevent_profile_propagations", Privileges: "SELECT, INSERT, UPDATE, DELETE"},
}

// setup creates the worker's own tables in Synapse's database. Synapse does
// not know them and ignores them; dropping them loses only what they hold.
// They are goevent_-named, and nothing of Synapse's schema is altered.
var setup = []string{
	// One row per user whose profile change is being propagated: the
	// scheduled task Synapse would keep (update_join_states, its result's
	// last_room_id), here, as main runs every task in scheduled_tasks.
	`CREATE TABLE IF NOT EXISTS goevent_profile_propagations (
		user_id      TEXT PRIMARY KEY,
		job_id       TEXT NOT NULL,
		entity       TEXT NOT NULL,
		last_room_id TEXT,
		updated_ts   BIGINT NOT NULL
	)`,
}

const (
	// qStagePush is add_push_actions_to_staging's simple_insert_many
	// (storage/databases/main/event_push_actions.py).
	qStagePush = `
		INSERT INTO event_push_actions_staging (event_id, user_id, actions, notif, highlight, unread, thread_id, inserted_ts)
		SELECT $1, k.user_id, k.actions, k.notif, k.highlight, k.unread, k.thread_id, $2
		FROM unnest($3::text[], $4::text[], $5::smallint[], $6::smallint[], $7::smallint[], $8::text[])
			AS k(user_id, actions, notif, highlight, unread, thread_id)`
	// qUnstagePush is remove_push_actions_from_staging.
	qUnstagePush = `DELETE FROM event_push_actions_staging WHERE event_id = $1`

	// qRoomParticipation is _set_room_participation_txn
	// (storage/databases/main/roommember.py), verbatim.
	qRoomParticipation = `
		UPDATE room_memberships
		SET participant = true
		WHERE event_id IN (
			SELECT event_id FROM local_current_membership
			WHERE user_id = $1 AND room_id = $2
		)
		AND NOT participant`

	// add_delayed_event_txn (storage/databases/main/delayed_events.py).
	qDelayedCount     = `SELECT COUNT(*) FROM delayed_events WHERE user_localpart = $1`
	qDelayedThreshold = `SELECT MAX(send_ts) FROM (
		SELECT * FROM delayed_events WHERE user_localpart = $1 ORDER BY send_ts ASC LIMIT $2
	) AS subquery`
	qInsertDelayed = `INSERT INTO delayed_events (delay_id, user_localpart, device_id, delay, send_ts, room_id,
		event_type, state_key, origin_server_ts, content, sticky_duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	qNextDelayed = `SELECT MIN(send_ts) FROM delayed_events WHERE is_processed = false`

	// qExistingGroups is _get_existing_groups_with_lock with lock=False
	// (storage/databases/state/deletion.py).
	qExistingGroups = `SELECT id FROM state_groups WHERE id = ANY($1)`
	// qLockGroups is _get_existing_groups_with_lock with lock=True, as
	// store_state_group calls it for the prev group.
	qLockGroups = `SELECT id FROM state_groups WHERE id = ANY($1) FOR KEY SHARE`
	// qBumpDeletion is _bump_deletion_txn.
	qBumpDeletion = `
		UPDATE state_groups_pending_deletion
		SET sequence_number = DEFAULT, insertion_ts = $1
		WHERE state_group = ANY($2)`
)

// State groups, as store_state_group writes them
// (storage/databases/state/store.py; goinbound's port).
const (
	// maxStateDeltaHops is MAX_STATE_DELTA_HOPS (state/bg_updates.py): a
	// delta chain this long is ended with a full group.
	maxStateDeltaHops = 100

	// qStateHops is _count_state_group_hops_txn.
	qStateHops = `
		WITH RECURSIVE state(state_group) AS (
			VALUES($1::bigint)
			UNION ALL
			SELECT prev_state_group FROM state_group_edges e, state s
			WHERE s.state_group = e.state_group
		)
		SELECT count(*) FROM state`
	qNextStateGroup   = `SELECT nextval('state_group_id_seq')`
	qInsertStateGroup = `INSERT INTO state_groups (id, room_id, event_id) VALUES ($1, $2, $3)`
	qInsertEdge       = `INSERT INTO state_group_edges (state_group, prev_state_group) VALUES ($1, $2)`
	qInsertState      = `
		INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id)
		SELECT $1, $2, k.type, k.state_key, k.event_id
		FROM unnest($3::text[], $4::text[], $5::text[]) AS k(type, state_key, event_id)`
)

// DB is the write connection.
type DB struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// Open connects. Behind pgcat, a transaction pooler, statements are never
// prepared and nothing relies on session state.
func Open(ctx context.Context, dsn string, maxConns int32) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("writer: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("writer: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("writer: %w", err)
	}
	return &DB{pool: pool, now: time.Now}, nil
}

// Setup creates the worker's own tables if they are missing.
func (d *DB) Setup(ctx context.Context) error {
	for _, q := range setup {
		if _, err := d.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("writer: setup: %w", err)
		}
	}
	return nil
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// PushRow is a row for event_push_actions_staging; Actions is the column as
// _serialize_action writes it.
type PushRow struct {
	UserID    string
	Actions   string
	Notif     bool
	Highlight bool
	Unread    bool
	ThreadID  string
}

// StagePush is add_push_actions_to_staging. Nothing is written for no rows.
func (d *DB) StagePush(ctx context.Context, eventID string, rows []PushRow) error {
	if len(rows) == 0 {
		return nil
	}
	n := len(rows)
	users, actions, threads := make([]string, n), make([]string, n), make([]string, n)
	notif, highlight, unread := make([]int16, n), make([]int16, n), make([]int16, n)
	for i, r := range rows {
		users[i], actions[i], threads[i] = r.UserID, r.Actions, r.ThreadID
		notif[i], highlight[i], unread[i] = b2i(r.Notif), b2i(r.Highlight), b2i(r.Unread)
	}
	if _, err := d.pool.Exec(ctx, qStagePush, eventID, d.now().UnixMilli(), users, actions, notif, highlight, unread, threads); err != nil {
		return fmt.Errorf("writer: stage push actions: %w", err)
	}
	return nil
}

// UnstagePush is remove_push_actions_from_staging, after persisting failed.
func (d *DB) UnstagePush(ctx context.Context, eventID string) error {
	if _, err := d.pool.Exec(ctx, qUnstagePush, eventID); err != nil {
		return fmt.Errorf("writer: unstage push actions: %w", err)
	}
	return nil
}

// SetRoomParticipation is set_room_participation.
func (d *DB) SetRoomParticipation(ctx context.Context, userID, roomID string) error {
	if _, err := d.pool.Exec(ctx, qRoomParticipation, userID, roomID); err != nil {
		return fmt.Errorf("writer: room participation: %w", err)
	}
	return nil
}

// DelayedEvent is a delayed_events row.
type DelayedEvent struct {
	DelayID, UserLocalpart string
	DeviceID               *string
	DelayMS, SendTS        int64
	RoomID, EventType      string
	StateKey               *string
	OriginServerTS         *int64
	Content                string
	StickyDurationMS       *int64
}

// DelayedLimitError is add_delayed_event's LimitExceededError: the user has
// limit delayed events already. RetryAfterMS is nil when the threshold has
// passed.
type DelayedLimitError struct{ RetryAfterMS *int64 }

func (e *DelayedLimitError) Error() string { return "writer: too many delayed events" }

// AddDelayedEvent is add_delayed_event: the row, unless the user has limit
// already, and the send time of the next delayed event due.
func (d *DB) AddDelayedEvent(ctx context.Context, ev DelayedEvent, limit int) (nextSendTS int64, err error) {
	err = pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, qDelayedCount, ev.UserLocalpart).Scan(&n); err != nil {
			return fmt.Errorf("writer: delayed events count: %w", err)
		}
		if n >= limit {
			var max int64
			if err := tx.QueryRow(ctx, qDelayedThreshold, ev.UserLocalpart, n-limit+1).Scan(&max); err != nil {
				return fmt.Errorf("writer: delayed events threshold: %w", err)
			}
			le := &DelayedLimitError{}
			if retry := max - d.now().UnixMilli(); retry > 0 {
				le.RetryAfterMS = &retry
			}
			return le
		}
		if _, err := tx.Exec(ctx, qInsertDelayed, ev.DelayID, ev.UserLocalpart, ev.DeviceID, ev.DelayMS, ev.SendTS,
			ev.RoomID, ev.EventType, ev.StateKey, ev.OriginServerTS, ev.Content, ev.StickyDurationMS); err != nil {
			return fmt.Errorf("writer: delayed event: %w", err)
		}
		if err := tx.QueryRow(ctx, qNextDelayed).Scan(&nextSendTS); err != nil {
			return fmt.Errorf("writer: next delayed event: %w", err)
		}
		return nil
	})
	return nextSendTS, err
}

// BumpDeletion is check_state_groups_and_bump_deletion with lock=False:
// delays any pending deletion of the groups that exist, and reports those
// that are gone.
func (d *DB) BumpDeletion(ctx context.Context, groups []int64) (missing []int64, err error) {
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, qExistingGroups, groups)
		if err != nil {
			return fmt.Errorf("writer: existing groups: %w", err)
		}
		have, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("writer: existing groups: %w", err)
		}
		if len(have) > 0 {
			if _, err := tx.Exec(ctx, qBumpDeletion, d.now().UnixMilli(), have); err != nil {
				return fmt.Errorf("writer: bump deletion: %w", err)
			}
		}
		held := map[int64]bool{}
		for _, g := range have {
			held[g] = true
		}
		for _, g := range groups {
			if !held[g] {
				missing = append(missing, g)
			}
		}
		return nil
	})
	return missing, err
}

// ErrMissingGroup is a prev state group that has been deleted.
var ErrMissingGroup = errors.New("writer: state group deleted")

// StateRow is one entry of a state map.
type StateRow struct{ Type, StateKey, EventID string }

// StoreStateGroup is store_state_group with a prev group: a delta of the
// rows on prev when its chain is short enough, otherwise the whole state,
// which full supplies (the state of prev, with the delta applied).
func (d *DB) StoreStateGroup(ctx context.Context, eventID, roomID string, prev int64, delta []StateRow, full func() ([]StateRow, error)) (int64, error) {
	var group int64
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, qLockGroups, []int64{prev})
		if err != nil {
			return fmt.Errorf("writer: lock groups: %w", err)
		}
		have, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("writer: lock groups: %w", err)
		}
		if len(have) == 0 {
			return fmt.Errorf("%w: trying to persist state with unpersisted prev_group %d", ErrMissingGroup, prev)
		}
		if _, err := tx.Exec(ctx, qBumpDeletion, d.now().UnixMilli(), have); err != nil {
			return fmt.Errorf("writer: bump deletion: %w", err)
		}
		var hops int
		if err := tx.QueryRow(ctx, qStateHops, prev).Scan(&hops); err != nil {
			return fmt.Errorf("writer: state hops: %w", err)
		}
		if hops >= maxStateDeltaHops {
			return nil
		}
		if group, err = insertGroupTx(ctx, tx, roomID, eventID, delta); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, qInsertEdge, group, prev); err != nil {
			return fmt.Errorf("writer: state group edge: %w", err)
		}
		return nil
	})
	if err != nil || group != 0 {
		return group, err
	}
	rows, err := full()
	if err != nil {
		return 0, err
	}
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var err error
		group, err = insertGroupTx(ctx, tx, roomID, eventID, rows)
		return err
	})
	return group, err
}

// StoreFullStateGroup is store_state_group without a prev group: the whole
// state.
func (d *DB) StoreFullStateGroup(ctx context.Context, eventID, roomID string, rows []StateRow) (int64, error) {
	var group int64
	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var err error
		group, err = insertGroupTx(ctx, tx, roomID, eventID, rows)
		return err
	})
	return group, err
}

// insertGroupTx allocates a group and stores its rows.
func insertGroupTx(ctx context.Context, tx pgx.Tx, roomID, eventID string, rows []StateRow) (int64, error) {
	var group int64
	if err := tx.QueryRow(ctx, qNextStateGroup).Scan(&group); err != nil {
		return 0, fmt.Errorf("writer: state group id: %w", err)
	}
	if _, err := tx.Exec(ctx, qInsertStateGroup, group, roomID, eventID); err != nil {
		return 0, fmt.Errorf("writer: state group: %w", err)
	}
	types := make([]string, len(rows))
	keys := make([]string, len(rows))
	ids := make([]string, len(rows))
	for i, r := range rows {
		types[i], keys[i], ids[i] = r.Type, r.StateKey, r.EventID
	}
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, qInsertState, group, roomID, types, keys, ids); err != nil {
			return 0, fmt.Errorf("writer: state group rows: %w", err)
		}
	}
	return group, nil
}

func b2i(b bool) int16 {
	if b {
		return 1
	}
	return 0
}

const (
	// qSaveProfileJob starts (or restarts) a user's propagation: a newer
	// change replaces an older one, from the first room.
	qSaveProfileJob = `
		INSERT INTO goevent_profile_propagations (user_id, job_id, entity, last_room_id, updated_ts)
		VALUES ($1, $2, $3, NULL, $4)
		ON CONFLICT (user_id) DO UPDATE
		SET job_id = EXCLUDED.job_id, entity = EXCLUDED.entity, last_room_id = NULL, updated_ts = EXCLUDED.updated_ts`
	// qCheckpointProfileJob records a room done, for this job only: a
	// cancelled one must not move a newer one on.
	qCheckpointProfileJob = `
		UPDATE goevent_profile_propagations SET last_room_id = $3, updated_ts = $4
		WHERE user_id = $1 AND job_id = $2`
	qDeleteProfileJob = `DELETE FROM goevent_profile_propagations WHERE user_id = $1 AND job_id = $2`
	qProfileJobs      = `SELECT user_id, job_id, entity, last_room_id FROM goevent_profile_propagations ORDER BY user_id`
)

// ProfileJob is a profile change whose join events are not all sent.
type ProfileJob struct {
	UserID, JobID, Entity string
	// LastRoomID is the last room done, "" for none.
	LastRoomID string
}

// SaveProfileJob records a user's propagation before it starts.
func (d *DB) SaveProfileJob(ctx context.Context, j ProfileJob) error {
	if _, err := d.pool.Exec(ctx, qSaveProfileJob, j.UserID, j.JobID, j.Entity, d.now().UnixMilli()); err != nil {
		return fmt.Errorf("writer: save profile job: %w", err)
	}
	return nil
}

// CheckpointProfileJob records the last room done.
func (d *DB) CheckpointProfileJob(ctx context.Context, userID, jobID, roomID string) error {
	if _, err := d.pool.Exec(ctx, qCheckpointProfileJob, userID, jobID, roomID, d.now().UnixMilli()); err != nil {
		return fmt.Errorf("writer: checkpoint profile job: %w", err)
	}
	return nil
}

// DeleteProfileJob forgets a finished propagation, if it is still this job.
func (d *DB) DeleteProfileJob(ctx context.Context, userID, jobID string) error {
	if _, err := d.pool.Exec(ctx, qDeleteProfileJob, userID, jobID); err != nil {
		return fmt.Errorf("writer: delete profile job: %w", err)
	}
	return nil
}

// ProfileJobs are the propagations left unfinished, to resume.
func (d *DB) ProfileJobs(ctx context.Context) ([]ProfileJob, error) {
	rows, err := d.pool.Query(ctx, qProfileJobs)
	if err != nil {
		return nil, fmt.Errorf("writer: profile jobs: %w", err)
	}
	var out []ProfileJob
	for rows.Next() {
		var j ProfileJob
		var last *string
		if err := rows.Scan(&j.UserID, &j.JobID, &j.Entity, &last); err != nil {
			rows.Close()
			return nil, fmt.Errorf("writer: profile jobs: %w", err)
		}
		if last != nil {
			j.LastRoomID = *last
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("writer: profile jobs: %w", err)
	}
	return out, nil
}
