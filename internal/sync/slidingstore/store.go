package slidingstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Timings, all Synapse's (storage/databases/main/sliding_sync.py,
// types/handlers/sliding_sync.py).
const (
	// UpdateLastUsedIntervalMS is how stale last_used_ts may get. Writing it on
	// every request would put a write on the read path of every sync for a
	// value only the reaper reads.
	UpdateLastUsedIntervalMS = int64(5 * 60 * 1000)
	// ConnectionExpiryMS is how long an unused connection survives. Expiring
	// one costs its client an M_UNKNOWN_POS and a re-bootstrap.
	ConnectionExpiryMS = int64(7 * 24 * 60 * 60 * 1000)
	// LazyMembersUpdateIntervalMS is how stale a lazy-member last_seen_ts may
	// get. Only used for eviction, so precision buys nothing.
	LazyMembersUpdateIntervalMS = int64(60 * 60 * 1000)
)

// ErrUnknownPosition means the `pos` a client sent names no connection state we
// hold, or names one belonging to somebody else.
//
// This is not a failure mode, it is the design's safety valve, and it maps to
// HTTP 400 M_UNKNOWN_POS. Any per-connection bookkeeping we get wrong degrades
// to a client re-bootstrapping its room list rather than to a client silently
// missing a room -- so when in doubt, raise this rather than guess. Synapse
// notes the same trade in current_sync_for_user: expiring the connection lets
// the client ask for a smaller range and get something on screen sooner, where
// silently treating an unknown position as "send everything" is slower AND
// wrong.
var ErrUnknownPosition = errors.New("slidingstore: unknown connection position")

// Config describes how to reach the database as the sliding sync role.
type Config struct {
	// DSN is a libpq connection string for the role from
	// deploy/sliding-sync-role.sql: owner of the `gosync` schema, with no
	// access to `public` at all.
	DSN string
	// MaxConns bounds the pool. Zero means 8.
	MaxConns int32
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
}

// Store holds per-connection sliding sync state.
type Store struct {
	pool *pgxpool.Pool
	now  func() int64
	// broad is why the role reaches beyond the gosync schema, or "".
	broad string
	// created is whether Open had to create the tables.
	created bool
}

// schemaSQL creates the tables when they are missing. See ensureSchema.
//
//go:embed schema.sql
var schemaSQL string

// Open connects and verifies the role can write its own schema.
//
// It also checks that the role CANNOT read `public`, but reports a role that
// can through Broad rather than refusing: some deployments have a single
// database user and cannot create a narrow one. The narrow role from
// deploy/sliding-sync-role.sql remains the recommendation.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	} else {
		pcfg.MaxConns = 8
	}
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	// Pinned here rather than trusted to the role. Every query below names its
	// tables unqualified, and Synapse has tables of the SAME NAMES in `public`.
	// A role whose search_path reaches `public` -- any role but the one from
	// sliding-sync-role.sql, e.g. a single shared database user -- would
	// otherwise read and write Synapse's own connection state for real clients.
	pcfg.ConnConfig.RuntimeParams["search_path"] = "gosync"

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("slidingstore: ping: %w", err)
	}

	s := &Store{
		pool: pool,
		now:  func() int64 { return time.Now().UnixMilli() },
	}
	// Before checkGrants, which names gosync.sliding_sync_connections and
	// errors if it does not exist.
	if err := s.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.checkGrants(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for metrics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Broad reports why the role can reach Synapse's own tables, or "" if it
// cannot.
func (s *Store) Broad() string { return s.broad }

// Created reports whether Open created the tables.
func (s *Store) Created() bool { return s.created }

// ensureSchema creates the gosync schema and its tables if they are missing.
//
// Only when missing, and the schema separately from the tables: CREATE SCHEMA
// needs CREATE on the database even with IF NOT EXISTS, which the narrow role
// from deploy/sliding-sync-role.sql does not hold -- that script creates the
// schema for it, and this creates the tables inside it. A single shared
// database user usually holds both.
func (s *Store) ensureSchema(ctx context.Context) error {
	var complete, schemaExists bool
	// to_regclass returns NULL, not an error, when the schema itself is absent.
	// The lazy-members table is created last, so its presence means all of it.
	if err := s.pool.QueryRow(ctx, `
		SELECT to_regclass('gosync.sliding_sync_connection_lazy_members') IS NOT NULL,
		       EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'gosync')`).
		Scan(&complete, &schemaExists); err != nil {
		return fmt.Errorf("slidingstore: check schema: %w", err)
	}
	if complete {
		return nil
	}
	if !schemaExists {
		if _, err := s.pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS gosync`,
			pgx.QueryExecModeSimpleProtocol); err != nil {
			return fmt.Errorf("slidingstore: create schema gosync (the role needs CREATE "+
				"on the database; otherwise run deploy/sliding-sync-role.sql): %w", err)
		}
	}
	// Several statements in one simple-protocol message run as one implicit
	// transaction, so a failure leaves nothing half-created.
	if _, err := s.pool.Exec(ctx, schemaSQL, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("slidingstore: create tables in gosync: %w", err)
	}
	s.created = true
	return nil
}

func (s *Store) checkGrants(ctx context.Context) error {
	var (
		user       string
		readOnly   string
		canInsert  bool
		canReadPub bool
	)
	// has_table_privilege raises if the table does not exist, so `public.events`
	// is checked through to_regclass instead: a role that genuinely cannot see
	// the schema should fail this check by returning false, not by erroring.
	const q = `
		SELECT current_user,
		       current_setting('default_transaction_read_only'),
		       has_table_privilege('gosync.sliding_sync_connections', 'INSERT'),
		       COALESCE(
		           (SELECT has_table_privilege(c.oid, 'SELECT')
		              FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		             WHERE n.nspname = 'public' AND c.relname = 'events'),
		           false)`
	if err := s.pool.QueryRow(ctx, q).Scan(&user, &readOnly, &canInsert, &canReadPub); err != nil {
		return fmt.Errorf("slidingstore: check grants: %w", err)
	}
	if readOnly == "on" {
		return fmt.Errorf("slidingstore: role %q has default_transaction_read_only=on "+
			"and cannot record connection state; point sliding_sync.dsn at the writing role", user)
	}
	if !canInsert {
		return fmt.Errorf("slidingstore: role %q cannot INSERT into "+
			"gosync.sliding_sync_connections; run deploy/sliding-sync-role.sql", user)
	}
	if canReadPub {
		s.broad = fmt.Sprintf("role %q can read public.events", user)
	}
	return nil
}

// GetAndClear loads the state for a connection position, and prunes.
//
// The name says "clear" because reading is a write, and there is no read-only
// path to fall back to. Three things happen:
//
//  1. The position's ownership is checked against (user, device, conn_id). A
//     `pos` is a value the client supplies, so being handed one is not evidence
//     of being entitled to it.
//  2. last_used_ts is bumped, at most every UpdateLastUsedIntervalMS.
//  3. Every OTHER position on the connection is deleted, and lazy-member rows
//     written against this one are promoted to apply to all future positions.
//
// Step 3 is the acknowledge-and-prune mechanic, and it is what makes forking
// safe. A client that abandons a long poll never learns the position we just
// wrote -- 1,102 of 27,465 live requests in the measured window ended in a 499
// -- so it comes back with the previous one. Until a position is used we cannot
// know which fork survived; once one IS used, every other is unreachable and
// goes. That bounds these tables at roughly one round trip of duplication
// instead of letting them grow forever.
func (s *Store) GetAndClear(
	ctx context.Context, userID, deviceID, connID string, position int64,
) (*PerConnectionState, error) {
	// A zero position is the sentinel for "no previous connection state", and
	// it is not rare: 2,409 of 25,767 live requests carrying a `pos` used it.
	// It must NOT be treated as an unknown position -- the client is telling us
	// it has no state, not presenting one we failed to find.
	if position == 0 {
		return &PerConnectionState{}, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var connectionKey int64
	var lastUsed *int64
	err = tx.QueryRow(ctx, `
		SELECT connection_key, last_used_ts
		  FROM sliding_sync_connection_positions
		  JOIN sliding_sync_connections USING (connection_key)
		 WHERE connection_position = $1
		   AND user_id = $2 AND effective_device_id = $3 AND conn_id = $4`,
		position, userID, deviceID, connID).Scan(&connectionKey, &lastUsed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrUnknownPosition
	}
	if err != nil {
		return nil, fmt.Errorf("slidingstore: look up position: %w", err)
	}

	now := s.now()
	if lastUsed == nil || now-*lastUsed > UpdateLastUsedIntervalMS {
		if _, err := tx.Exec(ctx,
			`UPDATE sliding_sync_connections SET last_used_ts = $1 WHERE connection_key = $2`,
			now, connectionKey); err != nil {
			return nil, fmt.Errorf("slidingstore: bump last_used_ts: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM sliding_sync_connection_positions
		 WHERE connection_key = $1 AND connection_position != $2`,
		connectionKey, position); err != nil {
		return nil, fmt.Errorf("slidingstore: prune positions: %w", err)
	}

	// Lazy-member rows written against this position now apply to every future
	// one. Safe only because the line above just removed every fork it could
	// have competed with, so this position is the connection's only history.
	if _, err := tx.Exec(ctx, `
		UPDATE sliding_sync_connection_lazy_members SET connection_position = NULL
		 WHERE connection_key = $1 AND connection_position = $2`,
		connectionKey, position); err != nil {
		return nil, fmt.Errorf("slidingstore: promote lazy members: %w", err)
	}

	state, err := s.loadState(ctx, tx, connectionKey, position)
	if err != nil {
		return nil, err
	}
	state.LastUsedMS = lastUsed

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("slidingstore: commit: %w", err)
	}
	return state, nil
}

func (s *Store) loadState(
	ctx context.Context, tx pgx.Tx, connectionKey, position int64,
) (*PerConnectionState, error) {
	// Every required_state row for the connection, so the unused ones can be
	// identified below without a second query.
	requiredState := map[int64]map[string]map[string]bool{}
	rows, err := tx.Query(ctx, `
		SELECT required_state_id, required_state
		  FROM sliding_sync_connection_required_state
		 WHERE connection_key = $1`, connectionKey)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: load required state: %w", err)
	}
	for rows.Next() {
		var id int64
		var encoded string
		if err := rows.Scan(&id, &encoded); err != nil {
			rows.Close()
			return nil, fmt.Errorf("slidingstore: load required state: %w", err)
		}
		decoded, err := DecodeRequiredState(encoded)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("slidingstore: decode required state %d: %w", id, err)
		}
		requiredState[id] = decoded
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("slidingstore: load required state: %w", err)
	}

	roomConfigs := map[string]RoomSyncConfig{}
	used := map[int64]bool{}
	rows, err = tx.Query(ctx, `
		SELECT room_id, timeline_limit, required_state_id
		  FROM sliding_sync_connection_room_configs
		 WHERE connection_position = $1`, position)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: load room configs: %w", err)
	}
	for rows.Next() {
		var roomID string
		var timelineLimit, requiredStateID int64
		if err := rows.Scan(&roomID, &timelineLimit, &requiredStateID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("slidingstore: load room configs: %w", err)
		}
		used[requiredStateID] = true
		roomConfigs[roomID] = RoomSyncConfig{
			TimelineLimit: int(timelineLimit),
			RequiredState: requiredState[requiredStateID],
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("slidingstore: load room configs: %w", err)
	}

	// Drop required_state rows nothing points at any more. This is only sound
	// because both queries above are complete: every row for the connection,
	// and every reference from the only position that still exists after the
	// prune. Without the prune, a fork could still be holding one.
	var unused []int64
	for id := range requiredState {
		if !used[id] {
			unused = append(unused, id)
		}
	}
	if len(unused) > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM sliding_sync_connection_required_state
			 WHERE connection_key = $1 AND required_state_id = ANY($2)`,
			connectionKey, unused); err != nil {
			return nil, fmt.Errorf("slidingstore: drop unused required state: %w", err)
		}
	}

	rooms := map[string]HaveSent{}
	receipts := map[string]HaveSent{}
	accountData := map[string]HaveSent{}
	rows, err = tx.Query(ctx, `
		SELECT stream, room_id, room_status, last_token
		  FROM sliding_sync_connection_streams
		 WHERE connection_position = $1`, position)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: load streams: %w", err)
	}
	for rows.Next() {
		var stream, roomID, status string
		var lastToken *string
		if err := rows.Scan(&stream, &roomID, &status, &lastToken); err != nil {
			rows.Close()
			return nil, fmt.Errorf("slidingstore: load streams: %w", err)
		}
		hs := HaveSent{Status: HaveSentFlag(status)}
		if lastToken != nil {
			hs.LastToken = *lastToken
		}
		switch stream {
		case StreamRooms:
			rooms[roomID] = hs
		case StreamReceipts:
			receipts[roomID] = hs
		case StreamAccountData:
			accountData[roomID] = hs
		}
		// An unrecognised stream name is ignored rather than rejected: a future
		// version writing a fourth stream must not make this one refuse every
		// position it wrote.
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("slidingstore: load streams: %w", err)
	}

	return &PerConnectionState{
		Rooms:       NewRoomStatusMap(rooms),
		Receipts:    NewRoomStatusMap(receipts),
		AccountData: NewRoomStatusMap(accountData),
		RoomConfigs: roomConfigs,
	}, nil
}

// Stream names, as stored in sliding_sync_connection_streams.stream.
const (
	StreamRooms       = "rooms"
	StreamReceipts    = "receipts"
	StreamAccountData = "account_data"
)

// Counts is the size of the connection store, for metrics.
type Counts struct {
	Connections int64
	Positions   int64
	// Rows maps a table name to its row count.
	Rows map[string]int64
}

// Count reports how much the connection store holds.
//
// Six counts in one round trip. These tables are small by design -- reading a
// position deletes the other positions on its connection, and the reaper
// removes connections nobody uses -- so a row count that climbs steadily means
// one of those two is not happening, which nothing else makes visible.
func (s *Store) Count(ctx context.Context) (Counts, error) {
	const q = `
		SELECT (SELECT count(*) FROM sliding_sync_connections),
		       (SELECT count(*) FROM sliding_sync_connection_positions),
		       (SELECT count(*) FROM sliding_sync_connection_required_state),
		       (SELECT count(*) FROM sliding_sync_connection_room_configs),
		       (SELECT count(*) FROM sliding_sync_connection_streams),
		       (SELECT count(*) FROM sliding_sync_connection_lazy_members)`
	var conns, positions, required, configs, streams, lazy int64
	if err := s.pool.QueryRow(ctx, q).Scan(
		&conns, &positions, &required, &configs, &streams, &lazy); err != nil {
		return Counts{}, fmt.Errorf("slidingstore: count: %w", err)
	}
	return Counts{
		Connections: conns,
		Positions:   positions,
		Rows: map[string]int64{
			"connections":    conns,
			"positions":      positions,
			"required_state": required,
			"room_configs":   configs,
			"streams":        streams,
			"lazy_members":   lazy,
		},
	}, nil
}
