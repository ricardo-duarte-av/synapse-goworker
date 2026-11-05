package typing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/roomhosts"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// Store is what the writer reads from Synapse's database. Read-only.
type Store interface {
	// LocalMembership is get_local_current_membership_for_user_in_room's
	// membership, "" for none.
	LocalMembership(ctx context.Context, userID, roomID string) (string, error)
	// ShadowBanned is the user's users.shadow_banned.
	ShadowBanned(ctx context.Context, userID string) (bool, error)
	// HostJoined is is_host_joined: some user of host is joined to the room.
	HostJoined(ctx context.Context, roomID, host string) (bool, error)
	// HostsApprox is get_current_hosts_in_room_or_partial_state_approximation:
	// the joined hosts, and for a partial-state room the servers in it when
	// we joined.
	HostsApprox(ctx context.Context, roomID string) (map[string]struct{}, error)
}

// DB is Store over a pool of its own.
type DB struct{ pool *pgxpool.Pool }

// Open connects through dsn, in Exec mode, as every pool here does (pgcat).
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("typing: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 1)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("typing: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("typing: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Pool is for the pool metrics.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

const qLocalMembership = `SELECT membership FROM local_current_membership WHERE room_id = $1 AND user_id = $2`

func (d *DB) LocalMembership(ctx context.Context, userID, roomID string) (string, error) {
	var m string
	err := d.pool.QueryRow(ctx, qLocalMembership, roomID, userID).Scan(&m)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return m, err
}

const qShadowBanned = `SELECT COALESCE(shadow_banned, false) FROM users WHERE name = $1`

func (d *DB) ShadowBanned(ctx context.Context, userID string) (bool, error) {
	var b bool
	err := d.pool.QueryRow(ctx, qShadowBanned, userID).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return b, err
}

func (d *DB) HostJoined(ctx context.Context, roomID, host string) (bool, error) {
	return roomhosts.HostJoined(ctx, d.pool, roomID, host)
}

func (d *DB) HostsApprox(ctx context.Context, roomID string) (map[string]struct{}, error) {
	return roomhosts.Approx(ctx, d.pool, roomID)
}

func domainOf(userID string) string { return userid.Domain(userID) }
