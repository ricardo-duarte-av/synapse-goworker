package typing

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// errInvalidHost is is_host_joined's Exception("Invalid host name").
var errInvalidHost = errors.New("Invalid host name")

const qHostJoined = `
	SELECT state_key FROM current_state_events
	WHERE membership = 'join'
		AND type = 'm.room.member'
		AND room_id = $1
		AND state_key LIKE $2
	LIMIT 1`

func (d *DB) HostJoined(ctx context.Context, roomID, host string) (bool, error) {
	// _check_host_room_membership: no LIKE wildcards in the host, and the
	// user found really is of that host.
	if strings.ContainsAny(host, "%_") {
		return false, errInvalidHost
	}
	var userID string
	err := d.pool.QueryRow(ctx, qHostJoined, roomID, "%:"+host).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if domainOf(userID) != host {
		return false, errInvalidHost
	}
	return true, nil
}

const (
	qPartialServers = `SELECT server_name FROM partial_state_rooms_servers WHERE room_id = $1`
	// get_current_hosts_in_room's PostgreSQL query.
	qCurrentHosts = `
		SELECT DISTINCT substring(state_key FROM '@[^:]*:(.*)$')
		FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND room_id = $1`
)

func (d *DB) HostsApprox(ctx context.Context, roomID string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	// The servers at join first, as Synapse reads them, to mitigate races
	// with un-partial-stating.
	for _, q := range []string{qPartialServers, qCurrentHosts} {
		rows, err := d.pool.Query(ctx, q, roomID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var h *string
			if err := rows.Scan(&h); err != nil {
				rows.Close()
				return nil, err
			}
			if h != nil {
				out[*h] = struct{}{}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// domainOf is UserID.from_string(user_id).domain: everything after the
// first colon.
func domainOf(userID string) string {
	_, domain, _ := strings.Cut(userID, ":")
	return domain
}
