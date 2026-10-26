// Package deviceinbox deletes to-device messages a device has acknowledged.
//
// It exists as a separate package, with a separate connection pool and a
// separate database role, because it is the ONE place this worker writes. The
// rest of the worker holds a role with only SELECT granted and
// default_transaction_read_only set (deploy/readonly-role.sql), and that
// guarantee is worth keeping literal: a bug anywhere in internal/store still
// cannot write. Here, and only here, it can -- and only DELETE, and only on
// device_inbox.
//
// Why write at all: Synapse's /sync deletes the to-device messages it has just
// handed over, bounded by the client's `since` (handlers/sync.py, just before
// the notifier wait). A worker that serves to_device without deleting hands a
// device the same room keys on every sync, forever. Serving that section and
// deleting it are one decision, not two, which is why Deps.Inbox being nil
// makes the worker omit to_device entirely rather than serve it undeleted.
package deviceinbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lru"
)

// deleteBatch is how many rows one DELETE covers.
//
// Synapse uses 1000 and loops, rather than one unbounded DELETE, so that a
// device with a large backlog cannot hold a long transaction open against a
// table its own inbox writer is inserting into.
const deleteBatch = 1000

// Config describes how to reach the database as the deleting role.
type Config struct {
	// DSN is a libpq connection string for a role granted SELECT and DELETE on
	// device_inbox and nothing else. See deploy/device-inbox-role.sql.
	DSN string
	// MaxConns bounds the pool. Deletion is one statement on the sync path, so
	// this wants to be small. Zero means 4.
	MaxConns int32
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
	// CacheEntries bounds the last-deleted-position cache. Zero means 20000.
	CacheEntries int
}

// Deleter removes acknowledged to-device messages.
type Deleter struct {
	pool *pgxpool.Pool
	// lastDeleted remembers how far each device has already been cleared, so a
	// client polling every 30 seconds does not issue a DELETE that can match
	// nothing. Synapse keeps the same cache for the same reason.
	lastDeleted *lru.Cache[key, int64]
	// broad is why the role is wider than device-inbox-role.sql grants, or "".
	broad string
}

type key struct{ userID, deviceID string }

// Open connects and verifies the role can delete from device_inbox.
//
// It also checks whether the role is too powerful, but reports that through
// Broad rather than refusing: some deployments have a single database user and
// cannot create a narrow one. The narrow role remains the recommendation.
func Open(ctx context.Context, cfg Config) (*Deleter, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("deviceinbox: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	} else {
		pcfg.MaxConns = 4
	}
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("deviceinbox: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("deviceinbox: ping: %w", err)
	}

	entries := cfg.CacheEntries
	if entries == 0 {
		entries = 20000
	}
	d := &Deleter{pool: pool, lastDeleted: lru.New[key, int64](entries)}
	if err := d.checkGrants(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return d, nil
}

// Close releases the pool.
func (d *Deleter) Close() {
	if d != nil && d.pool != nil {
		d.pool.Close()
	}
}

// Pool exposes the underlying pool for metrics.
func (d *Deleter) Pool() *pgxpool.Pool { return d.pool }

// Broad reports why the role holds more than SELECT and DELETE on
// device_inbox, or "" if it does not.
func (d *Deleter) Broad() string { return d.broad }

func (d *Deleter) checkGrants(ctx context.Context) error {
	var (
		user      string
		readOnly  string
		canDelete bool
		canInsert bool
		canEvents bool
	)
	const q = `
		SELECT current_user,
		       current_setting('default_transaction_read_only'),
		       has_table_privilege('device_inbox', 'DELETE'),
		       has_table_privilege('device_inbox', 'INSERT'),
		       has_table_privilege('events', 'DELETE')`
	if err := d.pool.QueryRow(ctx, q).Scan(
		&user, &readOnly, &canDelete, &canInsert, &canEvents); err != nil {
		return fmt.Errorf("deviceinbox: check grants: %w", err)
	}
	if readOnly == "on" {
		return fmt.Errorf("deviceinbox: role %q has default_transaction_read_only=on "+
			"and cannot delete; point to_device.dsn at the deleting role, not the read-only one", user)
	}
	if !canDelete {
		return fmt.Errorf("deviceinbox: role %q has no DELETE on device_inbox", user)
	}
	switch {
	case canEvents:
		d.broad = fmt.Sprintf("role %q can DELETE from events", user)
	case canInsert:
		d.broad = fmt.Sprintf("role %q can INSERT into device_inbox", user)
	}
	return nil
}

// DeleteUpTo removes to-device messages for one device at stream positions up
// to and including upTo, returning how many rows went.
//
// Mirrors delete_messages_for_device / delete_messages_for_device_between. Two
// details are carried over deliberately:
//
//   - The loop keeps a moving lower bound, so successive batches do not rescan
//     the rows the previous ones deleted.
//   - Each batch first finds MAX(stream_id) over its window and then deletes up
//     to it, rather than deleting with a LIMIT, which PostgreSQL cannot do.
func (d *Deleter) DeleteUpTo(ctx context.Context, userID, deviceID string, upTo int64) (int, error) {
	if d == nil || userID == "" || deviceID == "" || upTo <= 0 {
		return 0, nil
	}
	k := key{userID, deviceID}
	if last, ok := d.lastDeleted.Get(k); ok && last >= upTo {
		return 0, nil
	}

	var (
		from  int64
		total int
	)
	for {
		next, deleted, err := d.deleteBetween(ctx, userID, deviceID, from, upTo)
		if err != nil {
			return total, err
		}
		total += deleted
		if next == 0 {
			break
		}
		from = next
	}

	// Only ever forwards: a rewound `since` (which the comparator sends) must
	// not make us re-scan ground already cleared.
	if last, ok := d.lastDeleted.Get(k); !ok || upTo > last {
		d.lastDeleted.Add(k, upTo)
	}
	return total, nil
}

// deleteBetween deletes one batch, returning the position to continue from (0
// when the range is exhausted) and how many rows it removed.
func (d *Deleter) deleteBetween(ctx context.Context, userID, deviceID string,
	from, to int64) (int64, int, error) {

	const findQ = `
		SELECT MAX(stream_id) FROM (
			SELECT stream_id FROM device_inbox
			WHERE user_id = $1 AND device_id = $2
			  AND $3 < stream_id AND stream_id <= $4
			ORDER BY stream_id
			LIMIT $5
		) AS d`

	var maxStreamID *int64
	if err := d.pool.QueryRow(ctx, findQ, userID, deviceID, from, to, deleteBatch).
		Scan(&maxStreamID); err != nil {
		return 0, 0, fmt.Errorf("deviceinbox: find batch: %w", err)
	}
	if maxStreamID == nil {
		return 0, 0, nil
	}

	const delQ = `
		DELETE FROM device_inbox
		WHERE user_id = $1 AND device_id = $2
		  AND $3 < stream_id AND stream_id <= $4`

	tag, err := d.pool.Exec(ctx, delQ, userID, deviceID, from, *maxStreamID)
	if err != nil {
		return 0, 0, fmt.Errorf("deviceinbox: delete: %w", err)
	}
	n := int(tag.RowsAffected())
	if n < deleteBatch {
		return 0, n, nil
	}
	return *maxStreamID, n, nil
}
