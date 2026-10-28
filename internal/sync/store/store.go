// Package store provides read-only access to Synapse's PostgreSQL database.
//
// Every query here is a SELECT. The worker runs as a role with only SELECT
// granted and default_transaction_read_only set, so a bug cannot write even if
// it tries. See deploy/readonly-role.sql.
//
// That constraint is not only defensive. Synapse's own /sync deletes to-device
// messages it has just returned to a device; this worker cannot, and the
// asymmetry has to be handled deliberately rather than discovered.
package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lru"
)

// Store holds a pool of read-only connections to Synapse's database.
type Store struct {
	pool    *pgxpool.Pool
	caches  *caches
	derived *derivedCaches
	streams *streamCaches

	// instance_map is append-only, so the name-to-id mapping a stream token
	// needs is read once and kept for the process's life.
	instanceMu  sync.Mutex
	instanceIDs map[string]int
}

// caches hold immutable derived data. A state group's contents are fixed once
// written, so none of this needs invalidating on the write path; see
// Store.PurgeCaches for the one case that does.
type caches struct {
	// states is the worker's one state cache; sync uses its event -> state
	// group half.
	states        *statecache.Cache
	filteredState *lru.Cache[string, map[StateKey]StateEntry]
}

// Config describes how to reach the database.
type Config struct {
	// DSN is a libpq connection string. For a unix socket, set host to the
	// directory containing .s.PGSQL.5432, e.g.
	// "host=/var/sockets user=gosync_ro dbname=synapse-db".
	DSN string
	// MaxConns bounds the pool. Zero uses pgx's default.
	MaxConns int32
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
	// States is the worker's shared state cache, whose event -> state group
	// entries sync reads and fills. Nil builds one for this store alone, as
	// in tests.
	States *statecache.Cache
	// FilteredStateCacheEntries bounds the filtered state cache. Zero takes a
	// default; negative disables it, which makes "is the cache hiding a
	// bug?" an answerable question.
	FilteredStateCacheEntries int

	// Derived cache sizes, in entries. Zero takes a default; negative
	// disables that cache. See internal/store/derived.go for why these are
	// split into immutable and replication-invalidated.
	UserFilterCacheEntries        int
	RoomInfoCacheEntries          int
	RoomSummaryCacheEntries       int
	HistoryVisibilityCacheEntries int
	IgnoredUsersCacheEntries      int
	RoomsForUserCacheEntries      int

	// StreamCaches bounds the stream-change caches, in distinct stream
	// positions rather than entities. See internal/store/streamgates.go.
	StreamCaches StreamCacheSizes
}

// Open connects and verifies the database is reachable.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}

	// Exec, not DescribeExec: behind pgcat in transaction pooling mode,
	// DescribeExec's two round trips can land on different server
	// connections ("unnamed prepared statement does not exist", 26000).
	// Exec sends parse, bind and execute together.
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{
		pool:    pool,
		caches:  newCaches(cfg),
		derived: newDerivedCaches(cfg),
		streams: newStreamCaches(cfg.StreamCaches),
	}, nil
}

func newCaches(cfg Config) *caches {
	filtered := cfg.FilteredStateCacheEntries
	states := cfg.States
	if states == nil {
		states = statecache.New(0, 50000*64)
	}
	if filtered == 0 {
		// Filtered views are a handful of entries each, so many more fit than
		// whole state maps would.
		filtered = 20000
	}
	return &caches{
		states:        states,
		filteredState: lru.New[string, map[StateKey]StateEntry](filtered),
	}
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for tests and metrics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// IsReadOnly reports whether the connected role is restricted to reads.
//
// Checked at startup and reported rather than assumed: this worker only ever
// runs against a production Synapse database.
func (s *Store) IsReadOnly(ctx context.Context) (bool, error) {
	var setting string
	if err := s.queryRow(ctx, "IsReadOnly", `SHOW default_transaction_read_only`).Scan(&setting); err != nil {
		return false, fmt.Errorf("store: check read-only: %w", err)
	}
	return setting == "on", nil
}
