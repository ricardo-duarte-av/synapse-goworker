package store

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
)

// query, queryRow and txQuery are the only ways this package reaches the
// database, so that every round trip is counted exactly once.
//
// The `name` is the calling method, passed explicitly rather than recovered
// from the stack: runtime.Callers on the hot path would cost a meaningful
// fraction of a 14us query, and a wrong name is a mislabelled counter rather
// than a wrong answer, which makes the explicit form cheap to keep honest.
//
// Counting happens before the call, not after. A query that fails or is
// cancelled mid-flight still cost a round trip, and the point of this counter
// is to measure how often we talk to PostgreSQL.
func (s *Store) query(ctx context.Context, name, sql string, args ...any) (pgx.Rows, error) {
	metrics.DBQueries.WithLabelValues(name).Inc()
	return s.pool.Query(ctx, sql, args...)
}

func (s *Store) queryRow(ctx context.Context, name, sql string, args ...any) pgx.Row {
	metrics.DBQueries.WithLabelValues(name).Inc()
	return s.pool.QueryRow(ctx, sql, args...)
}

// txQuery counts a query issued inside a caller-managed transaction.
//
// The state-group walks need one: they run behind SET LOCAL enable_seqscan=off
// and so cannot use the pool directly. Note that such a query is three further
// round trips (BEGIN, SET LOCAL, ROLLBACK) that this counter does not see; the
// counter reports queries, not packets.
func txQuery(ctx context.Context, tx pgx.Tx, name, sql string, args ...any) (pgx.Rows, error) {
	metrics.DBQueries.WithLabelValues(name).Inc()
	return tx.Query(ctx, sql, args...)
}
