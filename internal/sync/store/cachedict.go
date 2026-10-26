package store

import (
	"context"
	"fmt"
)

// CacheDict returns roughly the last `limit` changes in a stream table as a map
// from entity to the position of that entity's most recent change, along with
// the oldest position the map can be trusted from.
//
// A port of Synapse's `DatabasePool.get_cache_dict`
// (`synapse/storage/database.py`), and it exists for one reason: without it a
// stream-change cache starts with its horizon at "now", every question falls
// below it, and every gate answers "changed" until live traffic has filled the
// cache. That failure is completely silent -- the queries simply never go away
// -- which is why the horizon is exported as a metric.
//
// Two details are load-bearing and both are Synapse's:
//
//   - The returned minimum is one ABOVE the smallest position seen, not the
//     smallest. Several of these tables can carry more than one row at the same
//     stream id, and the LIMIT may have cut through the middle of such a group,
//     so the smallest position we saw is precisely the one we might have an
//     incomplete picture of. Claiming to know it would be a false negative.
//   - An empty table yields maxValue, not zero. A cache that claims to know
//     everything since 0 when it has seen nothing would answer "unchanged" for
//     the entire history.
//
// Ordering by the stream column descending and keeping the FIRST row seen per
// entity is what makes one pass enough: the first row for an entity is its
// newest.
func (s *Store) CacheDict(
	ctx context.Context, table, entityColumn, streamColumn string, maxValue int64, limit int,
) (map[string]int64, int64, error) {
	// The identifiers are interpolated, which is safe only because every caller
	// is in this package and passes a constant. Keep it that way: nothing here
	// takes a table name from a request.
	q := fmt.Sprintf(
		`SELECT %s, %s FROM %s ORDER BY %s DESC LIMIT $1`,
		entityColumn, streamColumn, table, streamColumn,
	)

	rows, err := s.query(ctx, "CacheDict", q, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("store: cache dict for %s: %w", table, err)
	}
	defer rows.Close()

	cache := make(map[string]int64)
	minVal := int64(0)
	first := true
	for rows.Next() {
		var entity string
		var pos int64
		if err := rows.Scan(&entity, &pos); err != nil {
			return nil, 0, fmt.Errorf("store: cache dict for %s: %w", table, err)
		}
		if _, seen := cache[entity]; !seen {
			cache[entity] = pos
		}
		if first || pos < minVal {
			minVal, first = pos, false
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("store: cache dict for %s: %w", table, err)
	}

	if first {
		return cache, maxValue, nil
	}
	return cache, minVal + 1, nil
}
