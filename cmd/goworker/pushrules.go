package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

// eventsPosition is the events stream's MultiWriterIdGenerator
// get_current_token as a follower holds it: the least of its writers'
// positions. A writer seen on the bus since start has the position seen
// there; one not seen has its stream_positions row, which is where a
// follower starts it; with no row at all, the greatest stream_ordering.
func eventsPosition(ctx context.Context, pool *pgxpool.Pool, tr *replserver.Tracker, writers []string) (int64, error) {
	var least int64 = -1
	for _, w := range writers {
		p, ok := tr.Seen("events", w)
		if !ok {
			var v *int64
			if err := pool.QueryRow(ctx, `SELECT stream_id FROM stream_positions WHERE stream_name = 'events' AND instance_name = $1`, w).Scan(&v); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return 0, err
			}
			if v == nil {
				if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(stream_ordering), 0) FROM events`).Scan(&p); err != nil {
					return 0, err
				}
			} else {
				p = *v
			}
		}
		if least < 0 || p < least {
			least = p
		}
	}
	if least < 0 {
		least = 0
	}
	return least, nil
}
