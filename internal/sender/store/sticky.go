package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/dbtrace"
)

// The MSC4354 sticky event backlog (storage/databases/main/sticky_events.py,
// v1.164.1): destination_rooms_sticky_events_backlog holds, per destination
// and room, the sticky events stream position from which this server's own
// sticky events are still owed to that destination. Rows are hints -- the
// events may have expired since -- and Synapse writes them too (the events
// writer, for a server newly joined to a room), so all of this goes through
// the write pool.

// LocalUserPattern is user_is_local_like_pattern: a LIKE pattern matching
// this server's users.
func LocalUserPattern(serverName string) string { return "@%:" + serverName }

// StickyBacklog is one room's worth of owed sticky events.
type StickyBacklog struct {
	RoomID string
	// MaxPosition is the highest sticky stream position among EventIDs:
	// once they are delivered, the room's position moves past it.
	MaxPosition int64
	// EventIDs are in sticky stream order, oldest first.
	EventIDs []string
}

const (
	qBacklogRoom = `
		SELECT room_id, sticky_events_stream_position
		  FROM destination_rooms_sticky_events_backlog
		 WHERE destination = $1
		 LIMIT 1`
	qBacklogEvents = `
		SELECT event_id, stream_id FROM sticky_events
		 WHERE room_id = $1 AND $2 <= stream_id AND $3 < expires_at AND sender LIKE $4
		 ORDER BY stream_id ASC
		 LIMIT $5`
	// The anti-join: backlog rows of the destination with no unexpired local
	// sticky event at or after their position.
	qCleanBacklog = `
		WITH to_clean_up AS (
			SELECT backlog.room_id FROM destination_rooms_sticky_events_backlog AS backlog
			  LEFT JOIN sticky_events AS se
			    ON se.room_id = backlog.room_id
			   AND se.sender LIKE $1
			   AND $2 < se.expires_at
			   AND backlog.sticky_events_stream_position <= se.stream_id
			 WHERE se.event_id IS NULL AND backlog.destination = $3
		)
		DELETE FROM destination_rooms_sticky_events_backlog
		 WHERE destination = $3 AND room_id IN (SELECT room_id FROM to_clean_up)`
)

// BackloggedStickyEvents is get_backlogged_sticky_events_for_destination: up
// to limit owed sticky events from one room, or nil when nothing is owed. A
// room whose owed events have all expired is cleaned out of the backlog, and
// another room tried, in the same transaction.
func (w *Writer) BackloggedStickyEvents(ctx context.Context, destination, localPattern string,
	nowMS int64, limit int) (*StickyBacklog, error) {

	ctx = dbtrace.WithQueryName(ctx, "backlogged_sticky_events")
	var out *StickyBacklog
	err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		try := func() (*StickyBacklog, error) {
			var b StickyBacklog
			var pos int64
			err := tx.QueryRow(ctx, qBacklogRoom, destination).Scan(&b.RoomID, &pos)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			rows, err := tx.Query(ctx, qBacklogEvents, b.RoomID, pos, nowMS, localPattern, limit)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			b.MaxPosition = -1
			for rows.Next() {
				var id string
				var stream int64
				if err := rows.Scan(&id, &stream); err != nil {
					return nil, err
				}
				b.EventIDs = append(b.EventIDs, id)
				b.MaxPosition = max(b.MaxPosition, stream)
			}
			return &b, rows.Err()
		}
		first, err := try()
		if err != nil || first == nil || len(first.EventIDs) > 0 {
			out = first
			return err
		}
		if _, err := tx.Exec(ctx, qCleanBacklog, localPattern, nowMS, destination); err != nil {
			return err
		}
		second, err := try()
		if err == nil && second != nil && len(second.EventIDs) == 0 {
			// Cleaned a moment ago, so this cannot happen; Synapse asserts it.
			second = nil
		}
		out = second
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("store: backlogged sticky events for %s: %w", destination, err)
	}
	return out, nil
}

// MarkBackloggedStickySent is mark_backlogged_sticky_events_sent: the room's
// owed position moves past the delivered events.
func (w *Writer) MarkBackloggedStickySent(ctx context.Context, destination, roomID string, maxPosition int64) error {
	ctx = dbtrace.WithQueryName(ctx, "mark_backlogged_sticky_events_sent")
	const q = `
		INSERT INTO destination_rooms_sticky_events_backlog
			(destination, room_id, sticky_events_stream_position)
		VALUES ($1, $2, $3)
		ON CONFLICT (destination, room_id)
		DO UPDATE SET sticky_events_stream_position = EXCLUDED.sticky_events_stream_position`
	if _, err := w.pool.Exec(ctx, q, destination, roomID, maxPosition+1); err != nil {
		return fmt.Errorf("store: mark sticky backlog sent for %s: %w", destination, err)
	}
	return nil
}

// MarkStickyBacklogAfterCatchUp is
// mark_backlogged_sticky_events_after_catchup_transaction. A catch-up
// transaction sends one room's events and then moves the destination's
// last_successful_stream_ordering from old to new, leaving a gap of unsent
// events in other rooms. This server's sticky events in that gap -- except the
// ones the transaction itself carried -- become a backlog, at the room's
// earliest such position, never moving an existing one forwards.
func (w *Writer) MarkStickyBacklogAfterCatchUp(ctx context.Context, destination, localPattern string,
	old, new int64, sent []int64) error {

	ctx = dbtrace.WithQueryName(ctx, "mark_sticky_backlog_after_catchup")
	const q = `
		INSERT INTO destination_rooms_sticky_events_backlog AS backlog
			(destination, room_id, sticky_events_stream_position)
		SELECT $1, dr.room_id, MIN(se.stream_id)
		  FROM destination_rooms AS dr
		  JOIN sticky_events se USING (room_id)
		 WHERE dr.destination = $1
		   AND $2 < dr.stream_ordering
		   AND $2 < se.event_stream_ordering
		   AND se.event_stream_ordering < $3
		   AND se.sender LIKE $4
		   AND NOT (se.event_stream_ordering = ANY($5))
		 GROUP BY dr.room_id
		ON CONFLICT (destination, room_id)
		DO UPDATE SET sticky_events_stream_position = EXCLUDED.sticky_events_stream_position
		 WHERE EXCLUDED.sticky_events_stream_position < backlog.sticky_events_stream_position`
	if sent == nil {
		sent = []int64{}
	}
	if _, err := w.pool.Exec(ctx, q, destination, old, new, localPattern, sent); err != nil {
		return fmt.Errorf("store: sticky backlog after catch-up for %s: %w", destination, err)
	}
	return nil
}
