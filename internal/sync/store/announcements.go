package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tidwall/gjson"
)

// Server announcement rooms (Synapse 1.165, ServerAnnouncementsStore).
//
// server_announcement_rooms maps a configured room's `id` to the room main
// created for it. Synapse caches the mapping (get_server_announcement_config_
// ids_by_room_id, read on every sync) and invalidates it over the caches
// stream when main stores a new room, and on a room purge. So do we: the
// whole table is kept, and narrowed to the current configuration on each
// read, so a re-read homeserver.yaml applies without dropping it.
type announcementCache struct {
	mu    sync.Mutex
	rooms map[string]string // config_id -> room_id
	valid bool
	// gen moves on every invalidation, so a query that started before one
	// does not store a mapping the invalidation was meant to drop.
	gen uint64
}

// AnnouncementRoomsByConfigID returns server_announcement_rooms as
// config_id -> room_id, all of it. The caller narrows it to the configured
// ids, as get_server_announcement_config_ids_by_room_id selects them. The
// map is shared: do not write to it.
//
// A database without the table (a schema before 1.165) has no rooms.
func (s *Store) AnnouncementRoomsByConfigID(ctx context.Context) (map[string]string, error) {
	c := &s.announcements
	c.mu.Lock()
	if c.valid {
		rooms := c.rooms
		c.mu.Unlock()
		return rooms, nil
	}
	gen := c.gen
	c.mu.Unlock()

	rooms := map[string]string{}
	rows, err := s.query(ctx, "AnnouncementRooms",
		`SELECT config_id, room_id FROM server_announcement_rooms`)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return rooms, nil
		}
		return nil, fmt.Errorf("store: announcement rooms: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var configID, roomID string
		if err := rows.Scan(&configID, &roomID); err != nil {
			return nil, fmt.Errorf("store: announcement rooms: %w", err)
		}
		rooms[configID] = roomID
	}
	if err := rows.Err(); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("store: announcement rooms: %w", err)
	}

	c.mu.Lock()
	if c.gen == gen {
		c.rooms, c.valid = rooms, true
	}
	c.mu.Unlock()
	return rooms, nil
}

// InvalidateAnnouncementRooms drops the cached mapping: main stored a room
// (the caches row get_server_announcement_config_ids_by_room_id), or a room
// was purged.
func (s *Store) InvalidateAnnouncementRooms() {
	c := &s.announcements
	c.mu.Lock()
	c.valid, c.rooms = false, nil
	c.gen++
	c.mu.Unlock()
}

// EventPosition is a PersistedEventPosition: the writer and its stream
// ordering.
type EventPosition struct {
	InstanceName string
	Stream       int64
}

// FirstNonStateEventPositions is get_first_non_state_event_positions: in each
// room, the position of the first non-state, non-outlier event at or before
// maxStream, leaving out rooms that have none. One query per room, as
// Synapse's; announcement rooms are few.
func (s *Store) FirstNonStateEventPositions(ctx context.Context, roomIDs []string,
	maxStream int64) (map[string]EventPosition, error) {

	out := map[string]EventPosition{}
	for _, roomID := range roomIDs {
		var pos EventPosition
		err := s.queryRow(ctx, "FirstNonStateEventPositions", `
			SELECT COALESCE(instance_name, 'master'), stream_ordering FROM events
			 WHERE room_id = $1 AND stream_ordering <= $2
			   AND state_key IS NULL
			   AND NOT outlier
			 ORDER BY stream_ordering
			 LIMIT 1`, roomID, maxStream).Scan(&pos.InstanceName, &pos.Stream)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("store: first non-state event: %w", err)
		}
		out[roomID] = pos
	}
	return out, nil
}

// CreateEventTS is get_create_event_for_room(room_id).origin_server_ts: the
// create event named by the room's current state.
func (s *Store) CreateEventTS(ctx context.Context, roomID string) (int64, error) {
	if ts, ok := s.createTS.Load(roomID); ok {
		return ts.(int64), nil
	}
	var raw string
	err := s.queryRow(ctx, "CreateEventTS", `
		SELECT ej.json FROM current_state_events c
		  JOIN event_json ej USING (event_id)
		 WHERE c.room_id = $1 AND c.type = 'm.room.create' AND c.state_key = ''`,
		roomID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("store: create event: %w", err)
	}
	ts := gjson.Get(raw, "origin_server_ts").Int()
	s.createTS.Store(roomID, ts)
	return ts, nil
}

// RoomTags is get_tags_for_room as an m.tag content, `{"tags": {...}}`, or
// nil when the user has none in the room.
func (s *Store) RoomTags(ctx context.Context, userID, roomID string) ([]byte, error) {
	return s.roomTags(ctx, userID, roomID)
}
