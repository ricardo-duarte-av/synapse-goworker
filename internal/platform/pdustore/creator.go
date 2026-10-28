package pdustore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Queries for the event creator: what Synapse reads when it builds a local
// event, and what the shadow check reads to find the event Synapse built.

// qPrevEventsForRoom is _get_prev_events_for_room_txn
// (storage/databases/main/event_federation.py), verbatim: the ten deepest
// forward extremities. Ties in depth come back in no particular order.
const qPrevEventsForRoom = `SELECT e.event_id FROM event_forward_extremities AS f
	INNER JOIN events AS e USING (event_id)
	WHERE f.room_id = $1
	ORDER BY e.depth DESC
	LIMIT 10`

// PrevEventsForRoom is get_prev_events_for_room.
func (s *Store) PrevEventsForRoom(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, qPrevEventsForRoom, roomID)
	if err != nil {
		return nil, fmt.Errorf("pdustore: prev events for room: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("pdustore: prev events for room: %w", err)
	}
	return ids, nil
}

const qMaxDepthOf = `SELECT event_id, depth FROM events WHERE event_id = ANY($1)`

// Depths returns each known event's depth.
func (s *Store) Depths(ctx context.Context, ids []string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, qMaxDepthOf, ids)
	if err != nil {
		return nil, fmt.Errorf("pdustore: depths: %w", err)
	}
	out := make(map[string]int64, len(ids))
	var id string
	var depth int64
	_, err = pgx.ForEachRow(rows, []any{&id, &depth}, func() error {
		out[id] = depth
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pdustore: depths: %w", err)
	}
	return out, nil
}

// RoomMaxStream is the newest stream ordering in a room, 0 for none.
func (s *Store) RoomMaxStream(ctx context.Context, roomID string) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, qRoomMaxStream, roomID).Scan(&n); err != nil {
		return 0, fmt.Errorf("pdustore: room max stream: %w", err)
	}
	return n, nil
}

const qNewMembership = `SELECT m.event_id FROM room_memberships AS m
	INNER JOIN events AS e USING (event_id)
	WHERE m.room_id = $1 AND e.stream_ordering > $2 AND m.membership = $3
	  AND ($4 = '' OR m.user_id = $4) AND m.sender LIKE '%:' || $5
	ORDER BY e.stream_ordering DESC
	LIMIT 1`

// ErrNotFound is a lookup that found nothing.
var ErrNotFound = errors.New("pdustore: not found")

// NewMembership finds the newest membership event in a room, newer than
// stream ordering after, with the given membership, for target ("" for
// any), sent by a user of server. The membership endpoints answer {} and
// /join answers only the room ID, so this is how their event is found.
func (s *Store) NewMembership(ctx context.Context, roomID string, after int64, membership, target, server string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, qNewMembership, roomID, after, membership, target, server).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pdustore: new membership: %w", err)
	}
	return id, nil
}

// qTxnEvent is get_event_id_from_transaction_id_and_device_id
// (storage/databases/main/events_worker.py).
const qTxnEvent = `SELECT event_id FROM event_txn_id_device_id
	WHERE room_id = $1 AND user_id = $2 AND device_id = $3 AND txn_id = $4`

// TxnEvent is the event a client transaction already created, ErrNotFound
// for none.
func (s *Store) TxnEvent(ctx context.Context, roomID, userID, deviceID, txnID string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, qTxnEvent, roomID, userID, deviceID, txnID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pdustore: txn event: %w", err)
	}
	return id, nil
}

const qReadOnly = `SELECT current_setting('default_transaction_read_only')`

// ReadOnly reports whether the session refuses writes. Until write mode the
// worker connects read-only, and refuses to start otherwise.
func (s *Store) ReadOnly(ctx context.Context) (bool, error) {
	var v string
	if err := s.pool.QueryRow(ctx, qReadOnly).Scan(&v); err != nil {
		return false, fmt.Errorf("pdustore: read-only setting: %w", err)
	}
	return v == "on", nil
}

// qTokenInfo is _query_for_auth (storage/databases/main/registration.py),
// plus the user flags the requester and the native path need.
const qPartialStateRoom = `SELECT EXISTS (SELECT 1 FROM partial_state_rooms WHERE room_id = $1)`

// PartialState reports whether the room has partial state.
func (s *Store) PartialState(ctx context.Context, roomID string) (bool, error) {
	var b bool
	if err := s.pool.QueryRow(ctx, qPartialStateRoom, roomID).Scan(&b); err != nil {
		return false, fmt.Errorf("pdustore: partial state: %w", err)
	}
	return b, nil
}

const qEventRoom = `SELECT e.room_id FROM events AS e
	WHERE e.event_id = $1 AND NOT EXISTS (SELECT 1 FROM rejections AS r WHERE r.event_id = e.event_id)`

// EventRoom is the room of an event get_event would return: held, and not
// rejected. ErrNotFound otherwise.
func (s *Store) EventRoom(ctx context.Context, eventID string) (string, error) {
	var room string
	err := s.pool.QueryRow(ctx, qEventRoom, eventID).Scan(&room)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("pdustore: event room: %w", err)
	}
	return room, nil
}

const (
	// qIsRelationTarget is event_is_target_of_relation.
	qIsRelationTarget = `SELECT EXISTS (SELECT 1 FROM event_relations WHERE relates_to_id = $1)`
	// qHasRelation is event_includes_relation.
	qHasRelation = `SELECT EXISTS (SELECT 1 FROM event_relations WHERE event_id = $1)`
	// qHasAnnotated is has_user_annotated_event, verbatim.
	qHasAnnotated = `SELECT EXISTS (SELECT 1 FROM event_relations
		INNER JOIN events USING (event_id)
		WHERE relates_to_id = $1 AND relation_type = 'm.annotation' AND type = $2
		  AND sender = $3 AND aggregation_key = $4)`
)

// IsRelationTarget is event_is_target_of_relation.
func (s *Store) IsRelationTarget(ctx context.Context, eventID string) (bool, error) {
	return s.exists(ctx, "relation target", qIsRelationTarget, eventID)
}

// HasRelation is event_includes_relation.
func (s *Store) HasRelation(ctx context.Context, eventID string) (bool, error) {
	return s.exists(ctx, "has relation", qHasRelation, eventID)
}

// HasAnnotated is has_user_annotated_event.
func (s *Store) HasAnnotated(ctx context.Context, parent, eventType, sender, key string) (bool, error) {
	return s.exists(ctx, "annotated", qHasAnnotated, parent, eventType, sender, key)
}

func (s *Store) exists(ctx context.Context, what, q string, args ...any) (bool, error) {
	var b bool
	if err := s.pool.QueryRow(ctx, q, args...).Scan(&b); err != nil {
		return false, fmt.Errorf("pdustore: %s: %w", what, err)
	}
	return b, nil
}

const qRejectionColumn = `SELECT rejection_reason FROM events WHERE event_id = $1`

// RejectionColumn is events.rejection_reason, which the events writer fills
// from the context it was sent; nil for NULL.
func (s *Store) RejectionColumn(ctx context.Context, eventID string) (*string, error) {
	var v *string
	if err := s.pool.QueryRow(ctx, qRejectionColumn, eventID).Scan(&v); err != nil {
		return nil, fmt.Errorf("pdustore: rejection column: %w", err)
	}
	return v, nil
}

const qUserFlags = `SELECT COALESCE(admin, 0) <> 0, COALESCE(suspended, false) FROM users WHERE name = $1`

// UserFlags is get_user_by_id's existence, is_server_admin and
// get_user_suspended_status, for a user an appservice acts as.
func (s *Store) UserFlags(ctx context.Context, userID string) (exists, admin, suspended bool, err error) {
	err = s.pool.QueryRow(ctx, qUserFlags, userID).Scan(&admin, &suspended)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, false, nil
	}
	if err != nil {
		return false, false, false, fmt.Errorf("pdustore: user flags: %w", err)
	}
	return true, admin, suspended, nil
}

const qShadowBanned = `SELECT COALESCE(shadow_banned, false) FROM users WHERE name = $1`

// ShadowBanned is the requester's shadow_banned (get_user_by_access_token's
// users row); false for a user with no row.
func (s *Store) ShadowBanned(ctx context.Context, userID string) (bool, error) {
	var b bool
	err := s.pool.QueryRow(ctx, qShadowBanned, userID).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pdustore: shadow banned: %w", err)
	}
	return b, nil
}

const qDeviceExists = `SELECT EXISTS (SELECT 1 FROM devices WHERE user_id = $1 AND device_id = $2 AND NOT hidden)`

// DeviceExists is get_device: a device that is not hidden.
func (s *Store) DeviceExists(ctx context.Context, userID, deviceID string) (bool, error) {
	return s.exists(ctx, "device", qDeviceExists, userID, deviceID)
}
