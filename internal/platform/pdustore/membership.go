package pdustore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// The reads update_membership makes (handlers/room_member.py, v1.163.0).

const qProfile = `SELECT displayname, avatar_url FROM profiles WHERE full_user_id = $1`

// Profile is get_profile_displayname and get_profile_avatar_url: a local
// user's global profile. ok is false when the user has no profile row, which
// Synapse reads as a 404 and leaves both out.
func (s *Store) Profile(ctx context.Context, userID string) (displayname, avatarURL *string, ok bool, err error) {
	err = s.pool.QueryRow(ctx, qProfile, userID).Scan(&displayname, &avatarURL)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("pdustore: profile: %w", err)
	}
	return displayname, avatarURL, true, nil
}

const qRoomBlocked = `SELECT EXISTS (SELECT 1 FROM blocked_rooms WHERE room_id = $1)`

// RoomBlocked is is_room_blocked.
func (s *Store) RoomBlocked(ctx context.Context, roomID string) (bool, error) {
	return s.exists(ctx, "room blocked", qRoomBlocked, roomID)
}

const qLocalMembership = `SELECT membership, event_id FROM local_current_membership
	WHERE user_id = $1 AND room_id = $2`

// LocalMembership is get_local_current_membership_for_user_in_room: "" when
// the user has none.
func (s *Store) LocalMembership(ctx context.Context, userID, roomID string) (membership, eventID string, err error) {
	err = s.pool.QueryRow(ctx, qLocalMembership, userID, roomID).Scan(&membership, &eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("pdustore: local membership: %w", err)
	}
	return membership, eventID, nil
}

const qRatelimitOverride = `SELECT messages_per_second FROM ratelimit_override WHERE user_id = $1`

// RatelimitExempt is get_ratelimit_for_user read as can_do_action reads it:
// an override row whose messages_per_second is null or zero exempts the user
// from every limiter.
func (s *Store) RatelimitExempt(ctx context.Context, userID string) (bool, error) {
	var mps *int64
	err := s.pool.QueryRow(ctx, qRatelimitOverride, userID).Scan(&mps)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pdustore: ratelimit override: %w", err)
	}
	return mps == nil || *mps == 0, nil
}

const qRecentJoins = `SELECT e.received_ts FROM room_memberships AS m
	INNER JOIN events AS e USING (event_id)
	WHERE m.room_id = $1 AND m.membership = 'join' AND NOT e.outlier AND e.received_ts > $2
	ORDER BY e.received_ts`

// RecentJoins are the times (ms) of the room's join events since a moment:
// what every worker's per-room join limiter records as the events stream
// brings them (replication/tcp/client.py, notify_user_joined_room).
func (s *Store) RecentJoins(ctx context.Context, roomID string, sinceMS int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx, qRecentJoins, roomID, sinceMS)
	if err != nil {
		return nil, fmt.Errorf("pdustore: recent joins: %w", err)
	}
	ts, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("pdustore: recent joins: %w", err)
	}
	return ts, nil
}

const qRatelimitOverrideRates = `SELECT messages_per_second, burst_count FROM ratelimit_override WHERE user_id = $1`

// RatelimitOverride is get_ratelimit_for_user: whether the user has an
// override row, and its values.
func (s *Store) RatelimitOverride(ctx context.Context, userID string) (found bool, perSecond, burst *int64, err error) {
	err = s.pool.QueryRow(ctx, qRatelimitOverrideRates, userID).Scan(&perSecond, &burst)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, nil, nil
	}
	if err != nil {
		return false, nil, nil, fmt.Errorf("pdustore: ratelimit override: %w", err)
	}
	return true, perSecond, burst, nil
}

const qJoinedRooms = `SELECT room_id FROM current_state_events
	WHERE type = 'm.room.member' AND state_key = $1 AND membership = 'join'
	ORDER BY room_id`

// JoinedRooms is get_rooms_for_user, sorted as _update_join_states_task
// sorts it: the rooms the user is joined to.
func (s *Store) JoinedRooms(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, qJoinedRooms, userID)
	if err != nil {
		return nil, fmt.Errorf("pdustore: joined rooms: %w", err)
	}
	rooms, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("pdustore: joined rooms: %w", err)
	}
	return rooms, nil
}
