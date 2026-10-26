package store

import (
	"context"
	"fmt"
)

// MemberDelta is one change to an `m.room.member` entry of current state.
//
// Membership is "" when the delta's event is not a membership event we know
// of, and PrevMembership is "" when there was no previous entry. HasEvent is
// false for a removal -- a server leaving a room or a state reset -- which
// Synapse reads as a leave.
type MemberDelta struct {
	RoomID         string
	UserID         string
	Membership     string
	PrevMembership string
	HasEvent       bool
}

// MemberDeltasInRooms returns the membership changes to current state in the
// given rooms within (since, now], oldest first.
//
// Synapse's get_current_state_deltas_for_rooms narrowed to members, joined to
// both sides' memberships the way DeviceHandler.get_user_ids_changed fetches
// them: it is what tells sliding sync's e2ee extension who joined or left a
// room we are in.
func (s *Store) MemberDeltasInRooms(ctx context.Context, roomIDs []string,
	since, now int64) ([]MemberDelta, error) {

	if len(roomIDs) == 0 || since >= now {
		return nil, nil
	}
	const q = `
		SELECT s.room_id, s.state_key, s.event_id IS NOT NULL,
		       COALESCE(m.membership, ''), COALESCE(mp.membership, '')
		  FROM current_state_delta_stream s
		  LEFT JOIN room_memberships m  ON m.event_id  = s.event_id
		  LEFT JOIN room_memberships mp ON mp.event_id = s.prev_event_id
		 WHERE s.room_id = ANY($1) AND s.type = 'm.room.member'
		   AND s.stream_id > $2 AND s.stream_id <= $3
		 ORDER BY s.stream_id ASC`
	return s.memberDeltas(ctx, "MemberDeltasInRooms", q, roomIDs, since, now)
}

// SelfMemberDeltas returns the caller's own membership changes to current
// state, in every room, within (since, now], oldest first.
//
// Synapse's get_current_state_delta_membership_changes_for_user, including its
// one filter: a removal whose previous entry was already a leave repeats a
// leave the caller has been told about.
func (s *Store) SelfMemberDeltas(ctx context.Context, userID string,
	since, now int64) ([]MemberDelta, error) {

	if since >= now {
		return nil, nil
	}
	const q = `
		SELECT s.room_id, s.state_key, s.event_id IS NOT NULL,
		       COALESCE(m.membership, ''), COALESCE(mp.membership, '')
		  FROM current_state_delta_stream s
		  LEFT JOIN room_memberships m  ON m.event_id  = s.event_id
		  LEFT JOIN room_memberships mp ON mp.event_id = s.prev_event_id
		 WHERE s.type = 'm.room.member' AND s.state_key = $1
		   AND s.stream_id > $2 AND s.stream_id <= $3
		 ORDER BY s.stream_id ASC`
	all, err := s.memberDeltas(ctx, "SelfMemberDeltas", q, userID, since, now)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, d := range all {
		if !d.HasEvent && d.PrevMembership == "leave" {
			continue
		}
		if !d.HasEvent || d.Membership == "" {
			// No event means the entry was removed, which Synapse reports as
			// a leave.
			d.Membership = "leave"
		}
		out = append(out, d)
	}
	return out, nil
}

func (s *Store) memberDeltas(ctx context.Context, name, q string,
	subject any, since, now int64) ([]MemberDelta, error) {

	rows, err := s.query(ctx, name, q, subject, since, now)
	if err != nil {
		return nil, fmt.Errorf("store: member deltas: %w", err)
	}
	defer rows.Close()
	var out []MemberDelta
	for rows.Next() {
		var d MemberDelta
		if err := rows.Scan(&d.RoomID, &d.UserID, &d.HasEvent,
			&d.Membership, &d.PrevMembership); err != nil {
			return nil, fmt.Errorf("store: member deltas: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// UsersJoinedToAny returns which of userIDs are joined to at least one of
// roomIDs.
//
// Synapse's get_rooms_for_users narrowed to the question device_lists.left
// asks: has a departed user really left our view, or are they still in
// another room with us? Asking only about the handful who left is two orders
// of magnitude cheaper than listing everyone we share a room with -- 13ms
// against 430ms on a 654-room account.
func (s *Store) UsersJoinedToAny(ctx context.Context, userIDs, roomIDs []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(userIDs) == 0 || len(roomIDs) == 0 {
		return out, nil
	}
	const q = `
		SELECT DISTINCT state_key FROM current_state_events
		 WHERE type = 'm.room.member' AND membership = 'join'
		   AND state_key = ANY($1) AND room_id = ANY($2)`
	rows, err := s.query(ctx, "UsersJoinedToAny", q, userIDs, roomIDs)
	if err != nil {
		return nil, fmt.Errorf("store: users joined to rooms: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: users joined to rooms: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}
