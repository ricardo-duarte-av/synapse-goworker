package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// What GET /profile/{userId}[/{field}] reads (storage/databases/main/
// profile.py). The queries key the profiles row as each of Synapse's does:
// by full_user_id, but get_profile_field by the localpart in user_id.

// Profile is a local user's profiles row.
type Profile struct {
	DisplayName, AvatarURL *string
	// Fields is the fields column as jsonb text, nil when NULL.
	Fields []byte
}

// ProfileRow is the profiles row of a user ID, or false when there is none.
func (s *Store) ProfileRow(ctx context.Context, userID string) (Profile, bool, error) {
	var p Profile
	var fields *string
	err := s.queryRow(ctx, "ProfileRow",
		`SELECT displayname, avatar_url, fields::text FROM profiles WHERE full_user_id = $1`, userID).
		Scan(&p.DisplayName, &p.AvatarURL, &fields)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, false, nil
	}
	if err != nil {
		return Profile{}, false, fmt.Errorf("store: profile: %w", err)
	}
	if fields != nil {
		p.Fields = []byte(*fields)
	}
	return p, true, nil
}

// ProfileField is get_profile_field's query: whether the localpart has a
// profiles row, whether the field is in it, and its value as jsonb text.
// field must follow the namespaced grammar, which has no double quote.
func (s *Store) ProfileField(ctx context.Context, localpart, field string) (row, exists bool, value []byte, err error) {
	var ex *bool
	var v *string
	err = s.queryRow(ctx, "ProfileField",
		`SELECT JSONB_PATH_EXISTS(fields, $1::jsonpath), JSONB_EXTRACT_PATH(fields, $2)::text
		   FROM profiles WHERE user_id = $3`,
		`$."`+field+`"`, field, localpart).Scan(&ex, &v)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil, nil
	}
	if err != nil {
		return false, false, nil, fmt.Errorf("store: profile field: %w", err)
	}
	if v != nil {
		value = []byte(*v)
	}
	return true, ex != nil && *ex, value, nil
}

// ShareARoom is get_rooms_for_user of each not being disjoint.
func (s *Store) ShareARoom(ctx context.Context, a, b string) (bool, error) {
	var shared bool
	err := s.queryRow(ctx, "ShareARoom", `
		SELECT EXISTS (
			SELECT 1 FROM current_state_events x
			  JOIN current_state_events y USING (room_id)
			 WHERE x.type = 'm.room.member' AND x.state_key = $1 AND x.membership = 'join'
			   AND y.type = 'm.room.member' AND y.state_key = $2 AND y.membership = 'join')`,
		a, b).Scan(&shared)
	if err != nil {
		return false, fmt.Errorf("store: shared rooms: %w", err)
	}
	return shared, nil
}

// RatelimitExempt is get_ratelimit_for_user as can_do_action reads it: a
// ratelimit_override row whose messages_per_second is null or zero exempts
// the user from every limiter.
func (s *Store) RatelimitExempt(ctx context.Context, userID string) (bool, error) {
	var mps *int64
	err := s.queryRow(ctx, "RatelimitExempt",
		`SELECT messages_per_second FROM ratelimit_override WHERE user_id = $1`, userID).Scan(&mps)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: ratelimit override: %w", err)
	}
	return mps == nil || *mps == 0, nil
}
