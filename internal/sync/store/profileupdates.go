package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
)

// What profile updates in sync read (storage/databases/main/profile.py and
// roommember.py, v1.164.1): include_profile_updates_in_sync, MSC4429 in
// /sync and MSC4262 in sliding sync.

// ProfileUpdateAction values (api/constants.py ProfileUpdateAction).
const (
	ProfileUpdateJoinedRoom = "joined_room"
	ProfileUpdateLeftRoom   = "left_room"
	ProfileUpdateUpdate     = "update"
)

// ProfileUpdate is one profile_updates row a user receives.
type ProfileUpdate struct {
	StreamID int64
	UserID   string
	Action   string
	// Fields is affected_fields restricted to the fields asked for; nil when
	// the row has none (an action other than update).
	Fields []string
}

// ProfileUpdatesFor is get_profile_updates_for_user_and_fields: the rows in
// (from, to] that userID receives (profile_updates_per_user), in stream
// order, keeping an update only if it touched one of fields (all fields
// when all is set) and every other action.
func (s *Store) ProfileUpdatesFor(ctx context.Context, from, to int64, userID string,
	fields []string, all bool) ([]ProfileUpdate, error) {

	if from >= to || (!all && len(fields) == 0) {
		return nil, nil
	}
	fieldClause := `pu.affected_fields IS NOT NULL`
	args := []any{from, to, userID, ProfileUpdateUpdate}
	if !all {
		fieldClause = `EXISTS (SELECT 1 FROM jsonb_array_elements_text(pu.affected_fields) AS field_names(value)
			WHERE field_names.value = ANY($5))`
		args = append(args, fields)
	}
	rows, err := s.query(ctx, "ProfileUpdatesFor", `
		SELECT pu.stream_id, pu.user_id, pu.action, pu.affected_fields::text
		  FROM profile_updates AS pu
		 INNER JOIN profile_updates_per_user AS puf ON pu.stream_id = puf.stream_id
		 WHERE $1 < pu.stream_id AND pu.stream_id <= $2
		   AND puf.user_id = $3
		   AND (`+fieldClause+` OR pu.action != $4)
		 ORDER BY pu.stream_id ASC`, args...)
	if err != nil {
		return nil, fmt.Errorf("store: profile updates: %w", err)
	}
	defer rows.Close()
	wanted := map[string]bool{}
	for _, f := range fields {
		wanted[f] = true
	}
	var out []ProfileUpdate
	for rows.Next() {
		var u ProfileUpdate
		var affected *string
		if err := rows.Scan(&u.StreamID, &u.UserID, &u.Action, &affected); err != nil {
			return nil, fmt.Errorf("store: profile updates: %w", err)
		}
		if affected != nil {
			u.Fields = []string{}
			seen := map[string]bool{}
			for _, f := range gjson.Parse(*affected).Array() {
				if (all || wanted[f.Str]) && !seen[f.Str] {
					seen[f.Str] = true
					u.Fields = append(u.Fields, f.Str)
				}
			}
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ProfileField is one field of a profile, its value as JSON.
type ProfileField struct {
	Name  string
	Value json.RawMessage
}

// ProfileData is a profile's fields in Synapse's order: the custom fields
// as jsonb holds them, then displayname and avatar_url, each set over a
// custom field of the same name where there is one.
type ProfileData []ProfileField

// Get is a field's value, and whether it is set.
func (p ProfileData) Get(name string) (json.RawMessage, bool) {
	for _, f := range p {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}

// ProfileDataForUsers is get_profile_data_for_users: each user's profile,
// for the users with a profiles row (local users only).
func (s *Store) ProfileDataForUsers(ctx context.Context, users []string) (map[string]ProfileData, error) {
	out := map[string]ProfileData{}
	if len(users) == 0 {
		return out, nil
	}
	rows, err := s.query(ctx, "ProfileDataForUsers",
		`SELECT full_user_id, displayname, avatar_url, fields::text FROM profiles WHERE full_user_id = ANY($1)`, users)
	if err != nil {
		return nil, fmt.Errorf("store: profile data: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var user string
		var name, avatar, fields *string
		if err := rows.Scan(&user, &name, &avatar, &fields); err != nil {
			return nil, fmt.Errorf("store: profile data: %w", err)
		}
		var p ProfileData
		if fields != nil {
			gjson.Parse(*fields).ForEach(func(k, v gjson.Result) bool {
				p = p.set(k.Str, compactJSON(v.Raw))
				return true
			})
		}
		if name != nil {
			p = p.set("displayname", jsonStringRaw(*name))
		}
		if avatar != nil {
			p = p.set("avatar_url", jsonStringRaw(*avatar))
		}
		out[user] = p
	}
	return out, rows.Err()
}

// set is a dict assignment: in place if the key exists, else at the end.
func (p ProfileData) set(name string, v json.RawMessage) ProfileData {
	for i := range p {
		if p[i].Name == name {
			p[i].Value = v
			return p
		}
	}
	return append(p, ProfileField{name, v})
}

// LocalUsersSharingRoomWith is get_local_users_who_share_room_with_user:
// the local users joined (local_current_membership) to a room userID is
// joined to (current_state_events), the rooms limited to limit when it is
// not nil. userID is among them when it is in any such room.
func (s *Store) LocalUsersSharingRoomWith(ctx context.Context, userID string, limit []string) (map[string]bool, error) {
	out := map[string]bool{}
	if limit != nil && len(limit) == 0 {
		return out, nil
	}
	q := `
		SELECT DISTINCT l.user_id
		  FROM current_state_events c
		  JOIN local_current_membership l ON l.room_id = c.room_id AND l.membership = 'join'
		 WHERE c.type = 'm.room.member' AND c.state_key = $1 AND c.membership = 'join'`
	args := []any{userID}
	if limit != nil {
		q += ` AND c.room_id = ANY($2)`
		args = append(args, limit)
	}
	rows, err := s.query(ctx, "LocalUsersSharingRoomWith", q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: local users sharing a room: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("store: local users sharing a room: %w", err)
		}
		out[u] = true
	}
	return out, rows.Err()
}

// RoomsOfUsers is get_rooms_for_users, flattened: every room any of users
// is joined to.
func (s *Store) RoomsOfUsers(ctx context.Context, users []string) ([]string, error) {
	if len(users) == 0 {
		return nil, nil
	}
	rows, err := s.query(ctx, "RoomsOfUsers", `
		SELECT DISTINCT room_id FROM current_state_events
		 WHERE type = 'm.room.member' AND membership = 'join' AND state_key = ANY($1)`, users)
	if err != nil {
		return nil, fmt.Errorf("store: rooms of users: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, fmt.Errorf("store: rooms of users: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func compactJSON(raw string) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(raw)); err != nil {
		return json.RawMessage(raw)
	}
	return buf.Bytes()
}

// jsonStringRaw encodes s without escaping <, > and &, as Synapse's encoder.
func jsonStringRaw(s string) json.RawMessage {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}
