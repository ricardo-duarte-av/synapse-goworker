package slidingsync

import (
	"context"
	"encoding/json"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// The MSC4262 profiles extension, under include_profile_updates_in_sync
// (handlers/sliding_sync/extensions.py get_profiles_extension_response,
// v1.164.1). Only local users' profiles.

// profilesJSON is the extension's response: user ID to
// {"updated": {...}, "removed": [...]}, or null to forget the user.
type profilesJSON struct {
	Users map[string]json.RawMessage `json:"users"`
}

// profilesExtension is the extension's response, nil when there is nothing
// to say (Synapse's ProfilesExtension is falsy without users, and then
// neither serialised nor news).
func profilesExtension(ctx context.Context, d Deps, in extensionInputs, req *ProfilesExtension) (*profilesJSON, error) {
	if !req.Enabled {
		return nil, nil
	}
	// Absent fields mean all of them; an empty list, none.
	all := req.Fields == nil
	fields := map[string]bool{}
	for _, f := range req.Fields {
		fields[f] = true
	}
	users, lazyUsers, err := profileUserIDs(ctx, d, in)
	if err != nil {
		return nil, err
	}
	var out map[string]json.RawMessage
	if in.From == nil {
		out, err = initialProfiles(ctx, d, in.UserID, all, fields, users)
	} else {
		out, err = incrementalProfiles(ctx, d, in, all, req.Fields, fields, users, lazyUsers)
	}
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return &profilesJSON{Users: out}, nil
}

// profileUserIDs is _get_profile_ids_for_profiles_extension: the senders,
// member state and heroes of the rooms sent whose config lazy-loads members,
// and every local user sharing one of the other relevant rooms.
func profileUserIDs(ctx context.Context, d Deps, in extensionInputs) (all, lazy map[string]bool, err error) {
	lazyRooms := map[string]bool{}
	for room, sub := range in.Request.RoomSubscriptions {
		if lazyMembers(sub.RequiredState) {
			lazyRooms[room] = true
		}
	}
	for name, list := range in.Request.Lists {
		if !lazyMembers(list.RequiredState) {
			continue
		}
		for _, op := range in.Lists[name].Ops {
			for _, room := range op.RoomIDs {
				lazyRooms[room] = true
			}
		}
	}

	lazy = map[string]bool{}
	for room, res := range in.Rooms {
		if !lazyRooms[room] || res == nil {
			continue
		}
		for _, ev := range res.Timeline {
			if s := gjson.GetBytes(ev, "sender").Str; s != "" {
				lazy[s] = true
			}
		}
		for _, ev := range res.RequiredState {
			if gjson.GetBytes(ev, "type").Str == "m.room.member" {
				lazy[gjson.GetBytes(ev, "state_key").Str] = true
			}
		}
		for _, h := range res.Heroes {
			lazy[h.UserID] = true
		}
	}

	// actual_room_ids: every relevant room, sent this time or not.
	var nonLazy []string
	seen := map[string]bool{}
	note := func(room string) {
		if !seen[room] && !lazyRooms[room] {
			nonLazy = append(nonLazy, room)
		}
		seen[room] = true
	}
	for _, l := range in.Lists {
		for _, op := range l.Ops {
			for _, room := range op.RoomIDs {
				note(room)
			}
		}
	}
	for room, ok := range in.Subscribed {
		if ok {
			note(room)
		}
	}
	all = map[string]bool{}
	for u := range lazy {
		all[u] = true
	}
	if len(nonLazy) > 0 {
		members, err := d.Store.LocalUsersSharingRoomWith(ctx, in.UserID, nonLazy)
		if err != nil {
			return nil, nil, err
		}
		for u := range members {
			all[u] = true
		}
	}
	return all, lazy, nil
}

// lazyMembers is (m.room.member, $LAZY) being in a required_state.
func lazyMembers(rs [][2]string) bool {
	for _, p := range rs {
		if p[0] == "m.room.member" && p[1] == "$LAZY" {
			return true
		}
	}
	return false
}

// initialProfiles: each of users, and the caller, who has a profile, with
// the fields asked for, as "updated".
func initialProfiles(ctx context.Context, d Deps, userID string, all bool, fields map[string]bool,
	users map[string]bool) (map[string]json.RawMessage, error) {

	ids := make([]string, 0, len(users)+1)
	for u := range users {
		ids = append(ids, u)
	}
	if !users[userID] {
		ids = append(ids, userID)
	}
	data, err := d.Store.ProfileDataForUsers(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for u, p := range data {
		per := map[string]json.RawMessage{}
		for _, f := range p {
			if all || fields[f.Name] {
				per[f.Name] = f.Value
			}
		}
		if len(per) > 0 {
			out[u] = mustMarshal(map[string]any{"updated": per})
		}
	}
	return out, nil
}

// incrementalProfiles: the profile_updates rows since the connection's
// position, in stream order, a leave overriding an earlier join or update
// and a join an earlier leave. A user who left the last shared room, or has
// no profile, is null; one who joined a shared room, or a lazy room
// mentions, has all the fields asked for; anyone else only what changed,
// a field gone from the profile listed as "removed".
func incrementalProfiles(ctx context.Context, d Deps, in extensionInputs, all bool, wanted []string,
	fields map[string]bool, users, lazyUsers map[string]bool) (map[string]json.RawMessage, error) {

	updates, err := d.Store.ProfileUpdatesFor(ctx, in.From.ProfileUpdates, in.Now.ProfileUpdates,
		in.UserID, wanted, all)
	if err != nil {
		return nil, err
	}
	joined, updated, left := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, up := range updates {
		switch up.Action {
		case store.ProfileUpdateJoinedRoom:
			joined[up.UserID] = true
			delete(left, up.UserID)
		case store.ProfileUpdateUpdate:
			updated[up.UserID] = true
		case store.ProfileUpdateLeftRoom:
			left[up.UserID] = true
			delete(joined, up.UserID)
			delete(updated, up.UserID)
		}
	}
	for u := range joined {
		users[u] = true
	}
	for u := range updated {
		users[u] = true
	}
	out := map[string]json.RawMessage{}
	for u := range left {
		out[u] = json.RawMessage(`null`)
	}

	changed := map[string]map[string]bool{}
	for _, up := range updates {
		if up.Action != store.ProfileUpdateUpdate || len(up.Fields) == 0 || left[up.UserID] || !users[up.UserID] {
			continue
		}
		for _, name := range up.Fields {
			if all || fields[name] {
				if changed[up.UserID] == nil {
					changed[up.UserID] = map[string]bool{}
				}
				changed[up.UserID][name] = true
			}
		}
	}

	ids := make([]string, 0, len(users))
	for u := range users {
		ids = append(ids, u)
	}
	data, err := d.Store.ProfileDataForUsers(ctx, ids)
	if err != nil {
		return nil, err
	}
	for u := range users {
		if left[u] {
			continue
		}
		p, ok := data[u]
		if !ok {
			out[u] = json.RawMessage(`null`)
			continue
		}
		names := map[string]bool{}
		if joined[u] || lazyUsers[u] {
			for _, f := range p {
				names[f.Name] = true
			}
		}
		for name := range changed[u] {
			names[name] = true
		}
		if len(fields) > 0 {
			for name := range names {
				if !fields[name] {
					delete(names, name)
				}
			}
		}
		if len(names) == 0 {
			continue
		}
		per := map[string]json.RawMessage{}
		var removed []string
		for name := range names {
			v, ok := p.Get(name)
			if !ok && changed[u][name] {
				removed = append(removed, name)
				continue
			}
			if !ok {
				// Absent and not changed: Synapse's Absent sentinel reaches
				// the encoder; it cannot arise, as names come from p then.
				continue
			}
			per[name] = v
		}
		if len(per) == 0 && len(removed) == 0 {
			continue
		}
		entry := map[string]any{}
		if len(per) > 0 {
			entry["updated"] = per
		}
		if len(removed) > 0 {
			entry["removed"] = removed
		}
		out[u] = mustMarshal(entry)
	}
	return out, nil
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
