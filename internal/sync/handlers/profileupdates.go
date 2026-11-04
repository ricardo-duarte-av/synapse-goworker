package handlers

import (
	"context"
	"encoding/json"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lazyload"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Profile updates in /sync: MSC4429, under include_profile_updates_in_sync
// (handlers/sync.py _generate_sync_entry_for_profile_updates and
// _generate_initial_sync_entry_for_profile_updates, rest/client/sync.py,
// v1.164.1). Only local users' profiles; only for a filter naming
// org.matrix.msc4429.profile_fields.

// profileUsersKey is where the response carries them.
const profileUsersKey = "org.matrix.msc4429.users"

// syncProfileUpdates is the response's org.matrix.msc4429.users, or nil.
// joined and archived are the rooms already built, inviteSenders the
// senders of its invites and knocks: a lazy-loading sync names only the
// users they mention.
func syncProfileUpdates(ctx context.Context, d Deps, f *filter.Collection, userID, deviceID string,
	since *streamtoken.Token, now streamtoken.Token, joined, archived map[string]any,
	inviteSenders []string) (map[string]any, error) {

	if !d.ProfileUpdatesInSync || len(f.ProfileFields) == 0 {
		return nil, nil
	}
	fields := map[string]bool{}
	for _, name := range f.ProfileFields {
		fields[name] = true
	}

	var include map[string]bool
	if f.LazyLoadMembers() {
		include = map[string]bool{}
		add := func(u string) {
			if isMine(u, d.ServerName) {
				include[u] = true
			}
		}
		for _, s := range inviteSenders {
			add(s)
		}
		for _, entry := range joined {
			for _, s := range entrySenders(entry, "timeline", "msc4354_sticky") {
				add(s)
			}
		}
		for _, entry := range archived {
			for _, s := range entrySenders(entry, "timeline") {
				add(s)
			}
		}
	}

	var updates map[string]json.RawMessage
	var err error
	if since == nil {
		updates, err = initialProfileUpdates(ctx, d, userID, f.ProfileFields, include)
	} else {
		updates, err = incrementalProfileUpdates(ctx, d, userID, deviceID, f.ProfileFields, fields,
			since.ProfileUpdates, now.ProfileUpdates, include)
	}
	if err != nil || len(updates) == 0 {
		return nil, err
	}
	out := make(map[string]any, len(updates))
	for u, v := range updates {
		out[u] = map[string]json.RawMessage{"profile_updates": v}
	}
	return out, nil
}

// initialProfileUpdates: the local users sharing a room with the caller,
// and the caller, narrowed to include when that is not empty; each with the
// fields asked for that it has, and a user with none left out.
func initialProfileUpdates(ctx context.Context, d Deps, userID string, wanted []string,
	include map[string]bool) (map[string]json.RawMessage, error) {

	users, err := d.Store.LocalUsersSharingRoomWith(ctx, userID, nil)
	if err != nil {
		return nil, err
	}
	users[userID] = true
	if len(include) > 0 {
		for u := range users {
			if !include[u] {
				delete(users, u)
			}
		}
	}
	if len(users) == 0 {
		return nil, nil
	}
	data, err := d.Store.ProfileDataForUsers(ctx, setList(users))
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for u := range users {
		p, ok := data[u]
		if !ok {
			continue
		}
		per := map[string]json.RawMessage{}
		for _, name := range wanted {
			if v, ok := p.Get(name); ok {
				per[name] = v
			}
		}
		if len(per) > 0 {
			out[u] = mustJSON(per)
		}
	}
	return out, nil
}

// incrementalProfileUpdates: the profile_updates rows the caller receives
// since its last sync. A user who left the last shared room is null; one
// with no profile is null; otherwise the changed fields, or all the fields
// asked for of a user who joined a shared room, or of a user a lazy-loading
// sync mentions (less what this device was recently sent unchanged).
func incrementalProfileUpdates(ctx context.Context, d Deps, userID, deviceID string, wanted []string,
	fields map[string]bool, from, to int64, include map[string]bool) (map[string]json.RawMessage, error) {

	updates, err := d.Store.ProfileUpdatesFor(ctx, from, to, userID, wanted, false)
	if err != nil {
		return nil, err
	}
	left, joinedRoom, users := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, up := range updates {
		switch up.Action {
		case store.ProfileUpdateLeftRoom:
			left[up.UserID] = true
		case store.ProfileUpdateJoinedRoom:
			joinedRoom[up.UserID] = true
			users[up.UserID] = true
		case store.ProfileUpdateUpdate:
			users[up.UserID] = true
		}
	}
	for u := range include {
		users[u] = true
	}
	if len(users) == 0 && len(left) == 0 {
		return nil, nil
	}

	out := map[string]json.RawMessage{}
	if len(users) > 0 {
		changed := map[string]map[string]bool{}
		for _, up := range updates {
			if up.Action != store.ProfileUpdateUpdate || up.Fields == nil || len(up.Fields) == 0 || !users[up.UserID] {
				continue
			}
			if changed[up.UserID] == nil {
				changed[up.UserID] = map[string]bool{}
			}
			for _, name := range up.Fields {
				changed[up.UserID][name] = true
			}
		}
		data, err := d.Store.ProfileDataForUsers(ctx, setList(users))
		if err != nil {
			return nil, err
		}
		var sent *lazyload.Members
		if len(include) > 0 {
			sent = d.ProfileFieldsSent.For(lazyload.Key{UserID: userID, DeviceID: deviceID})
		}
		for u := range users {
			p, ok := data[u]
			if !ok {
				// Tells the client to forget the user's profile.
				out[u] = json.RawMessage(`null`)
				continue
			}
			// The fields it has and the fields that changed (a removed
			// field goes down as null), within base.
			base := fields
			lazy := len(include) > 0 && include[u]
			if !lazy && !joinedRoom[u] {
				base = changed[u]
			}
			names := map[string]bool{}
			for _, f := range p {
				if base[f.Name] {
					names[f.Name] = true
				}
			}
			for name := range changed[u] {
				if base[name] {
					names[name] = true
				}
			}
			per := map[string]json.RawMessage{}
			for name := range names {
				v, ok := p.Get(name)
				if !ok {
					v = json.RawMessage(`null`)
				}
				if lazy {
					key := u + "\x00" + name
					if sent.Sent(key) == string(v) {
						continue
					}
					sent.Record(key, string(v))
				}
				per[name] = v
			}
			if len(per) > 0 {
				out[u] = mustJSON(per)
			}
		}
	}
	for u := range left {
		out[u] = json.RawMessage(`null`)
	}
	return out, nil
}

// entrySenders are the senders of the events in the named sections of a
// built room entry.
func entrySenders(entry any, sections ...string) []string {
	m, ok := entry.(map[string]any)
	if !ok {
		return nil
	}
	var out []string
	for _, sec := range sections {
		block, ok := m[sec].(map[string]any)
		if !ok {
			continue
		}
		events, _ := block["events"].([]json.RawMessage)
		for _, ev := range events {
			if s := gjson.GetBytes(ev, "sender").Str; s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

func setList(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
