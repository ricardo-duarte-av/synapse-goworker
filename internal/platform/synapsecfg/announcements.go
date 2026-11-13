package synapsecfg

import (
	"errors"
	"strconv"
)

// Announcements is experimental_features.server_announcements
// (config/server_announcements.py, v1.165.0): rooms main creates and
// reconciles at start, delivered to every local user without a
// membership.
type Announcements struct {
	// Rooms is how many rooms are configured. None (the default) disables
	// the feature, and then every check Synapse makes on announcement rooms
	// finds none.
	Rooms int
	// List is the rooms as ServerAnnouncementsConfig.rooms holds them, in
	// order: what sync needs of each (its id, and who is not delivered it).
	// A room Synapse would refuse to start with is read as far as it goes.
	List []AnnouncementRoom
	// ManagerMXID is room_management_mxid, built even when no room is
	// configured: since v1.165.0 update_membership refuses to invite it as
	// it refuses the server notices user. Default
	// @_server_announcements:<server_name>.
	ManagerMXID string
}

// AnnouncementRoom is one configured room (ServerAnnouncementRoom).
type AnnouncementRoom struct {
	// ConfigID is its `id`: the key of server_announcement_rooms, not a
	// room ID.
	ConfigID string
	// ExcludedUserIDs are the users it is not delivered to.
	ExcludedUserIDs []string
}

// Configured is whether any announcement room is configured. Tag writes
// still go to main while it is (TagServlet's checks are not reproduced).
func (a Announcements) Configured() bool { return a.Rooms > 0 }

// ConfigIDs are the configured rooms' ids, in order.
func (a Announcements) ConfigIDs() []string {
	out := make([]string, 0, len(a.List))
	for _, r := range a.List {
		out = append(out, r.ConfigID)
	}
	return out
}

// ExcludedConfigIDs are the ids of the rooms that list userID in
// excluded_user_ids (get_server_announcement_room_ids_for_user).
func (a Announcements) ExcludedConfigIDs(userID string) map[string]bool {
	var out map[string]bool
	for _, r := range a.List {
		for _, u := range r.ExcludedUserIDs {
			if u == userID {
				if out == nil {
					out = map[string]bool{}
				}
				out[r.ConfigID] = true
				break
			}
		}
	}
	return out
}

// announcements reads it as ServerAnnouncementsConfig.read_config does,
// refusing what it refuses before looking at the rooms: the section and its
// rooms must be a mapping and a list. The rooms' own validation is Synapse's;
// a homeserver.yaml it accepts is all we read.
func announcements(experimental map[string]any, serverName string) (Announcements, error) {
	var a Announcements
	m := map[string]any{}
	if v, ok := experimental["server_announcements"]; ok {
		if m, ok = v.(map[string]any); !ok {
			return a, errors.New("synapsecfg: experimental_features.server_announcements must be a mapping")
		}
	}
	if v, ok := m["rooms"]; ok {
		rooms, ok := v.([]any)
		if !ok {
			return a, errors.New("synapsecfg: experimental_features.server_announcements.rooms must be a list")
		}
		a.Rooms = len(rooms)
		for _, raw := range rooms {
			room, _ := raw.(map[string]any)
			id, _ := room["id"].(string)
			if id == "" {
				continue
			}
			r := AnnouncementRoom{ConfigID: id}
			excluded, _ := room["excluded_user_ids"].([]any)
			for _, u := range excluded {
				if u, ok := u.(string); ok {
					r.ExcludedUserIDs = append(r.ExcludedUserIDs, u)
				}
			}
			a.List = append(a.List, r)
		}
	}
	// UserID(localpart, server_name).to_string() is "@%s:%s": a localpart
	// that is not a string (only validated when rooms are configured) is
	// str() of it.
	lp := "_server_announcements"
	if v, ok := m["room_management_localpart"]; ok {
		switch v := v.(type) {
		case string:
			lp = v
		case int:
			lp = strconv.Itoa(v)
		case bool:
			lp = map[bool]string{true: "True", false: "False"}[v]
		case nil:
			lp = "None"
		default:
			// A float or a container: Python's str() of it is not worth
			// reproducing for a user nobody can register.
			lp = ""
		}
	}
	if lp != "" {
		a.ManagerMXID = "@" + lp + ":" + serverName
	}
	return a, nil
}
