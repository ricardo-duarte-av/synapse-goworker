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
	// ManagerMXID is room_management_mxid, built even when no room is
	// configured: since v1.165.0 update_membership refuses to invite it as
	// it refuses the server notices user. Default
	// @_server_announcements:<server_name>.
	ManagerMXID string
}

// Configured is whether any announcement room is configured. What goworker
// does not reproduce of the feature (synthetic joins in /sync, tags and
// reads by users who are not members, receipts kept from everyone but
// their sender) goes to main while it is.
func (a Announcements) Configured() bool { return a.Rooms > 0 }

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
