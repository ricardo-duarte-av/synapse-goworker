package synapsecfg

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Profile is what a profile lookup reads (rest/client/profile.py,
// handlers/profile.py check_profile_query_allowed).
type Profile struct {
	// RequireAuth is require_auth_for_profile_requests (default false).
	RequireAuth bool
	// LimitToSharedRooms is limit_profile_requests_to_users_who_share_rooms
	// (default false): MSC1301.
	LimitToSharedRooms bool
	// Rc is rc_profile, the lookup ratelimiter (1/s, burst 500).
	Rc Ratelimit
}

type profileRaw struct {
	RequireAuth        *bool     `yaml:"require_auth_for_profile_requests"`
	LimitToSharedRooms *bool     `yaml:"limit_profile_requests_to_users_who_share_rooms"`
	RcProfile          yaml.Node `yaml:"rc_profile"`
}

func profileSettings(b []byte) (Profile, error) {
	var r profileRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return Profile{}, fmt.Errorf("synapsecfg: profile settings: %w", err)
	}
	p := Profile{RequireAuth: deref(r.RequireAuth, false), LimitToSharedRooms: deref(r.LimitToSharedRooms, false)}
	var err error
	if p.Rc, err = ratelimit("rc_profile", lastWins(&r.RcProfile), 1, 500); err != nil {
		return Profile{}, err
	}
	return p, nil
}
