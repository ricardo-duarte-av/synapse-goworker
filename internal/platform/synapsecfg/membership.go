package synapsecfg

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Membership is what update_membership reads from the configuration
// (handlers/room_member.py, config/server.py, config/ratelimiting.py,
// v1.163.0).
type Membership struct {
	// ServerNoticesMXID is server_notices.system_mxid_localpart as a user ID,
	// empty when server notices are off.
	ServerNoticesMXID string
	// BlockNonAdminInvites is block_non_admin_invites.
	BlockNonAdminInvites bool
	// AllowPerRoomProfiles is allow_per_room_profiles, default true.
	AllowPerRoomProfiles bool
	// IncludeProfileDataOnInvite is include_profile_data_on_invite, default
	// true.
	IncludeProfileDataOnInvite bool
	// AuthBlocking is hs_disabled or limit_usage_by_mau: check_auth_blocking
	// may then refuse a request.
	AuthBlocking bool
	// ConsentRequired is user_consent.block_events_error being set: events
	// are refused until the privacy policy is accepted.
	ConsentRequired bool
	// AvatarLimits is max_avatar_size or allowed_avatar_mimetypes being set:
	// a per-room avatar is then checked against the media repository.
	AvatarLimits bool
	// Modules is any module or legacy spam checker being configured: their
	// callbacks (user_may_invite, user_may_join_room, check_event_allowed,
	// ratelimit overrides) are Python.
	Modules bool

	JoinsLocal, JoinsPerRoom                         Ratelimit
	InvitesPerRoom, InvitesPerUser, InvitesPerIssuer Ratelimit
}

// Ratelimit is RatelimitSettings.
type Ratelimit struct {
	// Key is the limiter's name, as in the config.
	Key       string
	PerSecond float64
	Burst     int
}

type membershipRaw struct {
	ServerNotices              yaml.Node `yaml:"server_notices"`
	BlockNonAdminInvites       *bool     `yaml:"block_non_admin_invites"`
	AllowPerRoomProfiles       *bool     `yaml:"allow_per_room_profiles"`
	IncludeProfileDataOnInvite *bool     `yaml:"include_profile_data_on_invite"`
	HSDisabled                 *bool     `yaml:"hs_disabled"`
	LimitUsageByMAU            *bool     `yaml:"limit_usage_by_mau"`
	UserConsent                yaml.Node `yaml:"user_consent"`
	Modules                    yaml.Node `yaml:"modules"`
	SpamChecker                yaml.Node `yaml:"spam_checker"`
	RcJoins                    yaml.Node `yaml:"rc_joins"`
	RcJoinsPerRoom             yaml.Node `yaml:"rc_joins_per_room"`
	RcInvites                  yaml.Node `yaml:"rc_invites"`
	MaxAvatarSize              yaml.Node `yaml:"max_avatar_size"`
	AllowedAvatarMimetypes     yaml.Node `yaml:"allowed_avatar_mimetypes"`
}

func membership(b []byte, serverName string) (Membership, error) {
	var r membershipRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return Membership{}, fmt.Errorf("synapsecfg: membership settings: %w", err)
	}
	m := Membership{
		BlockNonAdminInvites:       deref(r.BlockNonAdminInvites, false),
		AllowPerRoomProfiles:       deref(r.AllowPerRoomProfiles, true),
		IncludeProfileDataOnInvite: deref(r.IncludeProfileDataOnInvite, true),
		AuthBlocking:               deref(r.HSDisabled, false) || deref(r.LimitUsageByMAU, false),
	}
	// ServerNoticesConfig: a server_notices section names the user.
	if sn := lastWins(&r.ServerNotices); len(sn) > 0 {
		lp, ok := sn["system_mxid_localpart"].(string)
		if !ok || lp == "" {
			return Membership{}, fmt.Errorf("synapsecfg: server_notices without system_mxid_localpart")
		}
		m.ServerNoticesMXID = "@" + lp + ":" + serverName
	}
	if uc := lastWins(&r.UserConsent); uc["block_events_error"] != nil {
		m.ConsentRequired = true
	}
	m.Modules = nonEmpty(&r.Modules) || nonEmpty(&r.SpamChecker)
	// ContentRepositoryConfig: a size of 0 or none, and no list, check nothing.
	m.AvatarLimits = truthyScalar(&r.MaxAvatarSize) || nonEmpty(&r.AllowedAvatarMimetypes)

	joins := lastWins(&r.RcJoins)
	invites := lastWins(&r.RcInvites)
	var err error
	if m.JoinsLocal, err = ratelimit("rc_joins.local", joins["local"], 0.1, 10); err != nil {
		return Membership{}, err
	}
	if m.JoinsPerRoom, err = ratelimit("rc_joins_per_room", nodeMap(&r.RcJoinsPerRoom), 1, 10); err != nil {
		return Membership{}, err
	}
	if m.InvitesPerRoom, err = ratelimit("rc_invites.per_room", invites["per_room"], 0.3, 10); err != nil {
		return Membership{}, err
	}
	if m.InvitesPerUser, err = ratelimit("rc_invites.per_user", invites["per_user"], 0.003, 5); err != nil {
		return Membership{}, err
	}
	if m.InvitesPerIssuer, err = ratelimit("rc_invites.per_issuer", invites["per_issuer"], 0.3, 10); err != nil {
		return Membership{}, err
	}
	return m, nil
}

// ratelimit is RatelimitSettings.parse: each value, or its default.
func ratelimit(key string, v any, perSecond float64, burst int) (Ratelimit, error) {
	out := Ratelimit{Key: key, PerSecond: perSecond, Burst: burst}
	m, _ := v.(map[string]any)
	if ps, ok := m["per_second"]; ok {
		f, err := parseFloat(ps, key+".per_second")
		if err != nil {
			return Ratelimit{}, err
		}
		out.PerSecond = f
	}
	if bc, ok := m["burst_count"]; ok {
		// int(...): a float burst is truncated.
		f, err := parseFloat(bc, key+".burst_count")
		if err != nil {
			return Ratelimit{}, err
		}
		out.Burst = int(f)
	}
	return out, nil
}

func parseFloat(v any, what string) (float64, error) {
	switch t := v.(type) {
	case int:
		return float64(t), nil
	case float64:
		return t, nil
	}
	return 0, fmt.Errorf("synapsecfg: %s: expected a number, got %T", what, v)
}

func nodeMap(n *yaml.Node) map[string]any {
	if n.Kind != yaml.MappingNode {
		return nil
	}
	return lastWins(n)
}

// nonEmpty reports whether a node holds something: a non-empty list or map.
func nonEmpty(n *yaml.Node) bool {
	switch n.Kind {
	case yaml.SequenceNode, yaml.MappingNode:
		return len(n.Content) > 0
	case yaml.ScalarNode:
		return n.Tag != "!!null"
	}
	return false
}

// truthyScalar is a scalar Python would read as true: not null, 0 or "".
func truthyScalar(n *yaml.Node) bool {
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" {
		return false
	}
	switch n.Value {
	case "", "0", "false", "False":
		return false
	}
	return true
}

func deref(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}
