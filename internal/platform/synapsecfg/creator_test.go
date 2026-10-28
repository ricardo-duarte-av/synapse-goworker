package synapsecfg

import (
	"net/netip"
	"testing"
)

func TestMembershipSettings(t *testing.T) {
	m, err := membership([]byte(`
server_notices:
  system_mxid_localpart: notices
rc_invites:
  per_room: {per_second: 100, burst_count: 100}
  per_user:
    per_second: 0.5
rc_joins_per_room: {burst_count: 3.9}
allow_per_room_profiles: false
`), "example.org")
	if err != nil {
		t.Fatal(err)
	}
	want := Membership{
		ServerNoticesMXID:          "@notices:example.org",
		IncludeProfileDataOnInvite: true,
		JoinsLocal:                 Ratelimit{"rc_joins.local", 0.1, 10},
		JoinsPerRoom:               Ratelimit{"rc_joins_per_room", 1, 3},
		InvitesPerRoom:             Ratelimit{"rc_invites.per_room", 100, 100},
		InvitesPerUser:             Ratelimit{"rc_invites.per_user", 0.5, 5},
		InvitesPerIssuer:           Ratelimit{"rc_invites.per_issuer", 0.3, 10},
	}
	if m != want {
		t.Errorf("got %+v\nwant %+v", m, want)
	}

	m, err = membership([]byte("modules:\n  - module: x\nuser_consent:\n  block_events_error: no\nhs_disabled: true\nmax_avatar_size: 10M\n"), "example.org")
	if err != nil {
		t.Fatal(err)
	}
	if !m.Modules || !m.ConsentRequired || !m.AuthBlocking || !m.AllowPerRoomProfiles || m.ServerNoticesMXID != "" || !m.AvatarLimits {
		t.Errorf("got %+v", m)
	}
}

func TestFederationIPRanges(t *testing.T) {
	r, err := federationIPRanges(nil)
	if err != nil {
		t.Fatal(err)
	}
	for ip, blocked := range map[string]bool{"10.1.2.3": true, "::ffff:192.168.1.1": true, "0.0.0.0": true, "8.8.8.8": false, "2a00::1": false} {
		if got := r.Blocked(netip.MustParseAddr(ip)); got != blocked {
			t.Errorf("%s: blocked %v", ip, got)
		}
	}
	r, _ = federationIPRanges([]byte("ip_range_whitelist: [10.1.0.0/16]\n"))
	if r.Blocked(netip.MustParseAddr("10.1.2.3")) || !r.Blocked(netip.MustParseAddr("10.2.0.1")) {
		t.Error("ip_range_whitelist")
	}
	r, _ = federationIPRanges([]byte("federation_ip_range_blacklist: [8.8.8.0/24]\nip_range_whitelist: [8.8.8.8]\n"))
	if !r.Blocked(netip.MustParseAddr("8.8.8.8")) || r.Blocked(netip.MustParseAddr("10.0.0.1")) {
		t.Error("federation_ip_range_blacklist")
	}
}

func TestDelayedSettings(t *testing.T) {
	d, err := delayedSettings([]byte("max_event_delay_duration: 24h\nrc_message: {per_second: 100, burst_count: 100}\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.MaxDelayMS != 86400000 || d.PerUser != 100 || !d.Enabled() || d.RcMessage.PerSecond != 100 || d.RcMessage.Burst != 100 {
		t.Errorf("%+v", d)
	}
	d, _ = delayedSettings(nil, map[string]any{"msc4140_max_delayed_events_per_user": 0})
	if d.Enabled() || d.RcMessage.PerSecond != 0.2 || d.RcMessage.Burst != 10 {
		t.Errorf("%+v", d)
	}
	d, _ = delayedSettings([]byte("max_event_delay_duration: 5000\nrc_messages_per_second: 3\n"), nil)
	if d.MaxDelayMS != 5000 || d.RcMessage.PerSecond != 3 || d.RcMessage.Burst != 10 {
		t.Errorf("%+v", d)
	}
}

func TestProfileWriterAndRedactionPeriod(t *testing.T) {
	cfg := mustParse(t, "server_name: example.org\nredaction_allowed_period: 7d\ninclude_profile_updates_in_sync: true\nstream_writers:\n  events: [p1, p2]\ninstance_map:\n  p1: {host: a, port: 1}\n  p2: {host: b, port: 2}\n")
	if got := cfg.Writers.ProfileWriter(); got != "p1" {
		t.Errorf("profile writer %q, want the first events writer", got)
	}
	if !cfg.RedactionAllowedPeriod || !cfg.IncludeProfileUpdatesInSync {
		t.Error("redaction_allowed_period and include_profile_updates_in_sync are set")
	}
	cfg = mustParse(t, "server_name: example.org\n")
	if got := cfg.Writers.ProfileWriter(); got != MainProcessInstanceName || cfg.RedactionAllowedPeriod || cfg.IncludeProfileUpdatesInSync {
		t.Errorf("defaults: %q, %v, %v", got, cfg.RedactionAllowedPeriod, cfg.IncludeProfileUpdatesInSync)
	}
}
