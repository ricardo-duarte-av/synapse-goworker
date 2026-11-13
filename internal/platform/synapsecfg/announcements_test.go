package synapsecfg

import "testing"

func TestAnnouncements(t *testing.T) {
	for name, tc := range map[string]struct {
		yaml       string
		configured bool
		manager    string
	}{
		"unset": {"", false, "@_server_announcements:x"},
		"no rooms": {"experimental_features:\n  server_announcements: {}\n",
			false, "@_server_announcements:x"},
		"empty rooms": {"experimental_features:\n  server_announcements:\n    rooms: []\n",
			false, "@_server_announcements:x"},
		"a room": {"experimental_features:\n  server_announcements:\n    rooms:\n      - id: news\n        room_name: News\n",
			true, "@_server_announcements:x"},
		"own manager": {"experimental_features:\n  server_announcements:\n    room_management_localpart: herald\n",
			false, "@herald:x"},
	} {
		t.Run(name, func(t *testing.T) {
			a := mustParse(t, "server_name: x\n"+tc.yaml).Announcements
			cfg := mustParse(t, "server_name: x\n"+tc.yaml)
			if cfg.Membership.AnnouncementManagerMXID != a.ManagerMXID {
				t.Errorf("Membership.AnnouncementManagerMXID %q", cfg.Membership.AnnouncementManagerMXID)
			}
			if a.Configured() != tc.configured || a.ManagerMXID != tc.manager {
				t.Errorf("got %+v, want configured=%v manager %s", a, tc.configured, tc.manager)
			}
		})
	}
}

// ServerAnnouncementsConfig.read_config refuses these, so Synapse would not
// start with them.
func TestAnnouncementsRefused(t *testing.T) {
	for name, y := range map[string]string{
		"section null":  "experimental_features:\n  server_announcements:\n",
		"section list":  "experimental_features:\n  server_announcements: []\n",
		"rooms mapping": "experimental_features:\n  server_announcements:\n    rooms: {}\n",
		"rooms null":    "experimental_features:\n  server_announcements:\n    rooms:\n",
	} {
		if _, err := parse([]byte("server_name: x\n"+y), "hs.yaml"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The rooms' ids and exclusions, which sync delivers by.
func TestAnnouncementRoomList(t *testing.T) {
	a := mustParse(t, `server_name: x
experimental_features:
  server_announcements:
    rooms:
      - id: news
        room_name: News
        excluded_user_ids: ["@bot:x", "@other:x"]
      - id: maint
        room_name: Maintenance
`).Announcements
	if got := a.ConfigIDs(); len(got) != 2 || got[0] != "news" || got[1] != "maint" {
		t.Fatalf("ConfigIDs %v", got)
	}
	if ex := a.ExcludedConfigIDs("@bot:x"); len(ex) != 1 || !ex["news"] {
		t.Errorf("excluded for @bot:x: %v", ex)
	}
	if ex := a.ExcludedConfigIDs("@someone:x"); len(ex) != 0 {
		t.Errorf("excluded for @someone:x: %v", ex)
	}
}
