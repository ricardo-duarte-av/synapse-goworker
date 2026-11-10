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
