package synapsecfg

import "testing"

func TestEventCache(t *testing.T) {
	for name, c := range map[string]struct {
		yaml string
		want EventCache
	}{
		"defaults":  {"", EventCache{Size: 10240, ExpiryMS: 30 * 60000}},
		"av":        {"event_cache_size: 100K\ncaches:\n  global_factor: 0.5\n  expire_caches: true\n", EventCache{Size: 102400, ExpiryMS: 30 * 60000}},
		"int":       {"event_cache_size: 5000\ncaches:\n  cache_entry_ttl: 2h\n", EventCache{Size: 5000, ExpiryMS: 2 * 3600000}},
		"no expiry": {"caches:\n  expire_caches: false\n", EventCache{Size: 10240}},
	} {
		got := mustParse(t, "server_name: x\n"+c.yaml).EventCache
		if got != c.want {
			t.Errorf("%s: %+v, want %+v", name, got, c.want)
		}
	}
}
