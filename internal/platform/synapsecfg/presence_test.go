package synapsecfg

import "testing"

func TestPresenceRatelimit(t *testing.T) {
	for yml, want := range map[string]Ratelimit{
		``:                {Key: "rc_presence.per_user", PerSecond: 0.1, Burst: 1},
		"rc_presence: {}": {Key: "rc_presence.per_user", PerSecond: 0.1, Burst: 1},
		"rc_presence:\n  per_user:\n    per_second: 0.5\n    burst_count: 1": {Key: "rc_presence.per_user", PerSecond: 0.5, Burst: 1},
		"rc_presence:\n  per_user:\n    burst_count: 2.9":                    {Key: "rc_presence.per_user", PerSecond: 0.1, Burst: 2},
	} {
		got, err := presenceRatelimit([]byte(yml))
		if err != nil || got != want {
			t.Errorf("%q: %+v %v", yml, got, err)
		}
	}
}
