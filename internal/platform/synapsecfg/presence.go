package synapsecfg

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type presenceRaw struct {
	RcPresence yaml.Node `yaml:"rc_presence"`
}

// presenceRatelimit is rc_presence.per_user (config/ratelimiting.py: 0.1/s,
// burst 1), parsed as RatelimitSettings.parse walks a dotted key: a level
// that is absent gives the defaults.
func presenceRatelimit(b []byte) (Ratelimit, error) {
	var r presenceRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return Ratelimit{}, fmt.Errorf("synapsecfg: rc_presence: %w", err)
	}
	perUser, _ := lastWins(&r.RcPresence)["per_user"].(map[string]any)
	return ratelimit("rc_presence.per_user", perUser, 0.1, 1)
}
