package synapsecfg

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type toDeviceRaw struct {
	RcKeyRequests yaml.Node `yaml:"rc_key_requests"`
}

// keyRequests is rc_key_requests (config/ratelimiting.py: 20/s, burst 100),
// the ratelimiter of m.room_key_request to-device messages.
func keyRequests(b []byte) (Ratelimit, error) {
	var r toDeviceRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return Ratelimit{}, fmt.Errorf("synapsecfg: rc_key_requests: %w", err)
	}
	return ratelimit("rc_key_requests", lastWins(&r.RcKeyRequests), 20, 100)
}
