package synapsecfg

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type wakeupRaw struct {
	RRPerRoom any `yaml:"federation_rr_transactions_per_room_per_second"`
}

// DefaultWakeupMaxDelay is 1 / federation_rr_transactions_per_room_per_second
// at its default of 50.
const DefaultWakeupMaxDelay = 20 * time.Millisecond

// wakeupMaxDelay is the federation sender's _DestinationWakeupQueue
// max_delay_s: 1.0 / federation_rr_transactions_per_room_per_second
// (config/ratelimiting.py, default 50; federation/sender/__init__.py). A
// value Python cannot divide by is refused, as Synapse fails to start on it.
func wakeupMaxDelay(b []byte) (time.Duration, error) {
	var r wakeupRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return 0, fmt.Errorf("synapsecfg: federation_rr_transactions_per_room_per_second: %w", err)
	}
	var v float64
	switch x := r.RRPerRoom.(type) {
	case nil:
		return DefaultWakeupMaxDelay, nil
	case int:
		v = float64(x)
	case float64:
		v = x
	default:
		return 0, fmt.Errorf("synapsecfg: federation_rr_transactions_per_room_per_second: %v is not a number", x)
	}
	if v <= 0 {
		return 0, fmt.Errorf("synapsecfg: federation_rr_transactions_per_room_per_second: %v", v)
	}
	return time.Duration(float64(time.Second) / v), nil
}
