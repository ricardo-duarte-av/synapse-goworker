package synapsecfg

import (
	"testing"
	"time"
)

func TestWakeupMaxDelay(t *testing.T) {
	for _, c := range []struct {
		yaml string
		want time.Duration
		err  bool
	}{
		{"server_name: a", 20 * time.Millisecond, false},
		{"federation_rr_transactions_per_room_per_second: 10", 100 * time.Millisecond, false},
		{"federation_rr_transactions_per_room_per_second: 0.5", 2 * time.Second, false},
		{"federation_rr_transactions_per_room_per_second: 0", 0, true},
		{"federation_rr_transactions_per_room_per_second: fast", 0, true},
	} {
		got, err := wakeupMaxDelay([]byte(c.yaml))
		if (err != nil) != c.err || got != c.want {
			t.Errorf("%q: %v, %v", c.yaml, got, err)
		}
	}
}
