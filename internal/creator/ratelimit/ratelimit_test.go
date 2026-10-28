package ratelimit

import (
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// The cases of tests/api/test_ratelimiting.py's test_allowed_via_can_do_action
// and test_record_action_which_doesnt_fill_bucket.
func TestLeakyBucket(t *testing.T) {
	now := time.Unix(0, 0)
	l := New(synapsecfg.Ratelimit{PerSecond: 0.1, Burst: 1}, func() time.Time { return now })

	if ok, retry := l.CanDo("k", true); !ok || retry != 10000 {
		t.Fatalf("first: %v %d", ok, retry)
	}
	now = time.Unix(5, 0)
	if ok, retry := l.CanDo("k", true); ok || retry != 5000 {
		t.Fatalf("at 5s: %v %d, want refused with 5000", ok, retry)
	}
	now = time.Unix(10, 0)
	if ok, _ := l.CanDo("k", true); !ok {
		t.Fatal("at 10s: refused")
	}

	// update=false checks without counting.
	l2 := New(synapsecfg.Ratelimit{PerSecond: 1, Burst: 2}, func() time.Time { return now })
	for range 5 {
		if ok, _ := l2.CanDo("r", false); !ok {
			t.Fatal("update=false counted")
		}
	}
	l2.Record("r", now)
	l2.Record("r", now)
	if ok, _ := l2.CanDo("r", false); ok {
		t.Fatal("two recorded actions should fill a bucket of 2")
	}
	now = now.Add(time.Second)
	if ok, _ := l2.CanDo("r", false); !ok {
		t.Fatal("one second later one should have leaked")
	}
}
