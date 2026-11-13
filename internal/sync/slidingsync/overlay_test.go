package slidingsync

import (
	"reflect"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// MutableOverlayMapping's iteration: entries set after the snapshot first,
// in first-set order (re-set after a delete goes last), then the snapshot's
// rows in their order, bar deleted and overridden ones.
func TestRoomOverlayOrder(t *testing.T) {
	under := map[string]store.SlidingRoom{"a": {}, "b": {}, "c": {}, "d": {}}
	o := newRoomOverlay(under, []string{"a", "b", "c", "d"})
	o.set("x", store.SlidingRoom{})
	o.set("c", store.SlidingRoom{Membership: "leave"}) // overrides a row
	o.pop("b")
	o.set("y", store.SlidingRoom{})
	o.pop("x")
	o.set("x", store.SlidingRoom{}) // back, at the end of the set ones
	o.pop("nope")
	if got, want := o.keys(), []string{"c", "y", "x", "a", "d"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keys %v, want %v", got, want)
	}
	if r, _ := o.get("c"); r.Membership != "leave" {
		t.Errorf("override lost")
	}
	if _, ok := o.get("b"); ok {
		t.Errorf("deleted entry visible")
	}
}
