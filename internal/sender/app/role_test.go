package app

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/synapsecfg"
)

func workerNamed(name string, senders ...string) *Worker {
	w := &Worker{cfg: Settings{InstanceName: name}}
	w.role.Store(w.roleFor(&synapsecfg.Config{SenderInstances: senders}))
	return w
}

// Not being named is a normal state: the worker follows the stream and owns
// no destination.
func TestUnlistedInstanceSendsNothing(t *testing.T) {
	w := workerNamed("goworker-1", "federation-sender-1")
	if w.Active() {
		t.Error("an unlisted instance thinks it is a sender")
	}
	if w.shouldHandle("example.org") {
		t.Error("an unlisted instance claimed a destination")
	}
}

func TestSoleSenderOwnsEveryDestination(t *testing.T) {
	w := workerNamed("goworker-1", "goworker-1")
	if !w.Active() {
		t.Fatal("a listed instance is not a sender")
	}
	for _, d := range []string{"a.example", "b.example", "matrix.org"} {
		if !w.shouldHandle(d) {
			t.Errorf("the only sender does not own %s", d)
		}
	}
}

// With two senders the destinations are split by Synapse's hashing, and the
// two halves cover everything exactly once.
func TestShardsSplitDestinations(t *testing.T) {
	a := workerNamed("a", "a", "b")
	b := workerNamed("b", "a", "b")
	var ours, theirs int
	for i := range 200 {
		d := string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".example"
		switch {
		case a.shouldHandle(d) && b.shouldHandle(d):
			t.Fatalf("%s is owned by both senders", d)
		case a.shouldHandle(d):
			ours++
		case b.shouldHandle(d):
			theirs++
		default:
			t.Fatalf("%s is owned by neither sender", d)
		}
	}
	if ours == 0 || theirs == 0 {
		t.Errorf("split %d/%d; both senders should own some destinations", ours, theirs)
	}
}
