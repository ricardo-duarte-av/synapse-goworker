package queue

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/txn"
)

// _DestinationWakeupQueue: destinations woken in the order queued (a second
// add keeps the first place), each followed by a pause of
// min(max_delay, 30 s / queued), and one with nothing new skipped without a
// pause.
func TestWakeLaterStaggers(t *testing.T) {
	s := &recordingSink{}
	m := testManager(t, s, 10)
	m.wakeupMaxDelay = time.Hour
	var pauses []time.Duration
	// Each pause waits for the transaction just woken, so the order sent is
	// the order woken.
	m.sleep = func(_ context.Context, d time.Duration) bool {
		waitFor(t, "the woken destination's transaction", func() bool { return len(s.sent()) == len(pauses)+1 })
		pauses = append(pauses, d)
		return true
	}

	// Queued while a drain is (as far as WakeLater knows) already running,
	// so this test's drain sees them all.
	m.wakeups.processing = true
	for _, name := range []string{"a.example", "idle.example", "b.example", "c.example"} {
		d := m.Get(name)
		if name != "idle.example" {
			d.EnqueueEDU(txn.EDU{Type: "m.presence"})
		}
		m.WakeLater(d)
	}
	m.WakeLater(m.Get("c.example")) // keeps its place
	m.WakeLater(m.Get("a.example"))
	m.drainWakeups(context.Background())

	waitFor(t, "three transactions", func() bool { return len(s.sent()) == 3 })
	var order []string
	for _, r := range s.sent() {
		order = append(order, r.Destination)
	}
	if !slices.Equal(order, []string{"a.example", "b.example", "c.example"}) {
		t.Errorf("woken in order %v", order)
	}
	// 4 queued: 7.5 s each; the idle one costs none.
	want := []time.Duration{7500 * time.Millisecond, 7500 * time.Millisecond, 7500 * time.Millisecond}
	if !slices.Equal(pauses, want) {
		t.Errorf("pauses %v, want %v", pauses, want)
	}
	if m.wakeups.processing || m.wakeups.order.Len() != 0 {
		t.Error("drain did not finish")
	}
}

// The default pause is max_delay when few are queued, and a drain ends
// cleanly so the next WakeLater starts another.
func TestWakeLaterDefaultPauseAndRestart(t *testing.T) {
	s := &recordingSink{}
	m := testManager(t, s, 10)
	var pauses []time.Duration
	done := make(chan struct{}, 4)
	m.sleep = func(_ context.Context, d time.Duration) bool {
		pauses = append(pauses, d)
		done <- struct{}{}
		return true
	}

	d := m.Get("a.example")
	d.EnqueueEDU(txn.EDU{Type: "m.presence"})
	m.WakeLater(d)
	<-done
	waitFor(t, "the drain to end", func() bool {
		m.wakeups.mu.Lock()
		defer m.wakeups.mu.Unlock()
		return !m.wakeups.processing
	})
	e := m.Get("b.example")
	e.EnqueueEDU(txn.EDU{Type: "m.presence"})
	m.WakeLater(e)
	<-done
	waitFor(t, "both sent", func() bool { return len(s.sent()) == 2 })
	if !slices.Equal(pauses, []time.Duration{20 * time.Millisecond, 20 * time.Millisecond}) {
		t.Errorf("pauses %v", pauses)
	}
}
