package notifier

import (
	"context"
	"testing"
	"time"
)

func TestWakesOnAnInterestingRoom(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, []string{"@me:e"})
	defer h.Close()

	go func() {
		time.Sleep(10 * time.Millisecond)
		n.OnStreamAdvance("events", 1, []string{"!a:e"}, nil)
	}()
	if !h.Wait(context.Background(), time.Second) {
		t.Error("should have woken for a room it declared")
	}
}

func TestDoesNotWakeOnSomebodyElsesRoom(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, []string{"@me:e"})
	defer h.Close()

	n.OnStreamAdvance("events", 1, []string{"!other:e"}, []string{"@other:e"})
	if h.Wait(context.Background(), 20*time.Millisecond) {
		t.Error("woke for a room and user it never declared")
	}
}

func TestWakesOnItsOwnUser(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, []string{"@me:e"})
	defer h.Close()

	n.OnStreamAdvance("to_device", 1, nil, []string{"@me:e"})
	if !h.Wait(context.Background(), time.Second) {
		t.Error("should have woken for its own user")
	}
}

// A row naming nobody wakes everyone. The conservative reading is that we do
// not know who it concerns, and a spurious wakeup costs one recomputation
// while a missed one costs a client its whole timeout.
func TestGlobalAdvanceWakesEveryone(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, []string{"@me:e"})
	defer h.Close()

	n.OnStreamAdvance("presence", 1, nil, nil)
	if !h.Wait(context.Background(), time.Second) {
		t.Error("should have woken for an unattributed advance")
	}
}

// An event arriving between Register and Wait must not be lost: that gap is
// exactly why interest is registered before the answer is computed.
func TestWakeBeforeWaitIsNotLost(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, nil)
	defer h.Close()

	n.OnStreamAdvance("events", 1, []string{"!a:e"}, nil)
	if !h.Wait(context.Background(), 50*time.Millisecond) {
		t.Error("a wakeup that arrived before Wait was lost")
	}
}

func TestTimeoutAndCancellation(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, nil)
	defer h.Close()
	if h.Wait(context.Background(), 10*time.Millisecond) {
		t.Error("should have timed out")
	}

	h2 := n.Register([]string{"!b:e"}, nil)
	defer h2.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h2.Wait(ctx, time.Second) {
		t.Error("a cancelled context should not report a wakeup")
	}
}

func TestCloseUnregisters(t *testing.T) {
	n := New()
	h := n.Register([]string{"!a:e"}, nil)
	if n.Waiters() != 1 {
		t.Fatalf("Waiters = %d, want 1", n.Waiters())
	}
	h.Close()
	if n.Waiters() != 0 {
		t.Errorf("Waiters = %d after Close, want 0", n.Waiters())
	}
	// Closing twice, and waking after close, must not panic.
	h.Close()
	n.OnStreamAdvance("events", 1, []string{"!a:e"}, nil)
}

func TestConcurrentWaitersAndWakes(t *testing.T) {
	n := New()
	done := make(chan bool, 16)
	for i := 0; i < 16; i++ {
		go func() {
			h := n.Register([]string{"!a:e"}, nil)
			defer h.Close()
			done <- h.Wait(context.Background(), 2*time.Second)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 8; i++ {
		go n.OnStreamAdvance("events", int64(i), []string{"!a:e"}, nil)
	}
	woke := 0
	for i := 0; i < 16; i++ {
		if <-done {
			woke++
		}
	}
	if woke != 16 {
		t.Errorf("%d of 16 waiters woke", woke)
	}
}
