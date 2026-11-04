package replserver

import (
	"context"
	"testing"
	"time"
)

func TestTracker(t *testing.T) {
	tr := NewTracker()
	tr.timeout = 200 * time.Millisecond
	ctx := context.Background()

	// Nothing seen from this writer: no wait.
	start := time.Now()
	tr.WaitForStreamPosition(ctx, "master", "caches", 10)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("waited on a writer never seen")
	}

	tr.Observe("POSITION caches master 3 5")
	start = time.Now()
	tr.WaitForStreamPosition(ctx, "master", "caches", 5)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("waited for a position already seen")
	}

	done := make(chan struct{})
	go func() { tr.WaitForStreamPosition(ctx, "master", "caches", 8); close(done) }()
	time.Sleep(20 * time.Millisecond)
	tr.Observe("RDATA caches master batch [\"a\",[\"b\"],1]") // no token yet
	tr.Observe("RDATA caches other 9 [\"a\",[\"b\"],1]")      // another writer
	select {
	case <-done:
		t.Fatal("released by a batch line or another writer")
	case <-time.After(20 * time.Millisecond):
	}
	tr.Observe("RDATA caches master 8 [\"a\",[\"b\"],1]")
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("not released at the position")
	}

	// A position never reached times out and goes on.
	start = time.Now()
	tr.WaitForStreamPosition(ctx, "master", "caches", 100)
	if d := time.Since(start); d < 150*time.Millisecond || d > time.Second {
		t.Fatalf("timeout after %v", d)
	}
	if len(tr.waiters) != 0 {
		t.Fatalf("waiters left behind: %v", tr.waiters)
	}
}
