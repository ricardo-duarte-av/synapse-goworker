package presence

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The failure this exists for: the presence writer moves to another instance
// while this worker is running. Nothing tells us -- the old socket simply stops
// accepting -- and every account we serve drifts offline until somebody
// restarts the process. A relay that could not be delivered is the only
// evidence, so it is what triggers the re-read.
func TestAMovedWriterIsAdoptedAndTheRelayRetried(t *testing.T) {
	var delivered atomic.Int64
	moved := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer moved.Close()

	var reads atomic.Int64
	c, err := New(Config{
		// Where the writer was when we started, and is no longer.
		Socket: "/nonexistent/av-edu-worker.sock",
		Secret: "s",
		Resolve: func() (Config, error) {
			reads.Add(1)
			return Config{URL: moved.URL, Secret: "s"}, nil
		},
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	if err := c.SetState(context.Background(), "@a:e.com", "D", StateOnline, true); err != nil {
		t.Fatalf("the relay failed rather than following the writer: %v", err)
	}
	if got := reads.Load(); got != 1 {
		t.Errorf("config was read %d times, want 1", got)
	}
	if got := delivered.Load(); got != 1 {
		t.Errorf("the new writer received %d relays, want 1", got)
	}

	// And the next relay goes straight there, with no further reads. A device
	// that has just been relayed is inside the throttle, so use another.
	if err := c.SetState(context.Background(), "@a:e.com", "D2", StateOnline, true); err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 1 {
		t.Errorf("config was re-read %d times; the adopted writer should just work", got)
	}
	if got := delivered.Load(); got != 2 {
		t.Errorf("the new writer received %d relays, want 2", got)
	}
}

// A writer that is merely DOWN must not turn every relay into a file read, and
// must not be retried into a second failure either.
func TestAWriterThatIsDownIsNotReReadRepeatedly(t *testing.T) {
	var reads atomic.Int64
	c, err := New(Config{
		Socket: "/nonexistent/av-edu-worker.sock",
		Secret: "s",
		Resolve: func() (Config, error) {
			reads.Add(1)
			// The config still says exactly what it said.
			return Config{Socket: "/nonexistent/av-edu-worker.sock", Secret: "s"}, nil
		},
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	for i := range 5 {
		if err := c.SetState(context.Background(), "@a:e.com", fmt.Sprintf("D%d", i), StateOnline, true); err == nil {
			t.Fatal("a relay to a writer that is down was reported as delivered")
		}
	}
	if got := reads.Load(); got != 1 {
		t.Errorf("config was read %d times in one cooldown, want 1", got)
	}

	// Past the cooldown it looks again: the writer may have moved since.
	c.now = func() time.Time { return time.Now().Add(2 * DefaultResolveCooldown) }
	_ = c.SetState(context.Background(), "@a:e.com", "later", StateOnline, true)
	if got := reads.Load(); got != 2 {
		t.Errorf("config was read %d times after the cooldown, want 2", got)
	}
}

// A timeout means the writer is there and slow. Re-reading a file helps
// nothing, and doing it per relay would make an overloaded writer expensive.
func TestOnlyConfigurableFailuresTriggerAReRead(t *testing.T) {
	for _, reason := range []string{"timeout", "client_gone"} {
		c := &Client{resolveCooldown: DefaultResolveCooldown, now: time.Now,
			resolve: func() (Config, error) {
				t.Errorf("%s prompted a re-read of Synapse's config", reason)
				return Config{}, nil
			}}
		if c.reresolve(reason) {
			t.Errorf("%s reported a changed config", reason)
		}
	}
}

// Half a config -- a file caught mid-write, a writer entry that vanished -- is
// refused rather than adopted. Adopting it would lose a working writer.
func TestAnEmptyResolveIsRefused(t *testing.T) {
	c, err := New(Config{Socket: "/nonexistent/s.sock", Secret: "s",
		Resolve: func() (Config, error) { return Config{}, nil }}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if c.reresolve("unreachable") {
		t.Error("an empty config was adopted")
	}
	if got := c.current.Load().addr; got != "/nonexistent/s.sock" {
		t.Errorf("writer address = %q, want the one we started with", got)
	}
}
