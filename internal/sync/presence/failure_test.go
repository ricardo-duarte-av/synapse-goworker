package presence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
)

// The reason label is the whole value of this metric: a moved writer, a
// rotated secret and an overloaded writer need three different fixes and look
// identical in a flat count. Each case below is produced for real rather than
// by constructing the error by hand, so a change in how net/http wraps things
// cannot silently reclassify them.
func TestFailureReasons(t *testing.T) {
	t.Run("a writer that answers non-200 is refused, not unreachable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				// What Synapse returns for a bad or missing bearer secret.
				w.WriteHeader(http.StatusInternalServerError)
			}))
		defer srv.Close()

		c, err := New(Config{URL: srv.URL, Secret: "wrong"}, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		err = c.SetState(context.Background(), "@a:e.com", "D", StateOnline, true)
		if err == nil {
			t.Fatal("a 500 was reported as success")
		}
		if got := failureReason(err); got != metrics.PresenceRefused {
			t.Errorf("reason = %q, want %q -- a reachable writer saying no is not "+
				"the same problem as one that is gone", got, metrics.PresenceRefused)
		}
	})

	t.Run("a socket that does not exist is unreachable", func(t *testing.T) {
		c, err := New(Config{Socket: "/nonexistent/av-edu-worker.sock", Secret: "s"},
			zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		err = c.SetState(context.Background(), "@a:e.com", "D", StateOnline, true)
		if err == nil {
			t.Fatal("dialling a missing socket was reported as success")
		}
		if got := failureReason(err); got != metrics.PresenceUnreachable {
			t.Errorf("reason = %q, want %q", got, metrics.PresenceUnreachable)
		}
	})

	t.Run("a writer that never answers is a timeout", func(t *testing.T) {
		block := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { <-block }))
		defer func() { close(block); srv.Close() }()

		c, err := New(Config{URL: srv.URL, Secret: "s", Timeout: 50 * time.Millisecond},
			zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		err = c.SetState(context.Background(), "@a:e.com", "D", StateOnline, true)
		if err == nil {
			t.Fatal("a hung writer was reported as success")
		}
		if got := failureReason(err); got != metrics.PresenceTimeout {
			t.Errorf("reason = %q, want %q -- an overloaded writer is not a "+
				"misconfigured one", got, metrics.PresenceTimeout)
		}
	})

	t.Run("a caller that hangs up is not counted against the writer", func(t *testing.T) {
		block := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { <-block }))
		defer func() { close(block); srv.Close() }()

		c, err := New(Config{URL: srv.URL, Secret: "s"}, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		err = c.SetState(ctx, "@a:e.com", "D", StateOnline, true)
		if err == nil {
			t.Fatal("a cancelled relay was reported as success")
		}
		if got := failureReason(err); got != metrics.PresenceClientGone {
			t.Errorf("reason = %q, want %q -- the syncing client hanging up is "+
				"neither our fault nor the writer's", got, metrics.PresenceClientGone)
		}
	})
}
