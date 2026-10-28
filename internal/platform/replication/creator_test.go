package replication

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// writer is a fake events writer listening on a unix socket, as the Synapse
// workers here do.
type eventsWriter struct {
	mu       sync.Mutex
	paths    []string
	auth     []string
	bodies   []string
	statuses []int // returned in order; 200 once exhausted
}

func (w *eventsWriter) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	w.mu.Lock()
	w.paths = append(w.paths, r.URL.EscapedPath())
	w.auth = append(w.auth, r.Header.Get("Authorization"))
	w.bodies = append(w.bodies, string(b))
	status := http.StatusOK
	if len(w.statuses) > 0 {
		status, w.statuses = w.statuses[0], w.statuses[1:]
	}
	w.mu.Unlock()
	rw.WriteHeader(status)
	if status == http.StatusOK {
		_, _ = rw.Write([]byte(`{"stream_id":12345,"event_id":"$persisted"}`))
		return
	}
	_, _ = rw.Write([]byte(`{"errcode":"M_LIMIT_EXCEEDED","error":"Too Many Requests","retry_after_ms":500}`))
}

func creatorClient(sock, secret string) *Client {
	c := NewClient(&synapsecfg.Config{
		ReplicationSecret: secret,
		InstanceMap: map[string]synapsecfg.Instance{
			"av-event-persister-1": {Name: "av-event-persister-1", Socket: sock},
			"master":               {Name: "master", Socket: sock},
		},
	}, Creator)
	c.sleep = func(context.Context, time.Duration) error { return nil }
	return c
}

var entry = []json.RawMessage{json.RawMessage(`{"event":{"type":"m.room.message"}}`)}

func send(c *Client, instance string) error {
	_, _, err := c.SendClientEvents(context.Background(), instance, entry)
	return err
}

func TestSendClientEventsWireFormat(t *testing.T) {
	w := &eventsWriter{}
	c := creatorClient(serve(t, w), "s3cret")

	stream, id, err := c.SendClientEvents(context.Background(), "av-event-persister-1", entry)
	if err != nil {
		t.Fatal(err)
	}
	if stream != 12345 || id != "$persisted" {
		t.Errorf("answer = %d %q", stream, id)
	}

	// POST /_synapse/replication/send_events/<txn_id>: no path args.
	parts := strings.Split(w.paths[0], "/")
	if len(parts) != 5 || parts[1] != "_synapse" || parts[2] != "replication" || parts[3] != "send_events" {
		t.Fatalf("path = %s", w.paths[0])
	}
	if len(parts[4]) != 10 {
		t.Errorf("txn_id = %q, want Synapse's random_string(10)", parts[4])
	}
	if w.auth[0] != "Bearer s3cret" {
		t.Errorf("Authorization = %q", w.auth[0])
	}
	if w.bodies[0] != `{"events":[{"event":{"type":"m.room.message"}}]}` {
		t.Errorf("body = %s", w.bodies[0])
	}
}

// The writer's refusal reaches the caller with its status and body, to be
// relayed to the client.
func TestWriterErrorIsReturnedWhole(t *testing.T) {
	w := &eventsWriter{statuses: []int{http.StatusTooManyRequests}}
	c := creatorClient(serve(t, w), "")
	err := send(c, "av-event-persister-1")
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 429 || !strings.Contains(he.Body, "M_LIMIT_EXCEEDED") {
		t.Fatalf("err = %v", err)
	}
	if len(w.paths) != 1 {
		t.Errorf("%d attempts at a 429, want 1", len(w.paths))
	}
}

func TestConflictIsPartialState(t *testing.T) {
	w := &eventsWriter{statuses: []int{http.StatusConflict}}
	c := creatorClient(serve(t, w), "")
	if err := send(c, "av-event-persister-1"); !errors.Is(err, ErrPartialStateConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestCreatorNoSecretNoAuthorization(t *testing.T) {
	w := &eventsWriter{}
	c := creatorClient(serve(t, w), "")
	if err := send(c, "master"); err != nil {
		t.Fatal(err)
	}
	if w.auth[0] != "" {
		t.Errorf("sent Authorization %q without a configured secret", w.auth[0])
	}
}

// "main" is how the yaml names the main process; it must reach "master".
func TestCreatorMainIsMaster(t *testing.T) {
	w := &eventsWriter{}
	c := creatorClient(serve(t, w), "")
	if !c.Has("main") {
		t.Fatal(`Has("main") = false`)
	}
	if err := send(c, "main"); err != nil {
		t.Fatal(err)
	}
}

// A 504 is retried with the SAME txn_id, so the writer's ResponseCache makes
// the retry a no-op if the first attempt did land.
func TestCreatorTimeoutIsRetriedWithTheSameTxnID(t *testing.T) {
	w := &eventsWriter{statuses: []int{http.StatusGatewayTimeout, http.StatusGatewayTimeout}}
	c := creatorClient(serve(t, w), "")
	if err := send(c, "master"); err != nil {
		t.Fatal(err)
	}
	if len(w.paths) != 3 {
		t.Fatalf("%d attempts, want 3", len(w.paths))
	}
	if w.paths[0] != w.paths[1] || w.paths[1] != w.paths[2] {
		t.Errorf("retries changed the txn_id: %v", w.paths)
	}
}

func TestCreatorServerErrorIsNotRetried(t *testing.T) {
	w := &eventsWriter{statuses: []int{http.StatusInternalServerError}}
	c := creatorClient(serve(t, w), "")
	err := send(c, "master")
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 500 {
		t.Fatalf("err = %v, want HTTP 500", err)
	}
	if len(w.paths) != 1 {
		t.Errorf("%d attempts at a 500, want 1", len(w.paths))
	}
}

func TestCreatorConnectErrorsAreRetriedThenGiveUp(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nobody-home.sock")
	c := creatorClient(missing, "")
	var slept []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }

	if err := send(c, "master"); err == nil {
		t.Fatal("sending to a socket nobody listens on succeeded")
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	if len(slept) != len(want) {
		t.Fatalf("backoff = %v, want %v", slept, want)
	}
	for i := range want {
		if slept[i] != want[i] {
			t.Fatalf("backoff = %v, want %v", slept, want)
		}
	}
}

func TestCreatorUnknownInstance(t *testing.T) {
	c := creatorClient("/nonexistent", "")
	if err := send(c, "nobody"); err == nil {
		t.Error("forwarded to an instance that is not in instance_map")
	}
}
