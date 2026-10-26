package presence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type capture struct {
	mu    sync.Mutex
	calls []map[string]any
	paths []string
	auth  []string
	code  int
}

func (c *capture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	c.mu.Lock()
	c.calls = append(c.calls, body)
	c.paths = append(c.paths, r.URL.Path)
	c.auth = append(c.auth, r.Header.Get("Authorization"))
	code := c.code
	c.mu.Unlock()
	if code == 0 {
		code = http.StatusOK
	}
	w.WriteHeader(code)
	_, _ = w.Write([]byte(`{}`))
}

func (c *capture) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func newTestClient(t *testing.T) (*Client, *capture, *time.Time) {
	t.Helper()
	cap := &capture{}
	srv := httptest.NewServer(cap)
	t.Cleanup(srv.Close)

	c, err := New(Config{URL: srv.URL, Secret: "s3cret", RelayInterval: 25 * time.Second},
		zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Now()
	c.now = func() time.Time { return clock }
	return c, cap, &clock
}

// The wire format is Synapse's, verified against the live writer on
// 2026-09-03: @test went offline -> online on exactly this body.
func TestSetStateSendsSynapsesShape(t *testing.T) {
	c, cap, _ := newTestClient(t)

	if err := c.SetState(context.Background(), "@a:e.com", "DEV1", StateOnline, true); err != nil {
		t.Fatal(err)
	}
	if cap.n() != 1 {
		t.Fatalf("made %d calls, want 1", cap.n())
	}
	if got, want := cap.paths[0], "/_synapse/replication/presence_set_state/@a:e.com"; got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
	if got := cap.auth[0]; got != "Bearer s3cret" {
		t.Errorf("auth = %q", got)
	}
	body := cap.calls[0]
	if body["device_id"] != "DEV1" {
		t.Errorf("device_id = %v", body["device_id"])
	}
	if body["is_sync"] != true {
		t.Errorf("is_sync = %v, want true -- without it the writer does not refresh last_user_sync_ts", body["is_sync"])
	}
	if body["force_notify"] != false {
		t.Errorf("force_notify = %v", body["force_notify"])
	}
	st, _ := body["state"].(map[string]any)
	if st["presence"] != "online" {
		t.Errorf("state = %v", body["state"])
	}
}

// A device with no ID must send JSON null, not "". Synapse keys its per-device
// state on device_id and treats None as its own key.
func TestSetStateSendsNullForAnAbsentDevice(t *testing.T) {
	c, cap, _ := newTestClient(t)
	if err := c.SetState(context.Background(), "@a:e.com", "", StateOnline, true); err != nil {
		t.Fatal(err)
	}
	if got, ok := cap.calls[0]["device_id"]; !ok || got != nil {
		t.Errorf("device_id = %#v, want JSON null", got)
	}
}

// A client syncs in a loop; the writer's timers are coarser. Relaying every
// sync would be one HTTP call per sync per device for no benefit.
func TestUnchangedSyncStateIsThrottled(t *testing.T) {
	c, cap, clock := newTestClient(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true); err != nil {
			t.Fatal(err)
		}
		*clock = clock.Add(time.Second)
	}
	// 20 seconds of syncing, 25s interval: one call.
	if cap.n() != 1 {
		t.Fatalf("made %d calls in 20s of syncing, want 1", cap.n())
	}

	*clock = clock.Add(10 * time.Second) // now past the interval
	if err := c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true); err != nil {
		t.Fatal(err)
	}
	if cap.n() != 2 {
		t.Errorf("made %d calls after the interval elapsed, want 2", cap.n())
	}
}

// The throttle is per device: one device going quiet must not silence another.
func TestThrottleIsPerDevice(t *testing.T) {
	c, cap, _ := newTestClient(t)
	ctx := context.Background()
	for _, dev := range []string{"DEV1", "DEV2", "DEV3"} {
		if err := c.SetState(ctx, "@a:e.com", dev, StateOnline, true); err != nil {
			t.Fatal(err)
		}
	}
	if cap.n() != 3 {
		t.Errorf("made %d calls for 3 devices, want 3", cap.n())
	}
}

// A state CHANGE is the whole point of the field and must never be throttled.
func TestAChangedStateIsNeverThrottled(t *testing.T) {
	c, cap, _ := newTestClient(t)
	ctx := context.Background()

	_ = c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true)
	_ = c.SetState(ctx, "@a:e.com", "DEV1", StateUnavailable, true)
	_ = c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true)

	if cap.n() != 3 {
		t.Errorf("made %d calls for three different states, want 3", cap.n())
	}
}

// An explicit update does not refresh the writer's last_user_sync_ts, so it
// must not leave a throttle entry behind that suppresses the next sync-driven
// one -- which does.
func TestAnExplicitUpdateClearsTheThrottle(t *testing.T) {
	c, cap, clock := newTestClient(t)
	ctx := context.Background()

	// The ordering is the point. A sync-driven update lands first and arms the
	// throttle; the explicit one that follows must disarm it, so the next
	// sync-driven update still goes through even though it falls inside the
	// interval and carries the same state. Assert it with an explicit call
	// LAST and the sync-driven pair around it, or the delete can be removed
	// without any test noticing.
	_ = c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true) // arms the throttle
	*clock = clock.Add(time.Second)
	_ = c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, false) // must disarm it
	*clock = clock.Add(time.Second)
	_ = c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true) // well inside 25s

	if cap.n() != 3 {
		t.Errorf("made %d calls, want 3: an explicit update does not refresh "+
			"last_user_sync_ts, so it must not leave the throttle armed", cap.n())
	}
	if cap.calls[1]["is_sync"] != false {
		t.Errorf("second call is_sync = %v, want false", cap.calls[1]["is_sync"])
	}
}

// A failed relay must not be remembered as sent. Remembering it would suppress
// every retry for a full interval, and the user would look offline for 25s
// because of one transient failure.
func TestAFailedRelayIsNotRemembered(t *testing.T) {
	c, cap, _ := newTestClient(t)
	ctx := context.Background()

	cap.mu.Lock()
	cap.code = http.StatusInternalServerError
	cap.mu.Unlock()

	if err := c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true); err == nil {
		t.Fatal("a 500 from the writer was reported as success")
	}
	if c.Tracked() != 0 {
		t.Error("a failed relay left a throttle entry behind")
	}

	cap.mu.Lock()
	cap.code = http.StatusOK
	cap.mu.Unlock()

	if err := c.SetState(ctx, "@a:e.com", "DEV1", StateOnline, true); err != nil {
		t.Fatal(err)
	}
	if cap.n() != 2 {
		t.Errorf("made %d calls, want 2: the retry was suppressed", cap.n())
	}
}

// Sending a state the writer rejects turns a sync into a 500, so it is caught
// here instead. `busy` is deliberately absent: it needs MSC3026.
func TestInvalidStatesAreRefusedLocally(t *testing.T) {
	c, cap, _ := newTestClient(t)
	for _, s := range []string{"busy", "", "ONLINE", "away"} {
		if err := c.SetState(context.Background(), "@a:e.com", "D", s, true); err == nil {
			t.Errorf("accepted invalid state %q", s)
		}
	}
	if cap.n() != 0 {
		t.Errorf("made %d calls for invalid states, want 0", cap.n())
	}
}

// Misconfiguration must stop the worker starting, not surface later as syncs
// failing against a writer that answers 500 to an unauthenticated call.
func TestNewRefusesBadConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no target", Config{Secret: "s"}, "socket or url"},
		{"both targets", Config{Socket: "/s", URL: "http://x", Secret: "s"}, "exactly one"},
		{"no secret", Config{Socket: "/s"}, "worker_replication_secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg, zerolog.Nop())
			if err == nil {
				t.Fatal("accepted a config that cannot work")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// The interval has to stay comfortably inside the timer it refreshes, or users
// flap offline between relays. Synapse takes 5/6 of the tighter of two timers.
func TestRelayIntervalMatchesSynapsesDerivation(t *testing.T) {
	// The deployment's defaults: sync_online_timeout 30s, granularity 60s.
	if got := DeriveRelayInterval(30*time.Second, 60*time.Second); got != 25*time.Second {
		t.Errorf("= %v, want 25s", got)
	}
	// The tighter of the two wins, whichever it is.
	if got := DeriveRelayInterval(60*time.Second, 12*time.Second); got != 10*time.Second {
		t.Errorf("= %v, want 10s", got)
	}
	if got := DeriveRelayInterval(0, 0); got != DefaultRelayInterval {
		t.Errorf("= %v, want the default for a nonsense config", got)
	}
}

// A nil client is the disabled case and must be inert, not a panic on the sync
// path.
func TestNilClientIsInert(t *testing.T) {
	var c *Client
	if err := c.SetState(context.Background(), "@a:e.com", "D", StateOnline, true); err != nil {
		t.Errorf("nil client returned %v", err)
	}
	c.Forget("@a:e.com", "D")
	if c.Tracked() != 0 {
		t.Error("nil client reported tracked devices")
	}
}
