package typing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

type fakeAuth struct{ v clientauth.Verdict }

func (f fakeAuth) Authenticate(context.Context, *http.Request) (clientauth.Verdict, error) {
	return f.v, nil
}

type fakeStore struct {
	membership string
	banned     bool
	hostJoined bool
	hosts      map[string]struct{}
}

func (f *fakeStore) LocalMembership(context.Context, string, string) (string, error) {
	return f.membership, nil
}
func (f *fakeStore) ShadowBanned(context.Context, string) (bool, error) { return f.banned, nil }
func (f *fakeStore) HostJoined(context.Context, string, string) (bool, error) {
	return f.hostJoined, nil
}
func (f *fakeStore) HostsApprox(context.Context, string) (map[string]struct{}, error) {
	return f.hosts, nil
}

type fakePresence struct {
	bumps int
	err   error
}

func (f *fakePresence) Bump(context.Context, string, string) error { f.bumps++; return f.err }

type world struct {
	app      *App
	store    *fakeStore
	presence *fakePresence
	rows     []Row
	hs       *synapsecfg.Config
	proxied  int
}

func newWorld(t *testing.T, writer bool) *world {
	t.Helper()
	w := &world{
		store:    &fakeStore{membership: "join", hostJoined: true, hosts: map[string]struct{}{"remote.test": {}}},
		presence: &fakePresence{},
		hs: &synapsecfg.Config{ServerName: "hs.test", TrackPresence: true,
			Experimental: map[string]any{}},
	}
	if writer {
		w.hs.Writers.Typing = []string{"gw"}
	} else {
		w.hs.Writers.Typing = []string{"master"}
	}
	w.app = New(Options{
		Instance: "gw",
		Synapse:  func() *synapsecfg.Config { return w.hs },
		Auth:     fakeAuth{clientauth.Verdict{Valid: true, UserID: "@me:hs.test", DeviceID: "DEV"}},
		Store:    w.store,
		Presence: w.presence,
		OnRow:    func(r Row) { w.rows = append(w.rows, r) },
		Fallback: http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { w.proxied++; rw.WriteHeader(299) }),
		Log:      zerolog.Nop(),
	})
	return w
}

func (w *world) put(t *testing.T, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest("PUT", path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	w.app.Handler().ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if rec.Code != 299 && rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("%s: no CORS headers", path)
	}
	return rec.Code, out
}

const myPath = "/_matrix/client/v3/rooms/%21r%3Ahs.test/typing/%40me%3Ahs.test"

func TestMatch(t *testing.T) {
	for p, want := range map[string]bool{
		myPath: true,
		"/_matrix/client/api/v1/rooms/!r:x/typing/@me:x":   true,
		"/_matrix/client/r0/rooms/!r:x/typing/@me:x":       true,
		"/_matrix/client/unstable/rooms/!r:x/typing/@me:x": true,
		"/_matrix/client/v3/rooms//typing/":                true, // [^/]*
		"/_matrix/client/v1/rooms/!r:x/typing/@me:x":       false,
		"/_matrix/client/v3/rooms/!r:x/typing":             false,
		"/_matrix/client/v3/rooms/!r:x/typing/@me:x/x":     false,
		"/_matrix/client/v3/rooms/!r:x/send/@me:x":         false,
	} {
		if got := Match("PUT", p); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
	if Match("GET", myPath) {
		t.Error("GET matched")
	}
}

func TestStartAndStop(t *testing.T) {
	w := newWorld(t, true)
	if code, body := w.put(t, myPath, `{"typing":true,"timeout":5000}`); code != 200 || len(body) != 0 {
		t.Fatalf("start: %d %v", code, body)
	}
	if code, _ := w.put(t, myPath, `{"typing":false}`); code != 200 {
		t.Fatalf("stop: %d", code)
	}
	if len(w.rows) != 2 || w.rows[0].RoomID != "!r:hs.test" || w.rows[0].Users[0] != "@me:hs.test" || len(w.rows[1].Users) != 0 {
		t.Fatalf("rows: %v", w.rows)
	}
	if w.presence.bumps != 2 {
		t.Fatalf("bumps: %d", w.presence.bumps)
	}
}

func TestClientErrors(t *testing.T) {
	for _, c := range []struct {
		name, path, body string
		setup            func(*world)
		code             int
		errcode, msg     string
	}{
		{"not json", myPath, `nope`, nil, 400, "M_NOT_JSON", "Content not JSON."},
		{"not an object", myPath, `[]`, nil, 400, "M_BAD_JSON", "Content must be a JSON object."},
		{"no typing key", myPath, `{}`, nil, 500, "M_UNKNOWN", "Internal server error"},
		{"string timeout", myPath, `{"typing":true,"timeout":"5"}`, nil, 500, "M_UNKNOWN", "Internal server error"},
		{"null timeout", myPath, `{"typing":true,"timeout":null}`, nil, 500, "M_UNKNOWN", "Internal server error"},
		{"bad user id", "/_matrix/client/v3/rooms/!r:hs.test/typing/me", `{"typing":true}`, nil,
			400, "M_INVALID_PARAM", "Expected UserID string to start with '@'"},
		{"no domain", "/_matrix/client/v3/rooms/!r:hs.test/typing/@me", `{"typing":true}`, nil,
			400, "M_INVALID_PARAM", "Expected UserID of the form '@localname:domain'"},
		{"remote user", "/_matrix/client/v3/rooms/!r:hs.test/typing/@me:elsewhere", `{"typing":true}`, nil,
			400, "M_UNKNOWN", "User is not hosted on this homeserver"},
		{"other user", "/_matrix/client/v3/rooms/!r:hs.test/typing/@you:hs.test", `{"typing":true}`, nil,
			400, "M_FORBIDDEN", "Cannot set another user's typing state"},
		{"not joined", myPath, `{"typing":true}`, func(w *world) { w.store.membership = "leave" },
			403, "M_FORBIDDEN", "User @me:hs.test not in room !r:hs.test"},
		{"presence fails", myPath, `{"typing":true}`, func(w *world) { w.presence.err = errors.New("down") },
			500, "M_UNKNOWN", "Internal server error"},
		{"guest", myPath, `{"typing":true}`, func(w *world) {
			w.app.auth = fakeAuth{clientauth.Verdict{Valid: true, UserID: "@me:hs.test", IsGuest: true}}
		}, 403, "M_GUEST_ACCESS_FORBIDDEN", "Guest access not allowed"},
	} {
		w := newWorld(t, true)
		if c.setup != nil {
			c.setup(w)
		}
		code, body := w.put(t, c.path, c.body)
		if code != c.code || body["errcode"] != c.errcode || body["error"] != c.msg {
			t.Errorf("%s: got %d %v, want %d %s %q", c.name, code, body, c.code, c.errcode, c.msg)
		}
		if len(w.rows) != 0 {
			t.Errorf("%s: typing changed: %v", c.name, w.rows)
		}
	}
}

func TestNotJoinedUnderMSC3848(t *testing.T) {
	w := newWorld(t, true)
	w.hs.Experimental["msc3848_enabled"] = true
	w.store.membership = ""
	_, body := w.put(t, myPath, `{"typing":true}`)
	if body["org.matrix.msc3848.unstable.errcode"] != "ORG.MATRIX.MSC3848.NOT_JOINED" || body["errcode"] != "M_FORBIDDEN" {
		t.Fatalf("got %v", body)
	}
}

func TestTimeoutAndTypingValues(t *testing.T) {
	w := newWorld(t, true)
	st := w.app.state.Load()
	// Capped at 120 s; a bool is an int; a huge number is capped too.
	for _, body := range []string{`{"typing":1,"timeout":999999}`, `{"typing":"yes","timeout":true}`, `{"typing":[0],"timeout":1e400}`} {
		if code, out := w.put(t, myPath, body); code != 200 {
			t.Fatalf("%s: %d %v", body, code, out)
		}
	}
	if st.until[member{"!r:hs.test", "@me:hs.test"}] > st.now()+MaxTimeout {
		t.Fatal("timeout not capped")
	}
	// Falsy values stop.
	for _, body := range []string{`{"typing":0}`, `{"typing":""}`, `{"typing":null}`, `{"typing":{}}`} {
		w.put(t, myPath, `{"typing":true}`)
		n := len(w.rows)
		w.put(t, myPath, body)
		if len(w.rows) != n+1 || len(w.rows[n].Users) != 0 {
			t.Fatalf("%s did not stop: %v", body, w.rows)
		}
	}
}

func TestShadowBanned(t *testing.T) {
	if testing.Short() {
		t.Skip("sleeps up to 10 s")
	}
	w := newWorld(t, true)
	w.store.banned = true
	if code, _ := w.put(t, myPath, `{"typing":true}`); code != 200 || len(w.rows) != 0 {
		t.Fatalf("got %d, rows %v", code, w.rows)
	}
}

func TestNotTheWriter(t *testing.T) {
	w := newWorld(t, false)
	if code, _ := w.put(t, myPath, `{"typing":true}`); code != 299 || w.proxied != 1 {
		t.Fatalf("not proxied: %d", code)
	}
	// Becoming the writer starts at 0; stopping drops the state.
	w.hs.Writers.Typing = []string{"gw"}
	w.app.SetSynapse(w.hs)
	if code, _ := w.put(t, myPath, `{"typing":true}`); code != 200 || w.rows[0].Token != 1 {
		t.Fatalf("as writer: %d %v", code, w.rows)
	}
	w.hs.Writers.Typing = []string{"other"}
	w.app.SetSynapse(w.hs)
	if w.app.Writing() {
		t.Fatal("still writing")
	}
	if err := w.app.ReceiveEDU(context.Background(), "remote.test", json.RawMessage(`{}`)); !errors.Is(err, ErrNotWriter) {
		t.Fatalf("EDU while not writing: %v", err)
	}
}

func TestReceiveEDU(t *testing.T) {
	ctx := context.Background()
	edu := func(room, user, typing string) json.RawMessage {
		return json.RawMessage(`{"room_id":"` + room + `","user_id":"` + user + `","typing":` + typing + `}`)
	}
	for _, c := range []struct {
		name    string
		origin  string
		content json.RawMessage
		setup   func(*world)
		applied bool
		err     bool
	}{
		{"applied", "remote.test", edu("!r:x", "@z:remote.test", "true"), nil, true, false},
		{"we are not in the room", "remote.test", edu("!r:x", "@z:remote.test", "true"),
			func(w *world) { w.store.hostJoined = false }, false, false},
		{"user not the origin's", "evil.test", edu("!r:x", "@z:remote.test", "true"), nil, false, false},
		{"origin not in the room", "remote.test", edu("!r:x", "@z:remote.test", "true"),
			func(w *world) { w.store.hosts = map[string]struct{}{} }, false, false},
		{"non-compliant", "remote.test", edu("!r:x", "@zé:remote.test", "true"), nil, false, false},
		{"empty localpart", "remote.test", edu("!r:x", "@:remote.test", "true"), nil, false, false},
		{"no room_id", "remote.test", json.RawMessage(`{"user_id":"@z:remote.test","typing":true}`), nil, false, true},
		{"bad user id", "remote.test", edu("!r:x", "z", "true"), nil, false, true},
		{"no typing", "remote.test", json.RawMessage(`{"room_id":"!r:x","user_id":"@z:remote.test"}`), nil, false, true},
	} {
		w := newWorld(t, true)
		if c.setup != nil {
			c.setup(w)
		}
		err := w.app.ReceiveEDU(ctx, c.origin, c.content)
		if (err != nil) != c.err || (len(w.rows) == 1) != c.applied {
			t.Errorf("%s: err=%v rows=%v", c.name, err, w.rows)
		}
	}
}

func TestFedSendEDU(t *testing.T) {
	w := newWorld(t, true)
	e := replserver.FedSendEDU(func(eduType string) replserver.EDUHandler {
		if eduType == "m.typing" {
			return w.app.ReceiveEDU
		}
		return nil
	})
	if e.Method != "POST" || !e.Cache || !e.WaitForStreams || e.Name != "fed_send_edu" {
		t.Fatalf("endpoint %+v", e)
	}
	content := map[string]json.RawMessage{
		"origin":  json.RawMessage(`"remote.test"`),
		"content": json.RawMessage(`{"room_id":"!r:x","user_id":"@z:remote.test","typing":true}`),
	}
	code, _, err := e.Handle(context.Background(), replRequest("m.typing", content))
	if err != nil || code != 200 || len(w.rows) != 1 {
		t.Fatalf("got %d %v %v", code, err, w.rows)
	}
	if _, _, err := e.Handle(context.Background(), replRequest("m.receipt", content)); err == nil {
		t.Fatal("handled an EDU type with no handler")
	}
}
