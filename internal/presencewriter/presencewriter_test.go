package presencewriter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

type fakeAuth struct{ v clientauth.Verdict }

func (f fakeAuth) Authenticate(context.Context, *http.Request) (clientauth.Verdict, error) {
	return f.v, nil
}

type memDB struct{ seq int64 }

func (m *memDB) DeleteStale(context.Context, string, []string) error         { return nil }
func (m *memDB) LastValue(context.Context, string) (int64, error)            { return m.seq, nil }
func (m *memDB) UpsertPosition(context.Context, string, string, int64) error { return nil }
func (m *memDB) NextVal(context.Context, string) (int64, error)              { m.seq++; return m.seq, nil }

type fakeStore struct {
	active  []State
	exempt  map[string]bool
	batches [][]int64
	written []State
	fail    bool
}

func (f *fakeStore) Active(context.Context) ([]State, error) { return f.active, nil }
func (f *fakeStore) Persist(_ context.Context, instance string, ids []int64, states []State) error {
	if f.fail {
		return errors.New("boom")
	}
	f.batches = append(f.batches, ids)
	f.written = append(f.written, states...)
	return nil
}
func (f *fakeStore) Updates(_ context.Context, _, upto int64, _ int) ([]streamwriter.Update, int64, bool, error) {
	return nil, upto, false, nil
}
func (f *fakeStore) RatelimitExempt(_ context.Context, u string) (bool, error) {
	return f.exempt[u], nil
}

func newApp(t *testing.T, hs *synapsecfg.Config, st *fakeStore, seq int64) (*App, *streamwriter.Writer) {
	t.Helper()
	w := streamwriter.New(streamwriter.Options{Instance: "gw", Log: zerolog.Nop()})
	a, err := New(context.Background(), Options{
		Instance: "gw",
		Synapse:  func() *synapsecfg.Config { return hs },
		Auth:     fakeAuth{clientauth.Verdict{Valid: true, UserID: "@me:hs.test", DeviceID: "D"}},
		Store:    st,
		Pool:     &memDB{seq: seq},
		Writer:   w,
		Fallback: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(299) }),
		Log:      zerolog.Nop(),
		Worker:   zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return a, w
}

func disabled(writers ...string) *synapsecfg.Config {
	hs := &synapsecfg.Config{RcPresence: synapsecfg.Ratelimit{Key: "rc_presence.per_user", PerSecond: 0.1, Burst: 1}}
	hs.Writers.Presence = writers
	return hs
}

func TestParseAndMatch(t *testing.T) {
	for p, want := range map[string]string{
		"/_matrix/client/v3/presence/@a:b/status":         "@a:b",
		"/_matrix/client/api/v1/presence/%40a%3Ab/status": "%40a%3Ab",
		"/_matrix/client/r0/presence//status":             "",
		"/_matrix/client/unstable/presence/a%2Fb/status":  "a%2Fb",
	} {
		got, ok := parse(p)
		if !ok || got != want {
			t.Errorf("%s: %q %v", p, got, ok)
		}
	}
	for _, p := range []string{
		"/_matrix/client/v3/presence/@a:b/status/",
		"/_matrix/client/v3/presence/a/b/status",
		"/_matrix/client/v4/presence/@a:b/status",
		"/_matrix/client/v3/presence/list/@a:b",
	} {
		if _, ok := parse(p); ok {
			t.Errorf("%s matched", p)
		}
	}
	if Match(http.MethodPost, "/_matrix/client/v3/presence/@a:b/status") || !Match(http.MethodPut, "/_matrix/client/v3/presence/@a:b/status") {
		t.Error("methods")
	}
}

func TestRole(t *testing.T) {
	a, w := newApp(t, disabled("other"), &fakeStore{}, 5)
	if a.Writing() || w.Stream(StreamName) != nil {
		t.Fatal("took a role not given")
	}
	hs := disabled("gw")
	hs.PresenceEnabled = true
	if _, err := New(context.Background(), Options{Instance: "gw", Synapse: func() *synapsecfg.Config { return hs },
		Store: &fakeStore{}, Pool: &memDB{}, Writer: streamwriter.New(streamwriter.Options{})}); err == nil {
		t.Fatal("took the role with presence enabled")
	}
	a, w = newApp(t, disabled("gw"), &fakeStore{}, 290023695)
	if !a.Writing() {
		t.Fatal("not writing")
	}
	if got := w.Positions(); got[StreamName] != 290023695 || got[FederationStreamName] != 0 || len(got) != 2 {
		t.Fatalf("positions %v", got)
	}
}

func TestStaleOffline(t *testing.T) {
	msg := "away"
	ts := int64(17)
	st := &fakeStore{}
	for i := range 1201 {
		st.active = append(st.active, State{UserID: "@u" + string(rune('a'+i%26)) + ":x", State: "online", StatusMsg: &msg,
			LastActiveTS: &ts, CurrentlyActive: true})
	}
	a, w := newApp(t, disabled("gw"), st, 100)
	if err := a.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.batches) != 3 || len(st.batches[0]) != 500 || len(st.batches[2]) != 201 {
		t.Fatalf("batches %d", len(st.batches))
	}
	if st.batches[0][0] != 101 || st.batches[2][200] != 1301 {
		t.Errorf("ids %d..%d", st.batches[0][0], st.batches[2][200])
	}
	s := st.written[0]
	if s.State != "offline" || s.StatusMsg != nil || s.CurrentlyActive || s.LastActiveTS == nil || *s.LastActiveTS != 17 {
		t.Errorf("state %+v", s)
	}
	if w.Positions()[StreamName] != 1301 {
		t.Errorf("position %d", w.Positions()[StreamName])
	}
	// Once only.
	if err := a.Run(context.Background()); err != nil || len(st.batches) != 3 {
		t.Error("ran twice")
	}

	// A failed write finishes its IDs: the position is not pinned.
	st = &fakeStore{active: []State{{UserID: "@a:x", State: "unavailable"}}, fail: true}
	a, w = newApp(t, disabled("gw"), st, 7)
	_ = a.Run(context.Background())
	if w.Positions()[StreamName] != 8 {
		t.Errorf("position after failure %d", w.Positions()[StreamName])
	}
}

func TestSetState(t *testing.T) {
	a, _ := newApp(t, disabled("gw"), &fakeStore{}, 1)
	cases := []struct {
		user, body string
		code       int // 0: an unexpected exception
		errcode    string
	}{
		{"@u:x", `{"device_id":"D","state":{"presence":"online"},"force_notify":false,"is_sync":true}`, 200, ""},
		{"@u:x", `{"state":{"presence":"offline","status_msg":"x"},"force_notify":true}`, 200, ""},
		{"@u:x", `{"state":{"presence":"busy"},"force_notify":false}`, 400, "M_UNKNOWN"},
		{"@u:x", `{"state":{"presence":"Online"},"force_notify":false}`, 400, "M_UNKNOWN"},
		{"@u:x", `{"state":{"presence":1},"force_notify":false}`, 400, "M_UNKNOWN"},
		{"@u:x", `{"state":{"presence":"bad","presence":"online"},"force_notify":false}`, 200, ""},
		{"@u:x", `{"state":{},"force_notify":false}`, 0, ""},
		{"@u:x", `{"state":["online"],"force_notify":false}`, 0, ""},
		{"@u:x", `{"state":null,"force_notify":false}`, 0, ""},
		{"@u:x", `{"state":{"presence":"online"}}`, 0, ""},
		{"@u:x", `{"force_notify":false}`, 0, ""},
		{"u:x", `{"state":{"presence":"online"},"force_notify":false}`, 400, "M_INVALID_PARAM"},
		{"@ux", `{}`, 400, "M_INVALID_PARAM"},
	}
	for _, c := range cases {
		var content map[string]json.RawMessage
		if err := json.Unmarshal([]byte(c.body), &content); err != nil {
			t.Fatal(err)
		}
		code, _, err := a.setState(context.Background(), replserver.Request{Args: map[string]string{"user_id": c.user}, Content: content})
		var serr *replserver.Error
		switch {
		case c.code == 200 && (err != nil || code != 200):
			t.Errorf("%s %s: %d %v", c.user, c.body, code, err)
		case c.code == 0 && (err == nil || errors.As(err, &serr)):
			t.Errorf("%s %s: want an internal error, got %d %v", c.user, c.body, code, err)
		case c.code == 400 && (!errors.As(err, &serr) || serr.Code != 400 || serr.Errcode != c.errcode):
			t.Errorf("%s %s: %v", c.user, c.body, err)
		}
	}
	hs := disabled("gw")
	hs.Experimental = map[string]any{"msc3026_enabled": true}
	a.Synapse = func() *synapsecfg.Config { return hs }
	content := map[string]json.RawMessage{"state": json.RawMessage(`{"presence":"busy"}`), "force_notify": json.RawMessage(`false`)}
	if code, _, err := a.setState(context.Background(), replserver.Request{Args: map[string]string{"user_id": "@u:x"}, Content: content}); code != 200 || err != nil {
		t.Errorf("busy under msc3026: %d %v", code, err)
	}
	if code, _, err := a.bump(context.Background(), replserver.Request{Args: map[string]string{"user_id": "@u:x"}, Content: map[string]json.RawMessage{}}); code != 200 || err != nil {
		t.Errorf("bump: %d %v", code, err)
	}
	var serr *replserver.Error
	if _, _, err := a.bump(context.Background(), replserver.Request{Args: map[string]string{"user_id": "x"}}); !errors.As(err, &serr) || serr.Code != 400 {
		t.Errorf("bump bad user: %v", err)
	}
}

func do(a *App, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func TestClient(t *testing.T) {
	now := time.Unix(1000, 0)
	st := &fakeStore{exempt: map[string]bool{}}
	a, _ := newApp(t, disabled("other"), st, 1)
	a.perUser = ratelimit.New(disabled().RcPresence, func() time.Time { return now })
	const me = "/_matrix/client/v3/presence/@me:hs.test/status"
	for _, c := range []struct {
		method, path, body string
		code               int
		want               string
	}{
		{"GET", "/_matrix/client/v3/presence/@other:x/status", "", 200, `{"presence":"offline"}`},
		{"GET", "/_matrix/client/v3/presence/other/status", "", 400, `{"errcode":"M_INVALID_PARAM","error":"Expected UserID string to start with '@'"}`},
		{"GET", "/_matrix/client/v3/presence/@other/status", "", 400, `{"errcode":"M_INVALID_PARAM","error":"Expected UserID of the form '@localname:domain'"}`},
		{"PUT", "/_matrix/client/v3/presence/@other:x/status", `{"presence":"online"}`, 403, `{"errcode":"M_FORBIDDEN","error":"Can only set your own presence state"}`},
		{"PUT", "/_matrix/client/v3/presence/%40me%3Ahs.test/status", `{"presence":"online","status_msg":"hi"}`, 200, `{}`},
		{"PUT", me, `{"presence":"online"}`, 429, `{"errcode":"M_LIMIT_EXCEEDED","error":"Too many requests","retry_after_ms":10000}`},
	} {
		rec := do(a, c.method, c.path, c.body)
		if rec.Code != c.code || rec.Body.String() != c.want {
			t.Errorf("%s %s %s: %d %s", c.method, c.path, c.body, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Retry-After") != "" {
			t.Errorf("%s: Retry-After set", c.path)
		}
	}
	// The body is checked after the limiter, so every check below spends
	// an action: the clock moves past the bucket each time.
	for _, c := range []struct{ body, want string }{
		{`{"presence":"online","status_msg":3}`, `{"errcode":"M_UNKNOWN","error":"status_msg must be a string."}`},
		{`{"presence":"online","status_msg":null}`, `{"errcode":"M_UNKNOWN","error":"status_msg must be a string."}`},
		{`{"status_msg":"x"}`, `{"errcode":"M_UNKNOWN","error":"Unable to parse state"}`},
		{`{"presence":"online","x":1}`, `{"errcode":"M_UNKNOWN","error":"Unable to parse state"}`},
		{`{"presence":"nonsense"}`, `{}`},
		{`[1]`, `{"errcode":"M_BAD_JSON","error":"Content must be a JSON object."}`},
		{`{`, `{"errcode":"M_NOT_JSON","error":"Content not JSON."}`},
	} {
		now = now.Add(11 * time.Second)
		rec := do(a, "PUT", me, c.body)
		if rec.Body.String() != c.want {
			t.Errorf("%s: %d %s", c.body, rec.Code, rec.Body.String())
		}
	}
	st.exempt["@me:hs.test"] = true
	for range 3 {
		if rec := do(a, "PUT", me, `{"presence":"online"}`); rec.Code != 200 {
			t.Errorf("exempt: %d", rec.Code)
		}
	}
	hs := disabled("other")
	hs.PresenceEnabled = true
	a.Synapse = func() *synapsecfg.Config { return hs }
	if rec := do(a, "GET", me, ""); rec.Code != 299 {
		t.Errorf("enabled: %d, not main", rec.Code)
	}
}
