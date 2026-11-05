package accountdata

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// fakeStore keeps the tables as maps; its ignored-user rule is the store's
// (tested against Postgres in store_test.go).
type fakeStore struct {
	mu      sync.Mutex
	global  map[string]string // user|type -> content
	room    map[string]string // user|room|type -> content
	tags    map[string][]Tag  // user|room
	members map[string]bool   // user|room
	ids     []int64
	err     error
}

func newFakeStore() *fakeStore {
	return &fakeStore{global: map[string]string{}, room: map[string]string{}, tags: map[string][]Tag{},
		members: map[string]bool{"@me:hs.test|!r:hs.test": true}}
}

func (f *fakeStore) AddUser(_ context.Context, instance string, id int64, u, t, c string) ([]cachestream.Row, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	if f.err != nil {
		return nil, f.err
	}
	if t == TypeIgnoredUserList {
		current, err := ignoredIn(c)
		if err != nil {
			return nil, err
		}
		if current[u] {
			return nil, ErrIgnoreSelf
		}
		f.global[u+"|"+t] = c
		return []cachestream.Row{{StreamID: 900, Instance: instance, CacheFunc: cacheIgnoredUsers, Keys: []string{u}}}, nil
	}
	f.global[u+"|"+t] = c
	return nil, nil
}

func (f *fakeStore) RemoveUser(_ context.Context, _ string, id int64, u, t string) (bool, []cachestream.Row, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	if c, ok := f.global[u+"|"+t]; !ok || c == "{}" {
		return false, nil, nil
	}
	f.global[u+"|"+t] = "{}"
	return true, nil, nil
}

func (f *fakeStore) AddRoom(_ context.Context, id int64, u, r, t, c string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.room[u+"|"+r+"|"+t] = c
	return f.err
}

func (f *fakeStore) RemoveRoom(_ context.Context, id int64, u, r, t string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	k := u + "|" + r + "|" + t
	if c, ok := f.room[k]; !ok || c == "{}" {
		return false, nil
	}
	f.room[k] = "{}"
	return true, nil
}

func (f *fakeStore) AddTag(_ context.Context, id int64, u, r, tag, c string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	k := u + "|" + r
	for i, t := range f.tags[k] {
		if t.Name == tag {
			f.tags[k][i].Content = json.RawMessage(c)
			return nil
		}
	}
	f.tags[k] = append(f.tags[k], Tag{tag, json.RawMessage(c)})
	return nil
}

func (f *fakeStore) RemoveTag(_ context.Context, id int64, u, r, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	k := u + "|" + r
	var keep []Tag
	for _, t := range f.tags[k] {
		if t.Name != tag {
			keep = append(keep, t)
		}
	}
	f.tags[k] = keep
	return nil
}

func (f *fakeStore) Global(_ context.Context, u, t string) (json.RawMessage, error) {
	if c, ok := f.global[u+"|"+t]; ok {
		return json.RawMessage(c), nil
	}
	return nil, nil
}

func (f *fakeStore) Room(_ context.Context, u, r, t string) (json.RawMessage, error) {
	if c, ok := f.room[u+"|"+r+"|"+t]; ok {
		return json.RawMessage(c), nil
	}
	return nil, nil
}

func (f *fakeStore) Tags(_ context.Context, u, r string) ([]Tag, error) { return f.tags[u+"|"+r], nil }

func (f *fakeStore) HasMembership(_ context.Context, u, r string) (bool, error) {
	return f.members[u+"|"+r], nil
}

func (f *fakeStore) Updates(context.Context, int64, int64, int) ([]Update, int64, bool, error) {
	return nil, 0, false, nil
}

type memDB struct {
	mu  sync.Mutex
	seq int64
}

func (m *memDB) DeleteStale(context.Context, string, []string) error         { return nil }
func (m *memDB) LastValue(context.Context, string) (int64, error)            { return m.seq, nil }
func (m *memDB) UpsertPosition(context.Context, string, string, int64) error { return nil }
func (m *memDB) NextVal(context.Context, string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	return m.seq, nil
}

type fakeCaches struct {
	calls     int
	published []cachestream.Row
	failPub   bool
}

func (c *fakeCaches) Do(write func() ([]cachestream.Row, error)) error {
	c.calls++
	rows, err := write()
	if err != nil {
		return err
	}
	if c.failPub && len(rows) > 0 {
		return &cachestream.PublishError{Err: errors.New("bus down")}
	}
	c.published = append(c.published, rows...)
	return nil
}

type fakeAuth struct{ v clientauth.Verdict }

func (f fakeAuth) Authenticate(context.Context, *http.Request) (clientauth.Verdict, error) {
	return f.v, nil
}

type world struct {
	app    *App
	store  *fakeStore
	caches *fakeCaches
	hs     *synapsecfg.Config
}

func newWorld(t *testing.T, msc3391 bool) *world {
	t.Helper()
	w := &world{store: newFakeStore(), caches: &fakeCaches{}}
	w.hs = &synapsecfg.Config{ServerName: "hs.test", Experimental: map[string]any{"msc3391_enabled": msc3391}}
	w.hs.Writers.AccountData = []string{"gw"}
	app, err := New(context.Background(), Options{
		Instance: "gw", Synapse: func() *synapsecfg.Config { return w.hs },
		Auth:  fakeAuth{clientauth.Verdict{Valid: true, UserID: "@me:hs.test", DeviceID: "D"}},
		Store: w.store, Pool: &memDB{seq: 100}, Caches: w.caches,
		PushRules: func(context.Context, string) (json.RawMessage, error) {
			return json.RawMessage(`{"global": {"override": []}}`), nil
		},
		Fallback: http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(299) }),
		Log:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	w.app = app
	return w
}

func (w *world) do(t *testing.T, method, path, body string) (int, string) {
	t.Helper()
	if !w.app.Match(method, path) {
		t.Fatalf("%s %s: not matched", method, path)
	}
	rec := httptest.NewRecorder()
	w.app.Handler().ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec.Code, rec.Body.String()
}

const (
	pGlobal = "/_matrix/client/v3/user/%40me%3Ahs.test/account_data/"
	pRoom   = "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/%21r%3Ahs.test/account_data/"
	pTags   = "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/%21r%3Ahs.test/tags"
	pDelete = "/_matrix/client/unstable/org.matrix.msc3391/user/%40me%3Ahs.test/account_data/"
)

func TestMatch(t *testing.T) {
	w := newWorld(t, true)
	for _, c := range []struct {
		method, path string
		want         bool
	}{
		{"PUT", pGlobal + "m.direct", true},
		{"GET", "/_matrix/client/r0/user/@me:hs.test/account_data/m.direct", true},
		{"GET", "/_matrix/client/unstable/user/@me:hs.test/account_data/m.direct", true},
		{"POST", pGlobal + "m.direct", false},
		{"DELETE", pGlobal + "m.direct", false},
		{"GET", "/_matrix/client/api/v1/user/@me:hs.test/account_data/m.direct", false},
		{"GET", pGlobal + "m.direct/", false},
		{"PUT", pRoom + "x", true},
		{"GET", pTags, true},
		{"PUT", pTags + "/u.work", true},
		{"DELETE", pTags + "/u.work", true},
		{"GET", pTags + "/u.work", false},
		{"DELETE", pDelete + "m.direct", true},
		{"DELETE", "/_matrix/client/v3/org.matrix.msc3391/user/@me:hs.test/account_data/x", false},
		{"DELETE", "/_matrix/client/unstable/org.matrix.msc3391/user/@me:hs.test/rooms/!r:hs.test/account_data/x", true},
		{"GET", "/_matrix/client/v3/user//account_data/", true}, // [^/]* matches empty
	} {
		if got := w.app.Match(c.method, c.path); got != c.want {
			t.Errorf("%s %s: %v", c.method, c.path, got)
		}
	}
	off := newWorld(t, false)
	if off.app.Match("DELETE", pDelete+"m.direct") {
		t.Error("MSC3391 DELETE matched with the MSC off")
	}
}

func TestPutAndGetGlobal(t *testing.T) {
	w := newWorld(t, true)
	code, body := w.do(t, "PUT", pGlobal+"m.direct", `{"b": 1.0, "a": "é", "b": [1, 2.50]}`)
	if code != 200 || body != "{}" {
		t.Fatalf("PUT: %d %s", code, body)
	}
	// json_encoder: dict semantics (the last b, in its first place),
	// compact, ensure_ascii, Python floats.
	if got := w.store.global["@me:hs.test|m.direct"]; got != `{"b":[1,2.5],"a":"`+"\\"+`u00e9"}` {
		t.Errorf("stored %s", got)
	}
	code, body = w.do(t, "GET", pGlobal+"m.direct", ``)
	if code != 200 || body != `{"b":[1,2.5],"a":"`+"\\"+`u00e9"}` {
		t.Errorf("GET: %d %s", code, body)
	}
	code, body = w.do(t, "GET", pGlobal+"m.nothing", ``)
	if code != 404 || !strings.Contains(body, `"Account data not found"`) || !strings.Contains(body, "M_NOT_FOUND") {
		t.Errorf("GET missing: %d %s", code, body)
	}
	code, body = w.do(t, "GET", pGlobal+"m.push_rules", ``)
	if code != 200 || body != `{"global":{"override":[]}}` {
		t.Errorf("GET push rules: %d %s", code, body)
	}
}

func TestMSC3391(t *testing.T) {
	w := newWorld(t, true)
	w.do(t, "PUT", pGlobal+"x", `{"k": 1}`)
	// {} is the DELETE.
	if code, _ := w.do(t, "PUT", pGlobal+"x", ` { } `); code != 200 {
		t.Fatal(code)
	}
	if got := w.store.global["@me:hs.test|x"]; got != "{}" {
		t.Errorf("stored %q", got)
	}
	if code, body := w.do(t, "GET", pGlobal+"x", ``); code != 404 {
		t.Errorf("GET {} with MSC3391: %d %s", code, body)
	}
	if code, _ := w.do(t, "DELETE", pDelete+"x", ``); code != 200 {
		t.Errorf("DELETE: %d", code)
	}

	off := newWorld(t, false)
	off.do(t, "PUT", pGlobal+"x", `{}`)
	if got := off.store.global["@me:hs.test|x"]; got != "{}" {
		t.Errorf("without MSC3391 {} is stored: %q", got)
	}
	if code, body := off.do(t, "GET", pGlobal+"x", ``); code != 200 || body != "{}" {
		t.Errorf("GET {} without MSC3391: %d %s", code, body)
	}
	// Room push rules are {}: 404 with MSC3391, 200 without.
	if code, _ := w.do(t, "GET", pRoom+"m.push_rules", ``); code != 404 {
		t.Errorf("room push rules, MSC3391: %d", code)
	}
	if code, body := off.do(t, "GET", pRoom+"m.push_rules", ``); code != 200 || body != "{}" {
		t.Errorf("room push rules: %d %s", code, body)
	}
}

func TestChecks(t *testing.T) {
	w := newWorld(t, true)
	other := "/_matrix/client/v3/user/%40you%3Ahs.test/"
	for _, c := range []struct {
		method, path, body string
		code               int
		want               string
	}{
		{"PUT", other + "account_data/x", `{}`, 403, `"Cannot add account data for other users."`},
		{"GET", other + "account_data/x", ``, 403, `"Cannot get account data for other users."`},
		{"PUT", other + "rooms/!r:hs.test/account_data/x", `{}`, 403, `"Cannot add account data for other users."`},
		{"GET", other + "rooms/!r:hs.test/tags", ``, 403, `"Cannot get tags for other users."`},
		{"DELETE", other + "rooms/!r:hs.test/tags/t", ``, 403, `"Cannot add tags for other users."`},
		{"DELETE", "/_matrix/client/unstable/org.matrix.msc3391/user/@you:hs.test/account_data/x", ``, 403,
			`"Cannot delete account data for other users."`},
		{"PUT", pGlobal + "m.fully_read", `{}`, 405, `"Cannot set m.fully_read through this API. Use /rooms/!roomId:server.name/read_markers"`},
		{"PUT", pGlobal + "m.push_rules", `{}`, 405, `"Cannot set m.push_rules through this API. Use /pushrules"`},
		{"DELETE", pDelete + "m.push_rules", ``, 405, `M_BAD_JSON`},
		{"PUT", "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/nope/account_data/x", `{}`, 400, `"nope is not a valid room ID"`},
		{"GET", "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/nope/account_data/x", ``, 400, `M_INVALID_PARAM`},
		// The room check comes before the type check.
		{"PUT", "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/nope/account_data/m.fully_read", `{}`, 400, `not a valid room ID`},
		{"PUT", pGlobal + "x", `[1]`, 400, `M_BAD_JSON`},
		{"PUT", pGlobal + "x", `{`, 400, `M_NOT_JSON`},
		{"PUT", pTags + "/" + strings.Repeat("é", 128), `{}`, 400, `"tag parameter's length is over 255 bytes"`},
		{"PUT", "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/%21other%3Ahs.test/tags/t", `{}`, 403,
			`"User @me:hs.test has no membership in room !other:hs.test"`},
		{"PUT", pGlobal + TypeIgnoredUserList, `{"ignored_users": {"@me:hs.test": {}}}`, 400, `"You cannot ignore yourself"`},
	} {
		code, body := w.do(t, c.method, c.path, c.body)
		if code != c.code || !strings.Contains(body, c.want) {
			t.Errorf("%s %s: %d %s, want %d %s", c.method, c.path, code, body, c.code, c.want)
		}
	}
}

func TestTags(t *testing.T) {
	w := newWorld(t, true)
	// 255 bytes is allowed.
	long := strings.Repeat("a", 255)
	for _, tag := range []string{"u.work", "m.favourite", long} {
		if code, body := w.do(t, "PUT", pTags+"/"+tag, `{"order": 0.5}`); code != 200 {
			t.Fatalf("%s: %d %s", tag, code, body)
		}
	}
	w.do(t, "DELETE", pTags+"/m.favourite", ``)
	code, body := w.do(t, "GET", pTags, ``)
	if code != 200 || body != `{"tags":{"u.work":{"order":0.5},"`+long+`":{"order":0.5}}}` {
		t.Errorf("%d %s", code, body)
	}
	// Removing a tag of a room one is not in is allowed.
	if code, _ := w.do(t, "DELETE", "/_matrix/client/v3/user/%40me%3Ahs.test/rooms/%21x%3Ahs.test/tags/t", ``); code != 200 {
		t.Errorf("DELETE elsewhere: %d", code)
	}
}

func TestIDsAndPosition(t *testing.T) {
	w := newWorld(t, true)
	ctx := context.Background()
	n, err := w.app.AddUser(ctx, CallerLocal, "@me:hs.test", "m.direct", json.RawMessage(`{}`))
	if err != nil || n != 101 {
		t.Fatalf("%d %v", n, err)
	}
	// A failed write spends its ID and the position still moves past it.
	if _, err := w.app.AddUser(ctx, CallerLocal, "@me:hs.test", TypeIgnoredUserList,
		json.RawMessage(`{"ignored_users": {"@me:hs.test": {}}}`)); !errors.Is(err, ErrIgnoreSelf) {
		t.Fatal(err)
	}
	// A remove that matches nothing spends an ID too.
	n, err = w.app.RemoveRoom(ctx, CallerLocal, "@me:hs.test", "!r:hs.test", "nothing")
	if err != nil || n != 103 {
		t.Fatalf("%d %v", n, err)
	}
	if got := w.store.ids; len(got) != 3 || got[0] != 101 || got[1] != 102 || got[2] != 103 {
		t.Errorf("ids %v", got)
	}
	// Only the ignored list goes through the caches stream.
	if w.caches.calls != 1 {
		t.Errorf("caches calls %d", w.caches.calls)
	}
	if _, err := w.app.AddUser(ctx, CallerLocal, "@me:hs.test", TypeIgnoredUserList,
		json.RawMessage(`{"ignored_users": {"@you:hs.test": {}}}`)); err != nil {
		t.Fatal(err)
	}
	if len(w.caches.published) != 1 {
		t.Errorf("published %v", w.caches.published)
	}
	// A committed write whose caches rows did not reach the bus succeeds.
	w.caches.failPub = true
	if _, err := w.app.AddUser(ctx, CallerLocal, "@me:hs.test", TypeIgnoredUserList,
		json.RawMessage(`{"ignored_users": {}}`)); err != nil {
		t.Errorf("publish failure failed the write: %v", err)
	}
}

func TestEncodeFailure(t *testing.T) {
	w := newWorld(t, true)
	// 1e400 is inf to Python, which json_encoder refuses (allow_nan=False).
	code, _ := w.do(t, "PUT", pRoom+"x", `{"n": 1e400}`)
	if code != 500 {
		t.Errorf("%d", code)
	}
}

func TestReplicationEndpoints(t *testing.T) {
	w := newWorld(t, true)
	eps := map[string]replserver.Endpoint{}
	for _, e := range w.app.Endpoints() {
		eps[e.Name] = e
		if e.Cache || !e.WaitForStreams || e.Method != "POST" {
			t.Errorf("%s: %+v", e.Name, e)
		}
	}
	if len(eps) != 6 {
		t.Fatalf("%d endpoints", len(eps))
	}
	ctx := context.Background()
	code, body, err := eps["add_tag"].Handle(ctx, replserver.Request{
		Args:    map[string]string{"user_id": "@me:hs.test", "room_id": "!new:hs.test", "tag": "u.work"},
		Content: map[string]json.RawMessage{"content": json.RawMessage(`{"order":1}`)},
	})
	if err != nil || code != 200 || body["max_stream_id"] != int64(101) {
		t.Fatalf("%d %v %v", code, body, err)
	}
	_, _, err = eps["add_user_account_data"].Handle(ctx, replserver.Request{
		Args:    map[string]string{"user_id": "@me:hs.test", "account_data_type": TypeIgnoredUserList},
		Content: map[string]json.RawMessage{"content": json.RawMessage(`{"ignored_users":{"@me:hs.test":{}}}`)},
	})
	var serr *replserver.Error
	if !errors.As(err, &serr) || serr.Code != 400 || serr.Msg != "You cannot ignore yourself" {
		t.Errorf("ignore self: %v", err)
	}
	if _, _, err := eps["add_room_account_data"].Handle(ctx, replserver.Request{
		Args:    map[string]string{"user_id": "u", "room_id": "r", "account_data_type": "t"},
		Content: map[string]json.RawMessage{},
	}); err == nil || errors.As(err, &serr) {
		t.Errorf("no content: %v", err)
	}
	off := newWorld(t, false)
	if n := len(off.app.Endpoints()); n != 4 {
		t.Errorf("without MSC3391: %d endpoints", n)
	}
}

func TestNotWriter(t *testing.T) {
	hs := &synapsecfg.Config{Experimental: map[string]any{}}
	hs.Writers.AccountData = []string{"other"}
	app, err := New(context.Background(), Options{Instance: "gw", Synapse: func() *synapsecfg.Config { return hs },
		Fallback: http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(299) }), Log: zerolog.Nop()})
	if err != nil || app.Writing() {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest("GET", pGlobal+"x", nil))
	if rec.Code != 299 {
		t.Errorf("%d", rec.Code)
	}
	if _, err := app.AddUser(context.Background(), CallerLocal, "u", "t", json.RawMessage(`{}`)); !errors.Is(err, ErrNotWriter) {
		t.Error(err)
	}
	hs.Writers.AccountData = []string{"gw", "x"}
	if _, err := New(context.Background(), Options{Instance: "gw", Synapse: func() *synapsecfg.Config { return hs }}); err == nil {
		t.Error("two writers accepted")
	}
}

func TestMergeRows(t *testing.T) {
	room := []idRow{{id: 3, room: "r"}, {id: 5, room: "r"}, {id: 9, room: "r"}}
	global := []idRow{{id: 3}, {id: 4}}
	tags := []idRow{{id: 1, dataType: "m.tag"}, {id: 8, dataType: "m.tag"}}
	got := mergeRows(8, room, global, tags)
	var ids []int64
	for _, r := range got {
		ids = append(ids, r.id)
	}
	want := []int64{1, 3, 3, 4, 5, 8}
	if len(ids) != len(want) {
		t.Fatalf("%v", ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("%v", ids)
		}
	}
	// On a tie, the room row (the first list) comes first.
	if got[1].room != "r" || got[2].room != "" {
		t.Errorf("tie order %+v", got[1:3])
	}
}

func TestIgnoredIn(t *testing.T) {
	for content, want := range map[string]int{
		`{"ignored_users":{"@a:x":{},"@b:x":{}}}`: 2,
		`{"ignored_users":[]}`:                    0,
		`{"ignored_users":"x"}`:                   0,
		`{}`:                                      0,
		`{"ignored_users":{"a.b":{}}}`:            1,
	} {
		got, err := ignoredIn(content)
		if err != nil || len(got) != want {
			t.Errorf("%s: %v %v", content, got, err)
		}
	}
	if _, err := ignoredIn(`[1]`); err == nil {
		t.Error("a list accepted")
	}
}
