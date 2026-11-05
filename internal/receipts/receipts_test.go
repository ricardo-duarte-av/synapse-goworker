package receipts

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

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// fakeStore keeps receipts_linearized as a map, with Synapse's rules for
// what a receipt replaces.
type fakeStore struct {
	mu        sync.Mutex
	events    map[string]int64 // event_id -> stream_ordering
	noRxTS    map[string]bool
	rows      map[string]Receipt // room|type|user|thread
	rowIDs    map[string]int64
	orderings map[string]Ordering
	account   map[string]json.RawMessage
	threads   map[string]string // event -> thread (both lookups)
	hostIn    bool
	hosts     map[string]struct{}
	graph     int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		events: map[string]int64{"$a": 10, "$b": 20}, noRxTS: map[string]bool{},
		rows: map[string]Receipt{}, rowIDs: map[string]int64{},
		orderings: map[string]Ordering{"$a": {1, 10}, "$b": {2, 20}},
		account:   map[string]json.RawMessage{}, threads: map[string]string{},
		hostIn: true, hosts: map[string]struct{}{"remote.test": {}},
	}
}

func key(r Receipt) string {
	t := "<null>"
	if r.ThreadID != nil {
		t = *r.ThreadID
	}
	return r.RoomID + "|" + r.ReceiptType + "|" + r.UserID + "|" + t
}

func (f *fakeStore) InsertLinearized(_ context.Context, r Receipt, id int64, _ string) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	so, known := f.events[r.EventID]
	if old, ok := f.rows[key(r)]; ok && known {
		if oso, ok := f.events[old.EventID]; ok && oso >= so {
			return false, false, nil
		}
	}
	f.rows[key(r)] = r
	f.rowIDs[key(r)] = id
	return true, !known || !f.noRxTS[r.EventID], nil
}
func (f *fakeStore) InsertGraph(context.Context, Receipt) error { f.graph++; return nil }
func (f *fakeStore) Updates(context.Context, string, int64, int64, int) ([]Update, int64, bool, error) {
	return nil, 0, false, nil
}
func (f *fakeStore) ThreadID(_ context.Context, e string) (string, error) {
	if t, ok := f.threads[e]; ok {
		return t, nil
	}
	return MainTimeline, nil
}
func (f *fakeStore) ThreadIDForReceipts(ctx context.Context, e string) (string, error) {
	return f.ThreadID(ctx, e)
}
func (f *fakeStore) RoomAccountData(_ context.Context, u, r, t string) (json.RawMessage, error) {
	return f.account[u+"|"+r+"|"+t], nil
}
func (f *fakeStore) EventOrdering(_ context.Context, e, room string) (Ordering, error) {
	if o, ok := f.orderings[e]; ok {
		return o, nil
	}
	return Ordering{}, &ErrNoEvent{e, room}
}
func (f *fakeStore) HostJoined(context.Context, string, string) (bool, error) { return f.hostIn, nil }
func (f *fakeStore) HostsApprox(context.Context, string) (map[string]struct{}, error) {
	return f.hosts, nil
}

// memIDs is a real generator over an in-memory sequence.
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

type fakeEvents struct{ access Access }

func (f *fakeEvents) Access(context.Context, string, string, string) (Access, error) {
	return f.access, nil
}

type fakeAccount struct {
	calls []string
	err   error
}

func (f *fakeAccount) AddRoomAccountData(_ context.Context, u, r, t string, c json.RawMessage) error {
	f.calls = append(f.calls, u+"|"+r+"|"+t+"|"+string(c))
	return f.err
}

type fakeAuth struct{ v clientauth.Verdict }

func (f fakeAuth) Authenticate(context.Context, *http.Request) (clientauth.Verdict, error) {
	return f.v, nil
}

type world struct {
	app     *App
	store   *fakeStore
	events  *fakeEvents
	account *fakeAccount
	seq     *memDB
	rows    []Row
	hs      *synapsecfg.Config
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{store: newFakeStore(), events: &fakeEvents{access: Visible}, account: &fakeAccount{},
		seq: &memDB{seq: 100}}
	w.hs = &synapsecfg.Config{ServerName: "hs.test", Experimental: map[string]any{}}
	w.hs.Writers.Receipts = []string{"gw"}
	app, err := New(context.Background(), Options{
		Instance: "gw", Synapse: func() *synapsecfg.Config { return w.hs },
		Auth:  fakeAuth{clientauth.Verdict{Valid: true, UserID: "@me:hs.test", DeviceID: "D"}},
		Store: w.store, Events: w.events, AccountData: w.account, Pool: w.seq,
		OnReceipt: func(r Row) { w.rows = append(w.rows, r) },
		Fallback:  http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(299) }),
		Log:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	w.app = app
	return w
}

func (w *world) post(t *testing.T, path, body string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	w.app.Handler().ServeHTTP(rec, httptest.NewRequest("POST", path, strings.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

const room = "!r:hs.test"

func receiptPath(rtype, event string) string {
	return "/_matrix/client/v3/rooms/%21r%3Ahs.test/receipt/" + rtype + "/" + strings.ReplaceAll(event, "$", "%24")
}

func TestMatch(t *testing.T) {
	for p, want := range map[string]bool{
		receiptPath("m.read", "$a"):                         true,
		"/_matrix/client/r0/rooms/!r:x/receipt/m.read/$a":   true,
		"/_matrix/client/unstable/rooms/!r:x/read_markers":  true,
		"/_matrix/client/api/v1/rooms/!r:x/read_markers":    false, // no v1
		"/_matrix/client/v3/rooms/!r:x/receipt/m.read":      false,
		"/_matrix/client/v3/rooms/!r:x/read_markers/x":      false,
		"/_matrix/client/v3/rooms/!r:x/receipt/m.read/$a/x": false,
	} {
		if got := Match("POST", p); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
	if Match("PUT", receiptPath("m.read", "$a")) {
		t.Error("PUT matched")
	}
}

func TestReceipt(t *testing.T) {
	w := newWorld(t)
	if code, body := w.post(t, receiptPath("m.read", "$a"), `{}`); code != 200 || len(body) != 0 {
		t.Fatalf("%d %v", code, body)
	}
	r := w.store.rows[room+"|m.read|@me:hs.test|<null>"]
	if r.EventID != "$a" || !strings.HasPrefix(r.Data, `{"ts":`) || w.store.graph != 1 || len(w.rows) != 1 {
		t.Fatalf("row %+v graph %d sender %v", r, w.store.graph, w.rows)
	}
	// An older event: nothing stored, but an ID spent.
	before := w.app.ids.Position()
	w.store.events["$z"] = 5
	if code, _ := w.post(t, receiptPath("m.read", "$z"), `{}`); code != 200 {
		t.Fatal(code)
	}
	if w.store.rows[room+"|m.read|@me:hs.test|<null>"].EventID != "$a" || w.app.ids.Position() != before+1 || len(w.rows) != 1 {
		t.Fatalf("an older receipt was taken: %+v, position %d", w.store.rows, w.app.ids.Position())
	}
	// A later one replaces it.
	w.post(t, receiptPath("m.read", "$b"), `{}`)
	if w.store.rows[room+"|m.read|@me:hs.test|<null>"].EventID != "$b" || len(w.rows) != 2 {
		t.Fatal("not replaced")
	}
}

func TestReceiptErrors(t *testing.T) {
	for _, c := range []struct {
		name, path, body string
		setup            func(*world)
		code             int
		errcode, msg     string
	}{
		{"bad room", "/_matrix/client/v3/rooms/nope/receipt/m.read/%24a", `{}`, nil, 400, "M_UNKNOWN", "A valid room ID and event ID must be specified"},
		{"bad event", receiptPath("m.read", "a"), `{}`, nil, 400, "M_UNKNOWN", "A valid room ID and event ID must be specified"},
		{"bad type", receiptPath("m.nope", "$a"), `{}`, nil, 400, "M_UNKNOWN", "Receipt type must be m.read, m.read.private, m.fully_read"},
		{"not json", receiptPath("m.read", "$a"), `x`, nil, 400, "M_NOT_JSON", "Content not JSON."},
		{"empty thread", receiptPath("m.read", "$a"), `{"thread_id":""}`, nil, 400, "M_INVALID_PARAM", "thread_id field must be a non-empty string"},
		{"thread not a string", receiptPath("m.read", "$a"), `{"thread_id":5}`, nil, 400, "M_INVALID_PARAM", "thread_id field must be a non-empty string"},
		{"thread on fully_read", receiptPath("m.fully_read", "$a"), `{"thread_id":"main"}`, nil, 400, "M_INVALID_PARAM", "thread_id is not compatible with m.fully_read receipts."},
		{"wrong thread", receiptPath("m.read", "$a"), `{"thread_id":"$root"}`, nil, 400, "M_INVALID_PARAM", "event_id $a is not related to thread $root"},
		{"forbidden", receiptPath("m.read", "$a"), `{}`, func(w *world) { w.events.access = Forbidden }, 403, "M_FORBIDDEN", "You don't have permission to access that event."},
		{"guest", receiptPath("m.read", "$a"), `{}`, func(w *world) {
			w.app.Auth = fakeAuth{clientauth.Verdict{Valid: true, UserID: "@g:hs.test", IsGuest: true}}
		}, 403, "M_GUEST_ACCESS_FORBIDDEN", "Guest access not allowed"},
		{"marker for an unknown event", receiptPath("m.fully_read", "$nope"), `{}`, nil, 404, "M_UNKNOWN", "Could not find event $nope in room !r:hs.test"},
		{"allow_backward off is ignored", receiptPath("m.read", "$a"), `{"com.beeper.allow_backward":"x"}`, nil, 200, "", ""},
		{"allow_backward not a bool", receiptPath("m.read", "$a"), `{"com.beeper.allow_backward":"x"}`, func(w *world) { w.hs.Experimental["msc4446_enabled"] = true },
			400, "M_INVALID_PARAM", "com.beeper.allow_backward must be a boolean."},
		{"allow_backward on m.read", receiptPath("m.read", "$a"), `{"com.beeper.allow_backward":true}`, func(w *world) { w.hs.Experimental["msc4446_enabled"] = true },
			400, "M_INVALID_PARAM", "com.beeper.allow_backward is only allowed to be true for m.fully_read."},
	} {
		w := newWorld(t)
		if c.setup != nil {
			c.setup(w)
		}
		code, body := w.post(t, c.path, c.body)
		if code != c.code || (c.errcode != "" && (body["errcode"] != c.errcode || body["error"] != c.msg)) {
			t.Errorf("%s: got %d %v, want %d %s %q", c.name, code, body, c.code, c.errcode, c.msg)
		}
	}
}

func TestMissingEventIsStill200(t *testing.T) {
	w := newWorld(t)
	w.events.access = Missing
	if code, _ := w.post(t, receiptPath("m.read", "$a"), `{}`); code != 200 || len(w.store.rows) != 0 {
		t.Fatalf("%d %v", code, w.store.rows)
	}
}

func TestThreadedReceipt(t *testing.T) {
	w := newWorld(t)
	w.store.threads["$b"] = "$root"
	if code, body := w.post(t, receiptPath("m.read", "$b"), `{"thread_id":"$root"}`); code != 200 {
		t.Fatal(code, body)
	}
	if code, _ := w.post(t, receiptPath("m.read", "$a"), `{"thread_id":"main"}`); code != 200 {
		t.Fatal(code)
	}
	if _, ok := w.store.rows[room+"|m.read|@me:hs.test|$root"]; !ok {
		t.Fatalf("rows %v", w.store.rows)
	}
	if _, ok := w.store.rows[room+"|m.read|@me:hs.test|main"]; !ok {
		t.Fatalf("rows %v", w.store.rows)
	}
}

func TestReadMarker(t *testing.T) {
	w := newWorld(t)
	path := "/_matrix/client/v3/rooms/%21r%3Ahs.test/read_markers"
	// Both kinds at once, an unknown one ignored.
	if code, body := w.post(t, path, `{"m.fully_read":"$a","m.read":"$a","m.nope":"$a"}`); code != 200 {
		t.Fatal(code, body)
	}
	if len(w.account.calls) != 1 || w.account.calls[0] != `@me:hs.test|!r:hs.test|m.fully_read|{"event_id":"$a"}` {
		t.Fatalf("account data %v", w.account.calls)
	}
	if _, ok := w.store.rows[room+"|m.read|@me:hs.test|<null>"]; !ok {
		t.Fatal("no m.read")
	}
	// Behind the stored marker: not moved.
	w.store.account["@me:hs.test|!r:hs.test|m.fully_read"] = json.RawMessage(`{"event_id":"$b"}`)
	w.post(t, path, `{"m.fully_read":"$a"}`)
	if len(w.account.calls) != 1 {
		t.Fatalf("moved backwards: %v", w.account.calls)
	}
	// MSC4446 allows it.
	w.hs.Experimental["msc4446_enabled"] = true
	w.post(t, path, `{"m.fully_read":"$a","com.beeper.allow_backward":true}`)
	if len(w.account.calls) != 2 {
		t.Fatalf("allow_backward: %v", w.account.calls)
	}
	// The stored marker's event is gone: the new one is ahead.
	w.store.account["@me:hs.test|!r:hs.test|m.fully_read"] = json.RawMessage(`{"event_id":"$gone"}`)
	w.post(t, path, `{"m.fully_read":"$a"}`)
	if len(w.account.calls) != 3 {
		t.Fatalf("gone: %v", w.account.calls)
	}
	// An empty stored marker counts as none.
	w.store.account["@me:hs.test|!r:hs.test|m.fully_read"] = json.RawMessage(`{}`)
	w.post(t, path, `{"m.fully_read":"$a"}`)
	if len(w.account.calls) != 4 {
		t.Fatalf("empty: %v", w.account.calls)
	}
	// A stored marker with no event_id: KeyError, 500.
	w.store.account["@me:hs.test|!r:hs.test|m.fully_read"] = json.RawMessage(`{"x":1}`)
	if code, _ := w.post(t, path, `{"m.fully_read":"$a"}`); code != 500 {
		t.Fatalf("no event_id: %d", code)
	}
	// Falsy values are skipped; a truthy non-string is a 500.
	if code, _ := w.post(t, path, `{"m.read":""}`); code != 200 {
		t.Fatal(code)
	}
	if code, _ := w.post(t, path, `{"m.read":5}`); code != 500 {
		t.Fatalf("non-string: %d", code)
	}
}

func TestRemoteEDU(t *testing.T) {
	ctx := context.Background()
	edu := func(s string) json.RawMessage { return json.RawMessage(s) }
	for _, c := range []struct {
		name    string
		origin  string
		content json.RawMessage
		setup   func(*world)
		stored  int
		err     bool
	}{
		{"stored", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":["$a"],"data":{"ts":1,"thread_id":"$t"}}}}}`), nil, 1, false},
		{"not in room", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":["$a"]}}}}`), func(w *world) { w.store.hostIn = false }, 0, false},
		{"origin not in room", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":["$a"]}}}}`), func(w *world) { w.store.hosts = nil }, 0, false},
		{"user of another server", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:evil.test":{"event_ids":["$a"]}}}}`), nil, 0, false},
		{"non-compliant", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@zé:remote.test":{"event_ids":["$a"]}}}}`), nil, 0, false},
		{"no event_ids", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{}}}}`), nil, 0, true},
		{"empty event_ids", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":[]}}}}`), nil, 0, false},
		{"several events", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":["$a","$b"]}}}}`), nil, 0, true},
		{"data not an object", "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":["$a"],"data":5}}}}`), nil, 0, true},
	} {
		w := newWorld(t)
		if c.setup != nil {
			c.setup(w)
		}
		err := w.app.ReceiveEDU(ctx, c.origin, c.content)
		if (err != nil) != c.err || len(w.store.rows) != c.stored {
			t.Errorf("%s: err=%v rows=%v", c.name, err, w.store.rows)
		}
	}
	// Data re-encoded as Synapse's json_encoder; a string thread_id kept,
	// "" included.
	w := newWorld(t)
	_ = w.app.ReceiveEDU(ctx, "remote.test", edu(`{"!r:hs.test":{"m.read":{"@z:remote.test":{"event_ids":["$a"],"data":{"ts":1.0, "thread_id":"", "x":"é"}}}}}`))
	r, ok := w.store.rows[room+"|m.read|@z:remote.test|"]
	if !ok || r.Data != `{"ts":1.0,"thread_id":"","x":"`+"\\"+`u00e9"}` {
		t.Fatalf("got %+v (%v)", r, w.store.rows)
	}
}

func TestNotWriter(t *testing.T) {
	hs := &synapsecfg.Config{ServerName: "hs.test"}
	hs.Writers.Receipts = []string{"av-edu-worker"}
	proxied := false
	a, err := New(context.Background(), Options{Instance: "gw", Synapse: func() *synapsecfg.Config { return hs },
		Fallback: http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { proxied = true }), Log: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	a.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", receiptPath("m.read", "$a"), strings.NewReader(`{}`)))
	if !proxied || a.Writing() {
		t.Fatal("not proxied")
	}
	if err := a.ReceiveEDU(context.Background(), "x", json.RawMessage(`{}`)); !errors.Is(err, ErrNotWriter) {
		t.Fatal(err)
	}
	// Two writers: refused.
	hs.Writers.Receipts = []string{"gw", "av-edu-worker"}
	if _, err := New(context.Background(), Options{Instance: "gw", Synapse: func() *synapsecfg.Config { return hs }, Log: zerolog.Nop()}); err == nil {
		t.Fatal("started as one of two writers")
	}
}

func TestValidRoomID(t *testing.T) {
	for s, want := range map[string]bool{
		"!a:hs.test": true, "!:hs.test": true, "!a:hs.test:8448": true, "!a:bad host": false,
		"!" + strings.Repeat("A", 43): true, "!" + strings.Repeat("A", 42): false, "a:hs.test": false, "": false,
	} {
		if got := validRoomID(s); got != want {
			t.Errorf("%q: %v", s, got)
		}
	}
}

func TestEncodeRow(t *testing.T) {
	th := "$t"
	got, err := encodeRow("!r:x", "m.read", "@é:x", "$a", &th, `{"ts":1}`)
	if err != nil || string(got) != `["!r:x","m.read","@`+"\\"+`u00e9:x","$a","$t",{"ts":1}]` {
		t.Fatalf("%s %v", got, err)
	}
	got, _ = encodeRow("!r:x", "m.read", "@u:x", "$a", nil, `{}`)
	if string(got) != `["!r:x","m.read","@u:x","$a",null,{}]` {
		t.Fatalf("%s", got)
	}
}
