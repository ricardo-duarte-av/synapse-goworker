package todevice

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

type remoteRow struct {
	id         int64
	dest, json string
}

type fakeStore struct {
	mu       sync.Mutex
	devices  map[string][]string // user -> visible devices
	local    map[int64][]UserMessages
	remote   []remoteRow
	inbox    map[string]bool // origin|message_id
	ids      []int64
	shares   bool
	cached   map[string]bool
	stale    []string
	exempt   bool
	failNext error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		devices: map[string][]string{"@bob:hs.test": {"B1", "B2"}, "@me:hs.test": {"D"}},
		local:   map[int64][]UserMessages{}, inbox: map[string]bool{}, cached: map[string]bool{},
	}
}

// deliver is what addLocalTxn would insert: user|device -> message JSON.
func (f *fakeStore) deliver(users []UserMessages) (map[string]string, error) {
	out := map[string]string{}
	for _, u := range users {
		devs := f.devices[u.UserID]
		if len(u.Devices) == 1 && u.Devices[0] == "*" {
			for _, d := range devs {
				if u.ByID["*"].NotObject {
					return nil, errNotObject
				}
				out[u.UserID+"|"+d] = u.ByID["*"].JSON
			}
			continue
		}
		for _, d := range u.Devices {
			for _, have := range devs {
				if have == d {
					if u.ByID[d].NotObject {
						return nil, errNotObject
					}
					out[u.UserID+"|"+d] = u.ByID[d].JSON
				}
			}
		}
	}
	return out, nil
}

func (f *fakeStore) AddLocal(_ context.Context, _ string, id int64, users []UserMessages) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	if _, err := f.deliver(users); err != nil {
		return err
	}
	f.local[id] = users
	return nil
}

func (f *fakeStore) AddRemote(_ context.Context, _ string, id int64, dest, edu string, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.remote = append(f.remote, remoteRow{id, dest, edu})
	return nil
}

func (f *fakeStore) AddFromRemote(_ context.Context, _ string, id int64, origin, msgID string, users []UserMessages, _ int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	if f.inbox[origin+"|"+msgID] {
		return false, nil
	}
	if _, err := f.deliver(users); err != nil {
		return false, err
	}
	f.inbox[origin+"|"+msgID] = true
	f.local[id] = users
	return true, nil
}

func (f *fakeStore) SharesRoom(context.Context, string) (bool, error) { return f.shares, nil }
func (f *fakeStore) CachedDevices(context.Context, string) (map[string]bool, error) {
	return f.cached, nil
}
func (f *fakeStore) MarkStale(_ context.Context, u string, _ int64) error {
	f.stale = append(f.stale, u)
	return nil
}
func (f *fakeStore) RatelimitExempt(context.Context, string) (bool, error) { return f.exempt, nil }
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

type fakeAuth struct{ v clientauth.Verdict }

func (f fakeAuth) Authenticate(context.Context, *http.Request) (clientauth.Verdict, error) {
	return f.v, nil
}

type world struct {
	app     *App
	store   *fakeStore
	dests   [][]string
	resyncs chan string
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{store: newFakeStore(), resyncs: make(chan string, 4)}
	hs := &synapsecfg.Config{ServerName: "hs.test", Experimental: map[string]any{}}
	hs.Writers.ToDevice = []string{"gw"}
	hs.RcKeyRequests = synapsecfg.Ratelimit{Key: "rc_key_requests", PerSecond: 20, Burst: 100}
	n := 0
	app, err := New(context.Background(), Options{
		Instance: "gw", Synapse: func() *synapsecfg.Config { return hs },
		Auth:           fakeAuth{clientauth.Verdict{Valid: true, UserID: "@me:hs.test", DeviceID: "D", TokenID: 7}},
		Store:          w.store,
		Pool:           &memDB{seq: 100},
		OnDestinations: func(d []string) { w.dests = append(w.dests, d) },
		Resync:         func(_ context.Context, u string) error { w.resyncs <- u; return nil },
		Fallback:       http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(299) }),
		Log:            zerolog.Nop(),
		Now:            func() time.Time { return time.Unix(1_700_000_000, 0) },
		MessageID:      func() string { n++; return "MSGID" + strings.Repeat("x", 10) + string(rune('a'+n)) },
	})
	if err != nil {
		t.Fatal(err)
	}
	w.app = app
	return w
}

func (w *world) put(t *testing.T, txn, body string) (int, string) {
	t.Helper()
	path := "/_matrix/client/v3/sendToDevice/m.room.encrypted/" + txn
	if !Match("PUT", path) {
		t.Fatal("no match")
	}
	rec := httptest.NewRecorder()
	w.app.Handler().ServeHTTP(rec, httptest.NewRequest("PUT", path, strings.NewReader(body)))
	return rec.Code, rec.Body.String()
}

func TestMatch(t *testing.T) {
	for p, want := range map[string]bool{
		"/_matrix/client/v3/sendToDevice/m.x/1":       true,
		"/_matrix/client/r0/sendToDevice/m.x/1":       true,
		"/_matrix/client/unstable/sendToDevice/m.x/1": true,
		"/_matrix/client/api/v1/sendToDevice/m.x/1":   false,
		"/_matrix/client/v3/sendToDevice/m.x":         false,
		"/_matrix/client/v3/sendToDevice/m.x/1/":      false,
		"/_matrix/client/v3/sendToDevice//":           true,
	} {
		if got := Match("PUT", p); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
	if Match("POST", "/_matrix/client/v3/sendToDevice/m.x/1") {
		t.Error("POST matched")
	}
}

func TestSendLocalAndRemote(t *testing.T) {
	w := newWorld(t)
	code, body := w.put(t, "t1", `{"messages": {
		"@bob:hs.test": {"B1": {"ciphertext": "é", "n": 1.0}, "NOPE": {"x": 1}},
		"@me:hs.test": {"*": {"k": "v"}},
		"@carol:remote.test": {"C": {"a": 1}},
		"@dave:remote.test": {},
		"not a user": {"X": {}},
		"@eve:other.test": {"E": {"b": 2}}
	}}`)
	if code != 200 || body != "{}" {
		t.Fatalf("%d %s", code, body)
	}
	// One ID for the local messages, one per EDU, in that order.
	if got := w.store.ids; len(got) != 3 || got[0] != 101 || got[1] != 102 || got[2] != 103 {
		t.Fatalf("ids %v", got)
	}
	got, _ := w.store.deliver(w.store.local[101])
	esc := "\\" + "u00e9"
	want := map[string]string{
		"@bob:hs.test|B1": `{"content":{"ciphertext":"` + esc + `","n":1.0},"type":"m.room.encrypted","sender":"@me:hs.test"}`,
		"@me:hs.test|D":   `{"content":{"k":"v"},"type":"m.room.encrypted","sender":"@me:hs.test"}`,
	}
	if len(got) != len(want) {
		t.Fatalf("delivered %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %s, want %s", k, got[k], v)
		}
	}
	if len(w.store.remote) != 2 || w.store.remote[0].dest != "remote.test" || w.store.remote[1].dest != "other.test" {
		t.Fatalf("remote %+v", w.store.remote)
	}
	edu := w.store.remote[0].json
	wantEDU := `{"sender":"@me:hs.test","type":"m.room.encrypted","message_id":"MSGIDxxxxxxxxxxb","messages":` +
		`{"@carol:remote.test":{"C":{"a":1}},"@dave:remote.test":{}},"org.matrix.opentracing_context":"{}"}`
	if edu != wantEDU {
		t.Errorf("EDU\n got  %s\n want %s", edu, wantEDU)
	}
	if len(w.dests) != 1 || strings.Join(w.dests[0], ",") != "remote.test,other.test" {
		t.Errorf("destinations %v", w.dests)
	}
}

func TestSendErrorsAndTxns(t *testing.T) {
	w := newWorld(t)
	for _, c := range []struct {
		body string
		code int
		want string
	}{
		{`{}`, 400, `"Missing params: ['messages']"`},
		{`{"messages": []}`, 500, "M_UNKNOWN"},
		{`{"messages": {"@bob:hs.test": []}}`, 500, "M_UNKNOWN"},
		{`{"messages": {"@bob:hs.test": {"B1": "string"}}}`, 500, "M_UNKNOWN"},
		{`{"messages": {"@bob:hs.test": {"B1": {"x": "` + strings.Repeat("a", 65536) + `"}}}}`, 413,
			`"To-device message for @bob:hs.test:B1 is too large to send (65544 > 65536)"`},
		{`{"messages": {"@bob:hs.test": {"B1": {"n": 1e400}}}}`, 500, "M_UNKNOWN"},
	} {
		code, body := w.put(t, "e", c.body)
		if code != c.code || !strings.Contains(body, c.want) {
			t.Errorf("%s: %d %s", c.body[:min(len(c.body), 60)], code, body)
		}
	}
	// Nothing was stored, and no error was cached: the same txn runs again.
	if len(w.store.ids) != 0 {
		t.Fatalf("ids %v", w.store.ids)
	}
	if code, _ := w.put(t, "e", `{"messages": {"@bob:hs.test": {"B1": {}}}}`); code != 200 {
		t.Fatal(code)
	}
	// A repeat is answered from the cache, and stores nothing.
	if code, body := w.put(t, "e", `{"messages": {"@bob:hs.test": {"B1": {"other": 1}}}}`); code != 200 || body != "{}" {
		t.Fatal(code, body)
	}
	if len(w.store.ids) != 1 {
		t.Errorf("repeat stored: %v", w.store.ids)
	}
}

func TestKeyRequestRatelimit(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	v := clientauth.Verdict{UserID: "@me:hs.test", DeviceID: "D"}
	send := func(to string) int {
		before := len(w.store.local)
		msgs := gjson.Parse(`{"` + to + `": {"B1": {"action": "request"}}}`)
		if err := w.app.Send(ctx, v, RoomKeyRequest, msgs); err != nil {
			t.Fatal(err)
		}
		last := w.store.local[w.store.ids[len(w.store.ids)-1]]
		_ = before
		return len(last)
	}
	for i := range 100 {
		if send("@bob:hs.test") != 1 {
			t.Fatalf("request %d dropped", i)
		}
	}
	if send("@bob:hs.test") != 0 {
		t.Error("the 101st was not dropped")
	}
	// To oneself is not limited.
	if send("@me:hs.test") != 1 {
		t.Error("a request to oneself was dropped")
	}
	// A user exempt by ratelimit_override is not limited.
	w.store.exempt = true
	if send("@bob:hs.test") != 1 {
		t.Error("an exempt user was limited")
	}
}

func TestReceiveEDU(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	edu := func(s string) error { return w.app.ReceiveEDU(ctx, "remote.test", json.RawMessage(s)) }
	ok := `{"sender":"@x:remote.test","type":"m.room.encrypted","message_id":"m1","messages":{"@bob:hs.test":{"B2":{"c":1}}}}`
	if err := edu(ok); err != nil {
		t.Fatal(err)
	}
	got, _ := w.store.deliver(w.store.local[101])
	if got["@bob:hs.test|B2"] != `{"content":{"c":1},"type":"m.room.encrypted","sender":"@x:remote.test"}` {
		t.Errorf("%v", got)
	}
	// The same message_id again: an ID is spent, nothing is stored.
	if err := edu(ok); err != nil {
		t.Fatal(err)
	}
	if len(w.store.ids) != 2 || len(w.store.local) != 1 {
		t.Errorf("repeat: ids %v local %d", w.store.ids, len(w.store.local))
	}
	// Spoofed and non-compliant senders are dropped without an error.
	for _, s := range []string{
		`{"sender":"@x:elsewhere.test","type":"t","message_id":"m2","messages":{}}`,
		`{"sender":"@X Y:remote.test","type":"t","message_id":"m3","messages":{}}`,
	} {
		if err := edu(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	// Errors Synapse raises.
	for _, s := range []string{
		`{"type":"t","message_id":"m","messages":{}}`,
		`{"sender":"@x:remote.test","message_id":"m","messages":{}}`,
		`{"sender":"@x:remote.test","type":"t","message_id":"m","messages":{"@bob:elsewhere.test":{"B":{}}}}`,
		`{"sender":"@x:remote.test","type":"t","message_id":"m","messages":{"bob":{"B":{}}}}`,
		`{"sender":"@x:remote.test","type":"t","message_id":5,"messages":{}}`,
	} {
		if err := edu(s); err == nil {
			t.Errorf("%s: no error", s)
		}
	}
	var serr *replserver.Error
	if err := edu(`{"sender":"@x:remote.test","type":"t","message_id":"m","messages":{"@bob:elsewhere.test":{"B":{}}}}`); !errors.As(err, &serr) || serr.Code != 400 {
		t.Errorf("not ours: %v", err)
	}
}

func TestUnknownDeviceResync(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	w.store.shares = true
	w.store.cached = map[string]bool{"KNOWN": true}
	req := func(dev string) error {
		return w.app.ReceiveEDU(ctx, "remote.test", json.RawMessage(`{"sender":"@x:remote.test","type":"m.room_key_request",
			"message_id":"`+dev+`","messages":{"@bob:hs.test":{"B1":{"action":"request","requesting_device_id":"`+dev+`"}}}}`))
	}
	if err := req("KNOWN"); err != nil {
		t.Fatal(err)
	}
	if len(w.store.stale) != 0 {
		t.Fatalf("stale %v", w.store.stale)
	}
	if err := req("NEW"); err != nil {
		t.Fatal(err)
	}
	if len(w.store.stale) != 1 || w.store.stale[0] != "@x:remote.test" {
		t.Fatalf("stale %v", w.store.stale)
	}
	select {
	case u := <-w.resyncs:
		if u != "@x:remote.test" {
			t.Error(u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no resync")
	}
	// No shared room: nothing.
	w.store.shares = false
	w.store.stale = nil
	if err := req("OTHER"); err != nil || len(w.store.stale) != 0 {
		t.Errorf("%v %v", err, w.store.stale)
	}
}

// testdata/split.jsonl: split_device_messages_into_edus's partitions,
// from Synapse 1.164.1 itself.
func TestSplitMatchesSynapse(t *testing.T) {
	w := newWorld(t)
	f, err := os.Open("testdata/split.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 16<<20)
	n := 0
	for sc.Scan() {
		var c struct {
			Sender, Type string
			Messages     json.RawMessage
			Parts        [][][2]json.RawMessage
		}
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		edus, err := w.app.splitEDUs(c.Sender, c.Type, members(gjson.ParseBytes(c.Messages)))
		if err != nil {
			t.Fatal(err)
		}
		var got [][][2]string
		for _, e := range edus {
			var part [][2]string
			gjson.Get(e, "messages").ForEach(func(u, d gjson.Result) bool {
				devs := []string{}
				d.ForEach(func(k, _ gjson.Result) bool { devs = append(devs, k.Str); return true })
				b, _ := json.Marshal(devs)
				part = append(part, [2]string{u.Str, string(b)})
				return true
			})
			got = append(got, part)
		}
		var want [][][2]string
		for _, p := range c.Parts {
			var part [][2]string
			for _, ud := range p {
				var u string
				_ = json.Unmarshal(ud[0], &u)
				devs := []string{}
				_ = json.Unmarshal(ud[1], &devs)
				b, _ := json.Marshal(devs)
				part = append(part, [2]string{u, string(b)})
			}
			want = append(want, part)
		}
		gj, _ := json.Marshal(got)
		wj, _ := json.Marshal(want)
		if string(gj) != string(wj) {
			t.Errorf("case %d (%s):\n got  %.300s\n want %.300s", n, c.Sender, gj, wj)
		}
		n++
	}
	if n == 0 {
		t.Fatal("no cases")
	}
}
