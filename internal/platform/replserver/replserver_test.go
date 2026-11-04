package replserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newServer(t *testing.T, opts Options) *Server {
	t.Helper()
	opts.Log = zerolog.Nop()
	if opts.Instance == "" {
		opts.Instance = "goworker-1"
	}
	return New(opts)
}

func do(t *testing.T, s *Server, method, target, body string, headers ...string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 && rec.Header().Get("Content-Type") == "application/json" {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: body %q: %v", method, target, rec.Body.String(), err)
		}
	}
	return rec.Code, out
}

func echo(ctx context.Context, req Request) (int, map[string]any, error) {
	return 200, map[string]any{"args": req.Args}, nil
}

func TestHealth(t *testing.T) {
	s := newServer(t, Options{})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 200 || rec.Body.String() != "OK" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestRouting(t *testing.T) {
	s := newServer(t, Options{})
	if err := s.Register(Endpoint{Name: "fed_send_edu", Method: "PUT", PathArgs: []string{"edu_type"}, Cache: true, Handle: echo}); err != nil {
		t.Fatal(err)
	}
	if err := s.Register(Endpoint{Name: "get_repl_stream_updates", Method: "GET", PathArgs: []string{"stream_name"}, Cache: true, Handle: echo}); err != nil {
		t.Fatal(err)
	}
	if err := s.Register(Endpoint{Name: "bare", Method: "PUT", Handle: echo}); err != nil {
		t.Fatal(err)
	}

	code, body := do(t, s, "PUT", "/_synapse/replication/fed_send_edu/m.typing/txn1", `{}`)
	if code != 200 || body["args"].(map[string]any)["edu_type"] != "m.typing" {
		t.Fatalf("got %d %v", code, body)
	}
	// Arguments are percent-decoded, a slash included.
	code, body = do(t, s, "PUT", "/_synapse/replication/fed_send_edu/a%2Fb%20c/txn2", `{}`)
	if code != 200 || body["args"].(map[string]any)["edu_type"] != "a/b c" {
		t.Fatalf("got %d %v", code, body)
	}
	// HEAD is GET.
	if code, _ := do(t, s, "HEAD", "/_synapse/replication/get_repl_stream_updates/typing/txn9?from_token=1&upto_token=2", ""); code != 200 {
		t.Fatalf("HEAD: %d", code)
	}
	if code, _ := do(t, s, "PUT", "/_synapse/replication/bare/", `{}`); code != 200 {
		t.Fatalf("NAME/: %d", code)
	}

	for _, c := range []struct {
		method, path string
		code         int
	}{
		{"PUT", "/_synapse/replication/nope/x/y", 404},
		{"PUT", "/_synapse/replication/fed_send_edu/m.typing", 404},         // no txn_id
		{"PUT", "/_synapse/replication/fed_send_edu/m.typing/t/extra", 404}, // too many
		{"PUT", "/_synapse/replication/fed_send_edu//txn", 404},             // empty segment
		{"POST", "/_synapse/replication/fed_send_edu/m.typing/txn3", 405},
		{"GET", "/_synapse/other", 404},
		{"PUT", "/_synapse/replication/bare", 404}, // NAME/ needs its slash
	} {
		code, body := do(t, s, c.method, c.path, `{}`)
		if code != c.code || body["errcode"] != "M_UNRECOGNIZED" || body["error"] != "Unrecognized request" {
			t.Errorf("%s %s: got %d %v, want %d M_UNRECOGNIZED", c.method, c.path, code, body, c.code)
		}
	}
}

func TestRegisterRefuses(t *testing.T) {
	s := newServer(t, Options{})
	for name, e := range map[string]Endpoint{
		"no handler":       {Name: "a", Method: "PUT"},
		"bad method":       {Name: "a", Method: "PATCH", Handle: echo},
		"slash in name":    {Name: "a/b", Method: "PUT", Handle: echo},
		"reserved arg":     {Name: "a", Method: "PUT", PathArgs: []string{"instance_name"}, Handle: echo},
		"waits, no Waiter": {Name: "a", Method: "PUT", WaitForStreams: true, Handle: echo},
	} {
		if err := s.Register(e); err == nil {
			t.Errorf("%s: registered", name)
		}
	}
	// A GET that waits for streams needs no Waiter: a GET carries no body.
	if err := s.Register(Endpoint{Name: "g", Method: "GET", WaitForStreams: true, Handle: echo}); err != nil {
		t.Fatal(err)
	}
	if err := s.Register(Endpoint{Name: "g", Method: "GET", Handle: echo}); err == nil {
		t.Fatal("registered a name twice")
	}
}

func TestAuth(t *testing.T) {
	secret := "s3cret"
	s := newServer(t, Options{Secret: func() string { return secret }})
	if err := s.Register(Endpoint{Name: "e", Method: "POST", Handle: echo}); err != nil {
		t.Fatal(err)
	}
	const path = "/_synapse/replication/e/"
	for name, headers := range map[string][]string{
		"missing":      nil,
		"two":          {"Authorization", "Bearer s3cret", "Authorization", "Bearer s3cret"},
		"wrong secret": {"Authorization", "Bearer nope"},
		"wrong scheme": {"Authorization", "Basic s3cret"},
		"extra part":   {"Authorization", "Bearer s3cret x"},
	} {
		code, body := do(t, s, "POST", path, `{}`, headers...)
		if code != 500 || body["errcode"] != "M_UNKNOWN" || body["error"] != "Internal server error" {
			t.Errorf("%s: got %d %v, want Synapse's RuntimeError 500", name, code, body)
		}
	}
	if code, _ := do(t, s, "POST", path, `{}`, "Authorization", "Bearer s3cret"); code != 200 {
		t.Fatalf("right secret: %d", code)
	}
	// Routing comes first: an unknown path is 404 whatever the header.
	if code, _ := do(t, s, "POST", "/_synapse/replication/nope/", `{}`); code != 404 {
		t.Fatalf("unknown path: %d", code)
	}
	// With no secret, Synapse checks nothing.
	secret = ""
	if code, _ := do(t, s, "POST", path, `{}`); code != 200 {
		t.Fatalf("no secret set: %d", code)
	}
}

func TestBody(t *testing.T) {
	s := newServer(t, Options{MaxBody: 64})
	var got Request
	if err := s.Register(Endpoint{Name: "e", Method: "POST", Handle: func(_ context.Context, req Request) (int, map[string]any, error) {
		got = req
		return 200, map[string]any{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Register(Endpoint{Name: "g", Method: "GET", Handle: func(_ context.Context, req Request) (int, map[string]any, error) {
		got = req
		return 200, map[string]any{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for body, want := range map[string]string{
		`not json`: "M_NOT_JSON",
		``:         "M_NOT_JSON",
		`[1]`:      "M_BAD_JSON",
		`"x"`:      "M_BAD_JSON",
		`{"a":"` + strings.Repeat("x", 100) + `"}`: "M_TOO_LARGE",
	} {
		code, out := do(t, s, "POST", "/_synapse/replication/e/", body)
		if out["errcode"] != want {
			t.Errorf("body %.20q: got %d %v, want %s", body, code, out, want)
		}
	}
	if code, _ := do(t, s, "POST", "/_synapse/replication/e/", ` {"a":1} `); code != 200 || string(got.Content["a"]) != "1" {
		t.Fatalf("object: %d %v", code, got.Content)
	}
	// A GET's body is ignored; its parameters are the query's.
	if code, _ := do(t, s, "GET", "/_synapse/replication/g/?from_token=3", `not json`); code != 200 ||
		len(got.Content) != 0 || got.Query.Get("from_token") != "3" {
		t.Fatalf("GET: %d %v %v", code, got.Content, got.Query)
	}
}

func TestHandlerErrors(t *testing.T) {
	s := newServer(t, Options{})
	_ = s.Register(Endpoint{Name: "synapse", Method: "POST", Handle: func(context.Context, Request) (int, map[string]any, error) {
		return 0, nil, &Error{Code: 400, Errcode: "M_UNKNOWN", Msg: "Unknown stream"}
	}})
	_ = s.Register(Endpoint{Name: "other", Method: "POST", Handle: func(context.Context, Request) (int, map[string]any, error) {
		return 0, nil, errors.New("database on fire")
	}})
	_ = s.Register(Endpoint{Name: "reserved", Method: "POST", Handle: func(context.Context, Request) (int, map[string]any, error) {
		return 200, map[string]any{StreamPositionKey: 1}, nil
	}})
	if code, body := do(t, s, "POST", "/_synapse/replication/synapse/", `{}`); code != 400 || body["error"] != "Unknown stream" {
		t.Errorf("SynapseError: %d %v", code, body)
	}
	for _, name := range []string{"other", "reserved"} {
		if code, body := do(t, s, "POST", "/_synapse/replication/"+name+"/", `{}`); code != 500 || body["error"] != "Internal server error" {
			t.Errorf("%s: %d %v", name, code, body)
		}
	}
}

type fakeWaiter struct {
	mu    sync.Mutex
	waits []string
}

func (f *fakeWaiter) WaitForStreamPosition(_ context.Context, instance, stream string, position int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waits = append(f.waits, instance+"/"+stream+"/"+itoa(position))
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

func TestStreamPositions(t *testing.T) {
	w := &fakeWaiter{}
	s := newServer(t, Options{
		Waiter:    w,
		Positions: func() map[string]int64 { return map[string]int64{"typing": 7, "caches": 42} },
	})
	_ = s.Register(Endpoint{Name: "waits", Method: "POST", WaitForStreams: true, Handle: func(context.Context, Request) (int, map[string]any, error) {
		return 200, map[string]any{"ok": true}, nil
	}})
	_ = s.Register(Endpoint{Name: "nowait", Method: "GET", Handle: func(context.Context, Request) (int, map[string]any, error) {
		return 200, map[string]any{"ok": true}, nil
	}})

	code, body := do(t, s, "POST", "/_synapse/replication/waits/",
		`{"_INT_STREAM_POS":{"instance_name":"main","streams":{"events":5}}}`)
	if code != 200 || body["ok"] != true {
		t.Fatalf("got %d %v", code, body)
	}
	pos, _ := body[StreamPositionKey].(map[string]any)
	if pos["typing"] != float64(7) || pos["caches"] != float64(42) {
		t.Errorf("our positions: %v", body[StreamPositionKey])
	}
	if len(w.waits) != 1 || w.waits[0] != "main/events/5" {
		t.Errorf("waits: %v", w.waits)
	}

	// Positions under our own name are not waited for.
	do(t, s, "POST", "/_synapse/replication/waits/",
		`{"_INT_STREAM_POS":{"instance_name":"goworker-1","streams":{"events":9}}}`)
	if len(w.waits) != 1 {
		t.Errorf("waited for our own positions: %v", w.waits)
	}

	// An endpoint that does not wait for streams carries none of ours.
	if _, body := do(t, s, "GET", "/_synapse/replication/nowait/", ``); body[StreamPositionKey] != nil {
		t.Errorf("positions on a non-waiting endpoint: %v", body)
	}

	// A malformed key is an exception in Synapse: 500.
	if code, _ := do(t, s, "POST", "/_synapse/replication/waits/", `{"_INT_STREAM_POS":"x"}`); code != 500 {
		t.Errorf("malformed positions: %d", code)
	}
}

func TestPositionsWithNoStreams(t *testing.T) {
	s := newServer(t, Options{Waiter: &fakeWaiter{}})
	_ = s.Register(Endpoint{Name: "waits", Method: "POST", WaitForStreams: true, Handle: func(context.Context, Request) (int, map[string]any, error) {
		return 200, nil, nil
	}})
	_, body := do(t, s, "POST", "/_synapse/replication/waits/", `{}`)
	if pos, ok := body[StreamPositionKey].(map[string]any); !ok || len(pos) != 0 {
		t.Fatalf("want an empty %s, got %v", StreamPositionKey, body)
	}
}

func TestCache(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	var calls atomic.Int32
	fail := false
	s := New(Options{Instance: "g", Log: zerolog.Nop(), now: func() time.Time { return now }})
	_ = s.Register(Endpoint{Name: "c", Method: "PUT", Cache: true, Handle: func(context.Context, Request) (int, map[string]any, error) {
		n := calls.Add(1)
		if fail {
			return 0, nil, errors.New("boom")
		}
		return 200, map[string]any{"n": n}, nil
	}})
	put := func(txn string) (int, map[string]any) {
		return do(t, s, "PUT", "/_synapse/replication/c/"+txn, `{}`)
	}

	_, a := put("t1")
	_, b := put("t1")
	if calls.Load() != 1 || a["n"] != b["n"] {
		t.Fatalf("a retry was handled again: calls=%d %v %v", calls.Load(), a, b)
	}
	// Another txn_id is another request.
	put("t2")
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	// After 30 minutes the answer is gone.
	now = now.Add(CacheTimeout + time.Second)
	put("t1")
	if calls.Load() != 3 {
		t.Fatalf("an expired answer was reused: calls=%d", calls.Load())
	}
	// A failure is not kept.
	fail = true
	if code, _ := put("t3"); code != 500 {
		t.Fatalf("failure: %d", code)
	}
	fail = false
	if code, _ := put("t3"); code != 200 || calls.Load() != 5 {
		t.Fatalf("a failure was cached: %d calls=%d", code, calls.Load())
	}
}

func TestCacheSharesARunningRequest(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var calls atomic.Int32
	s := newServer(t, Options{})
	_ = s.Register(Endpoint{Name: "c", Method: "PUT", Cache: true, Handle: func(context.Context, Request) (int, map[string]any, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return 200, map[string]any{"ok": true}, nil
	}})
	var wg sync.WaitGroup
	codes := make([]int, 3)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i], _ = do(t, s, "PUT", "/_synapse/replication/c/same", `{}`)
		}()
	}
	<-started
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("handled %d times", calls.Load())
	}
	for i, c := range codes {
		if c != 200 {
			t.Errorf("request %d: %d", i, c)
		}
	}
}

func TestCacheIsBounded(t *testing.T) {
	c := newResponseCache(2000, time.Now)
	big := map[string]json.RawMessage{"x": json.RawMessage(`"` + strings.Repeat("a", 600) + `"`)}
	for _, txn := range []string{"1", "2", "3", "4"} {
		_, _, _, _ = c.wrap(context.Background(), "e", txn, func() (int, map[string]json.RawMessage, error) {
			return 200, big, nil
		})
	}
	if c.bytes > 2000 {
		t.Fatalf("holds %d bytes", c.bytes)
	}
	if _, ok := c.entries[cacheKey{"e", "1"}]; ok {
		t.Fatal("the oldest answer was kept")
	}
	if _, ok := c.entries[cacheKey{"e", "4"}]; !ok {
		t.Fatal("the newest answer was evicted")
	}
}

func TestHandlerOutlivesTheCaller(t *testing.T) {
	done := make(chan error, 1)
	s := newServer(t, Options{})
	_ = s.Register(Endpoint{Name: "e", Method: "POST", Handle: func(ctx context.Context, _ Request) (int, map[string]any, error) {
		time.Sleep(20 * time.Millisecond)
		done <- ctx.Err()
		return 200, nil, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/_synapse/replication/e/", strings.NewReader(`{}`)).WithContext(ctx)
	cancel()
	s.ServeHTTP(httptest.NewRecorder(), req)
	if err := <-done; err != nil {
		t.Fatalf("the handler's context was cancelled with its caller: %v", err)
	}
}

var _ http.Handler = (*Server)(nil)
