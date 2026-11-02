package fedping

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedauth"
)

// network delivers matrix-federation:// requests to the named server's
// handler, in memory, and serves every server's published keys.
type network struct {
	mu       sync.Mutex
	handlers map[string]http.Handler
	keys     map[string][]byte // server -> /key/v2/server response
	down     map[string]bool
}

func (n *network) RoundTrip(req *http.Request) (*http.Response, error) {
	n.mu.Lock()
	h, down, keys := n.handlers[req.URL.Host], n.down[req.URL.Host], n.keys[req.URL.Host]
	n.mu.Unlock()
	if down || h == nil {
		return nil, errors.New("dial tcp: connection refused")
	}
	rec := httptest.NewRecorder()
	if strings.HasPrefix(req.URL.Path, "/_matrix/key/v2/server") {
		rec.Header().Set("Content-Type", "application/json")
		_, _ = rec.Write(keys)
		return rec.Result(), nil
	}
	sreq := httptest.NewRequest(req.Method, req.URL.RequestURI(), req.Body)
	sreq.Header = req.Header.Clone()
	sreq.Host = req.URL.Host
	h.ServeHTTP(rec, sreq)
	return rec.Result(), nil
}

// GetServerKeys is every verifier's key store: what each server publishes.
func (n *network) GetServerKeys(_ context.Context, server string, _ int64) ([]fedauth.ServerKeyJSON, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if raw, ok := n.keys[server]; ok {
		return []fedauth.ServerKeyJSON{{JSON: raw}}, nil
	}
	return nil, nil
}

func (n *network) add(t *testing.T, name string) *Server {
	t.Helper()
	key := federation.GenerateSigningKey()
	raw, err := json.Marshal(key.GenerateKeyResponse(name, nil))
	if err != nil {
		t.Fatal(err)
	}
	client := federation.NewClient(name, key, federation.NewInMemoryCache(), exhttp.SensibleClientSettings)
	client.HTTP = &http.Client{Transport: n}
	s := New(Options{
		ServerName: name,
		Verifier:   fedauth.New(name, fedauth.Options{DB: n}),
		Client:     client,
		PongDelay:  func() time.Duration { return 10 * time.Millisecond },
		Log:        zerolog.Nop(),
	})
	t.Cleanup(s.Close)
	n.mu.Lock()
	n.handlers[name] = s
	n.keys[name] = raw
	n.mu.Unlock()
	return s
}

func newNetwork() *network {
	return &network{handlers: map[string]http.Handler{}, keys: map[string][]byte{}, down: map[string]bool{}}
}

func TestMatch(t *testing.T) {
	for p, want := range map[string]bool{
		"/_matrix/federation/v1/ping":                                       true,
		"/_matrix/federation/v1/pong/":                                      true,
		"/_matrix/federation/unstable/uk.timedout.msc4524.tabletennis/ping": true,
		"/_matrix/federation/unstable/uk.timedout.msc4524.tabletennis/pong": true,
		"/_matrix/federation/v1/pings":                                      false,
		"/_matrix/federation/v1/ping/x":                                     false,
		"/_matrix/federation/v2/ping":                                       false,
	} {
		if Match(p) != want {
			t.Errorf("Match(%q) = %v, want %v", p, !want, want)
		}
	}
}

// The whole exchange: a signed ping answered, then the answer confirmed by a
// signed pong.
func TestPingPong(t *testing.T) {
	n := newNetwork()
	a := n.add(t, "a.example")
	n.add(t, "b.example")

	rep := a.Initiate(context.Background(), "b.example", 5*time.Second)
	if !rep.Answered || !rep.Ponged {
		t.Fatalf("report = %+v, want answered and ponged", rep)
	}
	if len(rep.Attempts) != 1 || !rep.Attempts[0].Authenticated || rep.Attempts[0].Status != 200 {
		t.Errorf("attempts = %+v, want one signed attempt answered 200", rep.Attempts)
	}
	if !strings.Contains(rep.Conclusion, "both ways") {
		t.Errorf("conclusion = %q", rep.Conclusion)
	}
}

// A refused signature is retried unsigned, naming us; the answer then says
// what the destination sees of our keys.
func TestUnsignedRetryAfterRefusedSignature(t *testing.T) {
	n := newNetwork()
	a := n.add(t, "a.example")
	n.add(t, "b.example")
	// b sees keys for a that are not the ones a signs with.
	other := federation.GenerateSigningKey()
	raw, _ := json.Marshal(other.GenerateKeyResponse("a.example", nil))
	n.mu.Lock()
	n.keys["a.example"] = raw
	n.mu.Unlock()

	rep := a.Initiate(context.Background(), "b.example", 300*time.Millisecond)
	if len(rep.Attempts) != 2 || rep.Attempts[0].Status != 401 || rep.Attempts[1].Authenticated {
		t.Fatalf("attempts = %+v, want a refused signed ping then an unsigned one", rep.Attempts)
	}
	if !rep.Answered {
		t.Fatalf("the unsigned ping was not answered: %+v", rep)
	}
	if d := rep.Attempts[1].Details; len(d) != 1 || !strings.Contains(d[0], "a.example") || !strings.Contains(d[0], other.ID.String()) {
		t.Errorf("details = %q, want the key IDs b fetched for a.example", d)
	}
	// b's pong is signed with b's key, which a verifies; a.example's
	// handler matches it.
	if !rep.Ponged {
		t.Errorf("no pong: %+v", rep)
	}
}

func post(s *Server, path, auth string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func errcode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		ErrCode string `json:"errcode"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	return e.ErrCode
}

func TestPingValidation(t *testing.T) {
	n := newNetwork()
	b := n.add(t, "b.example")
	const path = "/_matrix/federation/v1/ping"
	cases := []struct {
		body    string
		status  int
		errcode string
	}{
		{`nope`, 400, "M_NOT_JSON"},
		{`{"question":"abcdef"}`, 400, "M_MISSING_PARAM"},
		{`{"origin":"bad/name","question":"abcdef"}`, 400, "M_INVALID_PARAM"},
		{`{"origin":"a.example","question":"abc"}`, 400, "M_INVALID_PARAM"},
		{`{"origin":"a.example","question":"` + strings.Repeat("x", 256) + `"}`, 400, "M_INVALID_PARAM"},
	}
	for _, c := range cases {
		rec := post(b, path, "", c.body)
		if rec.Code != c.status || errcode(t, rec) != c.errcode {
			t.Errorf("%s: %d %s, want %d %s", c.body, rec.Code, errcode(t, rec), c.status, c.errcode)
		}
	}
	if rec := post(b, path, "X-Matrix origin=a.example,key=\"ed25519:x\",sig=\"AAAA\"", `{"question":"abcdef"}`); rec.Code != 401 {
		t.Errorf("a bad signature: %d, want 401", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, req)
	if rec.Code != 405 || errcode(t, rec) != "M_UNRECOGNIZED" {
		t.Errorf("GET: %d %s, want 405 M_UNRECOGNIZED", rec.Code, errcode(t, rec))
	}
}

// One ping per origin per 30 seconds, and per address when unsigned; a
// retransmitted question gets its first answer, not a 429 nor a new pong.
func TestRateLimitAndRetransmission(t *testing.T) {
	n := newNetwork()
	n.add(t, "a.example")
	b := n.add(t, "b.example")
	now := time.Unix(1_000_000, 0)
	b.now = func() time.Time { return now }
	const path = "/_matrix/federation/unstable/uk.timedout.msc4524.tabletennis/ping"

	first := post(b, path, "", `{"origin":"a.example","question":"question-1"}`)
	if first.Code != 200 {
		t.Fatalf("first ping: %d %s", first.Code, first.Body)
	}
	again := post(b, path, "", `{"origin":"a.example","question":"question-1"}`)
	if again.Code != 200 || !bytes.Contains(again.Body.Bytes(), []byte(answerOf(t, first))) {
		t.Errorf("retransmission: %d %s, want the first answer", again.Code, again.Body)
	}
	limited := post(b, path, "", `{"origin":"a.example","question":"question-2"}`)
	if limited.Code != 429 || errcode(t, limited) != "M_LIMIT_EXCEEDED" || limited.Header().Get("Retry-After") != "30" {
		t.Errorf("second question: %d %s Retry-After=%s, want 429", limited.Code, limited.Body, limited.Header().Get("Retry-After"))
	}
	// Another origin from the same address is limited too.
	if rec := post(b, path, "", `{"origin":"c.example","question":"question-3"}`); rec.Code != 429 {
		t.Errorf("same address, other origin: %d, want 429", rec.Code)
	}
	// The address is the one nginx appended, not one the client made up.
	if ip := clientIP(withXFF("6.6.6.6, 198.51.100.7")); ip != "198.51.100.7" {
		t.Errorf("clientIP = %q, want nginx's entry", ip)
	}
	now = now.Add(31 * time.Second)
	if rec := post(b, path, "", `{"origin":"a.example","question":"question-2"}`); rec.Code != 200 {
		t.Errorf("after 31s: %d, want 200", rec.Code)
	}
}

func answerOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var r pingResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil || r.Answer == "" {
		t.Fatalf("no answer in %s", rec.Body)
	}
	return r.Answer
}

func TestPongNeedsSignatureAndAKnownQuestion(t *testing.T) {
	n := newNetwork()
	a := n.add(t, "a.example")
	n.add(t, "b.example")
	const path = "/_matrix/federation/v1/pong"
	if rec := post(a, path, "", `{"question":"abcdef"}`); rec.Code != 401 || errcode(t, rec) != "M_UNAUTHORIZED" {
		t.Errorf("unsigned pong: %d %s, want 401 M_UNAUTHORIZED", rec.Code, errcode(t, rec))
	}
	// A signed pong nobody asked for.
	b := n.handlers["b.example"].(*Server)
	err := b.client.MakeRequest(context.Background(), "a.example", true, http.MethodPost,
		stablePath("pong"), pongRequest{Question: "never-asked"}, &struct{}{})
	if outcomeOf(err) != "not_found" {
		t.Errorf("unknown question: %v, want 404", err)
	}
}

// A destination that does not serve the MSC is reported as such, after the
// stable path is tried too.
func TestDestinationWithoutMSC(t *testing.T) {
	n := newNetwork()
	a := n.add(t, "a.example")
	n.mu.Lock()
	n.handlers["old.example"] = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeError(w, 404, "M_UNRECOGNIZED", "Unrecognized request")
	})
	n.mu.Unlock()
	rep := a.Initiate(context.Background(), "old.example", time.Second)
	if rep.Answered || len(rep.Attempts) != 2 || !strings.Contains(rep.Conclusion, "does not serve") {
		t.Errorf("report = %+v", rep)
	}

	n.mu.Lock()
	n.down["gone.example"] = true
	n.handlers["gone.example"] = a
	n.mu.Unlock()
	rep = a.Initiate(context.Background(), "gone.example", time.Second)
	if rep.Answered || !strings.Contains(rep.Conclusion, "could not reach") {
		t.Errorf("unreachable: %+v", rep)
	}
}

func withXFF(v string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Forwarded-For", v)
	return r
}
