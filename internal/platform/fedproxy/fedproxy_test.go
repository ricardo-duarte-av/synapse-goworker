package fedproxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

const testKey = "ed25519 a_test AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// serve runs a Proxy (in front of a Next that answers "next") on a unix
// socket, with transport as the destination.
func serve(t *testing.T, secret string, transport http.RoundTripper) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "repl.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	p := New(Options{
		Next:      http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "next") }),
		Secret:    func() string { return secret },
		Transport: transport,
		Log:       zerolog.Nop(),
	})
	srv := &http.Server{Handler: p}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

// raw sends a request as written and returns the answer.
func raw(t *testing.T, sock, request string) (*http.Response, string) {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func unreachable(t *testing.T) http.RoundTripper {
	return rtFunc(func(*http.Request) (*http.Response, error) {
		t.Error("the destination was contacted")
		return nil, errors.New("no")
	})
}

// _check_auth, byte for byte (json.dumps' spacing, the key "err").
func TestAuth(t *testing.T) {
	sock := serve(t, "s3cret", unreachable(t))
	line := "GET matrix-federation://remote.example/_matrix/federation/v1/version HTTP/1.1\r\nHost: remote.example\r\n"
	for name, tc := range map[string]struct{ headers, body string }{
		"missing":  {"", `{"errcode": "M_MISSING_TOKEN", "err": "Missing Proxy-Authorization header."}`},
		"repeated": {"Proxy-Authorization: Bearer s3cret\r\nProxy-Authorization: Bearer s3cret\r\n", `{"errcode": "M_UNAUTHORIZED", "err": "Too many Proxy-Authorization headers."}`},
		"wrong":    {"Proxy-Authorization: Bearer nope\r\n", `{"errcode": "M_UNAUTHORIZED", "err": "Invalid Proxy-Authorization header."}`},
		"basic":    {"Proxy-Authorization: Basic s3cret\r\n", `{"errcode": "M_UNAUTHORIZED", "err": "Invalid Proxy-Authorization header."}`},
		"spaces":   {"Proxy-Authorization: Bearer  s3cret\r\n", `{"errcode": "M_UNAUTHORIZED", "err": "Invalid Proxy-Authorization header."}`},
	} {
		resp, body := raw(t, sock, line+tc.headers+"Connection: close\r\n\r\n")
		if resp.StatusCode != 401 || body != tc.body || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: %d %q %q", name, resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
	}
}

// Without worker_replication_secret Synapse's assert fails: 502.
func TestNoSecret(t *testing.T) {
	sock := serve(t, "", unreachable(t))
	resp, body := raw(t, sock, "GET matrix-federation://remote.example/x?y=1 HTTP/1.1\r\nHost: remote.example\r\nProxy-Authorization: Bearer x\r\nConnection: close\r\n\r\n")
	if resp.StatusCode != 502 || !strings.HasPrefix(body, `{"errcode": "M_UNKNOWN", "err": "ProxyResource: Error when proxying request: GET matrix-federation://remote.example/x?y=1 -> `) {
		t.Errorf("%d %s", resp.StatusCode, body)
	}
}

// Anything else is the replication server's.
func TestOtherRequestsGoToNext(t *testing.T) {
	sock := serve(t, "s3cret", unreachable(t))
	if _, body := raw(t, sock, "GET /_synapse/replication/x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"); body != "next" {
		t.Errorf("%q", body)
	}
	if _, body := raw(t, sock, "GET http://h/_synapse/replication/x HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"); body != "next" {
		t.Errorf("http absolute-form: %q", body)
	}
}

// Three headers forwarded, the URI unchanged, the body with its length;
// the answer's status and headers back less the hop-by-hop ones and those
// Connection names.
func TestForwards(t *testing.T) {
	var got *http.Request
	var gotBody string
	sock := serve(t, "s3cret", rtFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		h.Set("Connection", "close, X-Private")
		h.Set("X-Private", "1")
		h.Set("Keep-Alive", "timeout=5")
		h.Set("Proxy-Authenticate", "x")
		h.Set("X-Kept", "yes")
		return &http.Response{StatusCode: 403, Header: h, Body: io.NopCloser(strings.NewReader(`{"errcode":"M_FORBIDDEN"}`))}, nil
	}))
	uri := "matrix-federation://remote.example:8448/_matrix/federation/v1/user/devices/%40a%3Aremote.example?x=%21y&z"
	resp, body := raw(t, sock, "PUT "+uri+" HTTP/1.1\r\nHost: remote.example:8448\r\n"+
		"Proxy-Authorization: Bearer s3cret\r\nAuthorization: X-Matrix origin=\"o\"\r\nUser-Agent: Synapse/1.164.1\r\n"+
		"Content-Type: application/json\r\nCookie: c\r\nX-Forwarded-For: 1.2.3.4\r\nAccept-Encoding: gzip\r\n"+
		"Content-Length: 7\r\nConnection: close\r\n\r\n{\"a\":1}")
	if got == nil {
		t.Fatal("not forwarded")
	}
	if got.URL.String() != uri || got.Method != "PUT" {
		t.Errorf("forwarded %s %s", got.Method, got.URL)
	}
	if got.URL.RequestURI() != "/_matrix/federation/v1/user/devices/%40a%3Aremote.example?x=%21y&z" {
		t.Errorf("the signed URI changed: %s", got.URL.RequestURI())
	}
	want := http.Header{"Authorization": {`X-Matrix origin="o"`}, "User-Agent": {"Synapse/1.164.1"}, "Content-Type": {"application/json"}}
	if len(got.Header) != len(want) {
		t.Errorf("forwarded headers %v", got.Header)
	}
	for k, v := range want {
		if got.Header.Get(k) != v[0] {
			t.Errorf("%s = %q", k, got.Header.Get(k))
		}
	}
	if gotBody != `{"a":1}` || got.ContentLength != 7 {
		t.Errorf("body %q length %d", gotBody, got.ContentLength)
	}
	if resp.StatusCode != 403 || body != `{"errcode":"M_FORBIDDEN"}` {
		t.Errorf("answer %d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Kept") != "yes" || resp.Header.Get("X-Private") != "" || resp.Header.Get("Keep-Alive") != "" || resp.Header.Get("Proxy-Authenticate") != "" {
		t.Errorf("answer headers %v", resp.Header)
	}
}

// A destination that cannot be reached is 502 with the failure.
func TestUnreachable(t *testing.T) {
	sock := serve(t, "s3cret", rtFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connection refused")
	}))
	resp, body := raw(t, sock, "GET matrix-federation://remote.example/_matrix/key/v2/server HTTP/1.1\r\nHost: remote.example\r\nProxy-Authorization: Bearer s3cret\r\nConnection: close\r\n\r\n")
	want := `{"errcode": "M_UNKNOWN", "err": "ProxyResource: Error when proxying request: GET matrix-federation://remote.example/_matrix/key/v2/server -> dial tcp: connection refused"}`
	if resp.StatusCode != 502 || body != want {
		t.Errorf("%d %s", resp.StatusCode, body)
	}
}

// The body streams: the first piece reaches the caller before the
// destination has sent the rest.
func TestStreams(t *testing.T) {
	pr, pw := io.Pipe()
	sock := serve(t, "s3cret", rtFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: pr}, nil
	}))
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET matrix-federation://r.example/_matrix/media/x HTTP/1.1\r\nHost: r.example\r\nProxy-Authorization: Bearer s3cret\r\n\r\n")
	go func() { _, _ = io.WriteString(pw, "first") }()
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "first" {
		t.Fatalf("first piece: %q %v", buf, err)
	}
	_ = pw.Close()
}

// A body that breaks off aborts the caller's connection: no clean end.
func TestBrokenBodyAborts(t *testing.T) {
	sock := serve(t, "s3cret", rtFunc(func(*http.Request) (*http.Response, error) {
		pr, pw := io.Pipe()
		go func() {
			_, _ = io.WriteString(pw, "part")
			_ = pw.CloseWithError(errors.New("reset"))
		}()
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: pr}, nil
	}))
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET matrix-federation://r.example/x HTTP/1.1\r\nHost: r.example\r\nProxy-Authorization: Bearer s3cret\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Error("a broken body ended cleanly")
	}
}

func TestDumpsString(t *testing.T) {
	for in, want := range map[string]string{
		`a"b\c`:            `"a\"b\\c"`,
		"\n\t\x01\x7f":     `"\n\t\u0001\u007f"`,
		"\u00e9\U0001F600": `"\u00e9\ud83d\ude00"`,
	} {
		if got := dumpsString(in); got != want {
			t.Errorf("%q: %s, want %s", in, got, want)
		}
	}
}

// B1 and B2 together: a process not named sends through a goworker
// that is, and the destination receives a request whose X-Matrix
// signature still holds over the method, URI and body it receives.
func TestEndToEndSignatureHolds(t *testing.T) {
	key, err := federation.ParseSynapseKey(testKey)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var bad []string
	seen := 0
	sock := serve(t, "s3cret", rtFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		seen++
		var content json.RawMessage
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			if len(b) > 0 {
				content = b
			}
		}
		sig := regexp.MustCompile(`sig="([^"]+)"`).FindStringSubmatch(r.Header.Get("Authorization"))
		want, err := key.SignJSON(struct {
			Method      string          `json:"method"`
			URI         string          `json:"uri"`
			Origin      string          `json:"origin"`
			Destination string          `json:"destination"`
			Content     json.RawMessage `json:"content,omitempty"`
		}{r.Method, r.URL.RequestURI(), "origin.example", r.URL.Host, content})
		if err != nil || sig == nil || sig[1] != want {
			bad = append(bad, r.Method+" "+r.URL.String())
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}))
	fedclient.SetOutbound(func() fedclient.Outbound {
		return fedclient.Outbound{Via: []synapsecfg.Instance{{Name: "goworker", Socket: sock}}, Secret: "s3cret"}
	})
	t.Cleanup(func() { fedclient.SetOutbound(func() fedclient.Outbound { return fedclient.Outbound{Direct: true} }) })

	c, err := fedclient.New("origin.example", testKey, nil, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var out any
	for _, call := range []struct {
		method string
		path   federation.URLPath
		body   any
	}{
		{http.MethodGet, federation.URLPath{"v1", "state_ids", "!room:remote.example"}, nil},
		{http.MethodGet, federation.URLPath{"v1", "user", "devices", "@a b/c:remote.example"}, nil},
		{http.MethodPut, federation.URLPath{"v1", "send", "txn 1"}, map[string]any{"pdus": []any{}, "edus": []any{map[string]any{"edu_type": "m.typing", "content": map[string]any{"é": "😀"}}}}},
		{http.MethodPost, federation.URLPath{"v1", "user", "keys", "query"}, map[string]any{"device_keys": map[string]any{}}},
	} {
		if err := c.MakeRequest(ctx, "remote.example:8448", true, call.method, call.path, call.body, &out); err != nil {
			t.Errorf("%s %v: %v", call.method, call.path, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if seen != 4 || len(bad) != 0 {
		t.Errorf("%d reached the destination; signature broken on %v", seen, bad)
	}
}
