package fedclient

import (
	"context"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

const testKey = "ed25519 a_test AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"

// fakeInstance is a named instance's listener: what it was asked, and a
// 200 for every request.
type fakeInstance struct {
	mu   sync.Mutex
	reqs []*http.Request
	body []string
	sock string
}

func newFakeInstance(t *testing.T) *fakeInstance {
	t.Helper()
	f := &fakeInstance{sock: filepath.Join(t.TempDir(), "repl.sock")}
	l, err := net.Listen("unix", f.sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.reqs = append(f.reqs, r)
		f.body = append(f.body, string(b))
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"server":{"name":"proxied"}}`))
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return f
}

func (f *fakeInstance) seen() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*http.Request(nil), f.reqs...)
}

func useOutbound(t *testing.T, o Outbound) {
	t.Helper()
	SetOutbound(func() Outbound { return o })
	t.Cleanup(func() { SetOutbound(func() Outbound { return Outbound{Direct: true} }) })
}

// A process not named sends every request to a named instance: the URI
// the signature covers in the request line, the destination as Host, the
// secret as a Bearer Proxy-Authorization, and its own Authorization,
// User-Agent and Content-Type.
func TestRestrictedSendsThroughTheInstance(t *testing.T) {
	f := newFakeInstance(t)
	useOutbound(t, Outbound{Via: []synapsecfg.Instance{{Name: "goworker", Socket: f.sock}}, Secret: "s3cret"})

	c, err := New("origin.example", testKey, nil, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	err = c.MakeRequest(context.Background(), "remote.example:8448", true, http.MethodPost,
		federation.URLPath{"v1", "user", "keys", "query"}, map[string]any{"device_keys": map[string]any{"@a:remote.example": []any{}}}, &out)
	if err != nil {
		t.Fatal(err)
	}
	reqs := f.seen()
	if len(reqs) != 1 {
		t.Fatalf("%d requests reached the instance", len(reqs))
	}
	r := reqs[0]
	if want := "matrix-federation://remote.example:8448/_matrix/federation/v1/user/keys/query"; r.RequestURI != want {
		t.Errorf("request line URI %q, want %q", r.RequestURI, want)
	}
	if r.Host != "remote.example:8448" {
		t.Errorf("Host %q", r.Host)
	}
	if got := r.Header.Values("Proxy-Authorization"); len(got) != 1 || got[0] != "Bearer s3cret" {
		t.Errorf("Proxy-Authorization %q", got)
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, `X-Matrix origin="origin.example",destination="remote.example:8448"`) {
		t.Errorf("Authorization %q", auth)
	}
	if r.Header.Get("Content-Type") != "application/json" || !strings.HasPrefix(r.Header.Get("User-Agent"), "Synapse/") {
		t.Errorf("Content-Type %q, User-Agent %q", r.Header.Get("Content-Type"), r.Header.Get("User-Agent"))
	}
	if r.Header.Get("Accept-Encoding") != "" {
		t.Errorf("Accept-Encoding %q added", r.Header.Get("Accept-Encoding"))
	}
	if !strings.Contains(f.body[0], "@a:remote.example") || r.ContentLength <= 0 {
		t.Errorf("body %q, length %d", f.body[0], r.ContentLength)
	}
	if out["server"] == nil {
		t.Errorf("answer not passed back: %v", out)
	}
}

// The query and an escaped path go as signed, byte for byte.
func TestRestrictedKeepsTheSignedURI(t *testing.T) {
	f := newFakeInstance(t)
	useOutbound(t, Outbound{Via: []synapsecfg.Instance{{Name: "w", Socket: f.sock}}, Secret: "x"})
	c, err := New("origin.example", testKey, nil, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	_ = c.Query(context.Background(), "remote.example", "profile", url.Values{"user_id": {"@a b:remote.example"}, "field": {"displayname"}}, &out)
	_ = c.MakeRequest(context.Background(), "remote.example", true, http.MethodGet,
		federation.URLPath{"v1", "state_ids", "!r:o", ""}, nil, &out)
	reqs := f.seen()
	if len(reqs) == 0 {
		t.Fatal("nothing reached the instance")
	}
	for _, r := range reqs {
		u := strings.TrimPrefix(r.RequestURI, "matrix-federation://remote.example")
		if u == r.RequestURI {
			t.Errorf("not absolute-form: %q", r.RequestURI)
			continue
		}
		// What X-Matrix signed is the path and query as the request line
		// carries them: the fake cannot check the signature, so check the
		// escapes survived.
		if strings.Contains(u, " ") {
			t.Errorf("unescaped: %q", u)
		}
	}
	if q := reqs[0].RequestURI; !strings.Contains(q, "/_matrix/federation/v1/query/profile?") || !strings.Contains(q, "%40a+b%3Aremote.example") {
		t.Errorf("query request line %q", q)
	}
}

// A process the setting names, or no setting, sends directly.
func TestDirectDoesNotUseTheInstance(t *testing.T) {
	f := newFakeInstance(t)
	useOutbound(t, Outbound{Direct: true, Via: []synapsecfg.Instance{{Name: "w", Socket: f.sock}}, Secret: "x"})
	c, err := New("origin.example", testKey, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Direct to a name that cannot resolve: it fails, and the instance is
	// never asked.
	_, _ = c.Version(context.Background(), "nowhere.invalid")
	if n := len(f.seen()); n != 0 {
		t.Errorf("%d requests went to the instance while direct", n)
	}
}

// _RandomSampleEndpoints: an instance that does not connect is skipped.
func TestRestrictedFailsOver(t *testing.T) {
	f := newFakeInstance(t)
	dead := filepath.Join(t.TempDir(), "dead.sock")
	useOutbound(t, Outbound{Via: []synapsecfg.Instance{{Name: "dead", Socket: dead}, {Name: "live", Socket: f.sock}}, Secret: "x"})
	c, err := New("origin.example", testKey, nil, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		// New connections each time, so both orders are tried.
		proxyTransport(currentOutbound().Via).CloseIdleConnections()
		if _, err := c.Version(context.Background(), "remote.example"); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if n := len(f.seen()); n != 6 {
		t.Errorf("%d of 6 reached the live instance", n)
	}
}

// Nothing is sent for a blocked IP literal (BlocklistingAgentWrapper) or
// with no instance to send through (ProxyAgent without endpoints).
func TestRestrictedRefuses(t *testing.T) {
	f := newFakeInstance(t)
	useOutbound(t, Outbound{Via: []synapsecfg.Instance{{Name: "w", Socket: f.sock}}, Secret: "x"})
	c, err := New("origin.example", testKey, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Version(context.Background(), "127.0.0.1:8448"); err == nil || !strings.Contains(err.Error(), "IP address blocked") {
		t.Errorf("blocked literal: %v", err)
	}
	if _, err := c.Version(context.Background(), "[::1]"); err == nil {
		t.Error("blocked v6 literal sent")
	}
	if n := len(f.seen()); n != 0 {
		t.Errorf("%d blocked requests reached the instance", n)
	}
	useOutbound(t, Outbound{Secret: "x"})
	if _, err := c.Version(context.Background(), "remote.example"); err == nil {
		t.Error("sent with no instance named")
	}
}

// Every federation client is built where outbound_federation_restricted_to
// is applied: NewMautrix (fedclient.New too), or a resolving transport
// wrapped in Restrict. A client built elsewhere would keep talking
// directly, unnoticed.
func TestEveryFederationClientIsRestricted(t *testing.T) {
	root := "../../.."
	builds := []string{"federation.NewClient(", "federation.NewServerResolvingTransport("}
	found := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "vendor") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(b)
		for _, call := range builds {
			if !strings.Contains(src, call) {
				continue
			}
			found++
			rel, _ := filepath.Rel(root, path)
			if filepath.Dir(rel) == filepath.Join("internal", "platform", "fedclient") {
				if !strings.Contains(src, "Restrict(") {
					t.Errorf("%s builds a federation client (%s) without Restrict", rel, call)
				}
				continue
			}
			// The offline sweeps generate traffic from wherever they run.
			if strings.HasPrefix(rel, "cmd"+string(filepath.Separator)) && strings.HasSuffix(filepath.Dir(rel), "sweep") {
				continue
			}
			if !strings.Contains(src, "fedclient.Restrict(") {
				t.Errorf("%s builds a federation client (%s) without fedclient.Restrict", rel, call)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found < 2 {
		t.Fatalf("found %d federation client constructions; the scan is not looking", found)
	}
}
