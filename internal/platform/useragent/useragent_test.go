package useragent

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/federation"
)

func reset(t *testing.T) {
	t.Helper()
	s, x := Fallback, ""
	server.Store(&s)
	suffix.Store(&x)
	known.Store(false)
	t.Cleanup(func() {
		server.Store(&s)
		suffix.Store(&x)
		known.Store(false)
	})
}

func TestFallbackUntilMainAnswers(t *testing.T) {
	reset(t)
	if Known() || Federation() != Fallback || HTTP() != Fallback {
		t.Fatalf("before main answers: known=%v %q %q", Known(), Federation(), HTTP())
	}
	if !Observe("Synapse/1.164.0") {
		t.Fatal("a new version was not reported as a change")
	}
	if Observe("Synapse/1.164.0") {
		t.Fatal("the same version was reported as a change")
	}
	if !Known() || Federation() != "Synapse/1.164.0" {
		t.Fatalf("after main answered: known=%v %q", Known(), Federation())
	}
}

// Only Synapse's version string is taken: a proxy's Server header, or none,
// leaves the last one in place.
func TestObserveIgnoresWhatIsNotSynapse(t *testing.T) {
	reset(t)
	Observe("Synapse/1.164.1")
	for _, h := range []string{"", "nginx/1.27", "TwistedWeb/24.11", "synapse/1.0", "Synapse/1.0\r\nX: y", "Synapse/1.0é"} {
		if Observe(h) {
			t.Errorf("Observe(%q) changed the version", h)
		}
	}
	if Federation() != "Synapse/1.164.1" {
		t.Fatalf("got %q", Federation())
	}
}

// matrixfederationclient.py sends version_string alone; BaseHttpClient
// appends user_agent_suffix after a space.
func TestSuffixOnlyOnTheHTTPForm(t *testing.T) {
	reset(t)
	Observe("Synapse/1.164.1")
	SetSuffix("example-suffix (contact: ops)")
	if got := Federation(); got != "Synapse/1.164.1" {
		t.Errorf("Federation() = %q", got)
	}
	if got := HTTP(); got != "Synapse/1.164.1 example-suffix (contact: ops)" {
		t.Errorf("HTTP() = %q", got)
	}
	SetSuffix("")
	if got := HTTP(); got != "Synapse/1.164.1" {
		t.Errorf("HTTP() without a suffix = %q", got)
	}
}

func uaServer(t *testing.T, tlsServer bool) (*httptest.Server, chan string) {
	got := make(chan string, 4)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"server":{"name":"x","version":"1"}}`))
	})
	var srv *httptest.Server
	if tlsServer {
		srv = httptest.NewTLSServer(h)
	} else {
		srv = httptest.NewServer(h)
	}
	t.Cleanup(srv.Close)
	return srv, got
}

// A federation request through a mautrix client sends Synapse's
// User-Agent, not mautrix's, and follows the version as it changes.
func TestClientSendsSynapsesUserAgent(t *testing.T) {
	reset(t)
	Observe("Synapse/1.164.1")
	SetSuffix("sfx")
	srv, got := uaServer(t, true)

	c := federation.NewClient("example.org", nil, federation.NewInMemoryCache(), exhttp.SensibleClientSettings)
	c.AllowIP = func(net.IP) bool { return true }
	Client(c)
	srt := c.HTTP.Transport.(*Transport).Next.(*federation.ServerResolvingTransport)
	srt.Transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	// An IP literal with a port resolves to itself: no .well-known.
	host := strings.TrimPrefix(srv.URL, "https://")
	for _, want := range []string{"Synapse/1.164.1", "Synapse/1.165.0"} {
		Observe(want)
		if _, err := c.Version(context.Background(), host); err != nil {
			t.Fatal(err)
		}
		if ua := <-got; ua != want {
			t.Errorf("federation request sent %q, want %q", ua, want)
		}
	}
}

// The .well-known lookup is BaseHttpClient's form, suffix and all.
func TestWellKnownLookupSendsTheHTTPForm(t *testing.T) {
	reset(t)
	Observe("Synapse/1.164.1")
	SetSuffix("sfx")
	srv, got := uaServer(t, false)

	srt := federation.NewServerResolvingTransport(nil, nil, exhttp.SensibleClientSettings)
	Resolving(srt)
	resp, err := srt.ResolveOpts.HTTPClient.Get(srv.URL + "/.well-known/matrix/server")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if ua := <-got; ua != "Synapse/1.164.1 sfx" {
		t.Errorf(".well-known lookup sent %q", ua)
	}
	if srt.ResolveOpts.HTTPClient == http.DefaultClient {
		t.Error("http.DefaultClient was modified rather than copied")
	}
	if _, ok := http.DefaultClient.Transport.(*Transport); ok {
		t.Error("http.DefaultClient's transport was replaced")
	}
}
