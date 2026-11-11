package fedclient

import (
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A .well-known lookup is one request per resolution: its connection must
// not be kept, nor be HTTP/2, which no idle limit bounds.
func TestWellKnownClientKeepsNoConnection(t *testing.T) {
	tr, ok := wellKnownClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport %T", wellKnownClient.Transport)
	}
	if !tr.DisableKeepAlives || tr.ForceAttemptHTTP2 {
		t.Errorf("keep-alives disabled %v, HTTP/2 forced %v", tr.DisableKeepAlives, tr.ForceAttemptHTTP2)
	}
	if tr.DialContext == nil {
		t.Error("no filtering dialer")
	}
}

// Against a server that speaks HTTP/2 when offered (matrix.org, anything
// behind Cloudflare), the lookup must still succeed: bb4715f stopped the
// transport speaking HTTP/2 but still offered it in the handshake, and
// every such lookup failed as a malformed response.
func TestWellKnownClientAgainstHTTP2Server(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"m.server":"`+r.Proto+`"}`)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	// The client's own TLS and protocol settings; only the dialer (which
	// refuses loopback) and the trusted roots are the test's.
	tr := wellKnownClient.Transport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tr.TLSClientConfig.RootCAs = pool
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != `{"m.server":"HTTP/1.1"}` {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}
