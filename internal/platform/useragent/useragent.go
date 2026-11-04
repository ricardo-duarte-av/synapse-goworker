// Package useragent is the User-Agent Synapse sends, which every request
// this worker makes on Synapse's behalf sends too: a remote server, or a
// Synapse worker, must not be able to tell us apart.
//
// Synapse sends hs.version_string, "Synapse/<SYNAPSE_VERSION>"
// (server.py), in two forms:
//
//   - Federation: the version string alone, on every request made through
//     the federation client (matrixfederationclient.py _send_request sets
//     it outright, so user_agent_suffix never reaches it): /send, key
//     fetches, notary queries, policy servers, backfill, profile queries.
//   - HTTP: the version string, then " " and user_agent_suffix when that is
//     set (http/client.py BaseHttpClient): replication to the stream
//     writers and main, and the .well-known lookups server discovery makes
//     (matrixfederationclient.py hands the suffixed one to
//     MatrixFederationAgent, which uses it only where a request has none).
//
// The version is main's, learnt from the Server header main answers with,
// which is that same version_string: exact, an override in
// SYNAPSE_VERSION_STRING included, and current after an upgrade, as long as
// it is asked again when Synapse restarts (Probe on REPLICATE) or seen on a
// proxied answer (Observe). Until main has answered once, Fallback stands in.
package useragent

import (
	"net/http"
	"strings"
	"sync/atomic"

	"maunium.net/go/mautrix/federation"
)

// Fallback is sent until main has been heard from: the Synapse version
// every part was last reviewed against (synapse-watch.yaml).
const Fallback = "Synapse/1.164.2"

var (
	server atomic.Pointer[string]
	suffix atomic.Pointer[string]
	known  atomic.Bool
)

func init() {
	s, x := Fallback, ""
	server.Store(&s)
	suffix.Store(&x)
}

// Observe takes main's Server header. It ignores anything that is not
// Synapse's version string (a proxy's, an empty one), and reports whether
// the value changed.
func Observe(header string) bool {
	if !strings.HasPrefix(header, "Synapse/") || !ascii(header) {
		return false
	}
	known.Store(true)
	if *server.Load() == header {
		return false
	}
	server.Store(&header)
	return true
}

// SetSuffix is homeserver.yaml's user_agent_suffix ("" when unset).
func SetSuffix(s string) {
	if !ascii(s) {
		s = ""
	}
	suffix.Store(&s)
}

// Known is whether main has been heard from.
func Known() bool { return known.Load() }

// Federation is what the federation client sends.
func Federation() string { return *server.Load() }

// HTTP is what Synapse's other HTTP clients send: replication, .well-known.
func HTTP() string {
	s, x := *server.Load(), *suffix.Load()
	if x == "" {
		return s
	}
	return s + " " + x
}

// Synapse encodes it as ASCII; anything else would raise there.
func ascii(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// Transport sets User-Agent to value() on every request, replacing what
// was there (mautrix's own).
type Transport struct {
	Next  http.RoundTripper
	Value func() string
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}
	ua := t.Value()
	if req.Header.Get("User-Agent") != ua {
		req = req.Clone(req.Context())
		req.Header.Set("User-Agent", ua)
	}
	return next.RoundTrip(req)
}

// Resolving makes a resolving transport's .well-known lookups send HTTP().
// mautrix otherwise makes them with http.DefaultClient, as Go-http-client.
func Resolving(srt *federation.ServerResolvingTransport) {
	opts := &federation.ResolveServerNameOpts{}
	if srt.ResolveOpts != nil {
		*opts = *srt.ResolveOpts
	}
	base := opts.HTTPClient
	if base == nil {
		base = http.DefaultClient
	}
	c := *base
	c.Transport = &Transport{Next: base.Transport, Value: HTTP}
	opts.HTTPClient = &c
	srt.ResolveOpts = opts
}

// Client makes a mautrix federation client send what Synapse's does:
// Federation() on its federation requests, HTTP() on its .well-known
// lookups and on ExtHTTP (a media download's redirect).
func Client(c *federation.Client) {
	if srt, ok := c.HTTP.Transport.(*federation.ServerResolvingTransport); ok {
		Resolving(srt)
	}
	c.HTTP.Transport = &Transport{Next: c.HTTP.Transport, Value: Federation}
	if c.ExtHTTP != nil {
		c.ExtHTTP.Transport = &Transport{Next: c.ExtHTTP.Transport, Value: HTTP}
	}
	c.UserAgent = Federation()
}
