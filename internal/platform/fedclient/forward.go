package fedclient

import (
	"net"
	"net/http"
	"time"

	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/useragent"
)

// Forwarding is the transport the federation proxy (platform/fedproxy)
// forwards other processes' requests with: what http/proxy.py's
// ProxyResource uses, the process's federation agent
// (MatrixFederationAgent behind BlocklistingAgentWrapper). It takes a
// matrix-federation:// request and resolves, filters and connects as
// every client here does: the shared Resolutions, AllowIP on every
// address dialled, .well-known through the filter, certificates as
// federation_verify_certificates says. Through Resolutions, our TTLs
// govern the other processes' traffic too, not Synapse's: a resolution
// reached without a .well-known delegation is kept 5 minutes, not an
// hour (resolveNoDelegationTTL, resolvecache.go), and one we cannot
// connect through is forgotten at once (ForgetOnFailure below). A request without a User-Agent
// gets HTTP() (MatrixFederationAgent's, the suffix included); one with
// keeps it. It does not decompress (no Accept-Encoding is added, as
// Twisted's agent adds none), retry, or back off: the caller's limiter
// decides. Restrict applies, so a process that may not federate itself
// forwards through a named instance, as Synapse's would.
func Forwarding() http.RoundTripper {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second, Control: DialControl}
	srt := federation.NewServerResolvingTransport(Resolutions, exhttp.DialerFunc(dialer.DialContext),
		exhttp.ClientSettings{TLSHandshakeTimeout: 10 * time.Second, IdleConnTimeout: 90 * time.Second})
	srt.Transport.MaxIdleConns = 512
	srt.Transport.MaxIdleConnsPerHost = 2
	srt.Transport.DisableCompression = true
	FilterWellKnown(srt)
	useragent.Resolving(srt)
	VerifyAsConfigured(srt.Transport)
	srt.Transport.ForceAttemptHTTP2 = true
	direct := &defaultUserAgent{next: srt}
	return &ForgetOnFailure{Next: Restrict(direct), Cache: Resolutions}
}

// defaultUserAgent sets HTTP() when the request has no User-Agent.
type defaultUserAgent struct{ next http.RoundTripper }

func (t *defaultUserAgent) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("User-Agent", useragent.HTTP())
	}
	return t.next.RoundTrip(req)
}
