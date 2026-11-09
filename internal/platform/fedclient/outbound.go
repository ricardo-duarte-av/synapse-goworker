package fedclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Synapse's outbound_federation_restricted_to, for the requests we make
// (matrixfederationclient.py, http/proxyagent.py ProxyAgent). When it is
// set and does not name this process, no federation request is sent
// directly: each goes to one of the named instances as an absolute-form
// request,
//
//	<METHOD> matrix-federation://<destination><path>?<query> HTTP/1.1
//	Host: <destination>
//	Proxy-Authorization: Bearer <worker_replication_secret>
//
// with its X-Matrix Authorization, User-Agent and Content-Type as they
// are, and the named instance resolves the destination, filters its
// addresses, connects and answers (http/proxy.py ProxyResource). The path
// and query are the ones the signature covers, unchanged. Each new
// connection tries the named instances' instance_map addresses in random
// order and takes the first that connects (_RandomSampleEndpoints); a
// connection is reused for any destination.
//
// An IP-literal destination in federation_ip_range_blocklist is refused
// here before anything is sent, as BlocklistingAgentWrapper refuses it
// around the proxy agent; a server name is the named instance's to
// filter. An empty list, or one whose instances have no address, sends
// nothing: Synapse's ProxyAgent then has no endpoint and raises
// SchemeNotSupported.
//
// The setting is read from homeserver.yaml's current copy on every
// request, so a re-read applies at once. Synapse reads it at start.

var outbound atomic.Pointer[func() Outbound]

// Outbound is what decides where a federation request goes.
type Outbound struct {
	// Direct is whether this process talks to other servers itself.
	Direct bool
	// Via are the instances to send through when not Direct.
	Via []synapsecfg.Instance
	// Secret is worker_replication_secret.
	Secret string
}

// OutboundFor is the decision for instance under hs.
func OutboundFor(hs *synapsecfg.Config, instance string) Outbound {
	if hs.FederatesDirectly(instance) {
		return Outbound{Direct: true}
	}
	return Outbound{Via: hs.FederationProxies(), Secret: hs.ReplicationSecret}
}

// SetOutbound installs where the decision comes from. Until it is
// installed, every request is direct.
func SetOutbound(f func() Outbound) { outbound.Store(&f) }

func currentOutbound() Outbound {
	if f := outbound.Load(); f != nil {
		return (*f)()
	}
	return Outbound{Direct: true}
}

var proxiedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goworker_federation_proxied_requests_total",
	Help: "Federation requests this process sent through another instance (outbound_federation_restricted_to), by outcome: answered (the instance answered, whatever the status), failed (no answer: no instance connected, or the connection failed), refused (not sent: a blocked IP literal, or no instance to send through).",
}, []string{"outcome"})

// Restrict wraps direct, a transport that resolves matrix-federation://
// URLs and connects itself: while the process may not federate
// directly, requests go through a named instance instead. Every
// federation client must have it, inside its User-Agent and Content-Type
// wrappers (they travel to the instance) and around the resolution.
func Restrict(direct http.RoundTripper) *Restricted {
	return &Restricted{Direct: direct}
}

// Restricted is Restrict's transport.
type Restricted struct {
	// Direct is the transport a request goes through when this process
	// talks to other servers itself.
	Direct http.RoundTripper
}

func (t *Restricted) RoundTrip(req *http.Request) (*http.Response, error) {
	o := currentOutbound()
	if o.Direct || req.URL.Scheme != "matrix-federation" {
		return t.Direct.RoundTrip(req)
	}
	resp, err := sendVia(o, req)
	switch {
	case errors.Is(err, errNotSent):
		proxiedRequests.WithLabelValues("refused").Inc()
	case err != nil:
		proxiedRequests.WithLabelValues("failed").Inc()
	default:
		proxiedRequests.WithLabelValues("answered").Inc()
	}
	return resp, err
}

// errNotSent marks a request refused before anything was sent.
var errNotSent = errors.New("not sent")

func sendVia(o Outbound, req *http.Request) (*http.Response, error) {
	if ip := net.ParseIP(req.URL.Hostname()); ip != nil && !AllowIP(ip) {
		closeBody(req)
		return nil, fmt.Errorf("fedclient: %w: IP address blocked: %s", errNotSent, ip)
	}
	if len(o.Via) == 0 {
		closeBody(req)
		return nil, fmt.Errorf("fedclient: %w: outbound_federation_restricted_to names no instance to send through: Unsupported scheme: 'matrix-federation'", errNotSent)
	}
	out := req.Clone(req.Context())
	// The request line carries the URI as given; the transport only
	// connects to the instance.
	out.URL = &url.URL{
		Scheme:   "http",
		Host:     "federation-proxy",
		Opaque:   "matrix-federation://" + req.URL.Host + req.URL.EscapedPath(),
		RawQuery: req.URL.RawQuery,
	}
	out.Host = req.URL.Host
	out.Header.Del("Proxy-Authorization")
	out.Header.Set("Proxy-Authorization", "Bearer "+o.Secret)
	return proxyTransport(o.Via).RoundTrip(out)
}

func closeBody(req *http.Request) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
}

// One transport per set of instances, so connections are pooled across
// requests and dropped with a set no longer configured.
var (
	proxyMu   sync.Mutex
	proxyKey  string
	proxyPool *http.Transport
)

func proxyTransport(via []synapsecfg.Instance) *http.Transport {
	var b strings.Builder
	for _, v := range via {
		b.WriteString(v.Socket + "|" + v.URL + ";")
	}
	key := b.String()
	proxyMu.Lock()
	defer proxyMu.Unlock()
	if proxyPool != nil && proxyKey == key {
		return proxyPool
	}
	if proxyPool != nil {
		proxyPool.CloseIdleConnections()
	}
	targets := append([]synapsecfg.Instance(nil), via...)
	proxyKey = key
	proxyPool = &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialAny(ctx, targets)
		},
		// Every destination shares the one pool key: a sender's concurrent
		// transactions need more than the default two idle connections.
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		// The instance forwards no Accept-Encoding; asking for gzip here
		// would change nothing but our own decoding.
		DisableCompression: true,
	}
	return proxyPool
}

// dialAny is _RandomSampleEndpoints: the instances in random order, the
// first that connects; the last failure when none does.
func dialAny(ctx context.Context, via []synapsecfg.Instance) (net.Conn, error) {
	var err error
	for _, i := range rand.Perm(len(via)) {
		var c net.Conn
		if c, err = dialInstance(ctx, via[i]); err == nil {
			return c, nil
		}
	}
	return nil, err
}

// dialInstance connects to an instance_map address: its socket, or its
// host and port, with TLS when it says tls (checked as federation
// certificates are, ProxyAgent's creatorForNetloc).
func dialInstance(ctx context.Context, inst synapsecfg.Instance) (net.Conn, error) {
	d := net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	if inst.Socket != "" {
		return d.DialContext(ctx, "unix", inst.Socket)
	}
	u, err := url.Parse(inst.URL)
	if err != nil {
		return nil, fmt.Errorf("fedclient: instance %s: %w", inst.Name, err)
	}
	c, err := d.DialContext(ctx, "tcp", u.Host)
	if err != nil || u.Scheme != "https" {
		return c, err
	}
	host := u.Hostname()
	tc := tls.Client(c, tlsConfigFor(host))
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return tc, nil
}

func tlsConfigFor(host string) *tls.Config {
	t := &http.Transport{TLSClientConfig: &tls.Config{ServerName: host}}
	VerifyAsConfigured(t)
	cfg := t.TLSClientConfig
	cfg.NextProtos = nil // HTTP/1.1: the request line carries the URI
	return cfg
}
