// Package fedproxy is the outbound federation proxy other Synapse
// processes use when homeserver.yaml's outbound_federation_restricted_to
// names this process (http/proxy.py ProxyResource, which every
// SynapseSite serves as a ProxySite). It sits in front of the replication
// listener, where they reach us at our instance_map address, and takes
// the requests whose request line is absolute-form with the
// matrix-federation scheme:
//
//	<METHOD> matrix-federation://<destination><path>?<query> HTTP/1.1
//	Proxy-Authorization: Bearer <worker_replication_secret>
//
// What it does, as ProxyResource:
//
//   - the secret first: exactly one Proxy-Authorization, "Bearer
//     <secret>" split on single spaces into exactly two parts, or 401
//     with M_MISSING_TOKEN "Missing Proxy-Authorization header.", or
//     M_UNAUTHORIZED "Too many Proxy-Authorization headers." / "Invalid
//     Proxy-Authorization header."; with no worker_replication_secret set,
//     Synapse's assert fails, which answers 502;
//   - only User-Agent, Authorization and Content-Type are forwarded (the
//     last of each, when not empty), with the body, and the request line's
//     URI unchanged: the caller's X-Matrix signature covers it;
//   - the destination is reached as every federation client here reaches
//     one (fedclient.Forwarding): the shared resolutions, Synapse's IP
//     filter, certificates as configured; no backoff and no retries,
//     which are the caller's;
//   - the answer has 90 seconds to start; its status and headers come
//     back, less the hop-by-hop ones and any the Connection header names,
//     and its body is streamed as it arrives. A body that fails half way
//     aborts our answer, so the caller does not take it for a whole one;
//   - any failure is 502 {"errcode": "M_UNKNOWN", "err": "ProxyResource:
//     Error when proxying request: <METHOD> <URI> -> <failure>"}, in
//     json.dumps' spacing and with the key "err", as Synapse writes it.
//     The failure itself is Go's error text, not Twisted's Failure repr:
//     the caller only logs it.
package fedproxy

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

// Scheme marks a proxied request.
const Scheme = "matrix-federation"

// Timeout is how long the destination has to answer (timeout_deferred's
// 90 s, longer than MatrixFederationHttpClient's own).
const Timeout = 90 * time.Second

// maxBufferedBody bounds a request body that arrives without a length,
// which is read whole so that the destination gets a Content-Length, as
// Twisted's agent sends one.
const maxBufferedBody = 64 << 20

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_federation_proxy_requests_total",
		Help: "Federation requests other Synapse processes sent through this one (outbound_federation_restricted_to), by outcome: answered (the destination answered, any status; the code label has it), unauthorized (401: a missing, repeated or wrong Proxy-Authorization), failed (502: unreachable, refused by the IP filter, timed out, or no secret configured).",
	}, []string{"outcome", "code"})
	latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "goworker_federation_proxy_seconds",
		Help:    "Time from a proxied request's arrival to the destination's response headers, for requests the destination answered.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 90},
	})
	bodyBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_federation_proxy_bytes_total",
		Help: "Body bytes the federation proxy carried, by direction: request (the caller's, to the destination) and response (the destination's, back to the caller).",
	}, []string{"direction"})
)

// Options builds a Proxy.
type Options struct {
	// Next serves every request that is not proxied: the replication
	// endpoints.
	Next http.Handler
	// Secret is worker_replication_secret, read on every request.
	Secret func() string
	// Transport reaches the destination (fedclient.Forwarding).
	Transport http.RoundTripper
	Log       zerolog.Logger
}

// Proxy is the handler.
type Proxy struct{ opts Options }

// New builds a Proxy.
func New(opts Options) *Proxy {
	if opts.Secret == nil {
		opts.Secret = func() string { return "" }
	}
	return &Proxy{opts: opts}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Scheme != Scheme {
		p.opts.Next.ServeHTTP(w, r)
		return
	}
	start := time.Now()
	outcome, code := p.serve(w, r, start)
	requests.WithLabelValues(outcome, strconv.Itoa(code)).Inc()
}

// errNoSecret is Synapse's failed assert: the proxy works only with auth.
var errNoSecret = errors.New("AssertionError: worker_replication_secret is not set")

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request, start time.Time) (string, int) {
	secret := p.opts.Secret()
	if secret == "" {
		return "failed", p.fail(w, r, errNoSecret)
	}
	if errcode, msg, err := checkAuth(r.Header.Values("Proxy-Authorization"), secret); err != nil {
		return "failed", p.fail(w, r, err)
	} else if errcode != "" {
		p.opts.Log.Warn().Str("reason", msg).Str("destination", r.URL.Host).Msg("federation proxy request refused")
		writeError(w, http.StatusUnauthorized, errcode, msg)
		return "unauthorized", http.StatusUnauthorized
	}

	body, length, err := requestBody(r)
	if err != nil {
		return "failed", p.fail(w, r, err)
	}
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	out, err := http.NewRequestWithContext(ctx, r.Method, r.RequestURI, body)
	if err != nil {
		return "failed", p.fail(w, r, err)
	}
	out.ContentLength = length
	if length == 0 {
		out.Body = http.NoBody
	}
	for _, h := range []string{"User-Agent", "Authorization", "Content-Type"} {
		if vs := r.Header.Values(h); len(vs) > 0 && vs[len(vs)-1] != "" {
			out.Header.Set(h, vs[len(vs)-1])
		}
	}

	timer := time.AfterFunc(Timeout, func() { cancel(errTimedOut) })
	resp, err := p.opts.Transport.RoundTrip(out)
	timedOut := !timer.Stop()
	if err != nil {
		if timedOut {
			err = errTimedOut
		}
		return "failed", p.fail(w, r, err)
	}
	defer resp.Body.Close()
	latency.Observe(time.Since(start).Seconds())
	bodyBytes.WithLabelValues("request").Add(float64(max(out.ContentLength, 0)))

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	n, err := stream(w, resp.Body)
	bodyBytes.WithLabelValues("response").Add(float64(n))
	if err != nil && r.Context().Err() == nil {
		// The destination's body failed: Synapse aborts the caller's
		// connection rather than finish a truncated answer.
		p.opts.Log.Debug().Err(err).Str("destination", r.URL.Host).Msg("federation proxy: the answer broke off")
		panic(http.ErrAbortHandler)
	}
	return "answered", resp.StatusCode
}

var errTimedOut = errors.New("defer.TimeoutError: timed out after 90 seconds")

// checkAuth is ProxyResource._check_auth. A secret that is not ASCII
// fails to decode there, which answers 502.
func checkAuth(headers []string, secret string) (errcode, msg string, err error) {
	switch {
	case len(headers) == 0:
		return "M_MISSING_TOKEN", "Missing Proxy-Authorization header.", nil
	case len(headers) > 1:
		return "M_UNAUTHORIZED", "Too many Proxy-Authorization headers.", nil
	}
	parts := strings.Split(headers[0], " ")
	if parts[0] == "Bearer" && len(parts) == 2 {
		if !ascii(parts[1]) {
			return "", "", errors.New("UnicodeDecodeError: 'ascii' codec can't decode the Proxy-Authorization secret")
		}
		if subtle.ConstantTimeCompare([]byte(parts[1]), []byte(secret)) == 1 {
			return "", "", nil
		}
	}
	return "M_UNAUTHORIZED", "Invalid Proxy-Authorization header.", nil
}

func ascii(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// requestBody is the caller's body with its length: as it arrives when
// the caller gave one, else read whole (to maxBufferedBody).
func requestBody(r *http.Request) (io.Reader, int64, error) {
	if r.ContentLength >= 0 {
		return r.Body, r.ContentLength, nil
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, maxBufferedBody+1))
	if err != nil {
		return nil, 0, err
	}
	if len(b) > maxBufferedBody {
		return nil, 0, fmt.Errorf("request body over %d bytes", maxBufferedBody)
	}
	return strings.NewReader(string(b)), int64(len(b)), nil
}

// hopByHop is HOP_BY_HOP_HEADERS_LOWERCASE.
var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"te": true, "trailers": true, "transfer-encoding": true, "upgrade": true,
}

// copyHeaders copies the destination's headers, less the hop-by-hop ones
// and those the (first) Connection header names
// (parse_connection_header_value).
func copyHeaders(dst, src http.Header) {
	drop := map[string]bool{}
	if c := src.Values("Connection"); len(c) > 0 {
		for _, o := range strings.Split(c[0], ",") {
			drop[strings.ToLower(strings.TrimSpace(o))] = true
		}
	}
	for k, vs := range src {
		lk := strings.ToLower(k)
		if hopByHop[lk] || drop[lk] {
			continue
		}
		dst[k] = append([]string(nil), vs...)
	}
}

// stream copies the body, flushing each piece as it arrives
// (_ProxyResponseBody.dataReceived).
func stream(w http.ResponseWriter, body io.Reader) (int64, error) {
	rc := http.NewResponseController(w)
	buf := make([]byte, 32<<10)
	var n int64
	for {
		k, err := body.Read(buf)
		if k > 0 {
			if _, werr := w.Write(buf[:k]); werr != nil {
				return n, werr
			}
			n += int64(k)
			_ = rc.Flush()
		}
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// fail answers 502 as _send_error_response does.
func (p *Proxy) fail(w http.ResponseWriter, r *http.Request, err error) int {
	p.opts.Log.Debug().Err(err).Str("method", r.Method).Str("destination", r.URL.Host).Msg("federation proxy request failed")
	writeError(w, http.StatusBadGateway, "M_UNKNOWN",
		fmt.Sprintf("ProxyResource: Error when proxying request: %s %s -> %s", r.Method, r.RequestURI, err))
	return http.StatusBadGateway
}

// writeError writes {"errcode": ..., "err": ...} as json.dumps does.
func writeError(w http.ResponseWriter, status int, errcode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"errcode": `+dumpsString(errcode)+`, "err": `+dumpsString(msg)+`}`)
}

// dumpsString is json.dumps of a str (ensure_ascii): everything outside
// space..tilde escaped, astral characters as surrogate pairs.
func dumpsString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r >= 0x20 && r <= 0x7e:
			b.WriteRune(r)
		case r > 0xffff:
			r1, r2 := utf16.EncodeRune(r)
			fmt.Fprintf(&b, `\u%04x\u%04x`, r1, r2)
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
