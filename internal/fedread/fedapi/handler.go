package fedapi

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/native"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
)

// Handler serves the federation read endpoints: /event, /state, /state_ids
// and /get_missing_events.
//
// Every request is answered natively. Anything that goes wrong before the
// first byte -- verification, an error, a timeout, a panic -- falls back to
// Synapse's main process with nothing written, so a defect here costs latency
// rather than correctness. Without a resolver (no database), everything is
// proxied.
type Handler struct {
	proxy   *proxy.Proxy
	limiter *ratelimit.Limiter
	log     zerolog.Logger

	// resolver and verifier are what make native serving possible: an answer
	// of our own, and proof of who asked for it. Both nil means everything
	// goes to Synapse.
	resolver   native.Resolver
	verifier   Verifier
	serverName string
	// nativeTimeout bounds a natively served answer. Exceeding it falls back
	// to the proxy, so a pathological room costs latency rather than a hung
	// request -- but the whole budget is spent *before* the proxy is asked, so
	// the client pays this plus Synapse's own time. It must stay short.
	nativeTimeout time.Duration
	// streamTimeout is the absolute backstop on a streamed native answer.
	//
	// Much longer than nativeTimeout: /state takes tens of seconds on a large
	// room, and the 5s budget chosen for /event would abandon every one of
	// them -- mid-stream, after bytes had already reached the client.
	//
	// It is deliberately *not* the latency budget. How long a streamed
	// response takes is set mostly by how fast the client drains it, which is
	// not ours to bound; streamIdleTimeout is what actually protects us. This
	// only stops a stream that progresses forever, and should never bind on a
	// real transfer.
	streamTimeout time.Duration
	// streamIdleTimeout bounds time *without progress* on a streamed answer.
	//
	// The real control. A client that stops accepting bytes is cut promptly; a
	// client that is merely slow is left to finish. Enforced as a write
	// deadline rather than a context, because cancelling a context does not
	// interrupt a Write already blocked in the kernel.
	//
	// Generous relative to what it guards: the gap between writes is one batch
	// of events plus, once per response, the auth-chain walk, which is
	// sub-second even on the room that needs the recursive fallback.
	streamIdleTimeout time.Duration

	// limitedOrigins tracks which servers have been rejected, so their count
	// can be exported without exporting their names.
	limitedMu      sync.Mutex
	limitedOrigins map[string]struct{}
}

// New builds a Handler. Without WithNative every request is proxied.
func New(serverName string, rc ratelimit.FederationSettings, p *proxy.Proxy, log zerolog.Logger, opts ...Option) *Handler {
	h := &Handler{
		proxy:             p,
		limiter:           ratelimit.New(rc),
		log:               log,
		serverName:        serverName,
		nativeTimeout:     5 * time.Second,
		streamTimeout:     900 * time.Second,
		streamIdleTimeout: 60 * time.Second,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Verifier authenticates an incoming federation request.
//
// A narrow interface rather than the concrete type, so the request path can be
// tested without signing keys -- and so the decision "what happens when
// verification fails" can be exercised, which is the one that decides whether
// a stranger can read room state.
type Verifier interface {
	Verify(r *http.Request) fedauth.Result
}

// Option configures a Handler.
type Option func(*Handler)

// WithStreamTimeout sets the absolute backstop on a streamed native answer.
func WithStreamTimeout(d time.Duration) Option {
	return func(h *Handler) {
		if d > 0 {
			h.streamTimeout = d
		}
	}
}

// WithStreamIdleTimeout bounds time without progress on a streamed answer.
func WithStreamIdleTimeout(d time.Duration) Option {
	return func(h *Handler) {
		if d > 0 {
			h.streamIdleTimeout = d
		}
	}
}

// WithNative supplies what native serving needs: our own answer, and
// verification of who is asking. Without it every request goes to Synapse.
func WithNative(r native.Resolver, v Verifier, timeout time.Duration) Option {
	return func(h *Handler) {
		h.resolver = r
		h.verifier = v
		if timeout > 0 {
			h.nativeTimeout = timeout
		}
	}
}

// Limiter exposes the rate limiter for metrics and maintenance.
func (h *Handler) Limiter() *ratelimit.Limiter { return h.limiter }

// noteLimited records that an origin was rejected and returns how many distinct
// origins have been. It answers "one noisy server or many?" without needing a
// per-origin metric label.
func (h *Handler) noteLimited(origin string) int {
	h.limitedMu.Lock()
	defer h.limitedMu.Unlock()
	if h.limitedOrigins == nil {
		h.limitedOrigins = make(map[string]struct{})
	}
	h.limitedOrigins[origin] = struct{}{}
	return len(h.limitedOrigins)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}

	// Match on the escaped path: the trailing parameter is percent-encoded and
	// must not be normalised on the way to the upstream.
	route, ok := Match(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}

	start := time.Now()

	origin := originFromAuth(r.Header.Get("Authorization"))

	endpointName := string(route.Endpoint)
	roomID := roomIDFor(route)
	eventID := eventIDFor(route, r)

	// Endpoints whose answer depends on the request body need it buffered.
	//
	// Three separate consumers read it and each one would otherwise leave the
	// stream empty for the next: mautrix's Authenticate reads it to verify the
	// signature (which covers the content), the native path parses it, and the
	// proxy forwards it on fallback. mautrix restores the body on the request
	// it *returns*, which Verify discards -- harmless while every endpoint was
	// a GET, and silently fatal for the first POST.
	var reqBody []byte
	if route.Endpoint.HasRequestBody() && r.Body != nil {
		var err error
		reqBody, err = io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
		_ = r.Body.Close()
		if err != nil {
			metrics.RequestsTotal.WithLabelValues(endpointName, "400").Inc()
			http.Error(w, `{"errcode":"M_NOT_JSON","error":"Could not read request body."}`, http.StatusBadRequest)
			return
		}
		if int64(len(reqBody)) > maxRequestBody {
			metrics.RequestsTotal.WithLabelValues(endpointName, "413").Inc()
			http.Error(w, `{"errcode":"M_TOO_LARGE","error":"Request body too large."}`, http.StatusRequestEntityTooLarge)
			return
		}
		restoreBody(r, reqBody)
	}

	// Tracked so a fallback can be accounted for end to end: the client pays
	// our attempt plus Synapse's, and until now the second half was invisible.
	var (
		fellBack       bool
		fallbackReason string
		nativeSpent    time.Duration
	)

	// Answered natively whenever we can.
	serveNatively := h.resolver != nil

	// Rate limiting applies only to what we answer ourselves.
	//
	// A request going to Synapse is protected by Synapse's own limiter, and
	// adding ours in front would put it through two limiters in series.
	//
	// The origin is the one claimed in the X-Matrix header, not a verified
	// one, and the limiter runs before verification on purpose: verifying can
	// require a network key fetch, so a limiter that ran after it could be
	// used to generate outbound load. Synapse limits on the claimed origin for
	// the same reason.
	if serveNatively {
		release, outcome, err := h.limiter.Acquire(r.Context(), origin)
		if err != nil {
			if errors.Is(err, ratelimit.ErrLimitExceeded) {
				metrics.RateLimitedTotal.WithLabelValues(endpointName).Inc()
				metrics.RequestsTotal.WithLabelValues(endpointName, "429").Inc()
				metrics.RateLimitedOrigins.Set(float64(h.noteLimited(origin)))
				// Logged at warn with the origin: metrics deliberately carry
				// no per-origin label, so this is where "which server" is
				// answered.
				h.log.Warn().
					Str("endpoint", endpointName).
					Str("origin", origin).
					Str("uri", r.URL.RequestURI()).
					Int("reject_limit", h.limiter.Settings().RejectLimit).
					Int("concurrent", h.limiter.Settings().Concurrent).
					Msg("Rate limited: rejected request")
				writeLimitExceeded(w, h.limiter.Settings())
				return
			}
			// The client went away while queued; there is nobody left to answer.
			metrics.RequestsTotal.WithLabelValues(endpointName, "canceled").Inc()
			return
		}
		// Observed for every acquisition, not only the delayed ones.
		//
		// A histogram fed only when the limiter delayed a request would report
		// a p50 of half a second and imply every request waits that long. The
		// zero waits are what make the quantiles mean anything, and they are
		// the overwhelming majority.
		metrics.RateLimitQueueWait.WithLabelValues(endpointName).Observe(outcome.Wait.Seconds())

		if outcome.Slept || outcome.Queued {
			if outcome.Slept {
				metrics.RateLimitSleptTotal.WithLabelValues(endpointName).Inc()
			}
			h.log.Info().
				Str("endpoint", endpointName).
				Str("origin", origin).
				Bool("slept", outcome.Slept).
				Bool("queued", outcome.Queued).
				Dur("wait_ms", outcome.Wait).
				Msg("Rate limited: delayed request")
		}

		nat := h.serveNative(w, r, endpointName, roomID, eventID, reqBody)
		release()
		if nat.Served {
			metrics.RequestsTotal.WithLabelValues(endpointName, "native").Inc()
			// Response size is observed on the proxy path only, so a promoted
			// endpoint reported none at all -- and payload size is what the
			// /state decision turns on.
			metrics.ResponseBytes.WithLabelValues(endpointName).Observe(float64(nat.Bytes))

			// Logged here because the request ends here: the shared log below
			// runs after the proxy forward, so until now everything we
			// answered ourselves was invisible. That was tolerable while
			// native traffic was a rounding error and is not once an endpoint
			// is promoted -- metrics deliberately carry no per-origin label,
			// so this line is the only place "which server asked" is recorded.
			//
			// No upstream was involved, so upstream_ms and backend are
			// omitted rather than reported as zero.
			withCache(withRoom(h.log.Info(), nat.RoomID), nat.Cache).
				Str("endpoint", endpointName).
				Str("param", route.Param).
				Str("served", "native").
				Str("status", strconv.Itoa(nat.Status)).
				Int64("bytes", nat.Bytes).
				Dur("total_ms", time.Since(start)).
				Str("origin", origin).
				Msg("Served federation request")
			return
		}
		fellBack, fallbackReason, nativeSpent = true, nat.Reason, nat.Spent
	}

	// A client that has gone needs no answer, from us or from Synapse.
	//
	// Falling back exists to give a remote server an answer we could not
	// produce ourselves. When it hung up there is nobody left to receive one,
	// and forwarding only asks Synapse to compute a response that will be
	// discarded -- on /state_ids that is seconds of database time per request,
	// so it is real work rather than a rounding error. On this deployment the
	// case is common rather than rare: Tuwunel-style servers hang up on
	// /state_ids constantly.
	//
	// The check is on the request context rather than the fallback reason
	// because that is the direct question: is anyone still waiting.
	if r.Context().Err() != nil {
		upstreamSkipped.WithLabelValues(endpointName).Inc()
		metrics.RequestsTotal.WithLabelValues(endpointName, "canceled").Inc()
		h.log.Debug().
			Str("endpoint", endpointName).
			Str("origin", origin).
			Bool("fell_back", fellBack).
			Str("fallback_reason", fallbackReason).
			Dur("total_ms", time.Since(start)).
			Msg("Client gone before we answered; not forwarding to Synapse")
		return
	}

	// Counted here rather than at the fallback itself: this metric means the
	// proxy went on to serve the request, which is only true once we have got
	// past the check above.
	if fellBack {
		nativeFallbackServed.WithLabelValues(endpointName).Inc()
	}

	// Verification consumed the buffered body; the proxy needs it again.
	restoreBody(r, reqBody)

	// Nothing is captured: the body streams through, which matters for a
	// /state answer of up to 97MB.
	res := h.proxy.Forward(w, r, 0)

	endpoint := string(route.Endpoint)
	if fellBack {
		fallbackUpstream.WithLabelValues(endpoint, fallbackReason, statusLabel(res)).Inc()
		fallbackTotalDuration.WithLabelValues(endpoint, fallbackReason).
			Observe((nativeSpent + res.Duration).Seconds())
	}
	metrics.RequestsTotal.WithLabelValues(endpoint, statusLabel(res)).Inc()
	metrics.UpstreamDuration.WithLabelValues(endpoint).Observe(res.Duration.Seconds())
	metrics.ResponseBytes.WithLabelValues(endpoint).Observe(float64(res.Bytes))
	if res.Err != nil {
		metrics.UpstreamErrorsTotal.WithLabelValues(endpoint, res.Backend).Inc()
	}

	ev := h.log.Info()
	switch {
	case res.Err != nil:
		ev = h.log.Error().Err(res.Err)
	case res.Canceled:
		// Remote servers hang up on slow /state requests routinely; this is
		// worth seeing at debug level, not as noise in the request log.
		ev = h.log.Debug()
	}
	withRoom(ev, roomIDFor(route)).
		Str("endpoint", endpoint).
		Str("param", route.Param).
		Str("served", "proxy").
		Bool("fell_back", fellBack).
		Str("fallback_reason", fallbackReason).
		Str("backend", res.Backend).
		Str("status", statusLabel(res)).
		Int64("bytes", res.Bytes).
		Dur("upstream_ms", res.Duration).
		Dur("total_ms", time.Since(start)).
		Str("origin", originFromAuth(r.Header.Get("Authorization"))).
		Msg("Served federation request")
}

// statusLabel renders the outcome for logs and metrics. A cancelled request
// never received a status, so reporting 0 would create a meaningless metric
// series; it gets its own label instead.
func statusLabel(res proxy.Result) string {
	if res.Canceled {
		return "canceled"
	}
	if res.Status == 0 {
		return "none"
	}
	return strconv.Itoa(res.Status)
}

// withRoom adds the decoded room ID to a log line, or nothing when it is not
// known -- a proxied /event never learns its room, and an empty field would
// read as a request about no room.
func withRoom(ev *zerolog.Event, roomID string) *zerolog.Event {
	if roomID == "" {
		return ev
	}
	return ev.Str("room_id", roomID)
}

// withCache adds whether the answer came from memory: cache is the summary
// (hit, miss, partial, joined) and cache_detail the per-cache hits/lookups.
// Omitted when nothing cacheable was looked up, as on a proxied request.
func withCache(ev *zerolog.Event, t *cache.Trace) *zerolog.Event {
	s := t.Summary()
	if s == "" {
		return ev
	}
	ev = ev.Str("cache", s)
	if d := t.Detail(); d != "" {
		ev = ev.Str("cache_detail", d)
	}
	return ev
}

// roomIDFor returns the decoded room ID for the endpoints that take one.
func roomIDFor(route Route) string {
	if route.Endpoint == EndpointEvent {
		return ""
	}
	return decodeParam(route.Param)
}

// eventIDFor returns the decoded event ID, which is the path parameter for
// /event and the required query parameter for the state endpoints.
func eventIDFor(route Route, r *http.Request) string {
	if route.Endpoint == EndpointEvent {
		return decodeParam(route.Param)
	}
	return r.URL.Query().Get("event_id")
}

// maxRequestBody bounds a buffered request body.
//
// /get_missing_events carries two event-ID lists and a limit; Synapse caps the
// limit at 20 but places no bound on earliest_events, so a remote server could
// send a very large list. One megabyte is far beyond any legitimate request and
// well below anything that threatens the process.
const maxRequestBody = 1 << 20

// restoreBody makes a buffered body readable again for the next consumer.
func restoreBody(r *http.Request, body []byte) {
	if body == nil {
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
}

// decodeParam percent-decodes a path parameter, returning it unchanged if it
// is not validly encoded.
func decodeParam(s string) string {
	if decoded, err := url.PathUnescape(s); err == nil {
		return decoded
	}
	return s
}
