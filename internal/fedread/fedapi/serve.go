package fedapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/cache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/native"
)

// nativeResult reports what the native path did.
//
// Status and Bytes exist so a natively served request can be logged like any
// other. They have no meaning unless Served is true.
type nativeResult struct {
	Served bool
	// Reason is why we fell back, when Served is false.
	Reason string
	Spent  time.Duration
	Status int
	Bytes  int64
	// RoomID is the room the answer was about, for the request log. /event
	// names only an event in its URL, so for it this is read from the answer.
	RoomID string
	// Cache records which cacheable lookups were answered from memory.
	Cache *cache.Trace
	// Meta carries what the body cannot: whether a DAG walk truncated.
	Meta native.Meta
	// StateResult is set for a streamed answer.
	StateResult *matrixstate.StateResult
}

// serveNative answers a request from our own implementation.
//
// It reports whether the request was answered. When it returns false nothing
// has been written and the caller must fall back to the proxy, which is the
// whole safety property of native serving: any doubt costs latency, never
// correctness. Nothing is written until a complete answer exists, so a failure
// halfway through cannot leave a half-written response.
func (h *Handler) serveNative(w http.ResponseWriter, r *http.Request, endpoint, roomID, eventID string, reqBody []byte) nativeResult {
	if h.resolver == nil {
		return nativeResult{Reason: "no_resolver"}
	}
	attemptStart := time.Now()

	// Every lookup made on this request's behalf records into the trace,
	// through the context, so the request log can say whether the answer came
	// from memory. Attached to the request itself so the streamed path, which
	// derives its own context from it, is traced too.
	traceCtx, trace := cache.WithTrace(r.Context())
	r = r.WithContext(traceCtx)

	// Verification happens on the real request. The answer is served, so an
	// unverified origin would be reading room state it may have no right to.
	//
	// The caller has already passed the rate limiter, deliberately before this
	// point: verification can require a network key fetch, and doing that first
	// would let unverifiable requests generate outbound load without ever being
	// limited. Acquiring it again here would deadlock against the slot the
	// caller is holding.
	//
	// A rejection falls back to the proxy rather than being served. Our
	// verifier has been wrong in both directions before, and Synapse verifies
	// independently — so falling back cannot let anything through that Synapse
	// would refuse, while serving our rejection could break legitimate
	// federation over a bug of ours.
	result := h.verifier.Verify(r)
	if !result.OK() {
		nativeFallback.WithLabelValues(endpoint, "auth_rejected").Inc()
		return nativeResult{Reason: "auth_rejected", Spent: time.Since(attemptStart)}
	}

	// A streamed endpoint takes a different path: it writes as it resolves, so
	// it cannot use the build-then-write shape the fallback guarantee relies
	// on. See serveStreamed for how that guarantee is narrowed rather than
	// dropped.
	if endpointStreams(endpoint) {
		res := h.serveStreamed(w, r, endpoint, roomID, eventID, result.Origin, attemptStart)
		res.RoomID = roomID
		res.Cache = trace
		return res
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.nativeTimeout)
	defer cancel()

	start := time.Now()
	body, status, meta, err := h.answer(ctx, endpoint, result.Origin, roomID, eventID, reqBody)
	elapsed := time.Since(start)

	if err != nil {
		// Distinguish our deadline expiring from the client hanging up. The
		// context carries both -- it is derived from the request's -- and
		// conflating them reports a remote server's disconnect as a timeout of
		// ours. On this deployment that is the common case, not the rare one:
		// Tuwunel-style servers hang up on /state_ids constantly.
		reason := "error"
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			reason = "timeout"
		case errors.Is(ctx.Err(), context.Canceled):
			reason = "client_gone"
		}
		nativeFallback.WithLabelValues(endpoint, reason).Inc()
		ev := h.log.Warn()
		if reason == "client_gone" {
			// Not a defect: nobody is left to answer.
			ev = h.log.Debug()
		}
		withCache(withRoom(ev.Err(err), roomID), trace).
			Str("endpoint", endpoint).Str("origin", result.Origin).
			Str("reason", reason).
			Dur("took", elapsed).
			Msg("Native answer not served; falling back to Synapse")
		return nativeResult{Reason: reason, Spent: time.Since(attemptStart)}
	}

	nativeDuration.WithLabelValues(endpoint).Observe(elapsed.Seconds())
	nativeServed.WithLabelValues(endpoint, statusText(status)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)

	spent := time.Since(attemptStart)
	nativeRequestDuration.WithLabelValues(endpoint).Observe(spent.Seconds())

	return nativeResult{
		Served: true,
		Meta:   meta,
		Spent:  spent,
		Status: status,
		Bytes:  int64(len(body)),
		RoomID: answeredRoom(roomID, status, body),
		Cache:  trace,
	}
}

// answer computes the response, converting a panic into an error so a bug in
// the native path falls back to the proxy rather than killing the connection.
func (h *Handler) answer(ctx context.Context, endpoint, origin, roomID, eventID string, reqBody []byte) (body []byte, status int, meta native.Meta, err error) {
	defer func() {
		if p := recover(); p != nil {
			nativeFallback.WithLabelValues(endpoint, "panic").Inc()
			h.log.Error().Interface("panic", p).Str("endpoint", endpoint).
				Msg("Native answer panicked; falling back to Synapse")
			body, status, meta, err = nil, 0, native.Meta{}, errPanicked
		}
	}()
	return native.Answer(ctx, h.resolver, h.serverName, native.Request{
		Endpoint: endpoint,
		Origin:   origin,
		RoomID:   roomID,
		EventID:  eventID,
		Body:     reqBody,
	})
}

// errPanicked marks a recovered panic, so the caller falls back to the proxy
// rather than treating it as a real answer.
var errPanicked = errors.New("native answer panicked")

func statusText(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "2xx"
	case status == http.StatusNotFound:
		return "404"
	case status == http.StatusForbidden:
		return "403"
	case status == http.StatusUnauthorized:
		return "401"
	default:
		return "other"
	}
}

// endpointStreams reports whether an endpoint writes its answer incrementally.
func endpointStreams(endpoint string) bool {
	return Endpoint(endpoint).Streams()
}

// answeredRoom names the room a served answer was about.
//
// The state endpoints carry the room in their URL. /event does not, so its
// room is read from the PDU we served. An error answer carries no PDU, and a
// room version 12 m.room.create has no room_id at all; both log without one
// rather than with a guess.
func answeredRoom(roomID string, status int, body []byte) string {
	if roomID != "" || status != http.StatusOK {
		return roomID
	}
	return gjson.GetBytes(body, "pdus.0.room_id").String()
}
