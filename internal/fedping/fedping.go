// Package fedping is MSC4524, the federation bi-directional ping: a server
// asks another a question (POST .../ping), is answered, and the other server
// then confirms the answer in a signed request of its own (POST .../pong).
// Both directions, the signatures each way and the key fetches behind them
// are exercised in one exchange.
//
// Synapse does not implement it; main answers both paths 404 M_UNRECOGNIZED,
// which is what the MSC says a server with the endpoints disabled returns. So
// nginx is the switch, as for everything else here: route the four paths to
// the worker to enable it, leave them on main to disable it.
//
// A ping is started from the metrics listener (Initiate), never from the
// public listener: it makes outbound requests on demand.
//
// https://github.com/matrix-org/matrix-spec-proposals/pull/4524
package fedping

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedauth"
)

// The unstable paths are the ones other servers send to until the MSC is
// merged; the stable ones are served too, so a server that jumps ahead
// still reaches us.
const (
	unstablePrefix = "/_matrix/federation/unstable/uk.timedout.msc4524.tabletennis/"
	stablePrefix   = "/_matrix/federation/v1/"
)

// unstablePath and stablePath are an endpoint's path as mautrix builds
// requests: one segment each, so none is escaped.
func unstablePath(which string) mautrix.BaseURLPath {
	return mautrix.BaseURLPath{"_matrix", "federation", "unstable", "uk.timedout.msc4524.tabletennis", which}
}

func stablePath(which string) mautrix.BaseURLPath {
	return mautrix.BaseURLPath{"_matrix", "federation", "v1", which}
}

// Match reports whether an escaped path is one of the four endpoints, with
// the optional trailing slash Synapse's servlets accept.
func Match(escapedPath string) bool {
	_, ok := endpoint(escapedPath)
	return ok
}

func endpoint(escapedPath string) (string, bool) {
	p := strings.TrimSuffix(escapedPath, "/")
	for _, prefix := range []string{unstablePrefix, stablePrefix} {
		if rest, ok := strings.CutPrefix(p, prefix); ok && (rest == "ping" || rest == "pong") {
			return rest, true
		}
	}
	return "", false
}

const (
	// minInterval is the MSC's recommended limit: one ping per origin every
	// 30 seconds. Unauthenticated pings are limited by client address too.
	minInterval = 30 * time.Second
	// maxBody bounds a ping or pong body: a question and a few lines of
	// details.
	maxBody = 64 << 10
	// answerTTL is how long a question's answer is kept, so a retransmitted
	// question gets the same answer and no second pong.
	answerTTL = 10 * time.Minute
	// maxTracked bounds each of the maps below. Every entry is a few hundred
	// bytes at most (a question is at most 255 characters), so a count is a
	// size bound.
	maxTracked = 4096
	// maxPongs bounds the pongs waiting to be sent: each holds a goroutine
	// and, when sent, a connection.
	maxPongs = 32
	// pongTimeout bounds one pong request.
	pongTimeout = 30 * time.Second
)

// Options builds a Server.
type Options struct {
	ServerName string
	// Verifier checks X-Matrix signatures, with the worker's shared keys.
	Verifier *fedauth.Verifier
	// Client signs as ServerName; it sends pongs and pings and fetches keys
	// for the diagnostics.
	Client *federation.Client
	// PongDelay is waited before a pong, against amplification (the MSC's
	// "arbitrary delay"). Nil waits 2 to 5 seconds.
	PongDelay func() time.Duration
	Log       zerolog.Logger
}

// Server answers pings and pongs and starts pings of its own.
type Server struct {
	serverName string
	verifier   *fedauth.Verifier
	client     *federation.Client
	pongDelay  func() time.Duration
	log        zerolog.Logger
	now        func() time.Time

	ctx    context.Context
	cancel context.CancelFunc
	pongs  chan struct{}

	mu       sync.Mutex
	limits   map[string]time.Time // bucket -> last accepted ping
	answered map[string]answered  // origin + "\x00" + question -> answer
	pending  map[string]*pending  // our question -> the ping we started
}

type answered struct {
	answer string
	at     time.Time
}

// New builds a Server. Close stops the pongs it has yet to send.
func New(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		serverName: opts.ServerName,
		verifier:   opts.Verifier,
		client:     opts.Client,
		pongDelay:  opts.PongDelay,
		log:        opts.Log,
		now:        time.Now,
		ctx:        ctx,
		cancel:     cancel,
		pongs:      make(chan struct{}, maxPongs),
		limits:     map[string]time.Time{},
		answered:   map[string]answered{},
		pending:    map[string]*pending{},
	}
	if s.pongDelay == nil {
		s.pongDelay = func() time.Duration { return 2*time.Second + randDuration(3*time.Second) }
	}
	return s
}

// Close abandons the pongs not yet sent.
func (s *Server) Close() { s.cancel() }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	which, ok := endpoint(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognized request")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognized request")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return // the remote server went away
	}
	if len(body) > maxBody {
		writeError(w, http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "Request body too large")
		return
	}
	if which == "ping" {
		s.servePing(w, r, body)
	} else {
		s.servePong(w, r, body)
	}
}

// verify checks the request's X-Matrix signature over body.
func (s *Server) verify(r *http.Request, body []byte) fedauth.Result {
	vr := r.Clone(r.Context())
	vr.Body = io.NopCloser(bytes.NewReader(body))
	return s.verifier.Verify(vr)
}

type pingRequest struct {
	Origin   string `json:"origin,omitempty"`
	Question string `json:"question"`
}

type pingResponse struct {
	Answer  string   `json:"answer"`
	Details []string `json:"details,omitempty"`
}

func (s *Server) servePing(w http.ResponseWriter, r *http.Request, body []byte) {
	var req pingRequest
	if err := json.Unmarshal(body, &req); err != nil {
		received.WithLabelValues("ping", "invalid").Inc()
		writeError(w, http.StatusBadRequest, "M_NOT_JSON", "Content not JSON.")
		return
	}

	authenticated := r.Header.Get("Authorization") != ""
	origin := req.Origin
	if authenticated {
		// The origin field is ignored alongside a signature.
		res := s.verify(r, body)
		if !res.OK() {
			received.WithLabelValues("ping", "unauthorized").Inc()
			s.log.Info().Str("claimed", federation.ParseXMatrixAuth(r.Header.Get("Authorization")).Origin).
				Str("error", res.Err.Err).Msg("MSC4524 ping: signature not verified")
			writeError(w, http.StatusUnauthorized, orString(res.Err.ErrCode, "M_UNAUTHORIZED"), res.Err.Err)
			return
		}
		origin = res.Origin
	} else if origin == "" {
		received.WithLabelValues("ping", "invalid").Inc()
		writeError(w, http.StatusBadRequest, "M_MISSING_PARAM", "An unauthenticated ping must name its origin")
		return
	} else if !id.ValidateServerName(origin) {
		received.WithLabelValues("ping", "invalid").Inc()
		writeError(w, http.StatusBadRequest, "M_INVALID_PARAM", "origin is not a server name")
		return
	}
	if n := utf8.RuneCountInString(req.Question); n < 6 || n > 255 {
		received.WithLabelValues("ping", "invalid").Inc()
		writeError(w, http.StatusBadRequest, "M_INVALID_PARAM", "question must be between 6 and 255 characters")
		return
	}

	answer, retransmitted, wait := s.admit(origin, clientIP(r), authenticated, req.Question)
	if wait > 0 {
		received.WithLabelValues("ping", "rate_limited").Inc()
		writeLimitExceeded(w, wait)
		return
	}
	if retransmitted {
		received.WithLabelValues("ping", "retransmitted").Inc()
		writeJSON(w, http.StatusOK, pingResponse{Answer: answer})
		return
	}

	var details []string
	if !authenticated {
		// The usual reason for retrying without a signature is that ours was
		// refused, so say what we see of the origin's keys.
		details = s.keyDiagnostics(r.Context(), origin)
	}
	received.WithLabelValues("ping", "answered").Inc()
	s.log.Info().Str("origin", origin).Bool("authenticated", authenticated).Strs("details", details).
		Msg("MSC4524 ping answered; pong to follow")
	writeJSON(w, http.StatusOK, pingResponse{Answer: answer, Details: details})
	s.schedulePong(origin, answer)
}

// admit applies the rate limits and the retransmission cache. It returns the
// answer to give, whether the question was seen before, and how long to wait
// when refused.
func (s *Server) admit(origin, ip string, authenticated bool, question string) (string, bool, time.Duration) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	key := origin + "\x00" + question
	if a, ok := s.answered[key]; ok && now.Sub(a.at) < answerTTL {
		return a.answer, true, 0
	}

	buckets := []string{"origin\x00" + origin}
	if !authenticated && ip != "" {
		buckets = append(buckets, "ip\x00"+ip)
	}
	var wait time.Duration
	for _, b := range buckets {
		if last, ok := s.limits[b]; ok {
			wait = max(wait, minInterval-now.Sub(last))
		}
	}
	if wait > 0 {
		return "", false, wait
	}
	if !prune(s.limits, len(buckets), func(t time.Time) bool { return now.Sub(t) >= minInterval }) ||
		!prune(s.answered, 1, func(a answered) bool { return now.Sub(a.at) >= answerTTL }) {
		// Full of live entries: thousands of origins pinged us in the last
		// half minute. Refuse rather than grow.
		return "", false, minInterval
	}
	for _, b := range buckets {
		s.limits[b] = now
	}
	answer := randomToken()
	s.answered[key] = answered{answer: answer, at: now}
	return answer, false, 0
}

// prune makes room for n entries, dropping the stale ones when the map is
// full. It reports false when there is still no room.
func prune[V any](m map[string]V, n int, stale func(V) bool) bool {
	if len(m)+n <= maxTracked {
		return true
	}
	for k, v := range m {
		if stale(v) {
			delete(m, k)
		}
	}
	return len(m)+n <= maxTracked
}

// schedulePong confirms answer to origin after the delay, in the background.
func (s *Server) schedulePong(origin, answer string) {
	select {
	case s.pongs <- struct{}{}:
	default:
		sent.WithLabelValues("pong", "dropped").Inc()
		s.log.Warn().Str("origin", origin).Msg("MSC4524: too many pongs waiting; this one is not sent")
		return
	}
	go func() {
		defer func() { <-s.pongs }()
		t := time.NewTimer(s.pongDelay())
		defer t.Stop()
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(s.ctx, pongTimeout)
		defer cancel()
		err := s.client.MakeRequest(ctx, origin, true, http.MethodPost,
			unstablePath("pong"), pongRequest{Question: answer}, &struct{}{})
		outcome := outcomeOf(err)
		sent.WithLabelValues("pong", outcome).Inc()
		ev := s.log.Info()
		if err != nil {
			ev = s.log.Warn().Err(err)
		}
		ev.Str("origin", origin).Str("outcome", outcome).Msg("MSC4524 pong sent")
	}()
}

type pongRequest struct {
	Question string   `json:"question"`
	Details  []string `json:"details,omitempty"`
}

func (s *Server) servePong(w http.ResponseWriter, r *http.Request, body []byte) {
	if r.Header.Get("Authorization") == "" {
		received.WithLabelValues("pong", "unauthorized").Inc()
		writeError(w, http.StatusUnauthorized, "M_UNAUTHORIZED", "Missing Authorization headers")
		return
	}
	res := s.verify(r, body)
	if !res.OK() {
		received.WithLabelValues("pong", "unauthorized").Inc()
		writeError(w, http.StatusUnauthorized, orString(res.Err.ErrCode, "M_UNAUTHORIZED"), res.Err.Err)
		return
	}
	var req pongRequest
	if err := json.Unmarshal(body, &req); err != nil {
		received.WithLabelValues("pong", "invalid").Inc()
		writeError(w, http.StatusBadRequest, "M_NOT_JSON", "Content not JSON.")
		return
	}
	if req.Question == "" {
		received.WithLabelValues("pong", "invalid").Inc()
		writeError(w, http.StatusBadRequest, "M_MISSING_PARAM", "Missing question")
		return
	}
	if !s.matchPong(res.Origin, req.Question, req.Details) {
		received.WithLabelValues("pong", "unknown").Inc()
		s.log.Info().Str("origin", res.Origin).Msg("MSC4524 pong for a question we did not ask")
		writeError(w, http.StatusNotFound, "M_NOT_FOUND", "Unknown question")
		return
	}
	received.WithLabelValues("pong", "matched").Inc()
	writeJSON(w, http.StatusOK, struct{}{})
}

// keyDiagnostics says, without the raw error, whether origin's signing keys
// can be fetched from it.
func (s *Server) keyDiagnostics(ctx context.Context, origin string) []string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := s.client.ServerKeys(ctx, origin)
	if err != nil {
		return []string{"Could not fetch signing keys for " + origin + ": " + describe(err)}
	}
	if resp.ServerName != origin {
		return []string{"The signing keys served for " + origin + " are for " + resp.ServerName}
	}
	if !resp.ValidUntilTS.Time.After(s.now()) {
		return []string{"The signing keys published by " + origin + " are past their valid_until_ts"}
	}
	// An unsigned ping carries no key ID, so which key the origin signs with
	// is for it to compare.
	ids := make([]string, 0, len(resp.VerifyKeys))
	for kid := range resp.VerifyKeys {
		ids = append(ids, string(kid))
	}
	slices.Sort(ids)
	return []string{"Fetched the signing keys of " + origin + " directly; current: " + strings.Join(ids, ", ")}
}

// describe is an error as a sentence that reveals nothing of ours: the class
// of failure, or the status the remote answered with.
func describe(err error) string {
	var herr mautrix.HTTPError
	if errors.As(err, &herr) && herr.Response != nil {
		msg := "the server answered HTTP " + strconv.Itoa(herr.Response.StatusCode)
		if herr.RespError != nil && herr.RespError.ErrCode != "" {
			msg += " " + herr.RespError.ErrCode
		}
		return msg
	}
	switch outcomeOf(err) {
	case "timeout":
		return "the request timed out"
	case "dns":
		return "the server name could not be resolved"
	case "refused":
		return "the connection was refused"
	case "tls":
		return "the TLS handshake failed"
	case "filtered":
		return "the server is not in our federation_domain_whitelist"
	}
	return "the request failed"
}

// outcomeOf classifies a request's error for metrics and diagnostics.
func outcomeOf(err error) string {
	if err == nil {
		return "ok"
	}
	var herr mautrix.HTTPError
	if errors.As(err, &herr) && herr.Response != nil {
		switch herr.Response.StatusCode {
		case http.StatusNotFound, http.StatusMethodNotAllowed:
			return "not_found"
		case http.StatusUnauthorized:
			return "unauthorized"
		case http.StatusTooManyRequests:
			return "rate_limited"
		}
		return "http_error"
	}
	msg := err.Error()
	switch {
	case errors.Is(err, federation.ErrServerNameFiltered):
		return "filtered"
	case errors.Is(err, context.DeadlineExceeded), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "server name resolution"):
		return "dns"
	case strings.Contains(msg, "connection refused"):
		return "refused"
	case strings.Contains(msg, "tls:"), strings.Contains(msg, "x509:"), strings.Contains(msg, "certificate"):
		return "tls"
	}
	return "error"
}

// clientIP is the address an unsigned ping is limited by: the last
// X-Forwarded-For entry, the one our nginx appended
// ($proxy_add_x_forwarded_for), else the connection's address. Not the first,
// as Synapse takes it: that one is whatever the client sent, and a limit on
// it is no limit. Behind a further proxy the last entry is that proxy, which
// only makes the limit stricter.
func clientIP(r *http.Request) string {
	if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
		entries := strings.Split(xff[len(xff)-1], ",")
		if last := strings.TrimSpace(entries[len(entries)-1]); last != "" {
			return last
		}
	}
	return r.RemoteAddr
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, errcode, msg string) {
	writeJSON(w, status, map[string]string{"errcode": errcode, "error": msg})
}

// writeLimitExceeded is Synapse's 429: M_LIMIT_EXCEEDED, retry_after_ms, and
// Retry-After rounded up to whole seconds.
func writeLimitExceeded(w http.ResponseWriter, wait time.Duration) {
	ms := int(wait.Milliseconds())
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(float64(ms)/1000))))
	writeJSON(w, http.StatusTooManyRequests, map[string]any{
		"errcode": "M_LIMIT_EXCEEDED", "error": "Too Many Requests", "retry_after_ms": ms,
	})
}

func orString(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func randDuration(limit time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(limit)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

var (
	received = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_msc4524_received_total",
		Help: "MSC4524 pings and pongs other servers sent us, by outcome.",
	}, []string{"endpoint", "outcome"})
	sent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_msc4524_sent_total",
		Help: "MSC4524 pings and pongs we sent, by outcome.",
	}, []string{"endpoint", "outcome"})
)
