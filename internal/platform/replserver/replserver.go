// Package replserver is the server side of Synapse's HTTP replication: the
// listener other Synapse processes reach this one on, at its instance_map
// address, as synapse/replication/http/_base.py's ReplicationEndpoint
// serves it.
//
// No endpoint is registered here. Each part that writes a stream registers
// what Synapse's writer of that stream serves (typing's fed_send_edu and
// get_repl_stream_updates, first). What this package owns is the contract
// every endpoint shares:
//
//   - the path, /_synapse/replication/<NAME>/<path args...>[/<txn_id>]
//     (NAME/ with its slash when there are none), each argument one
//     segment, percent-decoded (http/server.py
//     _get_handler_for_request): an unknown path is 404 and a known path
//     with another method 405, both M_UNRECOGNIZED "Unrecognized request";
//     HEAD is GET;
//   - the secret, checked before anything else, when homeserver.yaml sets
//     worker_replication_secret: exactly one "Authorization: Bearer
//     <secret>". Synapse raises RuntimeError for anything else, which
//     renders as 500 M_UNKNOWN "Internal server error" (_base.py:156);
//   - the body: {} for GET, otherwise a JSON object, or 400 M_NOT_JSON
//     "Content not JSON." / M_BAD_JSON "Content must be a JSON object."
//     (http/servlet.py);
//   - the caller's stream positions (_INT_STREAM_POS in the body:
//     {instance_name, streams}), waited for before the request is handled
//     (_base.py:445), so that what it wrote is visible here;
//   - a cached endpoint's answer, kept by txn_id for 30 minutes after it
//     completes and shared by requests that arrive while it runs, so a
//     retried request is not handled twice (ResponseCache, _base.py:129). A
//     failure is not kept;
//   - our own stream positions (_INT_STREAM_POS in the response:
//     {stream: token}) for every stream this process writes, on an endpoint
//     that waits for streams, so the caller sees our writes before it goes
//     on (_base.py:468).
//
// A handler runs to the end even if its caller hangs up: Synapse's
// replication endpoints are not cancellable, and a cached one's answer
// belongs to every request with its txn_id, not to the first.
package replserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

// Prefix is where replication endpoints live (REPLICATION_PREFIX).
const Prefix = "/_synapse/replication/"

// StreamPositionKey carries stream positions in a request body and in a
// response (_STREAM_POSITION_KEY).
const StreamPositionKey = "_INT_STREAM_POS"

// CacheTimeout is how long a cached endpoint's answer is kept after it
// completes (_base.py:133).
const CacheTimeout = 30 * time.Minute

// DefaultMaxBody is Synapse's request size limit on a process without the
// media repository: MAX_REQUEST_SIZE, 200 PDUs of 64 KiB
// (api/constants.py:72, app/_base.py:902). Synapse drops the connection of a
// request past it; we answer 413.
const DefaultMaxBody = 200 * 65536

// DefaultCacheBytes bounds the answers kept for retries. Synapse's
// ResponseCache is bounded only by its 30 minutes; ours is bounded by size
// too, and an entry evicted early only means a retry is handled again,
// which is what a retry after the 30 minutes gets anyway.
const DefaultCacheBytes = 16 << 20

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_replication_server_requests_total",
		Help: "Replication requests other Synapse processes made to this one, by endpoint and status code.",
	}, []string{"endpoint", "code"})
	duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goworker_replication_server_request_seconds",
		Help:    "Time to answer a replication request, by endpoint: the wait for the caller's streams included.",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30},
	}, []string{"endpoint"})
	cacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_replication_server_cache_total",
		Help: "Cached endpoints' requests, by endpoint and outcome: miss (handled), hit (an earlier answer), joined (one still running).",
	}, []string{"endpoint", "outcome"})
	cacheBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_replication_server_cache_bytes",
		Help: "Bytes of answers kept for retried replication requests.",
	})
	endpointsServed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_replication_server_endpoints",
		Help: "Replication endpoints registered. 0 is a listener with nothing to serve.",
	})
)

// Error is a SynapseError: the status, errcode and message an endpoint
// answers with. Any other error a handler returns is a 500 "Internal server
// error", as Synapse renders an unexpected exception.
type Error struct {
	Code    int
	Errcode string
	Msg     string
}

func (e *Error) Error() string { return fmt.Sprintf("%d %s: %s", e.Code, e.Errcode, e.Msg) }

// Request is what a handler gets.
type Request struct {
	// Args are the endpoint's path arguments, percent-decoded, by name.
	Args map[string]string
	// Content is the JSON body's members ({} for GET), the caller's
	// _INT_STREAM_POS included.
	Content map[string]json.RawMessage
	// Query is the URL's query, for a GET's parameters.
	Query url.Values
}

// Handler answers one request with a status and a JSON object. A body
// must not contain StreamPositionKey. A cached endpoint's handler is called
// once per txn_id, however many requests carry it.
type Handler func(ctx context.Context, req Request) (int, map[string]any, error)

// Endpoint is one ReplicationEndpoint.
type Endpoint struct {
	// Name is the endpoint's NAME, its path's first segment.
	Name string
	// Method is GET, POST or PUT.
	Method string
	// PathArgs are the names of the segments after Name (PATH_ARGS).
	PathArgs []string
	// Cache adds a txn_id segment and keeps the answer by it (CACHE). It is
	// Synapse's default, for a GET too; set it unless the endpoint sets
	// CACHE = False.
	Cache bool
	// WaitForStreams waits for the caller's stream positions before
	// handling and returns ours with the answer (WAIT_FOR_STREAMS).
	WaitForStreams bool
	Handle         Handler
}

// Waiter waits until this process has seen a stream reach a position
// written by another instance (ReplicationDataHandler.
// wait_for_stream_position): at most a few seconds, and then the request
// goes on regardless, as Synapse's does after its timeout.
type Waiter interface {
	WaitForStreamPosition(ctx context.Context, instance, stream string, position int64)
}

// Options builds a Server.
type Options struct {
	// Instance is this process's instance_name. A caller's positions under
	// it are not waited for: they are ours.
	Instance string
	// Secret is homeserver.yaml's worker_replication_secret, read on every
	// request; "" accepts every request, as Synapse does with none set.
	Secret func() string
	// Positions are the streams this process writes and how far each is:
	// what the response's _INT_STREAM_POS carries. Nil is none.
	Positions func() map[string]int64
	// Waiter is required as soon as an endpoint waits for streams.
	Waiter Waiter
	// MaxBody defaults to DefaultMaxBody; CacheBytes to DefaultCacheBytes.
	MaxBody    int64
	CacheBytes int64
	Log        zerolog.Logger
	// now is replaced in tests.
	now func() time.Time
}

// Server serves the registered endpoints.
type Server struct {
	opts Options

	mu     sync.RWMutex
	byName map[string]*Endpoint

	cache *responseCache
}

// New builds a Server with no endpoints.
func New(opts Options) *Server {
	if opts.Secret == nil {
		opts.Secret = func() string { return "" }
	}
	if opts.MaxBody <= 0 {
		opts.MaxBody = DefaultMaxBody
	}
	if opts.CacheBytes <= 0 {
		opts.CacheBytes = DefaultCacheBytes
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	return &Server{
		opts:   opts,
		byName: map[string]*Endpoint{},
		cache:  newResponseCache(opts.CacheBytes, opts.now),
	}
}

// Register adds an endpoint. Two with one name, an unknown method, or an
// endpoint that waits for streams on a server with no Waiter, are refused:
// the last would silently never wait.
func (s *Server) Register(e Endpoint) error {
	switch e.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut:
	default:
		return fmt.Errorf("replserver: %s: method %q", e.Name, e.Method)
	}
	if e.Name == "" || strings.Contains(e.Name, "/") || e.Handle == nil {
		return fmt.Errorf("replserver: endpoint %q needs a one-segment name and a handler", e.Name)
	}
	for _, a := range e.PathArgs {
		if a == "instance_name" || a == "txn_id" {
			// Both are reserved (_base.py:137, the txn_id suffix).
			return fmt.Errorf("replserver: %s: path argument %q is reserved", e.Name, a)
		}
	}
	if e.WaitForStreams && e.Method != http.MethodGet && s.opts.Waiter == nil {
		return fmt.Errorf("replserver: %s waits for the caller's streams, and this server has no Waiter", e.Name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byName[e.Name]; ok {
		return fmt.Errorf("replserver: endpoint %q registered twice", e.Name)
	}
	ep := e
	s.byName[e.Name] = &ep
	endpointsServed.Set(float64(len(s.byName)))
	return nil
}

// Endpoints is how many are registered.
func (s *Server) Endpoints() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byName)
}

// ServeHTTP serves /health, as every Synapse listener does, and the
// replication endpoints.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		_, _ = w.Write([]byte("OK"))
		return
	}
	start := time.Now()
	label := "unknown"
	code := s.serve(w, r, &label)
	requests.WithLabelValues(label, strconv.Itoa(code)).Inc()
	duration.WithLabelValues(label).Observe(time.Since(start).Seconds())
}

var (
	errUnrecognized = &Error{Code: http.StatusNotFound, Errcode: "M_UNRECOGNIZED", Msg: "Unrecognized request"}
	errMethod       = &Error{Code: http.StatusMethodNotAllowed, Errcode: "M_UNRECOGNIZED", Msg: "Unrecognized request"}
	errNotJSON      = &Error{Code: http.StatusBadRequest, Errcode: "M_NOT_JSON", Msg: "Content not JSON."}
	errNotObject    = &Error{Code: http.StatusBadRequest, Errcode: "M_BAD_JSON", Msg: "Content must be a JSON object."}
	errTooLarge     = &Error{Code: http.StatusRequestEntityTooLarge, Errcode: "M_TOO_LARGE", Msg: "Request content is too large"}
	errInternal     = &Error{Code: http.StatusInternalServerError, Errcode: "M_UNKNOWN", Msg: "Internal server error"}
)

// serve answers r and returns the status it answered with.
func (s *Server) serve(w http.ResponseWriter, r *http.Request, label *string) int {
	e, args, txnID, err := s.route(r)
	if err != nil {
		return writeError(w, err)
	}
	*label = e.Name

	if secret := s.opts.Secret(); secret != "" {
		if err := checkAuth(r.Header.Values("Authorization"), secret); err != nil {
			s.opts.Log.Warn().Str("endpoint", e.Name).Str("reason", err.Error()).
				Msg("replication request refused")
			return writeError(w, errInternal)
		}
	}

	content := map[string]json.RawMessage{}
	if e.Method != http.MethodGet {
		if content, err = s.readBody(w, r); err != nil {
			return writeError(w, err)
		}
	}
	ctx := context.WithoutCancel(r.Context())
	if err := s.waitForCaller(ctx, content); err != nil {
		return writeError(w, err)
	}

	req := Request{Args: args, Content: content, Query: r.URL.Query()}
	var (
		status int
		body   map[string]json.RawMessage
	)
	if e.Cache {
		var outcome string
		status, body, outcome, err = s.cache.wrap(r.Context(), e.Name, txnID, func() (int, map[string]json.RawMessage, error) {
			return run(ctx, e, req)
		})
		cacheLookups.WithLabelValues(e.Name, outcome).Inc()
	} else {
		status, body, err = run(ctx, e, req)
	}
	if err != nil {
		var serr *Error
		if !errors.As(err, &serr) {
			s.opts.Log.Error().Err(err).Str("endpoint", e.Name).Msg("replication request failed")
		}
		return writeError(w, err)
	}

	if e.WaitForStreams {
		// A copy: a cached body is shared.
		out := make(map[string]json.RawMessage, len(body)+1)
		for k, v := range body {
			out[k] = v
		}
		positions := map[string]int64{}
		if s.opts.Positions != nil {
			positions = s.opts.Positions()
		}
		raw, err := json.Marshal(positions)
		if err != nil {
			return writeError(w, err)
		}
		out[StreamPositionKey] = raw
		body = out
	}
	return writeJSON(w, status, body)
}

// route finds r's endpoint and its arguments.
func (s *Server) route(r *http.Request) (*Endpoint, map[string]string, string, error) {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), Prefix)
	if !ok {
		return nil, nil, "", errUnrecognized
	}
	segs := strings.Split(rest, "/")
	s.mu.RLock()
	e := s.byName[segs[0]]
	s.mu.RUnlock()
	if e == nil {
		return nil, nil, "", errUnrecognized
	}
	want := 1 + len(e.PathArgs)
	if e.Cache {
		want++
	}
	if want == 1 {
		// No arguments at all: the pattern is "^/_synapse/replication/NAME/$",
		// with its trailing slash (_base.py:399).
		if len(segs) != 2 || segs[1] != "" {
			return nil, nil, "", errUnrecognized
		}
		segs = segs[:1]
	}
	if len(segs) != want {
		return nil, nil, "", errUnrecognized
	}
	for _, seg := range segs {
		if seg == "" {
			// [^/]+ matches no empty segment.
			return nil, nil, "", errUnrecognized
		}
	}
	method := r.Method
	if method == http.MethodHead {
		method = http.MethodGet
	}
	if method != e.Method {
		return nil, nil, "", errMethod
	}
	args := make(map[string]string, len(e.PathArgs))
	for i, name := range e.PathArgs {
		args[name] = unquote(segs[1+i])
	}
	txnID := ""
	if e.Cache {
		txnID = unquote(segs[len(segs)-1])
	}
	return e, args, txnID, nil
}

// unquote is urllib.parse.unquote, which leaves a malformed escape as it is.
func unquote(s string) string {
	if v, err := url.PathUnescape(s); err == nil {
		return v
	}
	return s
}

// checkAuth is _base.py's _check_auth.
func checkAuth(headers []string, secret string) error {
	switch {
	case len(headers) == 0:
		return errors.New("Missing Authorization header.")
	case len(headers) > 1:
		return errors.New("Too many Authorization headers.")
	}
	parts := strings.Split(headers[0], " ")
	if len(parts) == 2 && parts[0] == "Bearer" && parts[1] == secret {
		return nil
	}
	return errors.New("Invalid Authorization header.")
}

func (s *Server) readBody(w http.ResponseWriter, r *http.Request) (map[string]json.RawMessage, error) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.opts.MaxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, errTooLarge
		}
		return nil, err
	}
	data = bytes.TrimSpace(data)
	if !json.Valid(data) {
		return nil, errNotJSON
	}
	var content map[string]json.RawMessage
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &content) != nil {
		return nil, errNotObject
	}
	return content, nil
}

// waitForCaller waits for the streams the caller says it wrote.
func (s *Server) waitForCaller(ctx context.Context, content map[string]json.RawMessage) error {
	raw, ok := content[StreamPositionKey]
	if !ok {
		return nil
	}
	var pos struct {
		InstanceName string           `json:"instance_name"`
		Streams      map[string]int64 `json:"streams"`
	}
	if err := json.Unmarshal(raw, &pos); err != nil {
		// Synapse's indexing fails the same way: an exception, a 500.
		return fmt.Errorf("replserver: %s: %w", StreamPositionKey, err)
	}
	if pos.InstanceName == s.opts.Instance || s.opts.Waiter == nil {
		// Our own writes need no wait; and with no Waiter, Register has
		// refused every endpoint whose callers send positions.
		return nil
	}
	for stream, position := range pos.Streams {
		s.opts.Waiter.WaitForStreamPosition(ctx, pos.InstanceName, stream, position)
	}
	return nil
}

// run calls the handler and encodes its body.
func run(ctx context.Context, e *Endpoint, req Request) (int, map[string]json.RawMessage, error) {
	status, body, err := e.Handle(ctx, req)
	if err != nil {
		return 0, nil, err
	}
	if _, ok := body[StreamPositionKey]; ok {
		return 0, nil, fmt.Errorf("replserver: %s: data to send contains %q", e.Name, StreamPositionKey)
	}
	out := make(map[string]json.RawMessage, len(body))
	for k, v := range body {
		raw, err := json.Marshal(v)
		if err != nil {
			return 0, nil, fmt.Errorf("replserver: %s: encoding %q: %w", e.Name, k, err)
		}
		out[k] = raw
	}
	return status, out, nil
}

func writeError(w http.ResponseWriter, err error) int {
	var serr *Error
	if !errors.As(err, &serr) {
		serr = errInternal
	}
	body := map[string]json.RawMessage{}
	body["errcode"], _ = json.Marshal(serr.Errcode)
	body["error"], _ = json.Marshal(serr.Msg)
	return writeJSON(w, serr.Code, body)
}

func writeJSON(w http.ResponseWriter, status int, body map[string]json.RawMessage) int {
	if body == nil {
		body = map[string]json.RawMessage{}
	}
	data, err := json.Marshal(body)
	if err != nil {
		status, data = http.StatusInternalServerError, []byte(`{"errcode":"M_UNKNOWN","error":"Internal server error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
	return status
}
