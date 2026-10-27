// Package txn is the front door for PUT /_matrix/federation/v1/send/{txnId}.
//
// Every /send request comes here. A transaction carrying only EDUs -- 96% of
// them on this homeserver, most of those presence that Synapse throws away --
// is answered here, by forwarding each EDU to the stream writer Synapse would
// forward it to; a transaction with PDUs is answered by write mode
// (PDUServer). Everything else goes to Synapse's main process, untouched:
// anything malformed, anything whose handling depends on a detail this worker
// does not reproduce, and PDUs when write mode is unavailable. The rule
// throughout is that when in doubt, Synapse answers.
package txn

import (
	"encoding/json"

	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/crypto/canonicaljson"
	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/edu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/synapseauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
)

// Verifier authenticates an X-Matrix request.
type Verifier interface {
	Verify(r *http.Request) fedauth.Result
}

// Upstream forwards a request to Synapse.
type Upstream interface {
	Forward(w http.ResponseWriter, r *http.Request, captureLimit int64) proxy.Result
}

// TxnStore reads what decides whether a transaction may be answered here.
type TxnStore interface {
	GetTxnState(ctx context.Context, origin, txnID string) (store.TxnState, error)
}

// PDUServer is write mode's /send side: PlanPDUs checks a transaction's
// PDUs without writing anything, and says when Synapse must answer instead.
type PDUServer interface {
	PlanPDUs(ctx context.Context, origin string, pdus []json.RawMessage, requestTime time.Time) (PDURun, string)
}

// PDURun stages a planned transaction's PDUs, returning each one's entry
// in the answer.
type PDURun interface {
	Run(ctx context.Context) map[string]json.RawMessage
}

// TxnResponses stores the answer given to a transaction
// (set_received_txn_response).
type TxnResponses interface {
	SetTxnResponse(ctx context.Context, origin, txnID string, code int, canonicalJSON []byte) error
}

// OriginUp marks origins in backoff as up (originup.Resetter).
type OriginUp interface {
	// Ready says whether resets currently work.
	Ready() bool
	// Reset marks origin as up, in the background; ip is where the request
	// came from, for the log.
	Reset(origin, ip string)
}

// EDUs plans and executes a transaction's EDUs.
type EDUs interface {
	Plan(ctx context.Context, origin string, edus []edu.EDU) ([]edu.Step, error)
	Execute(ctx context.Context, origin string, steps []edu.Step) error
}

// Options builds a Handler.
type Options struct {
	Verifier Verifier
	Upstream Upstream
	Store    TxnStore
	EDUs     EDUs
	// PDUServer is write mode: transactions with PDUs are served here. Nil
	// (no write role) proxies every transaction that carries PDUs.
	PDUServer PDUServer
	// Responses, when set, stores every answer given here.
	Responses TxnResponses
	// OriginUp, when set, marks an origin in backoff as up, so its
	// transactions are served here. Nil proxies them.
	OriginUp OriginUp
	// SynapseAuth, when set, judges a request as Synapse's
	// authenticate_request would, so that its refusals are answered here
	// in Synapse's words; without it they are proxied.
	SynapseAuth *synapseauth.Authenticator
	// DomainWhitelist is federation_domain_whitelist; nil means none.
	DomainWhitelist map[string]bool
	MaxBody         int64
	ForwardTimeout  time.Duration
	Log             zerolog.Logger
}

// Handler serves /send.
type Handler struct {
	verifier       Verifier
	upstream       Upstream
	store          TxnStore
	edus           EDUs
	whitelist      map[string]bool
	maxBody        int64
	forwardTimeout time.Duration
	coord          *coordinator
	pduServer      PDUServer
	responses      TxnResponses
	originUp       OriginUp
	sauth          *synapseauth.Authenticator
	log            zerolog.Logger
	// nudge keeps the copy NudgeSweep sends.
	nudge nudger
}

// responseCacheTTL is Synapse's _transaction_resp_cache timeout.
const responseCacheTTL = 30 * time.Second

// NewHandler builds a Handler.
func NewHandler(o Options) *Handler {
	return &Handler{
		verifier:       o.Verifier,
		upstream:       o.Upstream,
		store:          o.Store,
		edus:           o.EDUs,
		whitelist:      o.DomainWhitelist,
		maxBody:        o.MaxBody,
		forwardTimeout: o.ForwardTimeout,
		coord:          newCoordinator(responseCacheTTL),
		pduServer:      o.PDUServer,
		responses:      o.Responses,
		originUp:       o.OriginUp,
		sauth:          o.SynapseAuth,
		log:            o.Log,
	}
}

// Reasons a request is proxied that are not about its body. Fixed set, for
// metric labels.
const (
	reasonNotSend   = "not_send"
	reasonTooLarge  = "too_large"
	reasonAuth      = "auth_failed"
	reasonWhitelist = "not_whitelisted"
	reasonDBError   = "db_error"
	reasonBackoff   = "origin_in_backoff"
	reasonUncertain = "uncertain"
)

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := &txnRec{start: time.Now(), auth: r.Header.Get("Authorization")}
	defer rec.log(h.log)

	txnID, version, ok := matchSend(r)
	rec.txnID = txnID
	if !ok || version != "v1" || r.Method != http.MethodPut || txnID == "" {
		// Not something we handle: other methods, v2, an undecodable or
		// empty transaction id (Synapse raises on that). Synapse answers.
		h.proxyRaw(w, r, reasonNotSend, rec)
		return
	}
	body, complete, err := readBody(r, h.maxBody)
	if err != nil {
		// The sender went away mid-body. There is nobody to answer.
		rec.route, rec.reason = routeAbandoned, "body_read"
		requests.WithLabelValues(routeAbandoned, "body_read").Inc()
		return
	}
	rec.bodyBytes = len(body)
	if !complete {
		// Too big to buffer: stream it on, read part and all.
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		h.proxyRaw(w, r, reasonTooLarge, rec)
		return
	}

	h.serve(w, r, body, txnID, rec)
}

// serve handles a buffered v1 /send.
func (h *Handler) serve(w http.ResponseWriter, r *http.Request, body []byte, txnID string, rec *txnRec) {
	// Synapse parses the body and the headers before any key is looked
	// at: what it refuses there, it refuses whatever our verifier says.
	var sreq synapseauth.Request
	var pre *synapseauth.Prechecked
	if h.sauth != nil {
		sreq = synapseauth.FromHTTP(r, body)
		var res synapseauth.Result
		if pre, res = h.sauth.Precheck(sreq); res.Answer != nil {
			h.answerAuth(w, r, res, rec)
			return
		}
	}
	vr := h.verifier.Verify(withBody(r, body))
	if !vr.OK() && pre != nil {
		// Judged as Synapse would, with its keyring; answered here when
		// Synapse refuses it too and we have its words.
		if res := h.sauth.Verify(r.Context(), sreq, pre, rec.start.UnixMilli()); res.Answer != nil {
			h.answerAuth(w, r, res, rec)
			return
		} else if res.Origin != "" {
			h.log.Info().Str("origin", res.Origin).Str("ours", vr.Err.Err).
				Msg("Authentication failed here, but Synapse's rules accept it; proxied")
		}
	}
	if !vr.OK() {
		// Synapse gives the authoritative 401, or accepts a request we could
		// not verify -- a key server we cannot reach, say. Either way it is
		// Synapse's answer to give.
		res := h.proxyBody(w, r, body, reasonAuth, rec)
		// Synapse accepting what we refused is a request we could have
		// served; say which origin and why, so it can be looked into.
		claimed := federation.ParseXMatrixAuth(r.Header.Get("Authorization"))
		h.log.Info().Str("origin", claimed.Origin).Str("key_id", string(claimed.KeyID)).
			Str("ours", vr.Err.Err).Int("synapse_status", res.Status).
			Msg("Authentication failed here; Synapse answered")
		return
	}
	origin := vr.Origin
	rec.origin = origin

	if h.whitelist != nil && !h.whitelist[origin] {
		h.proxyBody(w, r, body, reasonWhitelist, rec)
		return
	}

	f, ok := h.claim(w, r, origin, txnID, rec)
	if !ok {
		return
	}

	if ans, ok := h.coord.lookup(origin, txnID); ok {
		h.coord.end(origin, f, nil)
		h.write(w, ans, routeNative, "cached", rec)
		return
	}

	d := h.decide(r.Context(), origin, txnID, body)
	if d.proxyReason != "" {
		h.proxyBody(w, r, body, d.proxyReason, rec)
		h.coord.end(origin, f, nil)
		return
	}
	if d.originDown {
		// As Synapse: on any signed request, before the transaction, and
		// without waiting for it.
		h.originUp.Reset(origin, clientIP(r))
	}
	if d.stored != nil {
		ans := answer{status: d.stored.Code, body: d.stored.JSON}
		h.coord.end(origin, f, &ans)
		h.write(w, ans, routeNative, "stored_response", rec)
		return
	}

	edu.Record(d.steps)
	rec.steps = d.steps

	// Detached from the sender's connection: Synapse finishes a transaction
	// whose sender hung up, and an EDU forwarded halfway is worse than one
	// forwarded fully or not at all.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), h.forwardTimeout)
	var pduResults map[string]json.RawMessage
	pduDone := make(chan struct{})
	go func() {
		defer close(pduDone)
		if d.pdus != nil {
			pduResults = d.pdus.Run(context.WithoutCancel(r.Context()))
		}
	}()
	err := h.edus.Execute(ctx, origin, d.steps)
	<-pduDone
	cancel()

	if err != nil {
		var fe *edu.ForwardError
		msg := "Error handling EDUs"
		if errors.As(err, &fe) {
			msg = fe.Error()
		}
		// Not remembered: the sender retries, and the retry must be tried
		// again rather than told it failed.
		h.coord.end(origin, f, nil)
		h.write(w, jsonError(http.StatusInternalServerError, "M_UNKNOWN", msg), routeNative, "forward_failed", rec)
		return
	}

	ans := answer{status: http.StatusOK, body: []byte(`{"pdus":{}}`)}
	if len(pduResults) > 0 {
		b, err := canonicaljson.Marshal(map[string]any{"pdus": pduResults})
		if err == nil {
			// Python's escapes: lowercase hex.
			b, err = pdu.Canonical(b)
		}
		if err != nil {
			h.log.Error().Err(err).Msg("Could not encode a transaction answer")
		} else {
			ans.body = b
		}
	}
	if h.responses != nil {
		// set_response: a retry is answered the same, by any process.
		sctx, scancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
		if err := h.responses.SetTxnResponse(sctx, origin, txnID, ans.status, ans.body); err != nil {
			h.log.Warn().Err(err).Str("origin", origin).Msg("Could not store a transaction answer")
		} else {
			// Stored, so Synapse answers a copy from received_transactions.
			h.nudge.remember(r, body, ans.body)
		}
		scancel()
	}
	h.coord.end(origin, f, &ans)
	h.write(w, ans, routeNative, "ok", rec)
}

// claim takes the origin's one-transaction-at-a-time slot, waiting out a
// concurrent attempt at the same transaction. It answers the request itself
// when it returns false.
func (h *Handler) claim(w http.ResponseWriter, r *http.Request, origin, txnID string, rec *txnRec) (*flight, bool) {
	for {
		own, wait, busy := h.coord.begin(origin, txnID)
		switch {
		case busy:
			h.write(w, jsonError(http.StatusTooManyRequests, "M_UNKNOWN", "Too many concurrent transactions"),
				routeNative, "concurrent_txn", rec)
			return nil, false
		case wait != nil:
			select {
			case <-wait.done:
				continue
			case <-r.Context().Done():
				rec.route, rec.reason = routeAbandoned, "waiting_for_retry"
				requests.WithLabelValues(routeAbandoned, "waiting_for_retry").Inc()
				return nil, false
			}
		default:
			return own, true
		}
	}
}

// decision is what to do with a transaction.
type decision struct {
	// proxyReason is set when Synapse must answer.
	proxyReason string
	// stored is set when Synapse already answered this transaction.
	stored *store.StoredResponse
	// steps is the EDU plan otherwise.
	steps []edu.Step
	// pdus is write mode's plan for the PDUs, if any.
	pdus PDURun
	// originDown is set when the origin is in backoff, so serving the
	// transaction means marking it as up.
	originDown bool
	// at is the request time write mode planned the PDUs with, Synapse's
	// request_time (federation_server.py:320).
	at time.Time
}

// decide works out whether a verified transaction can be answered here, and
// how. It has no effects.
func (h *Handler) decide(ctx context.Context, origin, txnID string, body []byte) decision {
	edus, pdus, reason := classify(body)
	if reason != "" {
		return decision{proxyReason: reason}
	}
	if len(pdus) > 0 && h.pduServer == nil {
		// Write mode is unavailable (no write role), so Synapse persists.
		return decision{proxyReason: reasonHasPDUs}
	}

	st, err := h.store.GetTxnState(ctx, origin, txnID)
	if err != nil {
		h.log.Warn().Err(err).Str("origin", origin).Msg("Could not read transaction state; leaving it to Synapse")
		return decision{proxyReason: reasonDBError}
	}
	down := st.RetryLastTS != 0
	if down && (h.originUp == nil || !h.originUp.Ready()) {
		// Synapse resets the backoff of an origin that sends us a signed
		// request, and tells every worker it is back up
		// (transport/server/_base.py:143). Without origin_up, or while it
		// is failing, only Synapse's worker can, so it gets the request.
		return decision{proxyReason: reasonBackoff}
	}
	if st.Previous != nil {
		return decision{stored: st.Previous, originDown: down}
	}

	steps, err := h.edus.Plan(ctx, origin, edus)
	if err != nil {
		if !errors.Is(err, edu.ErrUncertain) {
			h.log.Warn().Err(err).Str("origin", origin).Msg("Could not plan EDUs; leaving the transaction to Synapse")
			return decision{proxyReason: reasonDBError}
		}
		return decision{proxyReason: reasonUncertain}
	}
	d := decision{steps: steps, originDown: down}
	if len(pdus) > 0 {
		d.at = time.Now()
		run, why := h.pduServer.PlanPDUs(ctx, origin, pdus, d.at)
		if why != "" {
			return decision{proxyReason: why}
		}
		d.pdus = run
	}
	return d
}

func (h *Handler) proxyRaw(w http.ResponseWriter, r *http.Request, reason string, rec *txnRec) proxy.Result {
	res := h.upstream.Forward(w, r, 0)
	h.recordProxy(res, reason, rec)
	return res
}

func (h *Handler) proxyBody(w http.ResponseWriter, r *http.Request, body []byte, reason string, rec *txnRec) proxy.Result {
	res := h.upstream.Forward(w, withBody(r, body), 0)
	h.recordProxy(res, reason, rec)
	return res
}

func (h *Handler) recordProxy(res proxy.Result, reason string, rec *txnRec) {
	route := routeProxy
	if res.Canceled {
		route = routeAbandoned
	}
	rec.route, rec.reason, rec.status = route, reason, res.Status
	requests.WithLabelValues(route, reason).Inc()
	duration.WithLabelValues(routeProxy).Observe(time.Since(rec.start).Seconds())
	if res.Err != nil {
		upstreamErrors.Inc()
	}
}

func (h *Handler) write(w http.ResponseWriter, ans answer, route, outcome string, rec *txnRec) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(ans.status)
	_, _ = w.Write(ans.body)
	rec.route, rec.reason, rec.status = route, outcome, ans.status
	requests.WithLabelValues(route, outcome).Inc()
	duration.WithLabelValues(route).Observe(time.Since(rec.start).Seconds())
}

// answerAuth answers a request Synapse's authentication refuses, as it
// answers it: the SynapseError as canonical JSON.
func (h *Handler) answerAuth(w http.ResponseWriter, r *http.Request, res synapseauth.Result, rec *txnRec) {
	a := res.Answer
	claimed := federation.ParseXMatrixAuth(r.Header.Get("Authorization"))
	h.log.Info().Str("origin", claimed.Origin).Str("key_id", string(claimed.KeyID)).Int("status", a.Status).
		Str("error", a.Error).Msg("Authentication failed, answered as Synapse")
	h.write(w, jsonError(a.Status, a.Errcode, a.Error), routeNative, reasonAuth, rec)
}

// clientIP is the address a request came from, as Synapse takes it behind a
// proxy (x_forwarded, http/site.py): the first X-Forwarded-For entry, else
// the connection's address.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		return strings.TrimSpace(first)
	}
	return r.RemoteAddr
}

func jsonError(status int, errcode, msg string) answer {
	b, _ := marshalError(errcode, msg)
	return answer{status: status, body: b}
}

// readBody reads at most limit bytes. complete is false when the body is
// larger, in which case what was read is returned for the caller to replay.
func readBody(r *http.Request, limit int64) (body []byte, complete bool, err error) {
	if r.Body == nil {
		return nil, true, nil
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(b)) > limit {
		return b, false, nil
	}
	return b, true, nil
}

// withBody returns r carrying a fresh reader over body. Both verification and
// proxying consume the body, so each gets its own.
func withBody(r *http.Request, body []byte) *http.Request {
	r2 := r.Clone(r.Context())
	r2.Body = io.NopCloser(bytes.NewReader(body))
	r2.ContentLength = int64(len(body))
	r2.TransferEncoding = nil
	return r2
}
