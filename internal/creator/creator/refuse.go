package creator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
)

var refusals = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goevent_native_refusals_total",
	Help: "Requests refused natively, in Synapse's words, by endpoint and reason.",
}, []string{"endpoint", "reason"})

// refusal is a SynapseError that Synapse would raise for the request, given
// here as return_json_error renders it: the status, and cs_error's errcode
// and error, plus additional_fields. A refusal is only given where every
// check Synapse makes before it has passed, so the answer is the one Synapse
// would have given.
type refusal struct {
	// reason labels it in goevent_native_refusals_total.
	reason  string
	status  int
	errcode string
	msg     string
	extra   map[string]any
	headers map[string]string
	// raw is the whole body, as clientauth gives Synapse's: errcode and msg
	// are then only labels.
	raw []byte
}

// stop ends a request early: a fallback (the request is Synapse's), or a
// refusal (Synapse's answer, given here).
type stop struct {
	fallback string
	ref      *refusal
}

func fb(reason string) *stop { return &stop{fallback: reason} }

func (f *refusal) stop() *stop { return &stop{ref: f} }

// end answers a stopped request.
func (c *Creator) end(w http.ResponseWriter, s *stop) Outcome {
	if s.ref != nil {
		return c.refuse(w, s.ref)
	}
	return fallback(s.fallback)
}

func (c *Creator) refuse(w http.ResponseWriter, f *refusal) Outcome {
	b := f.raw
	if b == nil {
		body := map[string]any{"errcode": f.errcode, "error": f.msg}
		for k, v := range f.extra {
			body[k] = v
		}
		b = synapseJSON(body)
	}
	for k, v := range f.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.status)
	_, _ = w.Write(b)
	return Outcome{Result: "refused", Refused: f.reason}
}

// authReasons label get_user_by_req's refusals by errcode, with the reasons
// goevent counted them under.
var authReasons = map[string]string{
	"M_MISSING_TOKEN":  "missing_token",
	"M_UNKNOWN_TOKEN":  "unknown_token",
	"M_USER_LOCKED":    "user_locked",
	"M_FORBIDDEN":      "appservice_user",
	"M_UNKNOWN_DEVICE": "appservice_device",
}

// authRefusal is a clientauth refusal, its body as Synapse's.
func authRefusal(rej *clientauth.Rejection) *refusal {
	reason, ok := authReasons[rej.ErrCode]
	if !ok {
		reason = "auth_" + strings.ToLower(rej.ErrCode)
	}
	return &refusal{reason: reason, status: rej.Status, errcode: rej.ErrCode, raw: rej.Body}
}

// synapseJSON encodes a response body as Synapse's json encoder does:
// "<", ">" and "&" as they are. encoding/json would write "\u003c", so a
// refusal holding "user_level (0) < send_level (50)" differed from
// Synapse's byte for byte.
func synapseJSON(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// The refusals get_user_by_req makes (api/errors.py, api/auth/internal.py).
func missingToken(msg string) *refusal {
	return &refusal{reason: "missing_token", status: http.StatusUnauthorized, errcode: "M_MISSING_TOKEN", msg: msg}
}

// accessToken is get_access_token_from_request. ok is false when the token
// cannot be decoded as ASCII, which is Synapse's 500.
func accessToken(r *http.Request) (token string, ref *refusal, ok bool) {
	headers := r.Header.Values("Authorization")
	q, hasQuery := r.URL.Query()["access_token"]
	var raw string
	if len(headers) > 0 {
		if hasQuery {
			return "", missingToken("Mixing Authorization headers and access_token query parameters."), true
		}
		if len(headers) > 1 {
			return "", missingToken("Too many Authorization headers."), true
		}
		parts := strings.Split(headers[0], " ")
		if parts[0] != "Bearer" || len(parts) != 2 {
			return "", missingToken("Invalid Authorization header."), true
		}
		raw = parts[1]
	} else {
		if len(q) == 0 {
			return "", missingToken("Missing access token"), true
		}
		raw = q[0]
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] >= utf8.RuneSelf {
			return "", nil, false
		}
	}
	return raw, nil, true
}

// internalError is an exception that is not a SynapseError: Synapse's 500.
func internalError(reason string) *refusal {
	return &refusal{reason: reason, status: http.StatusInternalServerError, errcode: "M_UNKNOWN", msg: "Internal server error"}
}

// badRequest is SynapseError(400, msg): M_UNKNOWN unless given.
func badRequest(reason, msg string, errcode ...string) *refusal {
	code := "M_UNKNOWN"
	if len(errcode) > 0 {
		code = errcode[0]
	}
	return &refusal{reason: reason, status: http.StatusBadRequest, errcode: code, msg: msg}
}

// forbidden is AuthError(403, msg) or SynapseError(403, msg, M_FORBIDDEN).
func forbidden(reason, msg string) *refusal {
	return &refusal{reason: reason, status: http.StatusForbidden, errcode: "M_FORBIDDEN", msg: msg}
}
