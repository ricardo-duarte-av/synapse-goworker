// Package clientreq is what every native client endpoint does the way
// Synapse's servlets do: the requester (get_user_by_req), the JSON body
// (parse_json_object_from_request), a path argument (urllib's unquote),
// Python's truth and number comparisons of JSON values, and the error and
// answer bodies with Synapse's CORS headers.
package clientreq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
)

// MaxBody is Synapse's request limit on a process without the media
// repository (MAX_REQUEST_SIZE).
const MaxBody = 200 * 65536

// Error is a SynapseError as a client sees it.
type Error struct {
	Status int
	Body   map[string]any
	// Raw, when set, is the body already rendered (clientauth's, Synapse's
	// bytes).
	Raw []byte
}

func (e *Error) Error() string {
	if e.Raw != nil {
		return strconv.Itoa(e.Status) + " " + string(e.Raw)
	}
	return strconv.Itoa(e.Status) + " " + strconv.Quote(errString(e.Body))
}

func errString(b map[string]any) string {
	s, _ := b["error"].(string)
	return s
}

// New is SynapseError(status, msg, errcode).
func New(status int, errcode, msg string) *Error {
	return &Error{Status: status, Body: map[string]any{"errcode": errcode, "error": msg}}
}

// Internal is an unexpected exception's answer.
var Internal = New(http.StatusInternalServerError, "M_UNKNOWN", "Internal server error")

// Authenticator is clientauth's.
type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (clientauth.Verdict, error)
}

// User is get_user_by_req(request, allow_guest): the verdict, or the error
// the client gets. A verdict that cannot be reached is 502, never a 401:
// that would log a valid client out.
func User(ctx context.Context, auth Authenticator, r *http.Request, allowGuest bool) (clientauth.Verdict, *Error, error) {
	v, err := auth.Authenticate(ctx, r)
	if err != nil {
		return v, New(http.StatusBadGateway, "M_UNKNOWN", "Cannot validate the access token right now"), err
	}
	if !v.Valid {
		return v, &Error{Status: v.Rejection.Status, Raw: v.Rejection.Body}, nil
	}
	if v.IsGuest && !allowGuest {
		return v, New(http.StatusForbidden, "M_GUEST_ACCESS_FORBIDDEN", "Guest access not allowed"), nil
	}
	return v, nil, nil
}

// Unquote is urllib.parse.unquote, which leaves a malformed escape alone.
func Unquote(s string) string {
	if v, err := url.PathUnescape(s); err == nil {
		return v
	}
	return s
}

// ReadObject is parse_json_object_from_request.
func ReadObject(body io.Reader) (map[string]json.RawMessage, *Error) {
	_, obj, cerr := ReadObjectRaw(body)
	return obj, cerr
}

// ReadObjectRaw is ReadObject, with the body's bytes as well, for a value
// stored as Synapse re-encodes the dict it parsed (pyjson.Encode keeps the
// order and the last of a repeated key).
func ReadObjectRaw(body io.Reader) (json.RawMessage, map[string]json.RawMessage, *Error) {
	data, err := io.ReadAll(io.LimitReader(body, MaxBody+1))
	if err != nil || len(data) > MaxBody {
		return nil, nil, New(http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "Request content is too large")
	}
	data = bytes.TrimSpace(data)
	if !json.Valid(data) {
		return nil, nil, New(http.StatusBadRequest, "M_NOT_JSON", "Content not JSON.")
	}
	var obj map[string]json.RawMessage
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &obj) != nil {
		return nil, nil, New(http.StatusBadRequest, "M_BAD_JSON", "Content must be a JSON object.")
	}
	return data, obj, nil
}

var domainlessRoomID = regexp.MustCompile(`^[A-Za-z0-9\-_]{43}$`)

// ValidRoomID is RoomID.is_valid: "!localpart:domain" with a valid server
// name, or MSC4291's "!" and 43 characters of unpadded urlsafe base64.
func ValidRoomID(s string) bool {
	if !strings.HasPrefix(s, "!") {
		return false
	}
	if strings.Contains(s, ":") {
		_, domain, _ := strings.Cut(s[1:], ":")
		return pdu.ValidServerName(domain)
	}
	return domainlessRoomID.MatchString(s[1:])
}

// Number is a JSON value as Python compares it with an int: a number, or a
// bool (True is 1). Anything else cannot be compared (TypeError).
func Number(raw json.RawMessage) (float64, bool) {
	switch s := strings.TrimSpace(string(raw)); s {
	case "true":
		return 1, true
	case "false":
		return 0, true
	}
	return Float(raw)
}

// Float is a JSON number as Python's json reads it: one too large for a
// float is inf, not an error.
func Float(raw json.RawMessage) (float64, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "" || !(s[0] == '-' || s[0] >= '0' && s[0] <= '9') {
		return 0, false
	}
	var n json.Number
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return 0, false
	}
	return f, true
}

// Truthy is Python's bool() of a JSON value.
func Truthy(raw json.RawMessage) bool {
	switch s := strings.TrimSpace(string(raw)); {
	case s == "" || s == "null", s == "false", s == `""`, s == "[]", s == "{}":
		return false
	case s == "true":
		return true
	case s[0] == '[' || s[0] == '{':
		var v any
		_ = json.Unmarshal(raw, &v)
		switch c := v.(type) {
		case []any:
			return len(c) > 0
		case map[string]any:
			return len(c) > 0
		}
		return true
	case s[0] == '"':
		var str string
		_ = json.Unmarshal(raw, &str)
		return str != ""
	default:
		if f, ok := Float(raw); ok {
			return f != 0
		}
		return true
	}
}

// WriteRaw answers 200 with a body already rendered (json_encoder's).
func WriteRaw(w http.ResponseWriter, body []byte) {
	server.SetCORS(w.Header())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// Write answers with Synapse's CORS headers: the error, or body with 200.
func Write(w http.ResponseWriter, e *Error, body map[string]any) {
	server.SetCORS(w.Header())
	w.Header().Set("Content-Type", "application/json")
	if e != nil {
		w.WriteHeader(e.Status)
		if e.Raw != nil {
			_, _ = w.Write(e.Raw)
			return
		}
		data, _ := json.Marshal(e.Body)
		_, _ = w.Write(data)
		return
	}
	if body == nil {
		body = map[string]any{}
	}
	data, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errcode":"M_UNKNOWN","error":"Internal server error"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
