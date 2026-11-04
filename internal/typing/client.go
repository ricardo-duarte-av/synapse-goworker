package typing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
)

// Match is RoomTypingRestServlet's path, for PUT, its one method:
// /_matrix/client/{api/v1,r0,v3,unstable}/rooms/{roomId}/typing/{userId},
// each a segment that may be empty ([^/]*). Any other method goes to main,
// which answers it as Synapse does.
func Match(method, escapedPath string) bool {
	if method != http.MethodPut {
		return false
	}
	_, _, ok := parsePath(escapedPath)
	return ok
}

func parsePath(p string) (room, user string, ok bool) {
	rest, found := strings.CutPrefix(p, "/_matrix/client/")
	if !found {
		return "", "", false
	}
	for _, v := range []string{"api/v1/", "r0/", "v3/", "unstable/"} {
		if after, found := strings.CutPrefix(rest, v); found {
			seg := strings.Split(after, "/")
			if len(seg) == 4 && seg[0] == "rooms" && seg[2] == "typing" {
				return seg[1], seg[3], true
			}
			return "", "", false
		}
	}
	return "", "", false
}

// unquote is urllib.parse.unquote, which leaves a malformed escape alone.
func unquote(s string) string {
	if v, err := url.PathUnescape(s); err == nil {
		return v
	}
	return s
}

// maxBody bounds a typing request, as every Synapse request is bounded
// (MAX_REQUEST_SIZE).
const maxBody = 200 * 65536

// clientError is a SynapseError as a client sees it.
type clientError struct {
	status int
	body   map[string]any
	// raw, when set, is the body as clientauth has it, Synapse's bytes.
	raw []byte
}

func synapseError(status int, errcode, msg string) *clientError {
	return &clientError{status: status, body: map[string]any{"errcode": errcode, "error": msg}}
}

var errInternal = synapseError(http.StatusInternalServerError, matrixerr.CodeUnknown, "Internal server error")

// serveClient is RoomTypingRestServlet.on_PUT.
func (a *App) serveClient(w http.ResponseWriter, r *http.Request) {
	server.SetCORS(w.Header())
	state := a.state.Load()
	if state == nil {
		// Not the writer. Synapse raises (500) on a worker that is not;
		// main is the writer in the default configuration, and otherwise
		// says the same.
		requests.WithLabelValues("not_writer").Inc()
		a.notWriter.Do(func() {
			a.log.Warn().Msg("a typing request reached this process, which homeserver.yaml does " +
				"not name in stream_writers.typing; it goes to main. nginx and homeserver.yaml disagree")
		})
		a.fallback.ServeHTTP(w, r)
		return
	}
	outcome, cerr := a.handleClient(r.Context(), r, state)
	if cerr != nil {
		requests.WithLabelValues(outcome).Inc()
		if cerr.raw != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(cerr.status)
			_, _ = w.Write(cerr.raw)
			return
		}
		writeJSON(w, cerr.status, cerr.body)
		return
	}
	requests.WithLabelValues(outcome).Inc()
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (a *App) handleClient(ctx context.Context, r *http.Request, state *State) (string, *clientError) {
	// get_user_by_req, allow_guest=False.
	v, err := a.auth.Authenticate(ctx, r)
	if err != nil {
		a.log.Error().Err(err).Msg("typing: cannot validate a token")
		return "auth_unavailable", synapseError(http.StatusBadGateway, matrixerr.CodeUnknown,
			"Cannot validate the access token right now")
	}
	if !v.Valid {
		return "auth_rejected", &clientError{status: v.Rejection.Status, raw: v.Rejection.Body}
	}
	if v.IsGuest {
		return "guest", synapseError(http.StatusForbidden, matrixerr.CodeGuestForbidden, "Guest access not allowed")
	}

	rawRoom, rawUser, _ := parsePath(r.URL.EscapedPath())
	roomID := unquote(rawRoom)
	target := unquote(rawUser)
	if cerr := checkUserID(target); cerr != nil {
		return "bad_user_id", cerr
	}

	content, cerr := readObject(r.Body)
	if cerr != nil {
		return "bad_json", cerr
	}

	if a.trackPresence() {
		if err := a.presence.Bump(ctx, v.UserID, v.DeviceID); err != nil {
			a.log.Warn().Err(err).Str("user", v.UserID).Msg("typing: presence bump failed")
			return "presence_failed", errInternal
		}
	}

	// timeout = min(content.get("timeout", 30000), 120000)
	timeout := DefaultTimeout
	if raw, ok := content["timeout"]; ok {
		t, ok := pyNumber(raw)
		if !ok {
			// min() of a str, None, list or dict and an int: TypeError.
			return "bad_timeout", errInternal
		}
		timeout = t
	}
	timeout = min(timeout, MaxTimeout)

	rawTyping, ok := content["typing"]
	if !ok {
		// content["typing"]: KeyError.
		return "no_typing_key", errInternal
	}
	typing := truthy(rawTyping)

	// started_typing / stopped_typing, whose checks are the same.
	if domainOf(target) != a.serverName {
		return "not_local", synapseError(http.StatusBadRequest, matrixerr.CodeUnknown, "User is not hosted on this homeserver")
	}
	if target != v.UserID {
		return "other_user", synapseError(http.StatusBadRequest, matrixerr.CodeForbidden, "Cannot set another user's typing state")
	}
	banned := false
	if v.AppServiceID == "" {
		// An appservice's requester is never shadow-banned (create_requester).
		if banned, err = a.store.ShadowBanned(ctx, v.UserID); err != nil {
			a.log.Error().Err(err).Msg("typing: shadow_banned")
			return "db_error", errInternal
		}
	}
	if banned {
		// ShadowBanError after a random sleep; the servlet pretends.
		select {
		case <-time.After(time.Duration(1+rand.IntN(10)) * time.Second):
		case <-ctx.Done():
		}
		return "shadow_banned", nil
	}
	membership, err := a.store.LocalMembership(ctx, v.UserID, roomID)
	if err != nil {
		a.log.Error().Err(err).Msg("typing: membership")
		return "db_error", errInternal
	}
	if membership != "join" {
		// UnstableSpecAuthError(403, ..., NOT_JOINED): M_FORBIDDEN, and the
		// new errcode beside it under MSC3848.
		cerr := synapseError(http.StatusForbidden, matrixerr.CodeForbidden,
			"User "+v.UserID+" not in room "+roomID)
		if a.msc3848() {
			cerr.body["org.matrix.msc3848.unstable.errcode"] = "ORG.MATRIX.MSC3848.NOT_JOINED"
		}
		return "not_joined", cerr
	}

	if typing {
		state.Start(roomID, v.UserID, timeout)
		return "started", nil
	}
	state.Stop(roomID, v.UserID)
	return "stopped", nil
}

// checkUserID is UserID.from_string's two errors.
func checkUserID(s string) *clientError {
	if !strings.HasPrefix(s, "@") {
		return synapseError(http.StatusBadRequest, matrixerr.CodeInvalidParam, "Expected UserID string to start with '@'")
	}
	if !strings.Contains(s[1:], ":") {
		return synapseError(http.StatusBadRequest, matrixerr.CodeInvalidParam, "Expected UserID of the form '@localname:domain'")
	}
	return nil
}

// readObject is parse_json_object_from_request.
func readObject(body io.Reader) (map[string]json.RawMessage, *clientError) {
	data, err := io.ReadAll(io.LimitReader(body, maxBody+1))
	if err != nil || len(data) > maxBody {
		return nil, synapseError(http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "Request content is too large")
	}
	data = bytes.TrimSpace(data)
	if !json.Valid(data) {
		return nil, synapseError(http.StatusBadRequest, matrixerr.CodeNotJSON, "Content not JSON.")
	}
	var obj map[string]json.RawMessage
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &obj) != nil {
		return nil, synapseError(http.StatusBadRequest, "M_BAD_JSON", "Content must be a JSON object.")
	}
	return obj, nil
}

// pyNumber is a JSON value as Python compares it with an int: a number, or
// a bool (True is 1). Anything else cannot be compared.
func pyNumber(raw json.RawMessage) (float64, bool) {
	switch s := strings.TrimSpace(string(raw)); s {
	case "true":
		return 1, true
	case "false":
		return 0, true
	}
	return jsonFloat(raw)
}

// jsonFloat is a JSON number as Python's json reads it: one too large for a
// float is inf (or an int, which compares the same here), not an error.
func jsonFloat(raw json.RawMessage) (float64, bool) {
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

// truthy is Python's bool() of a JSON value.
func truthy(raw json.RawMessage) bool {
	switch s := strings.TrimSpace(string(raw)); {
	case s == "null", s == "false", s == `""`, s == "[]", s == "{}":
		return false
	case s == "true":
		return true
	case len(s) > 0 && (s[0] == '[' || s[0] == '{'):
		// Non-empty, however it is spaced.
		var v any
		_ = json.Unmarshal(raw, &v)
		switch c := v.(type) {
		case []any:
			return len(c) > 0
		case map[string]any:
			return len(c) > 0
		}
		return true
	case len(s) > 0 && s[0] == '"':
		var str string
		_ = json.Unmarshal(raw, &str)
		return str != ""
	default:
		if f, ok := jsonFloat(raw); ok {
			return f != 0
		}
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	data, err := json.Marshal(body)
	if err != nil {
		status, data = http.StatusInternalServerError, []byte(`{"errcode":"M_UNKNOWN","error":"Internal server error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// verdictAuth is what the endpoint needs of clientauth.
type verdictAuth interface {
	Authenticate(ctx context.Context, r *http.Request) (clientauth.Verdict, error)
}
