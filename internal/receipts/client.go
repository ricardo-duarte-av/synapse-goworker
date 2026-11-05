package receipts

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
)

// Endpoints, for metrics.
const (
	endpointReceipt     = "receipt"
	endpointReadMarkers = "read_markers"
)

// Match is the two servlets' paths, POST their one method (client_patterns
// without v1: r0, v3, unstable; each argument [^/]*):
//
//	/rooms/{roomId}/receipt/{receiptType}/{eventId}  ReceiptRestServlet
//	/rooms/{roomId}/read_markers                     ReadMarkerRestServlet
func Match(method, escapedPath string) bool {
	if method != http.MethodPost {
		return false
	}
	_, _, ok := parsePath(escapedPath)
	return ok
}

// parsePath returns the endpoint and its (escaped) arguments.
func parsePath(p string) (string, []string, bool) {
	rest, found := strings.CutPrefix(p, "/_matrix/client/")
	if !found {
		return "", nil, false
	}
	for _, v := range []string{"r0/", "v3/", "unstable/"} {
		after, found := strings.CutPrefix(rest, v)
		if !found {
			continue
		}
		seg := strings.Split(after, "/")
		switch {
		case len(seg) == 5 && seg[0] == "rooms" && seg[2] == "receipt":
			return endpointReceipt, []string{seg[1], seg[3], seg[4]}, true
		case len(seg) == 3 && seg[0] == "rooms" && seg[2] == "read_markers":
			return endpointReadMarkers, []string{seg[1]}, true
		}
		return "", nil, false
	}
	return "", nil, false
}

// Handler serves what Match accepts.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serve) }

func (a *App) serve(w http.ResponseWriter, r *http.Request) {
	endpoint, args, _ := parsePath(r.URL.EscapedPath())
	if !a.writing {
		// Synapse's servlet on a process that does not write receipts fails
		// the insert (an assertion, 500); main says the same, unless it is
		// the writer.
		requests.WithLabelValues(endpoint, "not_writer").Inc()
		a.notWriter.Do(func() {
			a.Log.Warn().Msg("a receipt request reached this process, which did not start as the " +
				"receipts writer; it goes to main. nginx and homeserver.yaml disagree, or goworker was not restarted")
		})
		a.Fallback.ServeHTTP(w, r)
		return
	}
	var (
		outcome string
		cerr    *clientreq.Error
	)
	if endpoint == endpointReceipt {
		outcome, cerr = a.serveReceipt(r.Context(), r, args)
	} else {
		outcome, cerr = a.serveReadMarkers(r.Context(), r, args)
	}
	requests.WithLabelValues(endpoint, outcome).Inc()
	clientreq.Write(w, cerr, nil)
}

var knownTypes = []string{TypeRead, TypeReadPrivate, TypeFullyRead}

// serveReceipt is ReceiptRestServlet.on_POST.
func (a *App) serveReceipt(ctx context.Context, r *http.Request, args []string) (string, *clientreq.Error) {
	v, cerr, err := clientreq.User(ctx, a.Auth, r, false)
	if err != nil {
		a.Log.Error().Err(err).Msg("receipts: cannot validate a token")
		return "auth_unavailable", cerr
	}
	if cerr != nil {
		return "auth_rejected", cerr
	}
	roomID, receiptType, eventID := clientreq.Unquote(args[0]), clientreq.Unquote(args[1]), clientreq.Unquote(args[2])

	if !validRoomID(roomID) || !strings.HasPrefix(eventID, "$") {
		return "bad_ids", clientreq.New(400, "M_UNKNOWN", "A valid room ID and event ID must be specified")
	}
	known := false
	for _, t := range knownTypes {
		known = known || t == receiptType
	}
	if !known {
		// ', '.join of a set: any order is the order Synapse has.
		return "bad_type", clientreq.New(400, "M_UNKNOWN", "Receipt type must be "+strings.Join(knownTypes, ", "))
	}
	body, cerr := clientreq.ReadObject(r.Body)
	if cerr != nil {
		return "bad_json", cerr
	}
	allowBackward := false
	if a.msc4446() {
		if raw, ok := body["com.beeper.allow_backward"]; ok {
			b, isBool := jsonBool(raw)
			if !isBool {
				return "bad_allow_backward", clientreq.New(400, "M_INVALID_PARAM", "com.beeper.allow_backward must be a boolean.")
			}
			allowBackward = b
		}
		if allowBackward && receiptType != TypeFullyRead {
			return "bad_allow_backward", clientreq.New(400, "M_INVALID_PARAM",
				"com.beeper.allow_backward is only allowed to be true for "+TypeFullyRead+".")
		}
	}

	var threadID *string
	if raw, ok := body["thread_id"]; ok {
		var t string
		if json.Unmarshal(raw, &t) != nil || t == "" {
			return "bad_thread", clientreq.New(400, "M_INVALID_PARAM", "thread_id field must be a non-empty string")
		}
		if receiptType == TypeFullyRead {
			return "bad_thread", clientreq.New(400, "M_INVALID_PARAM",
				"thread_id is not compatible with "+TypeFullyRead+" receipts.")
		}
		in, err := a.eventInThread(ctx, eventID, t)
		if err != nil {
			a.Log.Error().Err(err).Msg("receipts: thread")
			return "db_error", clientreq.Internal
		}
		if !in {
			return "bad_thread", clientreq.New(400, "M_INVALID_PARAM",
				"event_id "+eventID+" is not related to thread "+t)
		}
		threadID = &t
	}

	if cerr := a.bump(ctx, v.UserID, v.DeviceID); cerr != nil {
		return "presence_failed", cerr
	}

	if receiptType == TypeFullyRead {
		if err := a.readMarker(ctx, roomID, v.UserID, eventID, allowBackward); err != nil {
			return "read_marker_failed", a.asClient(err)
		}
		return "read_marker", nil
	}
	outcome, err := a.clientReceipt(ctx, roomID, receiptType, v.UserID, eventID, threadID)
	if err != nil {
		return "failed", a.asClient(err)
	}
	return outcome, nil
}

// serveReadMarkers is ReadMarkerRestServlet.on_POST.
func (a *App) serveReadMarkers(ctx context.Context, r *http.Request, args []string) (string, *clientreq.Error) {
	v, cerr, err := clientreq.User(ctx, a.Auth, r, false)
	if err != nil {
		a.Log.Error().Err(err).Msg("receipts: cannot validate a token")
		return "auth_unavailable", cerr
	}
	if cerr != nil {
		return "auth_rejected", cerr
	}
	roomID := clientreq.Unquote(args[0])
	if cerr := a.bump(ctx, v.UserID, v.DeviceID); cerr != nil {
		return "presence_failed", cerr
	}
	body, cerr := clientreq.ReadObject(r.Body)
	if cerr != nil {
		return "bad_json", cerr
	}
	allowBackward := false
	if a.msc4446() {
		if raw, ok := body["com.beeper.allow_backward"]; ok {
			b, isBool := jsonBool(raw)
			if !isBool {
				return "bad_allow_backward", clientreq.New(400, "M_INVALID_PARAM", "com.beeper.allow_backward must be a boolean.")
			}
			allowBackward = b
		}
	}
	var unknown []string
	for k := range body {
		if k == "com.beeper.allow_backward" && a.msc4446() {
			continue
		}
		isKnown := false
		for _, t := range knownTypes {
			isKnown = isKnown || t == k
		}
		if !isKnown {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		a.Log.Info().Strs("types", unknown).Msg("Ignoring unrecognized receipt types")
	}
	// The order of a Python set: any order is Synapse's.
	for _, receiptType := range knownTypes {
		raw, ok := body[receiptType]
		if !ok || !clientreq.Truthy(raw) {
			continue
		}
		var eventID string
		if json.Unmarshal(raw, &eventID) != nil {
			// A truthy non-string reaches the event store, which raises
			// ("TODO Add validation to reject non-string event IDs").
			return "bad_event_id", clientreq.Internal
		}
		if receiptType == TypeFullyRead {
			if err := a.readMarker(ctx, roomID, v.UserID, eventID, allowBackward); err != nil {
				return "read_marker_failed", a.asClient(err)
			}
			continue
		}
		if _, err := a.clientReceipt(ctx, roomID, receiptType, v.UserID, eventID, nil); err != nil {
			return "failed", a.asClient(err)
		}
	}
	return "ok", nil
}

// bump is bump_presence_active_time; its failure is the request's.
func (a *App) bump(ctx context.Context, userID, deviceID string) *clientreq.Error {
	if !a.trackPresence() {
		return nil
	}
	if err := a.Presence.Bump(ctx, userID, deviceID); err != nil {
		a.Log.Warn().Err(err).Str("user", userID).Msg("receipts: presence bump failed")
		return clientreq.Internal
	}
	return nil
}

// asClient is how Synapse renders what the handlers raise.
func (a *App) asClient(err error) *clientreq.Error {
	var cerr *clientError
	if errors.As(err, &cerr) {
		return &clientreq.Error{Status: cerr.status, Body: cerr.body}
	}
	var missing *ErrNoEvent
	if errors.As(err, &missing) {
		return clientreq.New(404, "M_UNKNOWN", missing.Error())
	}
	a.Log.Error().Err(err).Msg("receipts: request failed")
	return clientreq.Internal
}

// clientError is a SynapseError a handler raises.
type clientError struct {
	status int
	body   map[string]any
}

func (e *clientError) Error() string { s, _ := e.body["error"].(string); return s }

// eventInThread is ReceiptRestServlet._is_event_in_thread.
func (a *App) eventInThread(ctx context.Context, eventID, threadID string) (bool, error) {
	if threadID == MainTimeline {
		t, err := a.Store.ThreadID(ctx, eventID)
		return t == MainTimeline, err
	}
	t, err := a.Store.ThreadIDForReceipts(ctx, eventID)
	return t == threadID, err
}

// jsonBool is isinstance(v, bool).
func jsonBool(raw json.RawMessage) (bool, bool) {
	switch strings.TrimSpace(string(raw)) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

// validRoomID is RoomID.is_valid.
func validRoomID(s string) bool { return clientreq.ValidRoomID(s) }
