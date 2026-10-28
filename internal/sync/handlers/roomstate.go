package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// The paths, as RoomStateRestServlet and RoomStateEventRestServlet match
// them: against the raw path, each part then unquoted, so a state key may
// hold an encoded "/" (rest/client/room.py, client_patterns with v1).
var (
	roomStatePath = regexp.MustCompile(
		`^/_matrix/client/(?:api/v1|r0|v3|unstable)/rooms/([^/]*)/state$`)
	roomStateNoKeyPath = regexp.MustCompile(
		`^/_matrix/client/(?:api/v1|r0|v3|unstable)/rooms/([^/]*)/state/([^/]*)$`)
	roomStateKeyPath = regexp.MustCompile(
		`^/_matrix/client/(?:api/v1|r0|v3|unstable)/rooms/([^/]*)/state/([^/]*)/([^/]*)$`)
)

// RoomState serves GET /rooms/{roomId}/state, and
// /rooms/{roomId}/state/{eventType}[/{stateKey}]: get_state_events and
// get_room_data (handlers/message.py), which show a member the current
// state, a user who left (and has not forgotten) the state at their leave,
// and anyone else the current state of a world_readable room.
//
// Left to Synapse: a room whose state is still partial (Synapse waits for
// the rest), a leave without a state group (Synapse fails), and a path whose
// parts do not unquote.
func RoomState(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		raw := r.URL.EscapedPath()

		var roomID, eventType, stateKey string
		var key *store.StateKey
		ok := true
		if m := roomStatePath.FindStringSubmatch(raw); m != nil {
			setEndpoint(ann, "room_state")
			roomID, ok = unquote(m[1], ok)
		} else if m := roomStateNoKeyPath.FindStringSubmatch(raw); m != nil {
			setEndpoint(ann, "room_state_event")
			roomID, ok = unquote(m[1], ok)
			eventType, ok = unquote(m[2], ok)
			key = &store.StateKey{Type: eventType}
		} else if m := roomStateKeyPath.FindStringSubmatch(raw); m != nil {
			setEndpoint(ann, "room_state_event")
			roomID, ok = unquote(m[1], ok)
			eventType, ok = unquote(m[2], ok)
			stateKey, ok = unquote(m[3], ok)
			key = &store.StateKey{Type: eventType, StateKey: stateKey}
		} else {
			ok = false
		}
		if !ok {
			handBack(w, r, d, ann, "path")
			return
		}

		ctx := store.WithRequestCache(r.Context())
		partial, err := d.Store.IsPartialStateRoom(ctx, roomID)
		if err != nil {
			refuse(w, ann, http.StatusInternalServerError, *internalError(d, "partial state", err))
			return
		}
		if partial {
			handBack(w, r, d, ann, "partial_state")
			return
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		format := "content"
		if key != nil {
			// parse_string(request, "format", default="content",
			// allowed_values=["content", "event"]): the first value.
			if v, present := r.URL.Query()["format"]; present && len(v) > 0 {
				format = v[0]
				if format != "content" && format != "event" {
					refuse(w, ann, http.StatusBadRequest, matrixerr.Error{ErrCode: matrixerr.CodeInvalidParam,
						Error: "Query parameter 'format' must be one of ['content', 'event']"})
					return
				}
			}
		}

		body, status, mxErr, back := roomState(r, d, verdict, roomID, key, format)
		if back != "" {
			handBack(w, r, d, ann, back)
			return
		}
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func roomState(r *http.Request, d Deps, verdict clientauth.Verdict, roomID string, key *store.StateKey,
	format string) (body []byte, status int, mxErr *matrixerr.Error, back string) {

	ctx := store.WithRequestCache(r.Context())

	// check_user_in_room_or_world_readable(allow_departed_users=True).
	// get_local_current_membership_for_user_in_room's paranoia check first.
	if !isMine(verdict.UserID, d.ServerName) {
		return nil, http.StatusBadRequest, &matrixerr.Error{ErrCode: "M_BAD_JSON",
			Error: fmt.Sprintf("Provided user_id %s is a non-local user", verdict.UserID)}, ""
	}
	m, err := d.Store.LocalMembership(ctx, roomID, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "membership", err), ""
	}
	departed := false
	switch m.Membership {
	case "join":
	case "leave":
		forgot, err := d.Store.DidForget(ctx, verdict.UserID, roomID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "forgotten", err), ""
		}
		departed = !forgot
	}
	if m.Membership != "join" && !departed {
		vis, err := d.Store.HistoryVisibility(ctx, roomID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "history visibility", err), ""
		}
		if vis != "world_readable" {
			// AuthError's default errcode; %r of a UserID is the ID itself.
			return nil, http.StatusForbidden, &matrixerr.Error{ErrCode: matrixerr.CodeForbidden,
				Error: fmt.Sprintf("User %s not in room %s, and room previews are disabled",
					verdict.UserID, roomID)}, ""
		}
	}

	// The room's version only says how to redact. A room this server has
	// never seen has no current state, and no row: the answer is empty.
	version := ""
	if info, err := d.Store.RoomInfo(ctx, roomID); err == nil {
		version = info.RoomVersion
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, http.StatusInternalServerError, internalError(d, "room info", err), ""
	}

	var events []store.StateEvent
	if departed {
		events, err = d.Store.StateAtEvent(ctx, m.EventID, roomID, version, key)
		if errors.Is(err, store.ErrNoStateGroup) {
			return nil, 0, nil, "no_state_group"
		}
	} else {
		events, err = d.Store.CurrentStateInRowOrder(ctx, roomID, version, key)
	}
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "state", err), ""
	}

	if key != nil && len(events) == 0 {
		return nil, http.StatusNotFound, &matrixerr.Error{ErrCode: matrixerr.CodeNotFound,
			Error: "Event not found."}, ""
	}

	// create_config(requester=...): admin metadata for any server admin.
	admin, err := d.Store.IsServerAdmin(ctx, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "server admin", err), ""
	}
	cfg := clientevent.Config{
		Format: clientevent.FormatV1,
		Requester: clientevent.Requester{UserID: verdict.UserID, DeviceID: verdict.DeviceID,
			TokenID: verdict.TokenID, IsGuest: verdict.IsGuest, AppServiceID: verdict.AppServiceID},
		MSC4354Enabled:       d.MSC4354Enabled,
		IncludeAdminMetadata: admin,
	}
	if key != nil {
		cfg.Format = clientevent.FormatV2
	}
	timeNow := time.Now().UnixMilli()

	// Redacted state is served pruned, as get_events prunes it.
	ids := make([]string, len(events))
	for i, ev := range events {
		ids[i] = ev.EventID
	}
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "redactions", err), ""
	}
	out := make([]json.RawMessage, 0, len(events))
	for _, ev := range events {
		// FilteredEvent.state: no unsigned.membership.
		stored := ev.Stored
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "redaction", err), ""
		}
		b, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise state", err), ""
		}
		out = append(out, b)
	}

	if key != nil {
		if format == "event" {
			return out[0], http.StatusOK, nil, ""
		}
		// data.get_dict()["content"]: the (pruned) content alone.
		return []byte(gjson.GetBytes(out[0], "content").Raw), http.StatusOK, nil, ""
	}
	body, err = json.Marshal(out)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err), ""
	}
	return body, http.StatusOK, nil, ""
}

// unquote is urllib.parse.unquote for a path part. Python leaves a bad
// escape as it is; Go refuses it, and such a request goes to Synapse.
func unquote(s string, ok bool) (string, bool) {
	if !ok {
		return "", false
	}
	u, err := url.PathUnescape(s)
	if err != nil {
		return "", false
	}
	return u, true
}

// isMine is hs.is_mine_id: the user ID's server name is ours.
func isMine(userID, serverName string) bool {
	_, domain, ok := strings.Cut(userID, ":")
	return ok && domain == serverName
}

func setEndpoint(ann *server.Annotation, name string) {
	if ann != nil {
		ann.Endpoint = name
	}
}

// handBack sends a request this worker does not reproduce to Synapse.
func handBack(w http.ResponseWriter, r *http.Request, d Deps, ann *server.Annotation, why string) {
	if ann != nil {
		ann.Outcome = "handed_back"
		ann.Reason = why
	}
	if d.Fallback == nil {
		refuse(w, ann, http.StatusBadGateway, matrixerr.Error{ErrCode: matrixerr.CodeUnknown,
			Error: "Not served here"})
		return
	}
	d.Fallback.ServeHTTP(w, r)
}
