package presencewriter

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// parse is PresenceStatusRestServlet's client_patterns(
// "/presence/(?P<user_id>[^/]*)/status$", v1=True): api/v1, r0, v3,
// unstable; the user ID still escaped.
func parse(p string) (string, bool) {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return "", false
	}
	for _, v := range []string{"api/v1/", "r0/", "v3/", "unstable/"} {
		if after, ok := strings.CutPrefix(rest, v); ok {
			seg := strings.Split(after, "/")
			if len(seg) == 3 && seg[0] == "presence" && seg[2] == "status" {
				return seg[1], true
			}
			return "", false
		}
	}
	return "", false
}

// Match is GET and PUT on .../presence/{userId}/status; anything else goes
// to main, which answers it as Synapse does.
func Match(method, escapedPath string) bool {
	if method != http.MethodGet && method != http.MethodPut {
		return false
	}
	_, ok := parse(escapedPath)
	return ok
}

// Handler serves what Match accepts.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serve) }

func (a *App) serve(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	if a.tr == nil && a.Synapse().PresenceEnabled {
		// The enabled servlet reads and sets state through a presence
		// handler, which goworker has only as the writer.
		requests.WithLabelValues(method, "to_main").Inc()
		a.enabledToMain.Do(func() {
			a.Worker.Info().Msg("presence is enabled in homeserver.yaml; presence requests go to main")
		})
		a.Fallback.ServeHTTP(w, r)
		return
	}
	ctx := r.Context()
	v, cerr, err := clientreq.User(ctx, a.Auth, r, false)
	if err != nil {
		a.Log.Error().Err(err).Msg("presence: cannot validate a token")
		requests.WithLabelValues(method, "auth_unavailable").Inc()
		clientreq.Write(w, cerr, nil)
		return
	}
	if cerr != nil {
		requests.WithLabelValues(method, "auth_rejected").Inc()
		clientreq.Write(w, cerr, nil)
		return
	}
	escaped, _ := parse(r.URL.EscapedPath())
	target := clientreq.Unquote(escaped)
	if perr := userid.Check(target); perr != nil {
		requests.WithLabelValues(method, "refused").Inc()
		clientreq.Write(w, clientreq.New(http.StatusBadRequest, "M_INVALID_PARAM", perr.Msg), nil)
		return
	}
	if method == http.MethodGet {
		if a.tr == nil {
			// on_GET with presence disabled: before any visibility check.
			requests.WithLabelValues(method, "ok").Inc()
			clientreq.Write(w, nil, map[string]any{"presence": "offline"})
			return
		}
		body, outcome, cerr := a.get(ctx, v, target)
		requests.WithLabelValues(method, outcome).Inc()
		if cerr != nil {
			clientreq.Write(w, cerr, nil)
			return
		}
		clientreq.WriteRaw(w, body)
		return
	}
	outcome, cerr := a.put(ctx, r, v, target)
	requests.WithLabelValues(method, outcome).Inc()
	clientreq.Write(w, cerr, nil)
}

// put is on_PUT with track_presence false: everything but set_state.
func (a *App) put(ctx context.Context, r *http.Request, v clientauth.Verdict, target string) (string, *clientreq.Error) {
	if v.UserID != target {
		return "refused", clientreq.New(http.StatusForbidden, "M_FORBIDDEN", "Can only set your own presence state")
	}
	ok, retryMS, err := a.allowed(ctx, v)
	if err != nil {
		a.Log.Error().Err(err).Msg("presence: ratelimit_override")
		return "error", clientreq.Internal
	}
	if !ok {
		// Returned, not raised: no Retry-After header and no pause.
		return "ratelimited", &clientreq.Error{Status: http.StatusTooManyRequests, Body: map[string]any{
			"errcode": "M_LIMIT_EXCEEDED", "error": "Too many requests", "retry_after_ms": retryMS,
		}}
	}
	content, cerr := clientreq.ReadObject(r.Body)
	if cerr != nil {
		return "refused", cerr
	}
	unparsable := clientreq.New(http.StatusBadRequest, "M_UNKNOWN", "Unable to parse state")
	presenceRaw, ok := content["presence"]
	if !ok {
		return "refused", unparsable
	}
	delete(content, "presence")
	var msgRaw json.RawMessage
	if msg, ok := content["status_msg"]; ok {
		if len(msg) == 0 || msg[0] != '"' {
			return "refused", clientreq.New(http.StatusBadRequest, "M_UNKNOWN", "status_msg must be a string.")
		}
		msgRaw = msg
		delete(content, "status_msg")
	}
	if len(content) > 0 {
		return "refused", unparsable
	}
	if !a.Tracking() {
		return "ok", nil
	}
	// set_state(user, requester.device_id, state).
	var presence string
	if json.Unmarshal(presenceRaw, &presence) != nil || !a.validPresence(presence) {
		return "refused", clientreq.New(http.StatusBadRequest, "M_UNKNOWN", "Invalid presence state")
	}
	var msg *string
	if msgRaw != nil {
		var m string
		_ = json.Unmarshal(msgRaw, &m)
		msg = &m
	}
	sources.WithLabelValues("set_state").Inc()
	a.tr.op.Lock()
	err = a.tr.setState(ctx, v.UserID, devOf(v.DeviceID), presence, msg, false, false)
	a.tr.op.Unlock()
	if err != nil {
		a.Log.Error().Err(err).Str("user_id", v.UserID).Msg("presence: set_state")
		return "error", clientreq.Internal
	}
	return "ok", nil
}

// get is on_GET with presence enabled: someone else's presence only if a
// room is shared (excluded rooms aside), then get_state, formatted without
// the user ID.
func (a *App) get(ctx context.Context, v clientauth.Verdict, target string) ([]byte, string, *clientreq.Error) {
	if v.UserID != target {
		ok, err := a.isVisible(ctx, target, v.UserID)
		if err != nil {
			a.Log.Error().Err(err).Msg("presence: is_visible")
			return nil, "error", clientreq.Internal
		}
		if !ok {
			return nil, "refused", clientreq.New(http.StatusForbidden, "M_FORBIDDEN", "You are not allowed to see their presence.")
		}
	}
	states, err := a.tr.currentStates(ctx, []string{target})
	if err != nil {
		a.Log.Error().Err(err).Msg("presence: get_state")
		return nil, "error", clientreq.Internal
	}
	return formatPresence(states[target], a.Now().UnixMilli(), false).encode(), "ok", nil
}

// isVisible is is_visible: the two share a room not excluded.
func (a *App) isVisible(ctx context.Context, observed, observer string) (bool, error) {
	mine, err := a.Rooms.RoomsForUser(ctx, observer)
	if err != nil {
		return false, err
	}
	theirs, err := a.Rooms.RoomsForUser(ctx, observed)
	if err != nil {
		return false, err
	}
	set := map[string]struct{}{}
	for _, r := range mine {
		set[r] = struct{}{}
	}
	for _, r := range theirs {
		if _, ok := set[r]; ok && !has(a.tr.excluded, r) {
			return true, nil
		}
	}
	return false, nil
}

// allowed is Ratelimiter.ratelimit(requester) for rc_presence.per_user,
// keyed by the user: an appservice that is not rate limited, and an entity
// whose ratelimit_override has no rate, always may.
func (a *App) allowed(ctx context.Context, v clientauth.Verdict) (bool, int64, error) {
	if v.AppServiceID != "" && v.AppServiceUnlimited {
		return true, 0, nil
	}
	entity := v.Entity
	if entity == "" {
		entity = v.UserID
	}
	exempt, err := a.Store.RatelimitExempt(ctx, entity)
	if err != nil || exempt {
		return exempt, 0, err
	}
	ok, retry := a.perUser.CanDo(v.UserID, true)
	return ok, retry, nil
}
