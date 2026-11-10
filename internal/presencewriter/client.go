package presencewriter

import (
	"context"
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
	if a.Synapse().PresenceEnabled {
		// The enabled servlet reads and sets state through a presence
		// handler goworker does not have.
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
		// on_GET with presence disabled: before any visibility check.
		requests.WithLabelValues(method, "ok").Inc()
		clientreq.Write(w, nil, map[string]any{"presence": "offline"})
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
	if _, ok := content["presence"]; !ok {
		return "refused", unparsable
	}
	delete(content, "presence")
	if msg, ok := content["status_msg"]; ok {
		if len(msg) == 0 || msg[0] != '"' {
			return "refused", clientreq.New(http.StatusBadRequest, "M_UNKNOWN", "status_msg must be a string.")
		}
		delete(content, "status_msg")
	}
	if len(content) > 0 {
		return "refused", unparsable
	}
	return "ok", nil
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
