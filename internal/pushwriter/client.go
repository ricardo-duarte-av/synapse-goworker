package pushwriter

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

// parse is PushRuleRestServlet's client_patterns("/(?P<path>pushrules/.*)$",
// v1=True): api/v1, r0, v3, unstable; the path argument unquoted.
func parse(p string) (string, bool) {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return "", false
	}
	for _, v := range []string{"api/v1/", "r0/", "v3/", "unstable/"} {
		if after, ok := strings.CutPrefix(rest, v); ok {
			if strings.HasPrefix(after, "pushrules/") {
				return clientreq.Unquote(after), true
			}
			return "", false
		}
	}
	return "", false
}

// Match is GET, PUT and DELETE under /pushrules/; anything else goes to
// main, which answers as Synapse does.
func Match(method, escapedPath string) bool {
	if method != http.MethodGet && method != http.MethodPut && method != http.MethodDelete {
		return false
	}
	_, ok := parse(escapedPath)
	return ok
}

// Handler serves what Match accepts.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serve) }

func (a *App) serve(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	if !a.writing || (method == http.MethodGet && a.Rules == nil) {
		requests.WithLabelValues(method, "to_main").Inc()
		if !a.writing {
			a.notWriter.Do(func() {
				a.Log.Warn().Msg("a push rules request reached this process, which did not start as the " +
					"push-rules writer; it goes to main. nginx and homeserver.yaml disagree, or goworker was not restarted")
			})
		}
		a.Fallback.ServeHTTP(w, r)
		return
	}
	ctx := r.Context()
	path, _ := parse(r.URL.EscapedPath())
	v, cerr, err := clientreq.User(ctx, a.Auth, r, false)
	if err != nil {
		a.Log.Error().Err(err).Msg("push rules: cannot validate a token")
		requests.WithLabelValues(method, "auth_unavailable").Inc()
		clientreq.Write(w, cerr, nil)
		return
	}
	if cerr != nil {
		requests.WithLabelValues(method, "auth_rejected").Inc()
		clientreq.Write(w, cerr, nil)
		return
	}
	switch method {
	case http.MethodGet:
		body, cerr := a.serveGet(ctx, v.UserID, path)
		if cerr != nil {
			requests.WithLabelValues(method, "refused").Inc()
			clientreq.Write(w, cerr, nil)
			return
		}
		requests.WithLabelValues(method, "ok").Inc()
		clientreq.WriteRaw(w, body)
		return
	case http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, clientreq.MaxBody+1))
		if err != nil || len(body) > clientreq.MaxBody {
			cerr = clientreq.New(413, "M_TOO_LARGE", "Request content is too large")
			break
		}
		q := r.URL.Query()
		unlock := a.lock(v.UserID)
		cerr = a.put(ctx, v.UserID, path, body, q.Get("before"), q.Get("after"))
		unlock()
	case http.MethodDelete:
		unlock := a.lock(v.UserID)
		cerr = a.del(ctx, v.UserID, path)
		unlock()
	}
	if cerr != nil {
		requests.WithLabelValues(method, "refused").Inc()
	} else {
		requests.WithLabelValues(method, "ok").Inc()
	}
	clientreq.Write(w, cerr, nil)
}

func (a *App) serveGet(ctx context.Context, userID, path string) ([]byte, *clientreq.Error) {
	if _, _, err := a.loadable(ctx, userID); err != nil {
		a.Log.Error().Err(err).Str("user", userID).Msg("push rules: cannot load the ruleset")
		return nil, clientreq.Internal
	}
	rules, err := a.Rules(ctx, userID)
	if err != nil {
		a.Log.Error().Err(err).Msg("push rules: cannot read the ruleset")
		return nil, clientreq.Internal
	}
	part, cerr := a.get(rules, path)
	if cerr != nil {
		return nil, cerr
	}
	enc, err := pyjson.Encode(part)
	if err != nil {
		return nil, clientreq.Internal
	}
	return []byte(enc), nil
}

// Endpoints is copy_push_rules (replication/http/push.py), which main
// calls on the push-rules writer: POST, CACHE = False, WAIT_FOR_STREAMS.
func (a *App) Endpoints() []replserver.Endpoint {
	return []replserver.Endpoint{{
		Name: "copy_push_rules", Method: http.MethodPost, PathArgs: []string{"user_id", "old_room_id", "new_room_id"},
		WaitForStreams: true,
		Handle: func(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
			user := req.Args["user_id"]
			unlock := a.lock(user)
			defer unlock()
			if err := a.copyRules(ctx, req.Args["old_room_id"], req.Args["new_room_id"], user); err != nil {
				copies.WithLabelValues("error").Inc()
				if rerr, ok := err.(*RuleError); ok {
					return 0, nil, &replserver.Error{Code: rerr.Status, Errcode: rerr.Errcode, Msg: rerr.Msg}
				}
				return 0, nil, err
			}
			copies.WithLabelValues("ok").Inc()
			return 200, map[string]any{}, nil
		},
	}}
}
