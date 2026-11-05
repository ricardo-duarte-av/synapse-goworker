package typing

import (
	"context"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
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

// serveClient is RoomTypingRestServlet.on_PUT.
func (a *App) serveClient(w http.ResponseWriter, r *http.Request) {
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
	requests.WithLabelValues(outcome).Inc()
	clientreq.Write(w, cerr, nil)
}

func (a *App) handleClient(ctx context.Context, r *http.Request, state *State) (string, *clientreq.Error) {
	v, cerr, err := clientreq.User(ctx, a.auth, r, false)
	if err != nil {
		a.log.Error().Err(err).Msg("typing: cannot validate a token")
		return "auth_unavailable", cerr
	}
	if cerr != nil {
		if v.Valid && v.IsGuest {
			return "guest", cerr
		}
		return "auth_rejected", cerr
	}

	rawRoom, rawUser, _ := parsePath(r.URL.EscapedPath())
	roomID := clientreq.Unquote(rawRoom)
	target := clientreq.Unquote(rawUser)
	if cerr := checkUserID(target); cerr != nil {
		return "bad_user_id", cerr
	}

	content, cerr := clientreq.ReadObject(r.Body)
	if cerr != nil {
		return "bad_json", cerr
	}

	if a.trackPresence() {
		if err := a.presence.Bump(ctx, v.UserID, v.DeviceID); err != nil {
			a.log.Warn().Err(err).Str("user", v.UserID).Msg("typing: presence bump failed")
			return "presence_failed", clientreq.Internal
		}
	}

	// timeout = min(content.get("timeout", 30000), 120000)
	timeout := DefaultTimeout
	if raw, ok := content["timeout"]; ok {
		t, ok := clientreq.Number(raw)
		if !ok {
			// min() of a str, None, list or dict and an int: TypeError.
			return "bad_timeout", clientreq.Internal
		}
		timeout = t
	}
	timeout = min(timeout, MaxTimeout)

	rawTyping, ok := content["typing"]
	if !ok {
		// content["typing"]: KeyError.
		return "no_typing_key", clientreq.Internal
	}
	typing := clientreq.Truthy(rawTyping)

	// started_typing / stopped_typing, whose checks are the same.
	if domainOf(target) != a.serverName {
		return "not_local", clientreq.New(http.StatusBadRequest, matrixerr.CodeUnknown, "User is not hosted on this homeserver")
	}
	if target != v.UserID {
		return "other_user", clientreq.New(http.StatusBadRequest, matrixerr.CodeForbidden, "Cannot set another user's typing state")
	}
	banned := false
	if v.AppServiceID == "" {
		// An appservice's requester is never shadow-banned (create_requester).
		if banned, err = a.store.ShadowBanned(ctx, v.UserID); err != nil {
			a.log.Error().Err(err).Msg("typing: shadow_banned")
			return "db_error", clientreq.Internal
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
		return "db_error", clientreq.Internal
	}
	if membership != "join" {
		// UnstableSpecAuthError(403, ..., NOT_JOINED): M_FORBIDDEN, and the
		// new errcode beside it under MSC3848.
		cerr := clientreq.New(http.StatusForbidden, matrixerr.CodeForbidden,
			"User "+v.UserID+" not in room "+roomID)
		if a.msc3848() {
			cerr.Body["org.matrix.msc3848.unstable.errcode"] = "ORG.MATRIX.MSC3848.NOT_JOINED"
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

// checkUserID is UserID.from_string's two errors, as a client sees them.
func checkUserID(s string) *clientreq.Error {
	if err := userid.Check(s); err != nil {
		return clientreq.New(http.StatusBadRequest, matrixerr.CodeInvalidParam, err.Msg)
	}
	return nil
}

// verdictAuth is what the endpoint needs of clientauth.
type verdictAuth = clientreq.Authenticator
