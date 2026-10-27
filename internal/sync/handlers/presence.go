package handlers

import (
	"context"
	"net/http"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/presence"
)

// allowedPresence is SyncRestServlet.ALLOWED_PRESENCE.
//
// `busy` is deliberately absent even though the presence writer accepts it
// under MSC3026: the /sync servlet does not, so a client sending it gets a 400
// from Synapse and must get one here too.
var allowedPresence = map[string]bool{
	presence.StateOnline:      true,
	presence.StateOffline:     true,
	presence.StateUnavailable: true,
}

// parseSetPresence reads ?set_presence=, defaulting as Synapse does.
//
// Synapse uses parse_string with allowed_values, which answers
// 400 M_INVALID_PARAM on anything else rather than ignoring it.
func parseSetPresence(r *http.Request) (string, *matrixerr.Error) {
	v := r.URL.Query().Get("set_presence")
	if v == "" {
		return presence.StateOnline, nil
	}
	if !allowedPresence[v] {
		return "", &matrixerr.Error{
			ErrCode: matrixerr.CodeInvalidParam,
			Error:   "Query parameter 'set_presence' must be one of ['offline', 'online', 'unavailable']",
		}
	}
	return v, nil
}

// relayPresence tells the presence writer this user is syncing.
//
// Synapse calls presence_handler.user_syncing() around every /sync, and the
// state it passes is this parameter. Two details are carried over:
//
//   - `set_presence=offline` means DO NOT TOUCH presence, not "set me offline".
//     Synapse computes affect_presence = set_presence != offline and, when it
//     is false, hands the sync a null context manager that relays nothing. A
//     worker that instead relayed "offline" would actively push users offline
//     rather than merely leaving them alone.
//   - The update is marked as sync-driven, which is what makes the writer
//     refresh last_user_sync_ts and decline to override a BUSY state.
//
// A failure is logged and swallowed, exactly as the to-device deletion is. The
// cost of not relaying is that a user looks stale for a while; the cost of
// failing the sync is that their client stops working. Synapse would propagate
// the error here, and that is a deliberate deviation: our reason for existing
// is to serve the sync.
//
// Synapse also rate-limits this per user (rc_presence_per_user, 0.1/s with a
// burst of 1) and skips the relay when the limit is exceeded. We do not
// implement that limiter because our own throttle is stricter: one relay per
// device per 25 seconds against the limiter's one per ten. The limiter could
// therefore never fire on traffic that got past the throttle.
func relayPresence(ctx context.Context, d Deps, verdict clientauth.Verdict, state string) {
	if d.Presence == nil || state == presence.StateOffline {
		return
	}
	if err := d.Presence.SetState(ctx, verdict.UserID, verdict.DeviceID, state, true); err != nil {
		if clientGone(err) {
			// The caller hung up mid-relay. Not our failure, and not worth an
			// error line on an endpoint built to be abandoned.
			d.Log.Debug().Err(err).Msg("presence relay abandoned by the client")
			return
		}
		d.Log.Warn().Err(err).
			Str("user_id", verdict.UserID).Str("device_id", verdict.DeviceID).
			Str("state", state).
			Msg("could not relay presence; this user will look stale to others")
	}
}
