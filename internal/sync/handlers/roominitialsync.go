// Package handlers implements the client API endpoints.
package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/deviceinbox"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lazyload"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/notifier"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/presence"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Deps are what every handler needs.
type Deps struct {
	Store *store.Store
	Auth  *clientauth.Authenticator
	Log   zerolog.Logger
	// MSC4354Enabled mirrors Synapse's experimental.msc4354_enabled.
	MSC4354Enabled bool
	// MSC3391Enabled mirrors Synapse's experimental.msc3391_enabled.
	MSC3391Enabled bool
	// PushRuleFeatures gates individual base push rules.
	PushRuleFeatures pushrules.Features
	// MSC3773Enabled and MSC3874Enabled change only how a filter is read.
	MSC3773Enabled bool
	MSC3874Enabled bool
	// FilterTimelineLimit caps an inline filter's timeline limit. -1 disables
	// the cap; Synapse's default is 100.
	FilterTimelineLimit int
	// Replication supplies live stream positions and typing. Nil when
	// replication is disabled, in which case positions fall back to the
	// database and typing is never reported.
	Replication *replication.Subscriber
	// Notifier wakes long-polling syncs.
	Notifier *notifier.Notifier
	// LazyLoad remembers which member events each device has been sent, so a
	// lazy-loading sync does not repeat them.
	LazyLoad *lazyload.Cache
	// Inbox deletes acknowledged to-device messages. Nil when to_device is not
	// configured, in which case the section is omitted entirely rather than
	// served without ever being cleared.
	Inbox *deviceinbox.Deleter

	// Presence relays "this user is syncing" to Synapse's presence writer.
	// Nil when presence is not configured, in which case every account this
	// worker serves appears permanently offline -- see internal/presence.
	Presence *presence.Client

	// Sliding holds sliding sync's per-connection state. Nil when sliding sync
	// is not configured, in which case the endpoint is not registered at all.
	Sliding *slidingstore.Store
	// MSC3575Enabled mirrors Synapse's experimental.msc3575_enabled, the
	// homeserver-wide default a per-user override can flip either way.
	MSC3575Enabled bool
	// MSC4308Enabled gates the thread-subscriptions extension.
	MSC4308Enabled bool
	// ServerName is this homeserver's name, for deciding which user IDs are
	// local.
	ServerName string
	// ExcludedRooms are rooms configured never to appear in a sync.
	ExcludedRooms map[string]bool
	// Fallback is Synapse's main process, for a request a handler finds it
	// does not reproduce.
	Fallback http.Handler
}

// defaultPaginationLimit is Synapse's PaginationConfig default for the legacy
// initialSync endpoints.
const defaultPaginationLimit = 10

// RoomInitialSync serves /rooms/{roomId}/initialSync.
//
// Mirrors synapse/handlers/initial_sync.py room_initial_sync ->
// _room_initial_sync_joined.
func RoomInitialSync(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		if ann != nil {
			ann.Endpoint = "room_initial_sync"
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		roomID := r.PathValue("roomId")
		limit := intParam(r, "limit", defaultPaginationLimit)

		body, status, mxErr := roomInitialSync(r, d, verdict, roomID, limit)
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func roomInitialSync(r *http.Request, d Deps, verdict clientauth.Verdict, roomID string, limit int) (
	[]byte, int, *matrixerr.Error) {

	ctx := store.WithRequestCache(r.Context())
	ann := server.Annotate(ctx)

	info, err := d.Store.RoomInfo(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, http.StatusNotFound, &matrixerr.Error{
			ErrCode: matrixerr.CodeNotFound, Error: "Room not found"}
	}
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room info", err)
	}
	// Synapse checks this before anything else and answers 403.
	if info.Blocked {
		return nil, http.StatusForbidden, &matrixerr.Error{
			ErrCode: matrixerr.CodeForbidden, Error: "This room has been blocked on this server"}
	}

	membership, err := d.Store.CurrentMembership(ctx, roomID, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "membership", err)
	}
	switch membership.Membership {
	case "join":
		// The path implemented below.
	case "leave":
		// _room_initial_sync_parted resolves the room state *at the leave
		// event*, which needs state-group resolution this worker does not have
		// yet. Refusing is correct: a snapshot built from current state would
		// show a departed user everything that happened after they left.
		return nil, http.StatusNotImplemented, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown,
			Error:   "Snapshots of a room the user has left are not implemented yet"}
	default:
		return nil, http.StatusForbidden, &matrixerr.Error{
			ErrCode: matrixerr.CodeForbidden, Error: "You are not in the room."}
	}

	now, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	timeNow, mxErr := nowMillis(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}

	state, err := d.Store.CurrentState(ctx, roomID, info.RoomVersion)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "current state", err)
	}

	messages, start, err := d.Store.RecentEvents(ctx, roomID, info.RoomVersion, limit, now.Room)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "recent events", err)
	}

	// Visibility before serialisation: nothing that fails this check should be
	// rendered at all, let alone written to a response buffer.
	//
	// is_peeking is false: the user is joined, checked above.
	messages, memberships, err := filterVisible(ctx, d, roomID, verdict.UserID, messages, false, timeNow)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "visibility", err)
	}

	// Synapse attaches prev_content in the storage layer, before serialisation,
	// so the v1 format lifts it to the top level along with the other copy keys.
	//
	// TIMELINE ONLY. get_recent_events_for_room passes get_prev_content=True;
	// get_current_state does not. Synapse nevertheless emits prev_content on
	// *some* state events, because events_worker writes the field into the
	// shared cached event ("This mutates the cached event, but that's fine")
	// and a later reader of that cache picks it up. Whether it appears depends
	// on whether some other request happened to load that event first, which
	// is not reproducible and not ours to imitate. syncdiff tolerates it
	// upstream-only for exactly this reason.
	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "prev content", err)
	}

	requester := clientevent.Requester{
		UserID:   verdict.UserID,
		DeviceID: verdict.DeviceID,
		IsGuest:  verdict.IsGuest,
	}
	// Only needed for events stored without a device_id, which are rare.
	requester.TokenID = verdict.TokenID

	cfg := clientevent.Config{Format: clientevent.FormatV1, Requester: requester,
		MSC4354Enabled: d.MSC4354Enabled}

	// State events are redacted on read too, not just the timeline: Synapse
	// prunes in the storage layer, so a redacted m.room.topic in the current
	// state is just as redacted as a message.
	redactionIDs := timelineEventIDs(messages)
	for _, ev := range state {
		redactionIDs = append(redactionIDs, ev.EventID)
	}
	redactions, err := d.Store.Redactions(ctx, redactionIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "redactions", err)
	}

	// Synapse wraps state events with FilteredEvent.state(), whose membership is
	// None, so the state block carries no unsigned.membership. Timeline events
	// do carry it.
	stateJSON := make([]json.RawMessage, 0, len(state))
	for _, ev := range state {
		stored := ev.Stored
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "redaction", err)
		}
		b, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise state", err)
		}
		stateJSON = append(stateJSON, b)
	}

	chunk := make([]json.RawMessage, 0, len(messages))
	for i, ev := range messages {
		stored := ev.Stored
		stored.Membership = memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "redaction", err)
		}
		b, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise timeline", err)
		}
		chunk = append(chunk, b)
	}

	receipts, err := roomReceipts(ctx, d, roomID, verdict.UserID, now.Receipt)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
	}

	accountData, err := roomAccountData(ctx, d, verdict.UserID, roomID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "account data", err)
	}

	presence, err := roomPresence(ctx, d, state, timeNow)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "presence", err)
	}

	resp := map[string]any{
		"room_id":    roomID,
		"membership": membership.Membership,
		"messages": map[string]any{
			"chunk": chunk,
			// The room key moves; the other thirteen streams stay where the
			// now token put them.
			"start": now.WithRoomKey(start).String(),
			"end":   now.String(),
		},
		"state":        stateJSON,
		"presence":     presence,
		"receipts":     receipts,
		"account_data": accountData,
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err)
	}
	return body, http.StatusOK, nil
}

// nowToken returns the token bounding this response, derived from the
// database and replication (approximate; see the gosync repo's docs/tokens.md).
func nowToken(r *http.Request, d Deps) (streamtoken.Token, *matrixerr.Error) {
	tok, err := currentToken(r.Context(), d)
	if err != nil {
		return streamtoken.Token{}, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown, Error: "Internal server error"}
	}
	return tok, nil
}

// nowMillis returns the wall clock this response is computed against.
func nowMillis(r *http.Request, d Deps) (int64, *matrixerr.Error) {
	return time.Now().UnixMilli(), nil
}

func intParam(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return def
	}
	return n
}
