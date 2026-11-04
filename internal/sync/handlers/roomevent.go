package handlers

import (
	"net/http"
	"regexp"
	"slices"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// roomEventPath is RoomEventServlet's pattern (rest/client/room.py,
// client_patterns with v1), matched against the raw path.
var roomEventPath = regexp.MustCompile(
	`^/_matrix/client/(?:api/v1|r0|v3|unstable)/rooms/([^/]*)/event/([^/]*)$`)

// msc2815Param is the query parameter MSC2815 adds.
const msc2815Param = "fi.mau.msc2815.include_unredacted_content"

// RoomEvent serves GET /rooms/{roomId}/event/{eventId}: RoomEventServlet
// over EventHandler.get_event. The event as stored (redacted if it was),
// if it is in that room, was not rejected, and the caller may see it under
// filter_and_transform_events_for_client, peeking unless joined; with its
// bundled aggregations, in the v1 format. Anything else is Synapse's 404
// (a refusal by visibility looks the same, on purpose).
//
// Left to Synapse: MSC2815's unredacted content when it is enabled and
// asked for; an event whose sender is erased and whom the caller did not
// share the room with (Synapse serves it pruned; eventfilter withholds it);
// a room without a known room version; a path whose parts do not unquote.
func RoomEvent(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		setEndpoint(ann, "room_event")
		m := roomEventPath.FindStringSubmatch(r.URL.EscapedPath())
		ok := m != nil
		var roomID, eventID string
		if ok {
			roomID, ok = unquote(m[1], ok)
			eventID, ok = unquote(m[2], ok)
		}
		if !ok {
			handBack(w, r, d, ann, "path")
			return
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		// The parameter is parsed only when the MSC is enabled; then any
		// use of it, valid or not, is Synapse's.
		if d.MSC2815Enabled {
			if _, present := r.URL.Query()[msc2815Param]; present {
				handBack(w, r, d, ann, "msc2815")
				return
			}
		}

		body, mxErr, back := roomEvent(r, d, verdict, roomID, eventID)
		switch {
		case back != "":
			handBack(w, r, d, ann, back)
		case mxErr != nil:
			status := http.StatusNotFound
			if mxErr.ErrCode != matrixerr.CodeNotFound {
				status = http.StatusInternalServerError
			}
			refuse(w, ann, status, *mxErr)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}
	})
}

var eventNotFound = matrixerr.Error{ErrCode: matrixerr.CodeNotFound, Error: "Event not found."}

func roomEvent(r *http.Request, d Deps, verdict clientauth.Verdict, roomID, eventID string) (
	body []byte, mxErr *matrixerr.Error, back string) {

	ctx := store.WithRequestCache(r.Context())

	// store.get_event(check_room_id=room_id, allow_none=True).
	ev, found, err := d.Store.EventForClient(ctx, eventID)
	if err != nil {
		return nil, internalError(d, "event", err), ""
	}
	if !found || ev.RoomID != roomID {
		return nil, &eventNotFound, ""
	}
	if !clientevent.IsKnownRoomVersion(ev.RoomVersion) {
		// Synapse drops events of an unknown room version, and reads an
		// event without a rooms row by its format; both are rare.
		return nil, nil, "room_version"
	}

	// check_local_user_in_room: peeking unless joined now.
	m, err := d.Store.LocalMembership(ctx, roomID, verdict.UserID)
	if err != nil {
		return nil, internalError(d, "membership", err), ""
	}
	timeNow := time.Now().UnixMilli()
	res, err := eventfilter.ForClient(ctx, d.Store, roomID, verdict.UserID,
		[]store.TimelineEvent{ev}, m.Membership != "join", timeNow, d.MSC4354Enabled, nil)
	if err != nil {
		return nil, internalError(d, "visibility", err), ""
	}
	if slices.Contains(res.Pruned, eventID) {
		return nil, nil, "erased_sender"
	}
	if len(res.Events) == 0 {
		// The AuthError, turned into a 404 by the servlet.
		return nil, &eventNotFound, ""
	}

	// create_config(requester=...): admin metadata for any server admin.
	admin, err := d.Store.IsServerAdmin(ctx, verdict.UserID)
	if err != nil {
		return nil, internalError(d, "server admin", err), ""
	}
	cfg := clientevent.Config{
		Format: clientevent.FormatV1,
		Requester: clientevent.Requester{UserID: verdict.UserID, DeviceID: verdict.DeviceID,
			TokenID: verdict.TokenID, IsGuest: verdict.IsGuest, AppServiceID: verdict.AppServiceID},
		MSC4354Enabled:       d.MSC4354Enabled,
		IncludeAdminMetadata: admin,
	}

	redactions, err := d.Store.Redactions(ctx, []string{eventID})
	if err != nil {
		return nil, internalError(d, "redactions", err), ""
	}
	stored := ev.Stored
	stored.Membership = res.Memberships[0]
	if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
		return nil, internalError(d, "redaction", err), ""
	}
	body, err = clientevent.Serialize(stored, timeNow, cfg)
	if err != nil {
		return nil, internalError(d, "serialise", err), ""
	}

	// get_bundled_aggregations, always: per MSC2676 the event is the
	// original, its edit bundled beside it.
	vis, err := d.Store.VisibilityExtras(ctx, roomID, verdict.UserID, nil)
	if err != nil {
		return nil, internalError(d, "ignored users", err), ""
	}
	aggs, nestedIDs, err := bundleAggregations(ctx, d, verdict.UserID, res.Events, vis.IgnoredSenders)
	if err != nil {
		return nil, internalError(d, "aggregations", err), ""
	}
	if agg, ok := aggs[eventID]; ok {
		want := append([]string(nil), nestedIDs...)
		for _, a := range aggs {
			if a.ReplaceID() != "" {
				want = append(want, a.ReplaceID())
			}
		}
		nested, err := d.Store.EventsByID(ctx, want, ev.RoomVersion)
		if err != nil {
			return nil, internalError(d, "aggregated events", err), ""
		}
		body, err = attachAggregations(body, agg, aggs, nested, timeNow, cfg)
		if err != nil {
			return nil, internalError(d, "aggregations", err), ""
		}
	}
	return body, nil, ""
}
