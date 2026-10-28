package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// InitialSync serves /initialSync: a snapshot of every room the user is in.
//
// Mirrors synapse/handlers/initial_sync.py _snapshot_all_rooms. It is
// /rooms/{roomId}/initialSync fanned out, with three differences that are easy
// to miss:
//
//   - presence, receipts and account_data are GLOBAL here, not per room. The
//     per-room endpoint reports the presence of that room's members; this one
//     reports everyone the user shares any room with, and omits offline states
//     entirely.
//   - each room carries a `visibility` of public or private, from the room
//     directory rather than from history visibility. Two unrelated senses of
//     the word.
//   - an invite carries `inviter` and the invite event, and nothing else: no
//     timeline, no state.
func InitialSync(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		if ann != nil {
			ann.Endpoint = "initial_sync"
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		body, status, mxErr := initialSync(r, d, verdict)
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func initialSync(r *http.Request, d Deps, verdict clientauth.Verdict) ([]byte, int, *matrixerr.Error) {
	ctx := store.WithRequestCache(r.Context())
	ann := server.Annotate(ctx)

	limit := intParam(r, "limit", defaultPaginationLimit)
	includeArchived := boolParam(r, "archived")

	now, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}

	// The guarded caches may only answer for a token that replication has
	// caught up to. Set once per pass, from the token this response will
	// actually report; without it every guarded cache falls through to the
	// database, which is the correct default for anything with no token.
	ctx = store.WithHorizon(ctx, store.Horizon{
		Events:      now.Room.MaxStreamPos(),
		AccountData: now.AccountData,
	})
	timeNow, mxErr := nowMillis(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	memberships := []string{"invite", "join"}
	if includeArchived {
		// A left room needs the state as it was at the leave event, which needs
		// state groups. Refusing beats silently omitting the rooms the caller
		// explicitly asked for.
		return nil, http.StatusNotImplemented, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown,
			Error:   "archived=true is not implemented yet",
		}
	}

	rooms, err := d.Store.RoomsForUser(ctx, verdict.UserID, memberships)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "rooms for user", err)
	}

	requester := clientevent.Requester{
		UserID:   verdict.UserID,
		DeviceID: verdict.DeviceID,
		IsGuest:  verdict.IsGuest,
	}
	requester.TokenID = verdict.TokenID

	accountDataByRoom, err := d.Store.AllRoomAccountData(ctx, verdict.UserID, d.MSC3391Enabled)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room account data", err)
	}

	joined := make([]string, 0, len(rooms))
	for _, room := range rooms {
		if room.Membership == "join" {
			joined = append(joined, room.RoomID)
		}
	}
	receiptsByRoom, err := d.Store.MultiRoomReceipts(ctx, joined, now.Receipt.MaxStreamPos())
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
	}

	// NO requester, deliberately. _snapshot_all_rooms calls create_config
	// without one (initial_sync.py:171), so unsigned.transaction_id is never
	// revealed by /initialSync -- unlike /rooms/{roomId}/initialSync, which
	// does pass the requester and does reveal it. The asymmetry looks like an
	// oversight upstream; it is still the behaviour to match.
	cfg := clientevent.Config{Format: clientevent.FormatV1, MSC4354Enabled: d.MSC4354Enabled}

	roomsOut := make([]map[string]any, 0, len(rooms))
	for _, room := range rooms {
		entry := map[string]any{
			"room_id":    room.RoomID,
			"membership": room.Membership,
			// Note: the room directory's sense of visibility, not history
			// visibility. Unrelated concepts, same word.
			"visibility": visibilityOf(room),
		}

		if room.Membership == "invite" {
			// The sender of the membership event is the inviter.
			entry["inviter"] = room.Sender
			invite, err := d.Store.InviteEvent(ctx, room.EventID, room.RoomID, room.RoomVersion)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "invite event", err)
			}
			body, err := clientevent.Serialize(invite.Stored, timeNow, cfg)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "serialise invite", err)
			}
			entry["invite"] = json.RawMessage(body)
			roomsOut = append(roomsOut, entry)
			continue
		}

		snap, err := buildRoomSnapshot(ctx, d, room, verdict.UserID, now.Room, limit, timeNow,
			clientevent.Requester{})
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room snapshot", err)
		}

		entry["messages"] = map[string]any{
			"chunk": snap.Chunk,
			"start": now.WithRoomKey(snap.Start).String(),
			"end":   now.String(),
		}
		entry["state"] = snap.State

		adEvents, err := accountDataEvents(accountDataByRoom[room.RoomID])
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room account data", err)
		}
		entry["account_data"] = adEvents

		roomsOut = append(roomsOut, entry)
	}

	presenceStates, err := d.Store.SharedRoomPresence(ctx, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "presence", err)
	}
	presence, err := presenceEvents(presenceStates, timeNow)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "presence", err)
	}

	globalAD, err := d.Store.GlobalAccountData(ctx, verdict.UserID, d.MSC3391Enabled)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
	}
	globalADEvents, err := accountDataEvents(globalAD)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
	}

	receipts := make([]json.RawMessage, 0, len(joined))
	for _, roomID := range joined {
		// withThreads: the plural query stamps thread_id and applies MSC4102.
		ev, err := receiptEvent(roomID, receiptsByRoom[roomID], verdict.UserID, true)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
		}
		if ev != nil {
			receipts = append(receipts, ev)
		}
	}

	resp := map[string]any{
		"rooms":        roomsOut,
		"presence":     presence,
		"account_data": globalADEvents,
		"receipts":     receipts,
		"end":          now.String(),
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err)
	}
	return body, http.StatusOK, nil
}

func visibilityOf(room store.RoomForUser) string {
	if room.IsPublic {
		return "public"
	}
	return "private"
}

// boolParam reads a Matrix-style boolean query parameter.
func boolParam(r *http.Request, name string) bool {
	return r.URL.Query().Get(name) == "true"
}
