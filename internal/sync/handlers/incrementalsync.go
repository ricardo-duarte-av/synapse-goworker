package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// incrementalSync serves a /sync carrying a `since`.
//
// The shape differs from an initial sync in one structural way that governs
// everything else: a room appears ONLY if something happened in it. Synapse
// drops a room whose timeline, state, account data and ephemeral are all empty
// (`_generate_room_entry`), and a response where nothing at all happened has no
// `rooms` key whatsoever.
// rooms, when non-nil, is the caller's already-fetched membership list. The
// long poll computes one per pass to decide which rooms to wait on, and that
// answer is the same one this function would go and ask for; passing it down
// halves a query that runs on every sync of every client. Nil means fetch it,
// which is what the non-polling callers do.
func incrementalSync(r *http.Request, d Deps, verdict clientauth.Verdict, sinceRaw string,
	useStateAfter bool, f *filter.Collection, rooms []store.RoomForUser) (
	[]byte, int, *matrixerr.Error) {

	// A fresh memo per pass, not per request: a long poll runs this function
	// many times on one request context, and an answer cached at the start of
	// a five-minute poll is not an answer by the end of it.
	ctx := store.WithRequestCache(r.Context())
	ann := server.Annotate(ctx)

	since, err := streamtoken.Parse(sinceRaw)
	if err != nil {
		return nil, http.StatusBadRequest, &matrixerr.Error{
			ErrCode: matrixerr.CodeInvalidParam, Error: err.Error()}
	}

	now, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	now = now.AtLeast(since)

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
	// May wind now.ToDevice back, so it runs before next_batch is recorded.
	toDevice, err := toDeviceEvents(ctx, d, verdict, since.ToDevice, &now)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "to-device", err)
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	sincePos := since.Room.MaxStreamPos()
	nowPos := now.Room.MaxStreamPos()

	if rooms == nil {
		var err error
		rooms, err = d.Store.RoomsForUser(ctx, verdict.UserID, []string{"invite", "join"})
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "rooms for user", err)
		}
	}
	roomVersions := map[string]string{}
	joinedIDs := make([]string, 0, len(rooms))
	for _, room := range rooms {
		roomVersions[room.RoomID] = room.RoomVersion
		if room.Membership == "join" {
			joinedIDs = append(joinedIDs, room.RoomID)
		}
	}

	requester := clientevent.Requester{
		UserID: verdict.UserID, DeviceID: verdict.DeviceID, IsGuest: verdict.IsGuest,
	}
	requester.TokenID = verdict.TokenID
	cfg := clientevent.Config{
		Format:         eventFormat(f, clientevent.FormatV2NoRoomID),
		Requester:      requester,
		MSC4354Enabled: d.MSC4354Enabled,
		EventFields:    f.EventFields,
	}
	// A server admin who asked to see soft-failed events is told which they
	// are, matching Synapse's include_admin_metadata. The visibility filter
	// lets them through; without this they arrive indistinguishable from
	// ordinary events.
	if wants, err := d.Store.AdminWantsSoftFailedEvents(ctx, verdict.UserID); err == nil {
		cfg.IncludeAdminMetadata = wants
	}
	strippedCfg := cfg
	strippedCfg.IncludeStrippedRoomState = true

	timelineLimit := f.TimelineLimit()
	// ONE MORE than the limit, which is exactly what Synapse asks for
	// (`get_room_events_stream_for_rooms(limit=timeline_limit + 1)`). The extra
	// row is not spare capacity, it is the question being asked: a room that
	// returns it held more than the client may be given, and is therefore
	// `limited`. Loading a larger page instead makes that unanswerable, because
	// "we loaded more than we will return" stops meaning anything.
	//
	// Under-filling is handled where Synapse handles it, by re-paginating in
	// loadFilteredRecents rather than by over-fetching here.
	loadLimit := timelineLimit + 1
	timelines := map[string][]store.TimelineEvent{}
	if !f.BlocksAllRooms() && !f.BlocksAllRoomTimeline() && timelineLimit > 0 {
		timelines, err = d.Store.RoomTimelineSince(ctx, joinedIDs, roomVersions,
			sincePos, nowPos, loadLimit)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room timelines", err)
		}
	}

	accountDataByRoom := map[string][]store.AccountDataEntry{}
	if !f.BlocksAllRooms() && !f.BlocksAllRoomAccountData() {
		accountDataByRoom, err = d.Store.RoomAccountDataSince(ctx, verdict.UserID,
			since.AccountData, now.AccountData, d.MSC3391Enabled)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room account data", err)
		}
	}
	receiptsByRoom := map[string][]store.ReceiptRow{}
	if !f.BlocksAllRooms() && !f.BlocksAllRoomEphemeral() {
		receiptsByRoom, err = d.Store.ReceiptsSince(ctx, joinedIDs,
			since.Receipt.MaxStreamPos(), now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
		}
	}

	// Typing is bounded by the client's token, exactly as every other stream
	// is -- `ephemeral_by_room` asks the typing source for rooms whose serial
	// is above since_token.typing_key and no others.
	//
	// Reporting the CURRENT typists on every sync instead looks harmless and
	// is not: a room with somebody typing then makes every incremental sync
	// return immediately with the same event, the client stores next_batch and
	// asks again, and the pair spin at whatever rate the network allows for as
	// long as the typing lasts. gomuks did 35 requests a second for minutes.
	typingRooms := map[string]bool{}
	if d.Replication != nil {
		for _, id := range d.Replication.TypingChangedSince(since.Typing) {
			typingRooms[id] = true
		}
	}

	// Before any room entry is built: this moves the now token, and every
	// prev_batch in the response carries it.
	sticky, err := stickyByRoom(ctx, d, joinedIDs, since.StickyEvents, &now, timeNow)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "sticky events", err)
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	// One query for every membership change of this user in the window, used
	// twice: to decide which rooms are newly joined, and further down to
	// report invites.
	//
	// This is what makes the loop below cheap. A room can only have become
	// newly joined if the user's membership event for it lands in (since, now]
	// -- so a room with no membership change needs no probe at all, and on a
	// real account that is all but a handful of them. Synapse works the same
	// way: `_get_room_changes_for_incremental_sync` iterates
	// `mem_change_events_by_room_id` and never looks at the rest.
	changes, err := d.Store.MembershipChangesForUser(ctx, verdict.UserID, sincePos, nowPos)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "membership changes", err)
	}
	changesByRoom := map[string][]store.MembershipChange{}
	for _, c := range changes {
		changesByRoom[c.RoomID] = append(changesByRoom[c.RoomID], c)
	}

	// History gaps for every joined room in one query, on the same bounds the
	// per-room path would have used. The single-room form asked once per room
	// -- 653 round trips on this account for an answer that concerned two of
	// them -- and it was by a wide margin the most-called query on the worker.
	gapsByRoom, err := d.Store.TimelineGaps(ctx, joinedIDs, &since.Room, now.Room)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "timeline gaps", err)
	}
	gaps := newGapSet(gapsByRoom, &since.Room, now.Room)

	joinedRooms := map[string]any{}
	var newlyJoined []string
	for _, room := range rooms {
		if f.BlocksAllRooms() {
			break
		}
		if room.Membership != "join" {
			continue
		}

		// A room joined inside the window is "newly joined", and Synapse then
		// treats it exactly as an initial sync would: full state, a timeline
		// paginated back from now rather than only the events since `since`,
		// and `limited` set. The client has never seen this room, so sending
		// it a delta against a state it does not have would be meaningless.
		//
		// Deciding that costs up to three queries, so it is worth not asking.
		// Two gates, both Synapse's:
		//
		//  1. No membership event in the window means the membership at
		//     `since` is the membership now, which is `join`. This is the one
		//     that matters -- it is the difference between three queries per
		//     room and three queries per room that changed.
		//  2. A membership event that is not a join, in a room we are joined
		//     to now, means we left and rejoined inside the window. Newly
		//     joined by construction, and Synapse `continue`s before the state
		//     lookup rather than confirming what it already knows.
		roomChanges, changedHere := changesByRoom[room.RoomID]
		rejoined := false
		for _, c := range roomChanges {
			if c.Membership != "join" {
				rejoined = true
				break
			}
		}
		wasJoined := "join"
		switch {
		case !changedHere:
			// Gate 1. Membership at `since` is membership now.
		case rejoined:
			// Gate 2. Left and came back inside the window.
			wasJoined = ""
		default:
			wasJoined, err = d.Store.MembershipAtPosition(ctx, room.RoomID, verdict.UserID, since.Room)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "membership at since", err)
			}
		}
		if wasJoined != "join" {
			newlyJoined = append(newlyJoined, room.RoomID)
			// A newly joined room gets ALL its sticky events, historical ones
			// included, not only those since `since`, and without the
			// per-sync limit (sticky_events_for_newly_joined_rooms, #20233).
			if d.MSC4354Enabled {
				_, all, err := d.Store.StickyEvents(ctx, []string{room.RoomID}, 0, now.StickyEvents,
					timeNow, math.MaxInt32)
				if err != nil {
					return nil, http.StatusInternalServerError, internalError(d, "sticky events", err)
				}
				if sticky == nil {
					sticky = map[string][]string{}
				}
				sticky[room.RoomID] = all[room.RoomID]
			}
			// The room is given a full-state entry, but its timeline still
			// comes from the window this sync loaded: Synapse builds the
			// chunk first and marks the room newly joined afterwards, so the
			// prev_batch of an untrimmed timeline is the start of that chunk,
			// not the now token.
			raw := timelines[room.RoomID]
			src := timelineSource{upto: since, potential: raw, hasPotential: true, newlyJoined: true}
			if len(raw) > 0 {
				src.upto = now.WithRoomKey(streamtoken.Live(raw[0].StreamOrdering - 1))
			}
			entry, err := syncRoomEntry(ctx, d, room, verdict.UserID, now.Room, timeNow, cfg,
				accountDataByRoom[room.RoomID], now, useStateAfter, f, verdict.DeviceID, false,
				// nil receipts: this path replaces the whole ephemeral block
				// below, bounding it by `since` rather than by the now token,
				// so anything built here would be discarded.
				src, sticky[room.RoomID], typingRooms[room.RoomID], nil, false)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "room entry", err)
			}
			// Ephemeral is bounded by `since` even for a newly joined room:
			// receipts and typing are streams like any other, and the client
			// is not entitled to a replay of them just for joining. Built by
			// the same helper as every other room, because Synapse does not
			// distinguish: dropping typing here left out an m.typing that
			// Synapse sends.
			ephemeral, err := sinceEphemeral(d, room.RoomID, verdict.UserID,
				receiptsByRoom[room.RoomID], typingRooms[room.RoomID])
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "ephemeral", err)
			}
			ephemeral = stripRoomIDs(filterEphemeral(f, room.RoomID, ephemeral))
			entry["ephemeral"] = map[string]any{"events": ephemeral}
			joinedRooms[room.RoomID] = entry
			continue
		}

		entry, err := incrementalRoomEntry(ctx, d, room, verdict.UserID, since, now,
			timeNow, cfg, timelines[room.RoomID],
			accountDataByRoom[room.RoomID], receiptsByRoom[room.RoomID],
			useStateAfter, f, verdict.DeviceID, sticky[room.RoomID],
			typingRooms[room.RoomID], gaps)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room entry", err)
		}
		if entry != nil {
			joinedRooms[room.RoomID] = entry
		}
	}

	// Membership transitions visible in what we just emitted decide who else
	// the client must re-key. Synapse derives them from the response it has
	// built rather than from a query, so the two cannot disagree.
	joinedOrInvited, leaveCandidates := userChangesFromRooms(joinedRooms)
	leftUsers, err := resolveLeftUsers(ctx, d, leaveCandidates)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "previous memberships", err)
	}

	ignored, err := d.Store.IgnoredUsers(ctx, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "ignored users", err)
	}

	invitedRooms := map[string]any{}
	lastChange := map[string]store.MembershipChange{}
	for _, c := range changes {
		lastChange[c.RoomID] = c
	}
	for roomID, c := range lastChange {
		if incrementalSection(c.Membership) != "invite" {
			continue
		}
		// An invite from an ignored user is not reported at all. The sender of
		// the membership event is the inviter, which is exactly what Synapse
		// checks against the ignore list.
		if ignored[c.Sender] {
			continue
		}
		invite, err := d.Store.InviteEvent(ctx, c.EventID, roomID, roomVersions[roomID])
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "invite event", err)
		}
		body, err := clientevent.Serialize(invite.Stored, timeNow, strippedCfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise invite", err)
		}
		events, err := strippedStateEvents(body, "invite_room_state")
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise invite", err)
		}
		invitedRooms[roomID] = map[string]any{
			"invite_state": map[string]any{"events": events},
		}
	}

	// Rooms the caller left or was banned from during the window, and rooms
	// they knocked on.
	archivedRooms := map[string]any{}
	knockedRooms := map[string]any{}
	for roomID, c := range lastChange {
		switch incrementalSection(c.Membership) {
		case "leave":
			if _, stillJoined := timelines[roomID]; stillJoined {
				continue
			}
			if isCurrentlyJoined(rooms, roomID) {
				continue
			}
			entry, err := archivedRoomEntry(ctx, d, roomID, verdict.UserID, verdict.DeviceID, c,
				since, now, timeNow, cfg, d.MSC3391Enabled, useStateAfter, f)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "archived room", err)
			}
			if entry != nil {
				archivedRooms[roomID] = entry
			}
		case "knock":
			info, err := d.Store.RoomInfo(ctx, roomID)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "room info", err)
			}
			knock, err := d.Store.InviteEvent(ctx, c.EventID, roomID, info.RoomVersion)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "knock event", err)
			}
			body, err := clientevent.Serialize(knock.Stored, timeNow, strippedCfg)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "serialise knock", err)
			}
			// The stripped state a knock carries lives in the event's unsigned
			// block; the response lifts it out into knock_state.
			events, err := strippedStateEvents(body, "knock_room_state")
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "serialise knock", err)
			}
			knockedRooms[roomID] = map[string]any{
				"knock_state": map[string]any{"events": events},
			}
		}
	}

	resp := map[string]any{"next_batch": now.String()}

	if len(toDevice) > 0 {
		resp["to_device"] = map[string]any{"events": toDevice}
	}

	if !f.BlocksAllGlobalAccountData() {
		globalAD, err := d.Store.GlobalAccountDataSince(ctx, verdict.UserID,
			since.AccountData, now.AccountData, d.MSC3391Enabled)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
		}
		events, err := accountDataEvents(globalAD)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
		}

		// m.push_rules is not in account_data and has its own stream, so a
		// rule change is invisible to the query above. Synapse adds the whole
		// ruleset whenever the user's rules moved in the window. The token
		// comparison spares the query on almost every pass: the stream is quiet.
		if since.PushRules < now.PushRules {
			changed, err := d.Store.PushRulesChangedSince(ctx, verdict.UserID, since.PushRules, now.PushRules)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "push rules", err)
			}
			if changed {
				rulesEvent, err := pushRulesAccountData(ctx, d, verdict.UserID)
				if err != nil {
					return nil, http.StatusInternalServerError, internalError(d, "push rules", err)
				}
				events = append([]json.RawMessage{rulesEvent}, events...)
			}
		}

		if len(events) > 0 {
			events = filterAccountDataEvents(f, "", events)
			if len(events) > 0 {
				resp["account_data"] = map[string]any{"events": events}
			}
		}
	}

	var presenceStates []store.PresenceState
	if !f.BlocksAllPresence() {
		presenceStates, err = d.Store.PresenceSince(ctx, verdict.UserID, since.Presence, now.Presence)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "presence", err)
		}
	}
	// Joining a room entitles the caller to the presence of everyone already in
	// it, whatever the presence stream has been doing. Without this, a client
	// that joins a busy room sees every member as offline until each of them
	// happens to change state.
	// Presence is owed for anyone who just became visible: every member of a
	// room the caller joined, and anyone who joined or was invited to a room
	// they were already in. Neither group need have touched the presence
	// stream, so neither is found by the window query above.
	extraUsers := append([]string(nil), joinedOrInvited...)
	if f.BlocksAllPresence() {
		extraUsers = nil
	}
	if len(newlyJoined) > 0 && !f.BlocksAllPresence() {
		members, err := d.Store.JoinedMembersOf(ctx, newlyJoined)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "newly joined members", err)
		}
		extraUsers = append(extraUsers, members...)
	}
	if len(extraUsers) > 0 {
		seenExtra := map[string]bool{}
		extra := make([]string, 0, len(extraUsers))
		for _, m := range extraUsers {
			// A remote user with a non-compliant ID is dropped (#20338).
			if m == verdict.UserID || seenExtra[m] || !filter.SyncableUserID(m, d.ServerName) {
				continue
			}
			seenExtra[m] = true
			extra = append(extra, m)
		}
		stored, err := d.Store.PresenceForUsers(ctx, extra)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "extra presence", err)
		}
		// A user with no presence_stream row still gets an entry: the presence
		// handler substitutes a default offline state rather than omitting
		// them. Omitting them loses a member from every room whose users have
		// never set presence.
		byUser := make(map[string]store.PresenceState, len(stored))
		for _, p := range stored {
			byUser[p.UserID] = p
		}
		states := make([]store.PresenceState, 0, len(extra))
		for _, u := range extra {
			if p, ok := byUser[u]; ok {
				states = append(states, p)
				continue
			}
			states = append(states, store.PresenceState{UserID: u, State: "offline"})
		}

		seen := map[string]bool{}
		merged := make([]store.PresenceState, 0, len(presenceStates)+len(states))
		for _, p := range append(presenceStates, states...) {
			if seen[p.UserID] {
				continue
			}
			seen[p.UserID] = true
			merged = append(merged, p)
		}
		presenceStates = merged
	}
	presenceStates = filterPresence(f, presenceStates)
	if len(presenceStates) > 0 {
		events, err := syncPresenceEvents(presenceStates, timeNow)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "presence", err)
		}
		resp["presence"] = map[string]any{"events": events}
	}

	// device_lists tells an end-to-end encrypted client whose keys to re-fetch.
	// Missing an entry here leaves it unable to decrypt, which is why joining a
	// room adds every member: the client has none of their keys yet.
	changed, err := d.Store.DeviceListChanges(ctx, verdict.UserID, joinedIDs,
		since.DeviceList.MaxStreamPos(), now.DeviceList.MaxStreamPos())
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "device lists", err)
	}
	if len(newlyJoined) > 0 {
		members, err := d.Store.JoinedMembersOf(ctx, newlyJoined)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "newly joined members", err)
		}
		changed = append(changed, members...)
	}
	changed = append(changed, joinedOrInvited...)

	var left []string
	if len(leftUsers) > 0 {
		sharing, err := d.Store.UsersSharingAnyRoom(ctx, verdict.UserID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "shared rooms", err)
		}
		for _, u := range leftUsers {
			// Still in another room with us, so not left from our point of
			// view: telling the client otherwise makes it drop keys it needs.
			if !sharing[u] && filter.SyncableUserID(u, d.ServerName) {
				left = append(left, u)
			}
		}
	}

	deviceLists := map[string]any{}
	if len(changed) > 0 {
		seen := map[string]bool{}
		list := make([]string, 0, len(changed))
		for _, u := range changed {
			// A remote user with a non-compliant ID is dropped (#20338).
			if seen[u] || !filter.SyncableUserID(u, d.ServerName) {
				continue
			}
			seen[u] = true
			list = append(list, u)
		}
		deviceLists["changed"] = list
	}
	if len(left) > 0 {
		deviceLists["left"] = left
	}
	if len(deviceLists) > 0 {
		resp["device_lists"] = deviceLists
	}

	keys, err := d.Store.DeviceKeyCounts(ctx, verdict.UserID, verdict.DeviceID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "device keys", err)
	}
	resp["device_one_time_keys_count"] = keys.OneTimeKeys
	resp["device_unused_fallback_key_types"] = keys.UnusedFallbackKeyTypes

	roomsOut := map[string]any{}
	if len(joinedRooms) > 0 {
		roomsOut["join"] = joinedRooms
	}
	if len(invitedRooms) > 0 {
		roomsOut["invite"] = invitedRooms
	}
	if len(knockedRooms) > 0 {
		roomsOut["knock"] = knockedRooms
	}
	if len(archivedRooms) > 0 {
		roomsOut["leave"] = archivedRooms
	}
	if len(roomsOut) > 0 {
		resp["rooms"] = roomsOut
	}

	body, err := json.Marshal(resp)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err)
	}
	return body, http.StatusOK, nil
}

// incrementalRoomEntry builds one joined room's delta, or nil if nothing
// happened in it.
func incrementalRoomEntry(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	since, now streamtoken.Token, timeNow int64, cfg clientevent.Config,
	raw []store.TimelineEvent, accountData []store.AccountDataEntry,
	receipts []store.ReceiptRow, useStateAfter bool,
	f *filter.Collection, deviceID string, stickyIDs []string,
	typingChanged bool, gaps *gapSet) (map[string]any, error) {

	// Where the loaded chunk begins, which is Synapse's per-room `upto_token`
	// and the prev_batch of an untrimmed timeline. For a room whose events all
	// fit in the window that is the `since` token itself -- the whole token,
	// not just its room key, because Synapse hands `since_token` straight
	// through for a room that had nothing in it.
	upto := since
	if len(raw) > 0 {
		upto = now.WithRoomKey(streamtoken.Live(raw[0].StreamOrdering - 1))
	}
	sinceKey := since.Room

	messages, memberships, prevBatch, limited, err := loadFilteredRecents(ctx, d, room, userID,
		now, upto, &sinceKey, raw, true, false, timeNow, f, gaps)
	if err != nil {
		return nil, err
	}

	adEvents, err := accountDataEvents(filterAccountDataEntries(f, room.RoomID, accountData))
	if err != nil {
		return nil, err
	}

	ephemeral := []json.RawMessage{}
	if !f.BlocksAllRoomEphemeral() {
		var err error
		if ephemeral, err = sinceEphemeral(d, room.RoomID, userID, receipts, typingChanged); err != nil {
			return nil, err
		}
		ephemeral = stripRoomIDs(filterEphemeral(f, room.RoomID, ephemeral))
	}

	// A room with nothing in it is left out of the response entirely.
	//
	// Deliberately BEFORE the state delta, and deliberately not counting it.
	// Synapse decides this in `_generate_room_entry` before calling
	// compute_state_delta, so a room whose only news is a state change outside
	// the timeline is dropped -- the state block cannot keep a room alive on
	// its own. Checking afterwards, and counting state, emits rooms Synapse
	// omits; that only shows once a filter can empty the timeline while
	// leaving a state delta behind.
	if len(messages) == 0 && len(adEvents) == 0 && len(ephemeral) == 0 && len(stickyIDs) == 0 {
		return nil, nil
	}

	// Lazy loading restricts the state block to the memberships this timeline
	// needs. Unlike a full-state sync, our own membership is NOT added: the
	// client already has it, and Synapse only forces it in on the full path.
	// The sticky section first: its senders' memberships are lazy-loaded
	// too (#20362).
	stickyBlock, stickySenders, err := stickySection(ctx, d, room, userID, stickyIDs, messages, timeNow, cfg)
	if err != nil {
		return nil, err
	}
	var stateMembers map[string]bool
	if f.LazyLoadMembers() {
		stateMembers = timelineMembers(messages, useStateAfter)
		for _, s := range stickySenders {
			stateMembers[s] = true
		}
	}

	// MSC4222 reports what current state BECAME over the window, taken straight
	// from the state delta stream, rather than the state a client must apply
	// before the timeline.
	var (
		stateIDs map[store.StateKey]string
		changed  map[store.StateKey]bool
	)
	if useStateAfter {
		stateIDs, changed, err = incrementalStateAfter(ctx, d, room.RoomID, userID, since,
			now.Room, messages, stateMembers, true)
		if err != nil {
			return nil, err
		}
		dropAliases(stateIDs)
	} else {
		stateIDs, changed, err = incrementalStateDelta(ctx, d, room, messages, limited, since, now,
			stateMembers)
		if err != nil {
			return nil, err
		}
	}
	dedupeLazyMembers(d, f, userID, deviceID, stateIDs, changed, messages, false)

	summary := map[string]any{}
	var summaryValue any = summary
	if wantSummary(f, messages, stateIDs, limited, false) {
		computed, err := roomSummary(ctx, d, room.RoomID, userID, deviceID, f, stateIDs,
			messages, now.Room)
		if err != nil {
			return nil, err
		}
		if computed == nil {
			summaryValue = nil
		} else {
			summaryValue = computed
		}
	}

	stateEventIDs := make([]string, 0, len(stateIDs))
	for _, id := range stateIDs {
		stateEventIDs = append(stateEventIDs, id)
	}
	stateEvents, err := d.Store.EventsByID(ctx, stateEventIDs, room.RoomVersion)
	if err != nil {
		return nil, err
	}
	stateEventIDs = filterStateBlock(f, room.RoomID, stateEventIDs, stateEvents)

	ids := timelineEventIDs(messages)
	ids = append(ids, stateEventIDs...)
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return nil, err
	}

	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return nil, err
	}

	var aggs map[string]eventfilter.Aggregation
	var nestedIDs []string
	var nested map[string]store.StateEvent
	if limited {
		vis, err := d.Store.VisibilityExtras(ctx, room.RoomID, userID, nil)
		if err != nil {
			return nil, err
		}
		aggs, nestedIDs, err = bundleAggregations(ctx, d, userID, messages, vis.IgnoredSenders)
		if err != nil {
			return nil, err
		}
		if len(aggs) > 0 {
			want := append([]string(nil), nestedIDs...)
			for _, a := range aggs {
				if a.ReplaceID() != "" {
					want = append(want, a.ReplaceID())
				}
			}
			nested, err = d.Store.EventsByID(ctx, want, room.RoomVersion)
			if err != nil {
				return nil, err
			}
		}
	}

	timeline := make([]json.RawMessage, 0, len(messages))
	for i, ev := range messages {
		stored := ev.Stored
		stored.Membership = memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, err
		}
		if agg, ok := aggs[ev.EventID]; ok {
			body, err = attachAggregations(body, agg, aggs, nested, timeNow, cfg)
			if err != nil {
				return nil, err
			}
		}
		timeline = append(timeline, body)
	}

	stateJSON := make([]json.RawMessage, 0, len(stateEventIDs))
	for _, id := range stateEventIDs {
		ev, ok := stateEvents[id]
		if !ok {
			continue
		}
		stored := ev.Stored
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, err
		}
		stateJSON = append(stateJSON, body)
	}

	unread, err := d.Store.UnreadNotifications(ctx, room.RoomID, userID)
	if err != nil {
		return nil, err
	}

	entry := map[string]any{
		"timeline": map[string]any{
			"events":     timeline,
			"prev_batch": prevBatch.String(),
			"limited":    limited,
		},
		stateKeyName(ctx, useStateAfter): map[string]any{"events": stateJSON},
		"account_data":                   map[string]any{"events": adEvents},
		"ephemeral":                      map[string]any{"events": ephemeral},
		"summary":                        summaryValue,
	}
	applyUnreadCounts(entry, unread, f, d.MSC3773Enabled)
	if stickyBlock != nil {
		entry["msc4354_sticky"] = stickyBlock
	}
	return entry, nil
}

// incrementalStateDelta works out the `state` block for an incremental sync.
//
// Synapse's _compute_state_delta_for_incremental_sync. The shortcut at the top
// carries most of the traffic: when the timeline is unlimited AND its events
// form an unbroken chain from where the client left off, the `state` block is
// EMPTY -- everything that changed is in the timeline, and repeating it would
// be waste. Only a gap, or a fork in the DAG, makes a state block necessary.
//
// changed is the keys whose state differs from the state at the previous
// sync: the lazy-loaded members cache must not hold those back (#20289). The
// other keys are memberships of timeline senders, which the client may have.
func incrementalStateDelta(ctx context.Context, d Deps, room store.RoomForUser,
	messages []store.TimelineEvent, limited bool,
	since, now streamtoken.Token, members map[string]bool) (map[store.StateKey]string, map[store.StateKey]bool, error) {

	if !limited {
		// An empty timeline counts as linear, as in Synapse.
		linear := true
		if len(messages) > 0 {
			var err error
			if linear, err = isLinearTimeline(ctx, d, room.RoomID, messages, since.Room); err != nil {
				return nil, nil, err
			}
		}
		if linear {
			// Under lazy loading the state block is not empty even here: the
			// client may never have been sent the memberships of the senders in
			// this timeline, so those alone are returned. The caller dedupes
			// the ones it has already sent. Nothing else changed.
			if len(members) == 0 {
				return nil, nil, nil
			}
			// Only the members are kept, so only the members are fetched: at
			// the timeline's start, or with no timeline (the members are a
			// sticky section's senders) at its end.
			var state map[store.StateKey]string
			var err error
			if len(messages) > 0 {
				state, err = stateAtEvent(ctx, d, messages[0].EventID, members, memberList(members))
			} else {
				state, err = stateIDsAtFor(ctx, d, room.RoomID, now.Room, members, memberList(members))
			}
			if err != nil {
				return nil, nil, err
			}
			return keepOnlyMembers(state, members), nil, nil
		}
	}

	// Everything below asks the state store for only what a lazy-loading
	// client keeps, when there is such a restriction. Fetching a large room's
	// entire state map to discard all but a handful of members is the same
	// answer at a wholly different cost -- see LazyStateForGroup.
	wanted := memberList(members)

	var start map[store.StateKey]string
	var err error
	if len(messages) > 0 {
		start, err = stateAtEvent(ctx, d, messages[0].EventID, members, wanted)
		if err != nil {
			return nil, nil, err
		}
	}
	if start == nil {
		start, err = stateIDsAtFor(ctx, d, room.RoomID, now.Room, members, wanted)
		if err != nil {
			return nil, nil, err
		}
	}
	// The timeline start keeps the lazy-load restriction even on a gappy sync.
	// That looks backwards and is deliberate: _calculate_state works out which
	// members to send by subtracting the restricted timeline_start from what
	// the client already had, so restricting it is what lets the unrestricted
	// state at either end supply the rest.
	restrictToMembers(start, members)

	// A gappy sync turns lazy loading OFF for the two full state fetches. The
	// client has missed events it will never be shown, so it cannot rebuild
	// state from the timeline and needs the lot.
	ends := members
	if limited {
		ends = nil
	}

	endWanted := memberList(ends)
	previous, err := stateIDsAtFor(ctx, d, room.RoomID, since.Room, ends, endWanted)
	if err != nil {
		return nil, nil, err
	}
	restrictToMembers(previous, ends)
	end, err := stateIDsAtFor(ctx, d, room.RoomID, now.Room, ends, endWanted)
	if err != nil {
		return nil, nil, err
	}
	restrictToMembers(end, ends)

	state := calculateState(start, end, previous, messages, members != nil)
	changed := map[store.StateKey]bool{}
	for k, id := range state {
		if previous[k] != id {
			changed[k] = true
		}
	}
	return state, changed, nil
}

// stateAtEvent resolves the state at (strictly, just after) one event,
// restricted to what a lazy-loading client keeps when there is a restriction.
func stateAtEvent(ctx context.Context, d Deps, eventID string,
	members map[string]bool, wanted []string) (map[store.StateKey]string, error) {

	groups, err := d.Store.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return nil, err
	}
	group, ok := groups[eventID]
	if !ok {
		return nil, nil
	}
	if members != nil {
		return d.Store.LazyStateForGroup(ctx, group, wanted)
	}
	return d.Store.FullStateForGroup(ctx, group)
}

// stateIDsAtFor is StateIDsAt, lazy when the caller is lazy-loading.
func stateIDsAtFor(ctx context.Context, d Deps, roomID string, key streamtoken.RoomKey,
	members map[string]bool, wanted []string) (map[store.StateKey]string, error) {

	if members != nil {
		return d.Store.LazyStateIDsAt(ctx, roomID, key, wanted)
	}
	return d.Store.StateIDsAt(ctx, roomID, key)
}

// isLinearTimeline reports whether the timeline is an unbroken chain from the
// last event the client saw.
//
// Each event must name exactly its predecessor as its only prev_event. Anything
// else -- a fork, a merge, a gap -- means the client cannot reconstruct the
// room from the timeline alone and needs a state block.
func isLinearTimeline(ctx context.Context, d Deps, roomID string,
	messages []store.TimelineEvent, sinceKey streamtoken.RoomKey) (bool, error) {

	prev, err := d.Store.LastEventBefore(ctx, roomID, sinceKey)
	if err != nil {
		return false, err
	}
	for _, ev := range messages {
		prevEvents := gjson.GetBytes(ev.JSON, "prev_events").Array()
		if len(prevEvents) != 1 || prevEventID(prevEvents[0]) != prev {
			return false, nil
		}
		prev = ev.EventID
	}
	return true, nil
}

// prevEventID reads an entry of `prev_events`, which has two shapes.
//
// Room versions 1 and 2 store each entry as a `[event_id, hashes]` PAIR;
// everything since stores a bare event id. Reading the pair as a string yields
// the whole array, so a v1 room never looks linear and always gets a state
// block Synapse would have omitted. Nine of the 1,165 rooms on this server are
// v1 or v2, and one of them is in the test corpus.
func prevEventID(entry gjson.Result) string {
	if entry.IsArray() {
		if pair := entry.Array(); len(pair) > 0 {
			return pair[0].String()
		}
		return ""
	}
	return entry.String()
}

// calculateState is Synapse's _calculate_state.
//
//	(timeline_end | timeline_start) - previous_timeline_end - timeline_contains
//
// `previous_timeline_end` is what makes an incremental sync incremental: state
// the client already had is subtracted, so only what changed is sent.
func calculateState(start, end, previous map[store.StateKey]string,
	messages []store.TimelineEvent, lazyLoadMembers bool) map[store.StateKey]string {

	// The last state event per key in the timeline, not every one: an earlier
	// change to the same key still belongs in the state block.
	timelineContains := map[store.StateKey]string{}
	for _, ev := range messages {
		if ev.IsState {
			timelineContains[store.StateKey{Type: ev.Type, StateKey: ev.StateKey}] = ev.EventID
		}
	}
	inTimeline := map[string]bool{}
	for _, id := range timelineContains {
		inTimeline[id] = true
	}
	alreadySent := map[string]bool{}
	for _, id := range previous {
		alreadySent[id] = true
	}
	// Under lazy loading, a membership present at the timeline start is sent
	// even if the client already had it at the end of the previous sync. It has
	// to be: `start` was restricted to the timeline's senders, and the client
	// may never have been sent those memberships at all -- "already had it"
	// here means "the state said so", not "we told them".
	//
	// Only this set is relaxed. An event carried by the timeline itself is
	// still subtracted, lazy loading or not.
	if lazyLoadMembers {
		for k, id := range start {
			if k.Type == memberType {
				delete(alreadySent, id)
			}
		}
	}

	out := map[store.StateKey]string{}
	for _, m := range []map[store.StateKey]string{start, end} {
		for k, id := range m {
			if inTimeline[id] || alreadySent[id] || k.Type == "m.room.aliases" {
				continue
			}
			out[k] = id
		}
	}
	return out
}

// incrementalSection names the response section the caller's own membership
// change belongs in on an INCREMENTAL sync: "invite", "knock", "leave", or ""
// for a membership the joined path reports instead.
//
// A leave or a ban always lands in "leave" -- whoever sent it, and whatever the
// filter says. It is tempting to skip a self-leave when the filter does not ask
// for `include_leave`, because that is what Synapse does; but Synapse does it
// ONLY in _get_room_changes_for_initial_sync. The incremental path
// (_get_room_changes_for_incremental_sync) is explicit: "Always include
// leave/ban events. Just take the last one."
//
// We had the check here for a week, and the symptom is not obviously a sync
// bug: the client is never told it left, so the room stays in its room list,
// and an invite back into the same room does not render as an invite, because
// to the client it never left. Everybody else sees the leave normally, which
// points the finger anywhere but here.
func incrementalSection(membership string) string {
	switch membership {
	case "invite":
		return "invite"
	case "knock":
		return "knock"
	case "leave", "ban":
		return "leave"
	}
	return ""
}

// userChangesFromRooms works out who joined and who left, from the membership
// events in the response we are about to send.
//
// Synapse derives these from its own built response rather than from a query
// (SyncResultBuilder.calculate_user_changes), so the device-list section cannot
// disagree with the rooms section.
//
// A leave counts only when the previous membership was `join`: a leave that
// follows an invite is a declined invite, and the user was never visible to us
// in the first place.
func userChangesFromRooms(joined map[string]any) (joinedOrInvited []string, leaveCandidates map[string]string) {
	joinedSet := map[string]bool{}
	leaveCandidates = map[string]string{}

	consider := func(raw json.RawMessage, inTimeline bool) {
		if gjson.GetBytes(raw, "type").String() != "m.room.member" {
			return
		}
		user := gjson.GetBytes(raw, "state_key").String()
		if user == "" {
			return
		}
		switch gjson.GetBytes(raw, "content.membership").String() {
		case "join", "invite", "knock":
			joinedSet[user] = true
		default:
			// Whether this counts as "left" depends on the PREVIOUS
			// membership, which is not in our output: `replaces_state` names
			// the event to look it up from.
			// Only from the timeline. Synapse decides this by reading
			// `unsigned.prev_content` off its in-memory event, which
			// events_worker populates for the timeline but leaves on STATE
			// events only when some earlier reader happened to ask for it and
			// polluted the shared cache. Deriving `left` from the state block
			// therefore reproduces a cache artefact, not a rule.
			if !inTimeline {
				return
			}
			if replaces := gjson.GetBytes(raw, "unsigned.replaces_state").String(); replaces != "" {
				leaveCandidates[replaces] = user
				delete(joinedSet, user)
			}
		}
	}

	for _, entry := range joined {
		room, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		// Timeline only for the leave side; see the note on leaveCandidates.
		// Every spelling of the state block: MSC4222 renames it (stable and
		// unstable), and the membership scan must follow it or the
		// device-list and presence sections silently lose everyone who
		// joined.
		for _, section := range []string{"state", "state_after", "org.matrix.msc4222.state_after", "timeline"} {
			block, ok := room[section].(map[string]any)
			if !ok {
				continue
			}
			events, ok := block["events"].([]json.RawMessage)
			if !ok {
				continue
			}
			for _, ev := range events {
				consider(ev, section == "timeline")
			}
		}
	}

	for u := range joinedSet {
		joinedOrInvited = append(joinedOrInvited, u)
	}
	// A user who also appears as joined elsewhere in the response has not left.
	for replaces, user := range leaveCandidates {
		if joinedSet[user] {
			delete(leaveCandidates, replaces)
		}
	}
	return joinedOrInvited, leaveCandidates
}

// resolveLeftUsers keeps the leave events whose previous membership was `join`.
//
// A leave that follows an invite is a declined invite: the user was never
// visible to us, so they have not "left" anything.
func resolveLeftUsers(ctx context.Context, d Deps, candidates map[string]string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	previous, err := d.Store.MembershipOfEvents(ctx, ids)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for replaces, user := range candidates {
		if previous[replaces] != "join" || seen[user] {
			continue
		}
		seen[user] = true
		out = append(out, user)
	}
	return out, nil
}

func isCurrentlyJoined(rooms []store.RoomForUser, roomID string) bool {
	for _, r := range rooms {
		if r.RoomID == roomID && r.Membership == "join" {
			return true
		}
	}
	return false
}

// archivedRoomEntry builds the section for a room the caller left during the
// window.
//
// Everything is bounded by the LEAVE, not by `now`: a departed member is not
// entitled to what happened after they went. Synapse expresses that by setting
// both upto_token and end_token to the leave position, and the state delta is
// computed against that rather than the current state.
//
// The section itself is smaller than a joined room's: no ephemeral, no unread
// counts, no summary. Those describe a room you are in.
func archivedRoomEntry(ctx context.Context, d Deps, roomID, userID, deviceID string,
	change store.MembershipChange, since, now streamtoken.Token, timeNow int64,
	cfg clientevent.Config, msc3391, useStateAfter bool,
	f *filter.Collection) (map[string]any, error) {

	info, err := d.Store.RoomInfo(ctx, roomID)
	if err != nil {
		return nil, err
	}
	leaveKey := streamtoken.Live(change.StreamOrdering)
	room := store.RoomForUser{RoomID: roomID, RoomVersion: info.RoomVersion}
	endToken := now.WithRoomKey(leaveKey)

	var (
		raw          []store.TimelineEvent
		hasPotential bool
	)
	if change.OutOfBand {
		// An out-of-band membership -- a federated invite or its rejection --
		// arrived without the room's state, so there is nothing to paginate.
		// The membership event is the whole response.
		events, err := d.Store.EventsByID(ctx, []string{change.EventID}, info.RoomVersion)
		if err != nil {
			return nil, err
		}
		if ev, ok := events[change.EventID]; ok {
			raw = []store.TimelineEvent{{
				Stored: ev.Stored, Sender: ev.Sender, StateKey: ev.StateKey,
				IsState: true, StreamOrdering: change.StreamOrdering,
			}}
		}
		hasPotential = true
	}

	// Nothing is pre-loaded for an ordinary leave: Synapse passes
	// `events=None` and lets _load_filtered_recents walk back from the leave
	// position to the client's `since`. An out-of-band membership is the
	// exception -- it arrived without the room's state, so the membership event
	// is the whole timeline and there is nothing to paginate.
	sinceKey := since.Room
	messages, memberships, prevBatch, limited, err := loadFilteredRecents(ctx, d, room, userID,
		now, endToken, &sinceKey, raw, hasPotential, false, timeNow, f, nil)
	if err != nil {
		return nil, err
	}

	var stateMembers map[string]bool
	if f.LazyLoadMembers() {
		stateMembers = timelineMembers(messages, useStateAfter)
	}
	var (
		stateIDs map[store.StateKey]string
		changed  map[store.StateKey]bool
	)
	if !change.OutOfBand {
		if useStateAfter {
			stateIDs, changed, err = incrementalStateAfter(ctx, d, roomID, userID, since,
				leaveKey, messages, stateMembers, false)
			dropAliases(stateIDs)
		} else {
			stateIDs, changed, err = incrementalStateDelta(ctx, d, room, messages, limited, since,
				endToken, stateMembers)
		}
		if err != nil {
			return nil, err
		}
		// compute_state_delta dedupes lazy-loaded members for a left room
		// as for a joined one.
		dedupeLazyMembers(d, f, userID, deviceID, stateIDs, changed, messages, false)
	}

	accountData, err := d.Store.RoomAccountDataSince(ctx, userID,
		since.AccountData, now.AccountData, msc3391)
	if err != nil {
		return nil, err
	}
	adEvents, err := accountDataEvents(filterAccountDataEntries(f, roomID, accountData[roomID]))
	if err != nil {
		return nil, err
	}

	// As in incrementalRoomEntry: the state block does not keep a room in the
	// response, because Synapse decides this before it is computed.
	if len(messages) == 0 && len(adEvents) == 0 {
		return nil, nil
	}

	stateEventIDs := make([]string, 0, len(stateIDs))
	for _, id := range stateIDs {
		stateEventIDs = append(stateEventIDs, id)
	}
	stateEvents, err := d.Store.EventsByID(ctx, stateEventIDs, info.RoomVersion)
	if err != nil {
		return nil, err
	}
	stateEventIDs = filterStateBlock(f, roomID, stateEventIDs, stateEvents)
	redactions, err := d.Store.Redactions(ctx, append(timelineEventIDs(messages), stateEventIDs...))
	if err != nil {
		return nil, err
	}
	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return nil, err
	}

	timeline := make([]json.RawMessage, 0, len(messages))
	for i, ev := range messages {
		stored := ev.Stored
		stored.Membership = memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, err
		}
		timeline = append(timeline, body)
	}
	stateJSON := make([]json.RawMessage, 0, len(stateEventIDs))
	for _, id := range stateEventIDs {
		ev, ok := stateEvents[id]
		if !ok {
			continue
		}
		stored := ev.Stored
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, err
		}
		stateJSON = append(stateJSON, body)
	}

	return map[string]any{
		"timeline": map[string]any{
			"events": timeline, "prev_batch": prevBatch.String(), "limited": limited,
		},
		stateKeyName(ctx, useStateAfter): map[string]any{"events": stateJSON},
		"account_data":                   map[string]any{"events": adEvents},
	}, nil
}

// sinceEphemeral builds a room's ephemeral events for an incremental sync:
// typing, if this room's typists changed since the client's token, then the
// receipts in the window.
//
// In that order because ephemeral_by_room appends typing first. syncdiff keys
// ephemeral events by type and so cannot see the order; a raw diff can.
func sinceEphemeral(d Deps, roomID, userID string, receipts []store.ReceiptRow,
	typingChanged bool) ([]json.RawMessage, error) {

	ephemeral := []json.RawMessage{}
	// Only when this room's typists have actually changed since the client
	// last looked. See the note where typingRooms is built.
	if typingChanged {
		if ev, err := typingEvent(d, roomID); err != nil {
			return nil, err
		} else if ev != nil {
			ephemeral = append(ephemeral, ev)
		}
	}
	if len(receipts) > 0 {
		ev, err := receiptEvent(roomID, receipts, userID, true)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			ephemeral = append(ephemeral, ev)
		}
	}
	return ephemeral, nil
}
