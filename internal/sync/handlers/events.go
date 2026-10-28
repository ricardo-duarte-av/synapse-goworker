package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// defaultEventsTimeout is Synapse's EventStreamRestServlet.DEFAULT_LONGPOLL_TIME_MS.
//
// Note it is a DEFAULT, not a cap: /events long-polls even when the client says
// nothing, which is the opposite of /sync, where a missing timeout means
// "answer immediately".
const defaultEventsTimeout = 30 * time.Second

// minEventsTimeout is the floor Synapse applies to any non-zero timeout, so a
// client asking to wait 1ms cannot turn a long poll into a busy loop.
const minEventsTimeout = 500 * time.Millisecond

// defaultEventsLimit is PaginationConfig's default_limit for this endpoint.
const defaultEventsLimit = 10

// Events serves /events, the pre-sync event stream.
//
// It predates /sync and is deprecated, and this deployment sees exactly zero
// requests for it -- 558,398 for /sync in the same 21 hours. It is served
// anyway because it is the last endpoint in this worker's scope, and because
// it is the only one that exercises the notifier without the whole sync
// machinery in front of it.
//
// Mirrors EventStreamRestServlet -> EventStreamHandler.get_stream ->
// Notifier.get_events_for. The shape is much simpler than /sync's: one flat
// chunk of events and EDUs, in source order, between two tokens.
func Events(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		if ann != nil {
			ann.Endpoint = "events"
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		body, status, mxErr := eventStream(r, d, verdict, ann)
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func eventStream(r *http.Request, d Deps, verdict clientauth.Verdict, ann *server.Annotation) (
	[]byte, int, *matrixerr.Error) {

	ctx := r.Context()
	roomID := r.URL.Query().Get("room_id")

	// A guest may peek at one room but never at the whole account: without a
	// room there is no way to decide what they are entitled to see.
	if verdict.IsGuest && roomID == "" {
		return nil, http.StatusBadRequest, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown, Error: "Guest users must specify room_id param"}
	}

	limit := intParam(r, "limit", defaultEventsLimit)
	timeout := defaultEventsTimeout
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		ms := intParam(r, "timeout", 0)
		timeout = time.Duration(ms) * time.Millisecond
		if timeout > 0 && timeout < minEventsTimeout {
			timeout = minEventsTimeout
		}
	}
	// `raw` is a bare flag: its presence, not its value, asks for the
	// unmodified event rather than the client format.
	format := clientevent.FormatV1
	if r.URL.Query().Has("raw") {
		format = clientevent.FormatRaw
	}

	if roomID != "" {
		info, err := d.Store.RoomInfo(ctx, roomID)
		if errors.Is(err, store.ErrNotFound) {
			return nil, http.StatusNotFound, &matrixerr.Error{
				ErrCode: matrixerr.CodeNotFound, Error: "Room not found"}
		}
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room info", err)
		}
		// Checked before anything else, as Synapse does.
		if info.Blocked {
			return nil, http.StatusForbidden, &matrixerr.Error{
				ErrCode: matrixerr.CodeForbidden,
				Error:   "This room has been blocked on this server"}
		}
	}

	now, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	timeNow, mxErr := nowMillis(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}

	// No `from` means "start from here", which returns nothing and hands the
	// client a token to come back with.
	from := now
	if raw := r.URL.Query().Get("from"); raw != "" {
		parsed, err := streamtoken.Parse(raw)
		if err != nil {
			return nil, http.StatusBadRequest, &matrixerr.Error{
				ErrCode: matrixerr.CodeInvalidParam, Error: err.Error()}
		}
		from = parsed
		if ann != nil {
			ann.Since = raw
		}
	}
	now = now.AtLeast(from)

	roomIDs, isPeeking, mxErr := eventStreamRooms(ctx, d, verdict.UserID, roomID)
	if mxErr != nil {
		return nil, http.StatusForbidden, mxErr
	}

	cfg := clientevent.Config{
		Format: format,
		Requester: clientevent.Requester{
			UserID: verdict.UserID, DeviceID: verdict.DeviceID, IsGuest: verdict.IsGuest,
		},
		MSC4354Enabled: d.MSC4354Enabled,
	}
	// A server admin who asked to see soft-failed events is told which they
	// are, matching Synapse's include_admin_metadata. The visibility filter
	// lets them through; without this they arrive indistinguishable from
	// ordinary events.
	if wants, err := d.Store.AdminWantsSoftFailedEvents(ctx, verdict.UserID); err == nil {
		cfg.IncludeAdminMetadata = wants
	}
	cfg.Requester.TokenID = verdict.TokenID

	req := eventStreamRequest{
		userID: verdict.UserID, roomIDs: roomIDs, explicitRoom: roomID,
		isPeeking: isPeeking, limit: limit, timeNow: timeNow, cfg: cfg,
	}

	chunk, end, err := eventStreamChunk(ctx, d, req, from, now)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "event stream", err)
	}

	if len(chunk) == 0 && timeout > 0 && d.Notifier != nil {
		chunk, end, err = waitForEvents(r, d, req, from, timeout, ann)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "event stream", err)
		}
	}

	if ann != nil {
		ann.NextBatch = end.String()
	}
	// An empty chunk is `[]`, never `null`. A client that polls /events sees
	// this far more often than it sees anything else -- most requests time out
	// with nothing to report -- and a null would be the single most common
	// response this endpoint gives.
	if chunk == nil {
		chunk = []json.RawMessage{}
	}
	body, err := json.Marshal(map[string]any{
		"chunk": chunk,
		"start": from.String(),
		"end":   end.String(),
	})
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err)
	}
	return body, http.StatusOK, nil
}

// eventStreamRequest is everything the chunk builder needs that does not change
// between passes round the long poll.
type eventStreamRequest struct {
	userID       string
	roomIDs      []string
	explicitRoom string
	isPeeking    bool
	limit        int
	timeNow      int64
	cfg          clientevent.Config
}

// eventStreamRooms works out which rooms to stream, and whether the caller is
// peeking rather than a member.
//
// Mirrors Notifier._get_room_ids. A named room the caller has not joined is
// allowed only if the room is world-readable, and then only that room.
func eventStreamRooms(ctx context.Context, d Deps, userID, explicitRoom string) (
	[]string, bool, *matrixerr.Error) {

	joined, err := d.Store.RoomsForUser(ctx, userID, []string{"join"})
	if err != nil {
		return nil, false, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown, Error: "could not list rooms"}
	}
	ids := make([]string, 0, len(joined))
	for _, room := range joined {
		ids = append(ids, room.RoomID)
	}
	if explicitRoom == "" {
		return ids, false, nil
	}
	for _, id := range ids {
		if id == explicitRoom {
			return []string{explicitRoom}, false, nil
		}
	}
	vis, err := d.Store.HistoryVisibility(ctx, explicitRoom)
	if err != nil {
		return nil, false, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown, Error: "could not read history visibility"}
	}
	if vis != "world_readable" {
		return nil, false, &matrixerr.Error{
			ErrCode: matrixerr.CodeForbidden, Error: "Non-joined access not allowed"}
	}
	return []string{explicitRoom}, true, nil
}

// waitForEvents is the long poll: register interest, then look again.
//
// The order is the same as /sync's and for the same reason -- interest is
// registered BEFORE the answer is computed, so an event landing in the gap
// still wakes us.
func waitForEvents(r *http.Request, d Deps, req eventStreamRequest, from streamtoken.Token,
	timeout time.Duration, ann *server.Annotation) ([]json.RawMessage, streamtoken.Token, error) {

	ctx := r.Context()
	deadline := time.Now().Add(timeout)
	started := time.Now()

	metrics.SyncWaiters.Inc()
	defer metrics.SyncWaiters.Dec()
	defer func() {
		if ann != nil {
			ann.Waited = time.Since(started)
		}
	}()

	for {
		handle := d.Notifier.Register(req.roomIDs, []string{req.userID})

		now, mxErr := nowToken(r, d)
		if mxErr != nil {
			handle.Close()
			return nil, from, nil
		}
		now = now.AtLeast(from)
		chunk, end, err := eventStreamChunk(ctx, d, req, from, now)
		if err != nil || len(chunk) > 0 {
			handle.Close()
			return chunk, end, err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			handle.Close()
			return nil, end, nil
		}
		woken := handle.Wait(ctx, remaining)
		handle.Close()
		if !woken || ctx.Err() != nil {
			return nil, end, nil
		}
	}
}

// eventStreamChunk builds the flat chunk of everything that happened in
// (from, now], and the token the client should come back with.
//
// A port of Notifier.get_events_for's check_for_updates. Two things about the
// end token are easy to get wrong and both are deliberate here:
//
//   - It starts as the CLIENT'S token, not the current one, and only the
//     streams that actually produced something are advanced. A quiet stream
//     stays where the client left it, so nothing is ever skipped over.
//   - The room stream advances to the last event RETURNED, not to the current
//     position, because the limit may have cut the window short.
//
// The order of the chunk is the order of Synapse's source list -- room,
// presence, typing, receipts, account data -- and then the presence added for
// people who just joined. It is a list, so the order is part of the answer.
func eventStreamChunk(ctx context.Context, d Deps, req eventStreamRequest,
	from, now streamtoken.Token) ([]json.RawMessage, streamtoken.Token, error) {

	end := from
	var chunk []json.RawMessage

	roomEvents, roomKey, err := eventStreamRoomEvents(ctx, d, req, from, now)
	if err != nil {
		return nil, end, err
	}
	if from.Room.MaxStreamPos() != now.Room.MaxStreamPos() {
		end.Room = roomKey
	}

	serialised, joins, err := serialiseStreamEvents(ctx, d, req, roomEvents)
	if err != nil {
		return nil, end, err
	}
	chunk = append(chunk, serialised...)

	if from.Presence != now.Presence {
		states, err := d.Store.PresenceSince(ctx, req.userID, from.Presence, now.Presence)
		if err != nil {
			return nil, end, err
		}
		events, err := presenceEvents(states, req.timeNow)
		if err != nil {
			return nil, end, err
		}
		chunk = append(chunk, events...)
		end.Presence = now.Presence
	}

	if from.Typing != now.Typing && d.Replication != nil {
		inScope := map[string]bool{}
		for _, id := range req.roomIDs {
			inScope[id] = true
		}
		for _, id := range d.Replication.TypingChangedSince(from.Typing) {
			if !inScope[id] {
				continue
			}
			ev, err := typingEventFor(d, id)
			if err != nil {
				return nil, end, err
			}
			chunk = append(chunk, ev)
		}
		end.Typing = now.Typing
	}

	if from.Receipt.MaxStreamPos() != now.Receipt.MaxStreamPos() {
		byRoom, err := d.Store.ReceiptsSince(ctx, req.roomIDs,
			from.Receipt.MaxStreamPos(), now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, end, err
		}
		for _, roomID := range sortedRoomIDs(byRoom) {
			ev, err := receiptEvent(roomID, byRoom[roomID], req.userID, true)
			if err != nil {
				return nil, end, err
			}
			if ev != nil {
				chunk = append(chunk, ev)
			}
		}
		end.Receipt = now.Receipt
	}

	if from.AccountData != now.AccountData {
		events, err := eventStreamAccountData(ctx, d, req.userID, from.AccountData, now.AccountData)
		if err != nil {
			return nil, end, err
		}
		chunk = append(chunk, events...)
		end.AccountData = now.AccountData
	}

	// Presence for people the caller can now see. Joining a room means every
	// member is new to them; someone else joining means just that person.
	// Without this a client shows a room full of members with no presence at
	// all until each of them next changes state.
	if len(joins) > 0 {
		extra, err := joinPresence(ctx, d, req, joins)
		if err != nil {
			return nil, end, err
		}
		chunk = append(chunk, extra...)
	}

	return chunk, end, nil
}

// eventStreamRoomEvents loads the room half of the stream: everything in the
// caller's rooms, plus their own membership events wherever those landed.
//
// The membership query is not redundant. An invite or a leave happens in a room
// the caller is not joined to, so a query over the joined set cannot see it,
// and without it /events can never tell a client it has been invited anywhere.
func eventStreamRoomEvents(ctx context.Context, d Deps, req eventStreamRequest,
	from, now streamtoken.Token) ([]store.TimelineEvent, streamtoken.RoomKey, error) {

	fromPos, nowPos := from.Room.MaxStreamPos(), now.Room.MaxStreamPos()
	if fromPos == nowPos {
		return nil, now.Room, nil
	}

	roomVersions := map[string]string{}
	rooms, err := d.Store.RoomsForUser(ctx, req.userID, []string{"join"})
	if err != nil {
		return nil, now.Room, err
	}
	for _, room := range rooms {
		roomVersions[room.RoomID] = room.RoomVersion
	}

	events, err := d.Store.RoomEventsForward(ctx, req.roomIDs, roomVersions,
		fromPos, nowPos, req.limit)
	if err != nil {
		return nil, now.Room, err
	}

	// Peeking is scoped to one room by definition, and a peeker's own
	// memberships are not part of what they asked for.
	if !req.isPeeking {
		changes, err := d.Store.MembershipEventsForUser(ctx, req.userID, fromPos, nowPos)
		if err != nil {
			return nil, now.Room, err
		}
		events = append(events, changes...)
	}

	// One flat stream, in the order the server received things, deduplicated:
	// a membership event in a joined room comes back from both queries.
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].StreamOrdering < events[j].StreamOrdering
	})
	seen := make(map[string]bool, len(events))
	unique := events[:0]
	for _, ev := range events {
		if seen[ev.EventID] {
			continue
		}
		seen[ev.EventID] = true
		unique = append(unique, ev)
	}
	events = unique

	if req.limit > 0 && len(events) > req.limit {
		events = events[:req.limit]
	}
	if len(events) == 0 {
		return nil, now.Room, nil
	}
	// The client resumes AT the last event returned, not after it: this token
	// is where the next request starts from, exclusively.
	return events, streamtoken.Live(events[len(events)-1].StreamOrdering), nil
}

// serialiseStreamEvents renders the room events, and reports which rooms the
// caller joined inside the window.
func serialiseStreamEvents(ctx context.Context, d Deps, req eventStreamRequest,
	events []store.TimelineEvent) ([]json.RawMessage, []joinNotice, error) {

	if len(events) == 0 {
		return nil, nil, nil
	}

	// Visibility is decided per room, so the events are grouped rather than
	// filtered in one pass -- the history visibility of one room says nothing
	// about another.
	byRoom := map[string][]store.TimelineEvent{}
	order := []string{}
	for _, ev := range events {
		if _, ok := byRoom[ev.RoomID]; !ok {
			order = append(order, ev.RoomID)
		}
		byRoom[ev.RoomID] = append(byRoom[ev.RoomID], ev)
	}

	visible := make([]store.TimelineEvent, 0, len(events))
	memberships := map[string]string{}
	for _, roomID := range order {
		kept, ms, err := filterVisible(ctx, d, roomID, req.userID, byRoom[roomID],
			req.isPeeking, req.timeNow)
		if err != nil {
			return nil, nil, err
		}
		for i, ev := range kept {
			memberships[ev.EventID] = ms[i]
		}
		visible = append(visible, kept...)
	}
	sort.SliceStable(visible, func(i, j int) bool {
		return visible[i].StreamOrdering < visible[j].StreamOrdering
	})

	ids := make([]string, 0, len(visible))
	for _, ev := range visible {
		ids = append(ids, ev.EventID)
	}
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return nil, nil, err
	}
	prevTargets := make([]*clientevent.Stored, 0, len(visible))
	for i := range visible {
		prevTargets = append(prevTargets, &visible[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return nil, nil, err
	}

	var joins []joinNotice
	out := make([]json.RawMessage, 0, len(visible))
	for _, ev := range visible {
		stored := ev.Stored
		stored.Membership = memberships[ev.EventID]
		if err := attachRedaction(&stored, redactions, req.timeNow, req.cfg); err != nil {
			return nil, nil, err
		}
		body, err := clientevent.Serialize(stored, req.timeNow, req.cfg)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, body)

		if ev.Type == memberType &&
			gjson.GetBytes(ev.JSON, "content.membership").String() == "join" {
			joins = append(joins, joinNotice{
				roomID: ev.RoomID,
				userID: ev.StateKey,
				self:   ev.StateKey == req.userID,
			})
		}
	}
	return out, joins, nil
}

// joinNotice is a join the caller can see, and whose presence it should be
// told about.
type joinNotice struct {
	roomID string
	userID string
	// self distinguishes the caller joining -- which makes every member of
	// that room new to them -- from somebody else joining a room they are
	// already in, where only the joiner is news.
	self bool
}

// joinPresence renders presence for people who became visible to the caller.
//
// Once per join event, not once per user: Synapse loops over the events and
// extends the chunk each time, so three joins by the same person produce that
// person's presence three times. It looks like a bug and is not -- or at least,
// it is Synapse's and reproducing it is the contract.
//
// A user with no presence row still gets an entry. Synapse's get_states
// substitutes a default offline state rather than omitting them, and omitting
// them would lose every member who has never set presence.
func joinPresence(ctx context.Context, d Deps, req eventStreamRequest,
	joins []joinNotice) ([]json.RawMessage, error) {

	// Resolve who each join is about first, then read all their presence in
	// one query rather than one per join.
	perJoin := make([][]string, len(joins))
	wanted := map[string]bool{}
	for i, j := range joins {
		users := []string{j.userID}
		if j.self {
			members, err := d.Store.JoinedMembersOf(ctx, []string{j.roomID})
			if err != nil {
				return nil, err
			}
			users = members
		}
		perJoin[i] = users
		for _, u := range users {
			wanted[u] = true
		}
	}
	if len(wanted) == 0 {
		return nil, nil
	}
	all := make([]string, 0, len(wanted))
	for u := range wanted {
		all = append(all, u)
	}
	sort.Strings(all)
	stored, err := d.Store.PresenceForUsers(ctx, all)
	if err != nil {
		return nil, err
	}
	byUser := make(map[string]store.PresenceState, len(stored))
	for _, p := range stored {
		byUser[p.UserID] = p
	}

	var out []json.RawMessage
	for _, users := range perJoin {
		states := make([]store.PresenceState, 0, len(users))
		for _, u := range users {
			if p, ok := byUser[u]; ok {
				states = append(states, p)
				continue
			}
			states = append(states, store.PresenceState{UserID: u, State: "offline"})
		}
		events, err := presenceEvents(states, req.timeNow)
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	return out, nil
}

// eventStreamAccountData renders tags first and then account data, as Synapse's
// AccountDataEventSource does.
//
// Tags come from their own table and their own stream, so a tag change is
// reported as the room's WHOLE current tag set -- including an empty one, which
// is how a client learns the last tag was removed.
func eventStreamAccountData(ctx context.Context, d Deps, userID string,
	from, now int64) ([]json.RawMessage, error) {

	var out []json.RawMessage

	tags, err := d.Store.UpdatedTags(ctx, userID, from)
	if err != nil {
		return nil, err
	}
	for _, roomID := range sortedTagRooms(tags) {
		body, err := json.Marshal(map[string]any{
			"type": "m.tag", "content": tags[roomID], "room_id": roomID,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}

	global, err := d.Store.GlobalAccountDataSince(ctx, userID, from, now, d.MSC3391Enabled)
	if err != nil {
		return nil, err
	}
	for _, e := range global {
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}

	byRoom, err := d.Store.RoomAccountDataSince(ctx, userID, from, now, d.MSC3391Enabled)
	if err != nil {
		return nil, err
	}
	for _, roomID := range sortedAccountDataRooms(byRoom) {
		for _, e := range byRoom[roomID] {
			body, err := json.Marshal(map[string]any{
				"type": e.Type, "content": e.Content, "room_id": roomID,
			})
			if err != nil {
				return nil, err
			}
			out = append(out, body)
		}
	}
	return out, nil
}

// typingEventFor renders m.typing for /events, which unlike /sync's keeps the
// room_id: there is no room to nest it in.
func typingEventFor(d Deps, roomID string) (json.RawMessage, error) {
	users := d.Replication.TypingIn(roomID)
	if users == nil {
		users = []string{}
	}
	return json.Marshal(map[string]any{
		"type": "m.typing", "room_id": roomID,
		"content": map[string]any{"user_ids": users},
	})
}

func sortedRoomIDs(m map[string][]store.ReceiptRow) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedTagRooms(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedAccountDataRooms(m map[string][]store.AccountDataEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
