package slidingsync

import (
	"context"
	"encoding/json"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// The per-room result: what one room looks like in a sliding sync response.
//
// A port of `get_room_sync_data`, the largest single piece of the endpoint.
// Most of its difficulty is one question asked repeatedly -- has the client
// seen this before? -- because a sliding sync response is a delta against what
// THIS connection was last told, not against a stream position.

// BumpEventTypes are the event types that count as room activity for ordering.
//
// Synapse's SLIDING_SYNC_DEFAULT_BUMP_EVENT_TYPES. Note what is absent:
// membership, name and topic changes do not bump a room, so somebody joining a
// quiet room does not push it to the top of a client's list.
var BumpEventTypes = []string{
	"m.room.create",
	"m.room.message",
	"m.room.encrypted",
	"m.sticker",
	"m.call.invite",
	"m.poll.start",
	"m.beacon_info",
}

var bumpEventTypeSet = func() map[string]bool {
	out := make(map[string]bool, len(BumpEventTypes))
	for _, t := range BumpEventTypes {
		out[t] = true
	}
	return out
}()

// RoomResult is one room's entry in the response.
//
// Almost everything is a pointer or a slice so that "not sent" and "sent as
// empty" stay distinguishable: a client applies a delta, and an omitted field
// means "unchanged" where a present empty one means "now empty".
type RoomResult struct {
	Name   *string
	Avatar *string
	Heroes []Hero
	IsDM   bool

	// Initial says the client is being sent this room from scratch. It is NOT
	// the same as "the connection is new": a room can be initial on an
	// otherwise incremental sync, which is what happens when it scrolls into
	// the window for the first time.
	Initial bool

	RequiredState []json.RawMessage
	Timeline      []json.RawMessage
	StrippedState []json.RawMessage

	PrevBatch *string
	Limited   *bool
	NumLive   *int

	// UnstableExpandedTimeline signals that more timeline was sent than the
	// bounds would suggest, because the client raised its timeline_limit.
	UnstableExpandedTimeline bool

	BumpStamp    *int64
	JoinedCount  *int
	InvitedCount *int

	// NotificationCount and HighlightCount are always zero, matching Synapse,
	// which comments that they "are just dummy values" because notification
	// counts cannot be computed correctly server-side for encrypted rooms.
	NotificationCount int
	HighlightCount    int
}

// Hero is a member used to name a room that has no name of its own.
type Hero struct {
	UserID      string
	DisplayName *string
	AvatarURL   *string
}

// RoomDataRequest is everything computing one room's result depends on.
type RoomDataRequest struct {
	UserID  string
	RoomID  string
	Config  slidingstore.RoomSyncConfig
	Room    store.SlidingRoom
	Meta    store.SlidingJoinedRoom
	HasMeta bool

	// From is the incoming connection position's stream token, nil on an
	// initial sync.
	From *streamtoken.Token
	To   streamtoken.Token

	// Previous is the state this connection was last left in.
	Previous *slidingstore.PerConnectionState
	// New accumulates what this response tells the connection.
	New *slidingstore.PerConnectionState

	NewlyJoined bool
	NewlyLeft   bool
	IsDM        bool

	// NowMS is the wall clock the response is built against, so `unsigned.age`
	// is computed once per request rather than drifting between rooms.
	NowMS int64
	// DeviceID identifies the caller's session, for `unsigned.transaction_id`:
	// a client sees its OWN transaction ids and nobody else's.
	DeviceID string
	// TokenID is the access token's row id, the fallback for events stored
	// before device ids were recorded.
	TokenID int64

	// PreviouslyReturnedLazy and LazyMemberLastSeen come from the connection's
	// lazy-member table, loaded by the caller for the users this room's
	// timeline makes relevant. Empty means "we have told this connection
	// nothing", which errs towards re-sending a membership rather than
	// withholding one.
	PreviouslyReturnedLazy map[string]bool
	LazyMemberLastSeen     map[string]int64
}

// GetRoomData builds one room's entry.
func GetRoomData(ctx context.Context, d Deps, req RoomDataRequest) (*RoomResult, error) {
	res := &RoomResult{IsDM: req.IsDM}

	// A state reset can remove somebody from a room with no leave event at all,
	// leaving a membership with no event behind it. Such a room is presented as
	// if seen for the first time, with no state -- there is nothing coherent to
	// send as a delta.
	stateResetOutOfRoom := req.Room.EventID == "" && req.Room.Membership != ""

	prevConfig, hasPrevConfig := configFor(req.Previous, req.RoomID)

	// Decide where the timeline starts, and whether this room is being sent
	// from scratch.
	//
	// Historical messages are sent -- rather than only what happened in the
	// token range -- whenever the client has nothing to show otherwise: an
	// initial sync, a newly joined room, or a room this connection has never
	// been told about. A client that scrolls a new room into view needs a
	// screenful, not the two messages that arrived since its last poll.
	var fromBound *streamtoken.RoomKey
	initial := true
	ignoreTimelineBound := false

	if req.From != nil && !req.NewlyJoined && !stateResetOutOfRoom {
		status := req.Previous.Rooms.HaveSentRoom(req.RoomID)
		switch status.Status {
		case slidingstore.FlagLive:
			k := req.From.Room
			fromBound = &k
			initial = false
		case slidingstore.FlagPreviously:
			k, err := streamtoken.ParseRoomKey(status.LastToken)
			if err != nil {
				return nil, err
			}
			fromBound = &k
			initial = false
		case slidingstore.FlagNever:
			fromBound = nil
			initial = true
		}

		// A raised timeline_limit means the client wants more history than it
		// has. Synapse ignores the lower bound and flags the response rather
		// than marking it initial, because ElementX only needs `limited` and
		// re-sending the full state would be wasteful. Its own comment calls
		// this "XXX: Odd behavior" and expects it to change.
		if hasPrevConfig && prevConfig.TimelineLimit < req.Config.TimelineLimit {
			ignoreTimelineBound = true
		}
	}
	res.Initial = initial

	timeline, limited, prevBatch, numLive, err := buildTimeline(ctx, d, req, fromBound, ignoreTimelineBound)
	if err != nil {
		return nil, err
	}
	res.Timeline = timeline.raw
	res.Limited = limited
	res.PrevBatch = prevBatch
	res.NumLive = numLive
	res.UnstableExpandedTimeline = ignoreTimelineBound

	// An invited or knocked room has no timeline and no state we can resolve --
	// the user is not in it. The stripped state on their own membership event is
	// the only thing that identifies the room to them.
	if req.Room.Membership == "invite" || req.Room.Membership == "knock" {
		stripped, memberEvent, err := d.Store.InviteOrKnockStrippedState(ctx, req.Room.EventID)
		if err != nil {
			return nil, err
		}
		if len(stripped) > 0 {
			gjson.ParseBytes(stripped).ForEach(func(_, v gjson.Result) bool {
				res.StrippedState = append(res.StrippedState, json.RawMessage(v.Raw))
				return true
			})
		}
		if len(memberEvent) > 0 {
			res.StrippedState = append(res.StrippedState, stripEvent(memberEvent))
		}
	}

	// What changed in current state during the token range. Computed early on an
	// incremental sync because it decides whether the expensive things below
	// need doing at all.
	deltas := map[store.StateKey]string{}
	membershipChanged, nameChanged, avatarChanged := false, false, false

	if !initial && fromBound != nil {
		// A rejected remote invite leaves the room without ever entering
		// current state, so its leave never appears in the delta stream. Being
		// told the room is newly left is the only way to find it.
		if req.NewlyLeft && req.Room.EventID != "" {
			membershipChanged = true
			deltas[store.StateKey{Type: memberEventType, StateKey: req.UserID}] = req.Room.EventID
		}

		got, err := d.Store.CurrentStateDeltas(ctx, req.RoomID,
			fromBound.MaxStreamPos(), req.To.Room.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for k, eventID := range got {
			deltas[k] = eventID
			switch {
			case k.Type == memberEventType:
				membershipChanged = true
			case k.Type == "m.room.name" && k.StateKey == "":
				nameChanged = true
			case k.Type == "m.room.avatar" && k.StateKey == "":
				avatarChanged = true
			}
		}
	}

	if err := d.applyRoomMetadata(ctx, req, res,
		initial, nameChanged, avatarChanged, membershipChanged); err != nil {
		return nil, err
	}

	if err := d.applyBumpStamp(ctx, req, res, timeline, initial, limited); err != nil {
		return nil, err
	}

	limitedFlag := limited != nil && *limited
	if err := d.resolveRequiredState(ctx, req, res, timeline, deltas, initial, limitedFlag); err != nil {
		return nil, err
	}
	_ = hasPrevConfig

	return res, nil
}

type timelineResult struct {
	raw     []json.RawMessage
	events  []store.TimelineEvent
	lastPos int64
}

// buildTimeline paginates backwards from the token and returns the page in
// ascending order.
func buildTimeline(
	ctx context.Context, d Deps, req RoomDataRequest,
	fromBound *streamtoken.RoomKey, ignoreTimelineBound bool,
) (timelineResult, *bool, *string, *int, error) {

	var out timelineResult
	if req.Config.TimelineLimit <= 0 ||
		req.Room.Membership == "invite" || req.Room.Membership == "knock" {
		// No timeline for invite/knock: there is only stripped state.
		return out, nil, nil, nil, nil
	}

	// Nobody sees past their own leave or ban.
	toBound := req.To.Room
	if req.Room.Membership == "leave" || req.Room.Membership == "ban" {
		toBound = streamtoken.Live(req.Room.EventStream)
	}

	timelineFrom := fromBound
	if ignoreTimelineBound {
		timelineFrom = nil
	}

	// Topological ordering for a historical view, stream ordering for updates.
	// The distinction is not cosmetic: an event backfilled today sits in a
	// different place in each order, so the wrong one returns a different set
	// of events. Synapse chooses the same way, on the same condition.
	var (
		events  []store.TimelineEvent
		newKey  streamtoken.RoomKey
		limited bool
		err     error
	)

	if timelineFrom == nil {
		events, newKey, limited, err = d.Store.PaginateBackwards(
			ctx, req.RoomID, req.Room.RoomVersion, req.Config.TimelineLimit, toBound)
	} else {
		events, newKey, limited, err = d.Store.PaginateBackwardsStream(
			ctx, req.RoomID, req.Room.RoomVersion, req.Config.TimelineLimit, toBound, *timelineFrom)
	}
	if err != nil {
		return out, nil, nil, nil, err
	}

	// Visibility. Synapse calls filter_and_transform_events_for_client here, and
	// skipping it does not merely lose a field -- it serves history the caller
	// may not be entitled to. It is invisible in a room with `shared` history
	// visibility, which is most of them, so it has to be structural rather than
	// remembered: the same internal/eventfilter both endpoints use.
	//
	// is_peeking is "not joined": someone reading a world-readable room they
	// are not in gets the rules for an outsider.
	filtered, err := eventfilter.ForClient(ctx, d.Store, req.RoomID, req.UserID,
		events, req.Room.Membership != "join", req.NowMS, d.MSC4354Enabled, nil)
	if err != nil {
		return out, nil, nil, nil, err
	}
	events = filtered.Events

	// A server admin who asked to see soft-failed events is also told WHICH
	// events those are; showing them without saying so would leave no way to
	// tell one from an ordinary event.
	cfg := d.EventConfigAdmin(req.UserID, req.DeviceID, req.TokenID, filtered.AdminMetadata)

	// Redactions. A redacted event must be served pruned, with
	// `unsigned.redacted_because` explaining why -- Synapse applies this on
	// READ rather than rewriting the stored event, so an endpoint that skips it
	// serves the ORIGINAL CONTENT. Not a cosmetic difference, and invisible
	// until a compared room happens to have a redaction in its recent timeline.
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.EventID
	}
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return out, nil, nil, nil, err
	}

	// Bundled aggregations, for a limited timeline only -- which an initial
	// room always is. A client given the whole history can aggregate for
	// itself; one given a window cannot see the replies that fall outside it.
	var aggs map[string]eventfilter.Aggregation
	var nested map[string]store.StateEvent
	if limited {
		vis, err := d.Store.VisibilityExtras(ctx, req.RoomID, req.UserID, nil)
		if err != nil {
			return out, nil, nil, nil, err
		}
		var nestedIDs []string
		aggs, nestedIDs, err = eventfilter.BundleAggregations(
			ctx, d.Store, req.UserID, events, vis.IgnoredSenders)
		if err != nil {
			return out, nil, nil, nil, err
		}
		if len(aggs) > 0 {
			// A bundle carries whole events -- a thread's latest reply, an
			// edit -- so they have to be loaded and serialised too.
			want := append([]string(nil), nestedIDs...)
			for _, a := range aggs {
				if a.ReplaceID() != "" {
					want = append(want, a.ReplaceID())
				}
			}
			nested, err = d.Store.EventsByID(ctx, want, req.Room.RoomVersion)
			if err != nil {
				return out, nil, nil, nil, err
			}
		}
	}

	out.events = events
	for i, e := range events {
		// The stored PDU is not a client event. Room version 3 and later derive
		// the event ID from a hash rather than storing it, `unsigned` is rebuilt
		// from an allowlist rather than passed through, and a redacted event has
		// to be pruned on read. Every one of those is invisible until compared
		// against Synapse -- the first version of this emitted stored JSON and
		// produced five timeline events with empty event IDs.
		stored := e.Stored
		// MSC4115: the caller's membership at this event, which the visibility
		// decision has just worked out. Reading it from anywhere else would
		// mean resolving the same state a second time.
		if i < len(filtered.Memberships) {
			stored.Membership = filtered.Memberships[i]
		}
		if err := eventfilter.AttachRedaction(&stored, redactions, req.NowMS, cfg); err != nil {
			return out, nil, nil, nil, err
		}
		body, err := clientevent.Serialize(stored, req.NowMS, cfg)
		if err != nil {
			return out, nil, nil, nil, err
		}
		if agg, ok := aggs[e.EventID]; ok {
			body, err = eventfilter.AttachAggregations(body, agg, aggs, nested, req.NowMS, cfg)
			if err != nil {
				return out, nil, nil, nil, err
			}
		}
		out.raw = append(out.raw, json.RawMessage(body))
	}

	// num_live counts the events inside the token range, so a client can tell a
	// mention that just arrived from one scrolled into view. It cannot be
	// derived from `initial`: a room outside the window bumps into it because
	// of a mention and arrives with initial=true carrying one live event.
	var numLive *int
	if req.From != nil {
		n := 0
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].StreamOrdering > req.From.Room.MaxStreamPos() {
				n++
				continue
			}
			// Reverse-chronological, so the first non-live event ends it.
			break
		}
		numLive = &n
	}

	prev := req.To.WithRoomKey(newKey).String()
	return out, &limited, &prev, numLive, nil
}

// applyRoomMetadata fills in the name, heroes and membership counts.
//
// All three are skipped on an unchanged incremental sync, which is the point:
// they are the expensive part of a room entry, and on a quiet poll none of them
// can have moved.
func (d Deps) applyRoomMetadata(
	ctx context.Context, req RoomDataRequest, res *RoomResult,
	initial, nameChanged, avatarChanged, membershipChanged bool,
) error {
	if initial || nameChanged {
		// A room whose m.room.name is absent, null or empty is a room with no
		// name -- the spec is explicit, and treating "" as a name gives a
		// client a blank title instead of the heroes it should fall back to.
		ids, err := d.Store.FilteredStateAt(ctx, req.RoomID, req.To.Room,
			[]store.StateKey{{Type: "m.room.name", StateKey: ""}})
		if err != nil {
			return err
		}
		if eventID, ok := ids[store.StateKey{Type: "m.room.name", StateKey: ""}]; ok {
			events, err := d.Store.EventsByID(ctx, []string{eventID}, req.Room.RoomVersion)
			if err != nil {
				return err
			}
			if ev, ok := events[eventID]; ok {
				if name := gjson.GetBytes(ev.JSON, "content.name").String(); name != "" {
					res.Name = &name
				}
			}
		}
	}

	if initial || avatarChanged {
		ids, err := d.Store.FilteredStateAt(ctx, req.RoomID, req.To.Room,
			[]store.StateKey{{Type: "m.room.avatar", StateKey: ""}})
		if err != nil {
			return err
		}
		if eventID, ok := ids[store.StateKey{Type: "m.room.avatar", StateKey: ""}]; ok {
			events, err := d.Store.EventsByID(ctx, []string{eventID}, req.Room.RoomVersion)
			if err != nil {
				return err
			}
			if ev, ok := events[eventID]; ok {
				if url := gjson.GetBytes(ev.JSON, "content.url").String(); url != "" {
					res.Avatar = &url
				}
			}
		}
	}

	// Heroes only matter when the room cannot name itself, and only change when
	// the membership does.
	var summary *store.MemberSummary
	if res.Name == nil && (initial || membershipChanged) {
		if req.Room.Membership != "join" {
			// Synapse has a TODO here: it does not know how to summarise a room
			// the user has left, and returns nothing rather than a wrong
			// answer. We inherit that rather than invent one.
			summary = &store.MemberSummary{}
		} else {
			s, err := d.Store.RoomSummary(ctx, req.RoomID)
			if err != nil {
				return err
			}
			summary = &s
		}
		for _, userID := range summary.Heroes(req.UserID) {
			res.Heroes = append(res.Heroes, Hero{UserID: userID})
		}
	}

	if (initial || membershipChanged) && req.Room.Membership == "join" {
		if summary != nil {
			joined, invited := summary.Counts["join"], summary.Counts["invite"]
			res.JoinedCount, res.InvitedCount = &joined, &invited
		} else {
			counts, err := d.Store.MemberCounts(ctx, req.RoomID)
			if err != nil {
				return err
			}
			joined, invited := counts["join"], counts["invite"]
			res.JoinedCount, res.InvitedCount = &joined, &invited
		}
	}
	return nil
}

// applyBumpStamp works out the position of the room's last activity.
//
// Omitted when it cannot have changed, which is the common case on a quiet
// incremental sync: the client keeps the value it already has and its room list
// does not move.
func (d Deps) applyBumpStamp(
	ctx context.Context, req RoomDataRequest, res *RoomResult,
	timeline timelineResult, initial bool, limited *bool,
) error {
	alwaysReturn := req.Room.Membership != "join" ||
		limited == nil || *limited || initial

	if req.Room.Membership == "join" {
		stamp, ok, err := d.bumpStamp(ctx, req, timeline, alwaysReturn)
		if err != nil {
			return err
		}
		if ok {
			res.BumpStamp = &stamp
		}
	}

	if res.BumpStamp == nil && alwaysReturn {
		pos := req.Room.EventStream
		res.BumpStamp = &pos
	}

	// A negative stream ordering means a backfilled event, and negative and
	// positive orderings are not comparable -- they mean different things. Zero
	// puts the room at the bottom of the list until something happens in it,
	// which is the only sensible answer.
	if res.BumpStamp != nil && *res.BumpStamp < 0 {
		zero := int64(0)
		res.BumpStamp = &zero
	}
	return nil
}

func (d Deps) bumpStamp(
	ctx context.Context, req RoomDataRequest, timeline timelineResult, checkOutside bool,
) (int64, bool, error) {
	// The timeline we are already returning is the cheapest place to look.
	for i := len(timeline.events) - 1; i >= 0; i-- {
		if !bumpEventTypeSet[timeline.events[i].Type] {
			continue
		}
		if pos := timeline.events[i].StreamOrdering; pos > 0 {
			return pos, true, nil
		}
	}
	if !checkOutside {
		// Not a limited sync, so nothing outside the timeline can have bumped
		// the room since the client last looked.
		return 0, false, nil
	}

	// The precomputed value, which is right whenever it is safely below the
	// token. It is only a stream ordering, so "safely" means below the token's
	// MINIMUM position -- a vector-clock token cannot be compared to it
	// otherwise.
	if req.HasMeta && req.Meta.BumpStamp != nil {
		if *req.Meta.BumpStamp < req.To.Room.Stream {
			if *req.Meta.BumpStamp > 0 {
				return *req.Meta.BumpStamp, true, nil
			}
			return 0, false, nil
		}
	} else if req.HasMeta {
		// The tables are up to date and say there is no bump event: a room
		// freshly joined over federation whose events are all backfilled.
		return 0, false, nil
	}

	pos, ok, err := d.Store.LastBumpEventPosBefore(
		ctx, req.RoomID, BumpEventTypes, req.To.Room.MaxStreamPos())
	if err != nil || !ok || pos <= 0 {
		return 0, false, err
	}
	return pos, true, nil
}

func configFor(state *slidingstore.PerConnectionState, roomID string) (slidingstore.RoomSyncConfig, bool) {
	if state == nil {
		return slidingstore.RoomSyncConfig{}, false
	}
	cfg, ok := state.RoomConfigs[roomID]
	return cfg, ok
}

// stripEvent reduces an event to the fields a client may see for a room it is
// not in: type, state_key, content and sender.
func gjsonString(raw []byte, path string) *string {
	v := gjson.GetBytes(raw, path)
	if v.Type != gjson.String {
		return nil
	}
	s := v.String()
	return &s
}

func stripEvent(raw []byte) json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, field := range []string{"type", "state_key", "content", "sender"} {
		if v := gjson.GetBytes(raw, field); v.Exists() {
			out[field] = json.RawMessage(v.Raw)
		}
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return json.RawMessage("{}")
	}
	return encoded
}
