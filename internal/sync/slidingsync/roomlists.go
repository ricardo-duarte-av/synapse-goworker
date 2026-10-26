package slidingsync

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/deviceinbox"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Room list computation: which rooms could appear in each list, in what order,
// and which of them the window actually covers.
//
// Ported from SlidingSyncRoomLists._compute_interested_rooms_new_tables. Only
// the new-tables path exists here. Synapse keeps a fallback for deployments
// whose background updates have not finished; ours refuses to serve instead,
// because answering from tables known to be incomplete means a room silently
// missing from a client's list, which nobody reports as a bug. See
// store.SlidingSyncTablesReady.

// RoomLists is what a request's lists and subscriptions resolve to.
type RoomLists struct {
	// Lists maps a list key to its result.
	Lists map[string]ListResult
	// Relevant maps every room the response must describe to the config it was
	// asked for -- the union across every list and subscription that named it.
	Relevant map[string]slidingstore.RoomSyncConfig
	// AllRooms is every room that could appear in any list, INCLUDING those
	// outside the requested ranges. Extensions are scoped to it rather than to
	// the window, so a receipt in a room just off-screen is not lost.
	AllRooms map[string]bool
	// Membership is each room's membership as the response should describe it.
	Membership map[string]store.SlidingRoom
	// DMRooms is the `m.direct` set, needed for the is_dm response flag as well
	// as the filter.
	DMRooms map[string]bool

	// NewlyJoined and NewlyLeft name rooms whose membership changed inside the
	// token range. Both change how a room is BUILT, not just whether it is
	// listed: a newly joined room is sent in full even on an incremental sync,
	// and a newly left one is kept so the user's own leave is the last thing
	// they see.
	NewlyJoined map[string]bool
	NewlyLeft   map[string]bool
}

// ListResult is one list's answer.
type ListResult struct {
	// Count is the size of the FILTERED list, not of the window. It is how a
	// client knows there is more to scroll to.
	Count int
	Ops   []ListOp
}

// ListOp is a sliding window operation. Simplified sliding sync has exactly
// one, `SYNC`: the old MSC3575 vocabulary of INSERT/DELETE/INVALIDATE is what
// "simplified" removed.
type ListOp struct {
	Op      string
	Range   [2]int
	RoomIDs []string
}

// Deps are what sliding sync needs from the rest of the worker.
type Deps struct {
	Store *store.Store
	// ExcludedRooms are rooms configured never to appear in a sync.
	ExcludedRooms map[string]bool

	// MSC4354Enabled mirrors the experimental flag; a sticky event carries its
	// remaining lifetime when it is on.
	MSC4354Enabled bool

	// ServerName decides which user IDs are local, which is what tells a
	// partially stated room apart from a useless one. See MustAwaitFullState.
	ServerName string

	// Sliding is the per-connection state store. Nil means sliding sync is not
	// configured, and Build refuses rather than inventing a position.
	Sliding *slidingstore.Store

	// The extensions need what classic sync needs for the same sections.
	//
	// Inbox is nil unless to_device is configured, in which case that
	// extension answers empty rather than serving messages it will never
	// delete -- serving and deleting are one decision, as they are for /sync.
	Inbox *deviceinbox.Deleter
	// Replication supplies typing, which exists nowhere else. Nil means the
	// typing extension is always empty.
	Replication *replication.Subscriber

	MSC3391Enabled bool
	MSC4308Enabled bool
	// PushRuleFeatures gates individual base push rules, for the synthesised
	// m.push_rules account data event.
	PushRuleFeatures pushrules.Features
}

// EventConfig builds the serialisation config for one requester.
//
// Sliding sync serialises events exactly as classic sync does -- same
// allowlisted `unsigned`, same redaction on read, same event_id insertion for
// room version 3 and later. Sharing internal/clientevent rather than writing a
// second serialiser is the whole reason the two endpoints agree byte for byte
// on an event body.
func (d Deps) EventConfig(userID, deviceID string, tokenID int64) clientevent.Config {
	return d.EventConfigAdmin(userID, deviceID, tokenID, false)
}

// EventConfigAdmin is EventConfig with the server-admin metadata switch, which
// only a caller that has already established the requester is an admin asking
// for soft-failed events may set.
func (d Deps) EventConfigAdmin(userID, deviceID string, tokenID int64, adminMetadata bool) clientevent.Config {
	return clientevent.Config{
		IncludeAdminMetadata: adminMetadata,
		// The same shape /sync emits: `room_id` is stripped, because the room
		// is the key of the map the event sits in. Verified against the
		// reference, whose timeline events carry exactly content, event_id,
		// origin_server_ts, sender, state_key, type and unsigned.
		Format: clientevent.FormatV2NoRoomID,
		// The device and token identify the caller's SESSION, which is what
		// `unsigned.transaction_id` is keyed on: a client sees its own
		// transaction ids and nobody else's.
		Requester: clientevent.Requester{
			UserID: userID, DeviceID: deviceID, TokenID: tokenID,
		},
		MSC4354Enabled: d.MSC4354Enabled,
	}
}

// ComputeRoomLists resolves a request's lists and subscriptions into rooms.
// from is the incoming connection position's stream token, nil on an initial
// sync. It bounds "newly joined" and "newly left"; without it there is no range
// for anything to be newly anything IN.
func ComputeRoomLists(
	ctx context.Context, d Deps, userID string, req *Request,
	from *streamtoken.Token, now streamtoken.Token,
) (*RoomLists, error) {
	rooms, err := d.Store.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	// Rooms the user left THEMSELVES are excluded from the query above, because
	// on a busy account they are the bulk of the rows and almost never wanted.
	// But a room left after the sync's token is one the user was still in at
	// that token, so those come back separately.
	selfLeaves, err := d.Store.SlidingSelfLeavesAfter(ctx, userID, now.Room.Stream)
	if err != nil {
		return nil, err
	}
	for roomID, r := range selfLeaves {
		rooms[roomID] = r
	}

	// An invite from an ignored user must not appear. This is not cosmetic: the
	// main account ignores 161 users and had four invites from 2025 surface in
	// classic sync for exactly this reason.
	ignored, err := d.Store.IgnoredUsers(ctx, userID)
	if err != nil {
		return nil, err
	}
	for roomID, r := range rooms {
		if r.Membership == "invite" && r.Sender != "" && ignored[r.Sender] {
			delete(rooms, roomID)
		}
	}

	for roomID := range d.ExcludedRooms {
		delete(rooms, roomID)
	}

	// Which rooms changed membership inside the token range. Needed before the
	// relevance filter below, because a newly left room survives it.
	newlyJoined, newlyLeft, err := newlyJoinedAndLeft(ctx, d, userID, from, now)
	if err != nil {
		return nil, err
	}

	// The snapshot describes memberships as they are NOW; this sync answers as
	// of `now.Room`, which may be behind. Undo anything that happened after it.
	rewind, err := rewindToToken(ctx, d, userID, rooms, now)
	if err != nil {
		return nil, err
	}
	for roomID, change := range rewind {
		if change.drop {
			delete(rooms, roomID)
			continue
		}
		if _, known := rooms[roomID]; !known {
			// A room the user is not in now but was at the token.
			rooms[roomID] = change.room
			continue
		}
		rooms[roomID] = change.room
	}

	// A room left inside the range is not in the snapshot -- the query excludes
	// self-leaves -- so it has to be put back, or the client is never told it
	// left.
	for roomID, c := range newlyLeft {
		if _, present := rooms[roomID]; present {
			continue
		}
		rooms[roomID] = store.SlidingRoom{
			RoomID:        roomID,
			Sender:        c.Sender,
			Membership:    c.Membership,
			EventID:       c.EventID,
			RoomVersion:   c.RoomVersion,
			EventInstance: c.Instance,
			EventStream:   c.StreamPos,
			// Its metadata is not recoverable from the delta stream, and a room
			// being left needs none of it: the response carries the leave and
			// little else.
			HasKnownState: true,
		}
	}

	// A room the user has left is gone unless they were kicked, or unless they
	// left inside this range -- their own leave is the last thing they should
	// see, and a `newly_left` room is exactly the case where they have not seen
	// it yet.
	for roomID, r := range rooms {
		if _, isNewlyLeft := newlyLeft[roomID]; !membershipIsRelevant(userID, r) && !isNewlyLeft {
			delete(rooms, roomID)
		}
	}

	dmRooms, err := d.Store.DirectRooms(ctx, userID)
	if err != nil {
		return nil, err
	}

	leftIDs := make(map[string]bool, len(newlyLeft))
	for roomID := range newlyLeft {
		leftIDs[roomID] = true
	}

	out := &RoomLists{
		Lists:       map[string]ListResult{},
		Relevant:    map[string]slidingstore.RoomSyncConfig{},
		AllRooms:    map[string]bool{},
		Membership:  rooms,
		DMRooms:     dmRooms,
		NewlyJoined: newlyJoined,
		NewlyLeft:   leftIDs,
	}

	// Metadata for every candidate room, in one query. Everything the filters
	// and the response need -- name, type, encryption, bump_stamp, the room's
	// latest event -- is precomputed by Synapse's event persister, so none of
	// this is a state resolution.
	roomIDs := make([]string, 0, len(rooms))
	for roomID := range rooms {
		roomIDs = append(roomIDs, roomID)
	}
	meta, err := d.Store.SlidingJoinedRooms(ctx, roomIDs)
	if err != nil {
		return nil, err
	}

	var tags map[string]map[string]bool
	if requestUsesTagFilters(req) {
		if tags, err = d.Store.RoomTagsForUser(ctx, userID); err != nil {
			return nil, err
		}
	}

	// Rooms still backfilling after a faster join. Fetched once: the set is
	// small server-wide, and a request asks about hundreds of rooms.
	partial, err := d.Store.PartialStateRooms(ctx)
	if err != nil {
		return nil, err
	}
	isLocal := IsLocalUser(d.ServerName)

	for listKey, list := range req.Lists {
		filtered := rooms
		if list.Filters != nil {
			if filtered, err = filterRooms(rooms, meta, list.Filters, dmRooms, tags); err != nil {
				return nil, fmt.Errorf("list %q: %w", listKey, err)
			}
		}
		for roomID := range filtered {
			out.AllRooms[roomID] = true
		}

		// Built once per list rather than once per room: normalising
		// required_state is not free and every room in a list shares it.
		listConfig := NewRoomSyncConfig(list.CommonRoomParameters)

		// A partially stated room is hidden only if THIS list's required_state
		// cannot be satisfied from partial state -- which means asking for a
		// remote membership. Hiding it otherwise would keep a freshly joined
		// room off the client's screen for as long as backfill takes.
		if len(partial) > 0 && MustAwaitFullState(listConfig, isLocal) {
			narrowed := make(map[string]store.SlidingRoom, len(filtered))
			for roomID, r := range filtered {
				if !partial[roomID] {
					narrowed[roomID] = r
				}
			}
			filtered = narrowed
		}

		ops, err := windowRooms(ctx, d, list, filtered, meta, now)
		if err != nil {
			return nil, fmt.Errorf("list %q: %w", listKey, err)
		}
		for _, op := range ops {
			for _, roomID := range op.RoomIDs {
				out.addRelevant(roomID, listConfig)
			}
		}
		out.Lists[listKey] = ListResult{Count: len(filtered), Ops: ops}
	}

	for roomID, sub := range req.RoomSubscriptions {
		// A subscription to a room the user has no membership in is silently
		// dropped rather than rejected, matching Synapse's TODO. Rejecting
		// would let one stale room ID in a client's saved state break every
		// subsequent request.
		if _, ok := rooms[roomID]; !ok {
			continue
		}
		subConfig := NewRoomSyncConfig(sub.CommonRoomParameters)
		if partial[roomID] && MustAwaitFullState(subConfig, isLocal) {
			continue
		}
		out.AllRooms[roomID] = true
		out.addRelevant(roomID, subConfig)
	}

	return out, nil
}

// addRelevant records a room and the config it was asked for, taking the union
// when several lists name the same room. Sending the intersection would let one
// list's presence in the response silently degrade another's.
func (r *RoomLists) addRelevant(roomID string, cfg slidingstore.RoomSyncConfig) {
	if existing, ok := r.Relevant[roomID]; ok {
		r.Relevant[roomID] = CombineRoomSyncConfig(existing, cfg)
		return
	}
	r.Relevant[roomID] = cfg
}

// membershipIsRelevant decides whether a room still concerns the user.
//
// Everything except a leave, plus kicks and bans -- a leave whose sender is
// somebody else. An empty sender means a state reset removed the user with no
// leave event at all, and such a room is no longer theirs.
//
// Ported from filter_membership_for_sync. Synapse also keeps `newly_left`
// rooms, so that a user's own leave is the last thing they see; that depends on
// the newly-left computation, which is not implemented here yet -- see
// docs/milestones.md.
func membershipIsRelevant(userID string, r store.SlidingRoom) bool {
	if r.Membership != "leave" {
		return true
	}
	return r.Sender != "" && r.Sender != userID
}

func requestUsesTagFilters(req *Request) bool {
	for _, list := range req.Lists {
		if list.Filters != nil && (len(list.Filters.Tags) > 0 || len(list.Filters.NotTags) > 0) {
			return true
		}
	}
	return false
}

// filterRooms narrows a list before it is sorted.
//
// Every field ANDs with the others, and an ABSENT field means no filter rather
// than false -- which is why each is a pointer. Ported from
// filter_rooms_using_tables.
func filterRooms(
	rooms map[string]store.SlidingRoom,
	meta map[string]store.SlidingJoinedRoom,
	f *Filters,
	dmRooms map[string]bool,
	tags map[string]map[string]bool,
) (map[string]store.SlidingRoom, error) {
	// Synapse raises NotImplementedError for this rather than ignoring it, and
	// so do we: silently returning unfiltered rooms would show a client rooms
	// it asked not to see.
	if len(f.Spaces) > 0 {
		return nil, errSpacesFilter
	}

	out := make(map[string]store.SlidingRoom, len(rooms))
	for roomID, r := range rooms {
		m, hasMeta := meta[roomID]

		if f.IsDM != nil && dmRooms[roomID] != *f.IsDM {
			continue
		}
		if f.IsInvite != nil && (r.Membership == "invite") != *f.IsInvite {
			continue
		}
		if f.IsEncrypted != nil && r.IsEncrypted != *f.IsEncrypted {
			continue
		}
		if f.RoomTypes != nil && !roomTypeMatches(f.RoomTypes, r.RoomType) {
			continue
		}
		// not_room_types wins over room_types where they overlap, so it is
		// applied second and unconditionally.
		if f.NotRoomTypes != nil && roomTypeMatches(f.NotRoomTypes, r.RoomType) {
			continue
		}
		if f.RoomNameLike != nil {
			if !hasMeta || m.RoomName == nil ||
				!strings.Contains(strings.ToLower(*m.RoomName), strings.ToLower(*f.RoomNameLike)) {
				continue
			}
		}
		if len(f.Tags) > 0 && !anyTag(tags[roomID], f.Tags) {
			continue
		}
		// not_tags takes priority over tags, so a room with both is excluded.
		if len(f.NotTags) > 0 && anyTag(tags[roomID], f.NotTags) {
			continue
		}
		out[roomID] = r
	}
	return out, nil
}

// errSpacesFilter mirrors Synapse's NotImplementedError for the `spaces` list
// filter.
var errSpacesFilter = fmt.Errorf("the `spaces` filter is not implemented")

// ErrSpacesFilter reports whether an error is the unimplemented `spaces` filter.
func ErrSpacesFilter(err error) bool { return err == errSpacesFilter }

// roomTypeMatches handles the null entry, which means "rooms with no type" --
// an ordinary room rather than a space. A client asking for spaces AND ordinary
// rooms sends [null, "m.space"].
func roomTypeMatches(wanted []*string, actual *string) bool {
	for _, w := range wanted {
		if w == nil && actual == nil {
			return true
		}
		if w != nil && actual != nil && *w == *actual {
			return true
		}
	}
	return false
}

func anyTag(have map[string]bool, wanted []string) bool {
	for _, t := range wanted {
		if have[t] {
			return true
		}
	}
	return false
}

// windowRooms sorts a filtered list and cuts the requested ranges out of it.
func windowRooms(
	ctx context.Context, d Deps, list List,
	filtered map[string]store.SlidingRoom,
	meta map[string]store.SlidingJoinedRoom,
	now streamtoken.Token,
) ([]ListOp, error) {
	// No ranges, or slow_get_all_rooms, means the whole list in one op.
	ranges := list.Ranges
	if list.SlowGetAllRooms || len(ranges) == 0 {
		ranges = [][2]int{{0, len(filtered) - 1}}
		if len(filtered) == 0 {
			return nil, nil
		}
	}

	limit := 0
	for _, rng := range ranges {
		if rng[1]+1 > limit {
			limit = rng[1] + 1
		}
	}

	sorted, err := sortRooms(ctx, d, filtered, meta, now, limit)
	if err != nil {
		return nil, err
	}

	ops := make([]ListOp, 0, len(ranges))
	for _, rng := range ranges {
		start, end := rng[0], rng[1]
		roomIDs := []string{}
		if start < len(sorted) {
			max := end - start + 1
			for _, r := range sorted[start:] {
				if len(roomIDs) >= max {
					break
				}
				roomIDs = append(roomIDs, r)
			}
		}
		ops = append(ops, ListOp{Op: "SYNC", Range: rng, RoomIDs: roomIDs})
	}
	return ops, nil
}

// sortRooms orders rooms by the position of the last event the user should see,
// newest first.
//
// The key differs by membership, and the difference is a visibility rule rather
// than an optimisation: a user who left, was kicked, banned or is merely
// invited should not see anything past that point, so their membership event's
// position is the room's position for them. Only a joined user is ordered by
// the room's own latest event.
//
// That latest event comes from sliding_sync_joined_rooms, already fetched for
// the metadata, so the common case costs no extra query. A room whose newest
// event is AHEAD of the sync's token needs the newest event at or below it
// instead -- rare, since a token is usually current, and batched when it
// happens.
//
// Ported from sort_rooms.
func sortRooms(
	ctx context.Context, d Deps,
	rooms map[string]store.SlidingRoom,
	meta map[string]store.SlidingJoinedRoom,
	now streamtoken.Token,
	limit int,
) ([]string, error) {
	cap := now.Room.MaxStreamPos()
	positions := make(map[string]int64, len(rooms))
	var recheck []string

	for roomID, r := range rooms {
		if r.Membership != "join" {
			positions[roomID] = r.EventStream
			continue
		}
		m, ok := meta[roomID]
		if !ok {
			// Joined per the snapshot but absent from the joined-rooms table.
			// Fall back to the membership position rather than dropping the
			// room: a room missing from a list is worse than one ordered a
			// little too low.
			positions[roomID] = r.EventStream
			continue
		}
		if m.EventStream > cap {
			recheck = append(recheck, roomID)
			continue
		}
		positions[roomID] = m.EventStream
	}

	if len(recheck) > 0 {
		capped, err := d.Store.LastEventPosBefore(ctx, recheck, cap)
		if err != nil {
			return nil, err
		}
		for _, roomID := range recheck {
			if pos, ok := capped[roomID]; ok {
				positions[roomID] = pos
			}
			// A room with no visible event at or below the token has no
			// position and drops out of the list, which is correct: there is
			// nothing in it the client may see yet.
		}
	}

	ids := make([]string, 0, len(positions))
	for roomID := range positions {
		ids = append(ids, roomID)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := positions[ids[i]], positions[ids[j]]
		if a != b {
			return a > b
		}
		// stream_ordering is unique, so this only breaks ties between the
		// membership positions of different rooms. Sorted by room ID so the
		// answer is deterministic rather than map-order.
		return ids[i] < ids[j]
	})
	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}
	return ids, nil
}

// NumRoomsThreshold and MinimumNotUsedAge are Synapse's, and the pair is the
// whole point: a connection with a great many rooms to catch up on is better
// told to start over than served a response that takes seconds to build, but
// only if it has been idle long enough that starting over is not a loop.
const (
	NumRoomsThreshold = 100
	MinimumNotUsedAge = int64(60 * 60 * 1000)
)

// filterRoomsToSend narrows the rooms a response will describe to those the
// client actually needs.
//
// Without it every response re-sends every room in the window, which is the
// difference between a sliding sync and a very expensive `/sync`. Measured
// before this existed: a second request with nothing changed returned all five
// rooms again.
//
// A room is sent when the client has not been given it, when it was given an
// incomplete version, when its config has grown, or when something happened in
// it. Everything else the client already has.
func filterRoomsToSend(
	ctx context.Context, d Deps,
	relevant map[string]slidingstore.RoomSyncConfig,
	meta map[string]store.SlidingJoinedRoom,
	prev *slidingstore.PerConnectionState,
	from *streamtoken.Token, nowMS int64,
) (map[string]slidingstore.RoomSyncConfig, error) {

	// An initial request has nothing to compare against.
	if from == nil || len(relevant) == 0 || prev == nil {
		return relevant, nil
	}

	should := map[string]bool{}
	roomIDs := make([]string, 0, len(relevant))
	for roomID := range relevant {
		roomIDs = append(roomIDs, roomID)
	}
	sort.Strings(roomIDs)

	for _, roomID := range roomIDs {
		cfg := relevant[roomID]
		if prevCfg, ok := prev.RoomConfigs[roomID]; ok {
			// A config that has GROWN needs sending even with no new events:
			// the client raised its timeline limit or asked for more state, and
			// the extra is not something a delta can carry.
			if !sameRoomSyncConfig(CombineRoomSyncConfig(prevCfg, cfg), prevCfg) {
				should[roomID] = true
				continue
			}
		}
		switch prev.Rooms.HaveSentRoom(roomID).Status {
		case slidingstore.FlagNever:
			// Never sent, so the client needs it whatever has happened.
			should[roomID] = true
		case slidingstore.FlagPreviously:
			// PREVIOUSLY means "sent, AND there are updates we withheld", so
			// there is by definition something to send.
			should[roomID] = true
		case slidingstore.FlagLive:
			// Everything up to `from` has been sent; only new events matter.
		}
	}

	updated, err := roomsWithUpdates(ctx, d, roomIDs, meta, from)
	if err != nil {
		return nil, err
	}

	// A connection with a lot to catch up on is told to start over instead.
	// Synapse's reasoning: the client chooses its own window, so it will come
	// back with a small one, and that is faster than building a huge response.
	// Guarded on the connection having been idle, or a client with a genuinely
	// large window would be reset on every request.
	if len(updated) > NumRoomsThreshold && prev.LastUsedMS != nil &&
		nowMS-*prev.LastUsedMS > MinimumNotUsedAge {
		return nil, slidingstore.ErrUnknownPosition
	}

	for _, roomID := range updated {
		should[roomID] = true
	}

	out := make(map[string]slidingstore.RoomSyncConfig, len(should))
	for roomID := range should {
		if cfg, ok := relevant[roomID]; ok {
			out[roomID] = cfg
		}
	}
	return out, nil
}

// roomsWithUpdates returns the rooms that may have had an event since `from`.
//
// False positives are free -- they cost one room described that need not have
// been -- and a false negative loses a client an event, so both sources here
// err upwards. The stream cache answers without a query when it can; below its
// horizon the precomputed per-room event position does, which is Synapse's
// fallback too.
func roomsWithUpdates(
	ctx context.Context, d Deps, roomIDs []string,
	meta map[string]store.SlidingJoinedRoom, from *streamtoken.Token,
) ([]string, error) {

	narrowed := d.Store.RoomsWithEventsSince(roomIDs, from.Room.Stream)
	if len(narrowed) < len(roomIDs) {
		// The cache answered.
		return narrowed, nil
	}

	// Below the horizon, or disarmed. Use the materialised per-room position
	// rather than accepting "every room", which would put us back where we
	// started.
	out := make([]string, 0, len(roomIDs))
	for _, roomID := range roomIDs {
		m, ok := meta[roomID]
		if !ok {
			// No metadata: cannot rule it out, so send it.
			out = append(out, roomID)
			continue
		}
		if m.EventStream > from.Room.Stream {
			out = append(out, roomID)
		}
	}
	return out, nil
}

func sameRoomSyncConfig(a, b slidingstore.RoomSyncConfig) bool {
	if a.TimelineLimit != b.TimelineLimit {
		return false
	}
	if len(a.RequiredState) != len(b.RequiredState) {
		return false
	}
	for eventType, keys := range a.RequiredState {
		other, ok := b.RequiredState[eventType]
		if !ok || len(keys) != len(other) {
			return false
		}
		for k := range keys {
			if !other[k] {
				return false
			}
		}
	}
	return true
}
