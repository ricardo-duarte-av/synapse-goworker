package slidingsync

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// The extension section: the parts of a sync that are not rooms.
//
// Ported from SlidingSyncExtensionHandler. Eight are implemented, which is
// every one Synapse implements; MSC4262 (profiles) only under
// include_profile_updates_in_sync, as Synapse.
//
// Every extension is OFF unless the request enables it. That is the MSC's
// design and it is what makes the endpoint cheap: a client that only wants a
// room list pays for nothing else.

// extensionInputs is what building the extension section needs from the rest of
// the response.
type extensionInputs struct {
	UserID   string
	DeviceID string
	Request  *Request

	From  *streamtoken.Token
	Now   streamtoken.Token
	NowMS int64

	// Lists and Subscribed are what the response actually contains, which is
	// what the reserved `lists`/`rooms` extension keys select from.
	Lists      map[string]ListResult
	Subscribed map[string]bool
	// NewlyJoined is the rooms joined inside the token range, which the
	// sticky events extension sends from scratch.
	NewlyJoined map[string]bool
	// Rooms is what was described this time, for the receipts extension's
	// "re-send receipts for a room sent from scratch" rule.
	Rooms map[string]*RoomResult
	// Membership supplies room versions for serialising sticky events.
	Membership map[string]store.SlidingRoom

	Previous *slidingstore.PerConnectionState
	New      *slidingstore.PerConnectionState
}

// buildExtensions assembles the extension section, and reports whether any of
// it is NEWS.
//
// Presence is not news, and the difference is a hot loop. `e2ee` always carries
// one-time-key counts, `to_device` always carries a `next_batch`, `typing` and
// `receipts` always carry a `rooms` object. Synapse gives each extension its
// own truthiness rule for exactly this reason -- its comment on e2ee cites
// element-android#3725 for why the key counts must be sent yet must not count.
func buildExtensions(
	ctx context.Context, d Deps, in extensionInputs,
) (map[string]json.RawMessage, bool, error) {

	out := map[string]json.RawMessage{}
	news := false
	ext := in.Request.Extensions
	if ext == nil {
		return out, false, nil
	}

	if ext.ToDevice != nil && ext.ToDevice.Enabled {
		v, err := toDeviceExtension(ctx, d, in, ext.ToDevice)
		if err != nil {
			return nil, false, err
		}
		if err := set(out, "to_device", v); err != nil {
			return nil, false, err
		}
		// The messages, not the next_batch: a token that has moved is not
		// something the client needs waking for.
		news = news || toDeviceIsNews(v)
	}
	if ext.E2EE != nil && ext.E2EE.Enabled {
		v, err := e2eeExtension(ctx, d, in)
		if err != nil {
			return nil, false, err
		}
		if err := set(out, "e2ee", v); err != nil {
			return nil, false, err
		}
		// ONLY the device list changes. The one-time-key counts are always sent
		// and must never count as news -- that is the whole hot loop.
		news = news || e2eeIsNews(v)
	}
	if ext.AccountData != nil && ext.AccountData.Enabled {
		v, err := accountDataExtension(ctx, d, in, &ext.AccountData.ExtensionScope)
		if err != nil {
			return nil, false, err
		}
		if err := set(out, "account_data", v); err != nil {
			return nil, false, err
		}
		news = news || accountDataIsNews(v)
	}
	if ext.Receipts != nil && ext.Receipts.Enabled {
		v, err := receiptsExtension(ctx, d, in, &ext.Receipts.ExtensionScope)
		if err != nil {
			return nil, false, err
		}
		if err := set(out, "receipts", v); err != nil {
			return nil, false, err
		}
		news = news || receiptsIsNews(v)
	}
	if ext.Typing != nil && ext.Typing.Enabled {
		v := typingExtension(d, in, &ext.Typing.ExtensionScope)
		if err := set(out, "typing", v); err != nil {
			return nil, false, err
		}
		news = news || typingIsNews(v)
	}
	if ext.ThreadSubscriptions != nil && ext.ThreadSubscriptions.Enabled && d.MSC4308Enabled {
		v, err := threadSubscriptionsExtension(ctx, d, in, ext.ThreadSubscriptions)
		if err != nil {
			return nil, false, err
		}
		// Synapse omits this one entirely when there is nothing to say, unlike
		// the others, which are present whenever enabled.
		if v != nil {
			if err := set(out, "io.element.msc4308.thread_subscriptions", v); err != nil {
				return nil, false, err
			}
			news = news || threadSubscriptionsIsNews(v)
		}
	}
	// Called with the extension off too: rooms that gain sticky events
	// meanwhile are tracked (Synapse 1.165).
	if d.MSC4354Enabled {
		v, err := stickyEventsExtension(ctx, d, in, ext.StickyEvents)
		if err != nil {
			return nil, false, err
		}
		if v != nil {
			if err := set(out, "org.matrix.msc4354.sticky_events", v); err != nil {
				return nil, false, err
			}
			news = news || stickyEventsIsNews(v)
		}
	}
	if ext.Profiles != nil && d.ProfileUpdatesInSync {
		v, err := profilesExtension(ctx, d, in, ext.Profiles)
		if err != nil {
			return nil, false, err
		}
		// Present, and news, only with users in it.
		if v != nil {
			if err := set(out, "org.matrix.msc4262.profiles", v); err != nil {
				return nil, false, err
			}
			news = true
		}
	}
	return out, news, nil
}

// Each extension's own definition of NEWS -- whether it should stop a long
// poll from waiting. Named functions rather than expressions inline above,
// because they are the whole difference between a working long poll and a hot
// loop, and a rule written inline is a rule that cannot be tested.
//
// The shared shape: a field that is present on EVERY response is never news.
// Synapse gives each extension its own __bool__ for exactly this reason.

// toDeviceIsNews: the messages, not the `next_batch`. A token that moved is not
// something to wake a client for.
func toDeviceIsNews(v *toDeviceJSON) bool { return len(v.Events) > 0 }

// e2eeIsNews: the device list changes ONLY.
//
// The one-time-key counts are sent on every response and must never count --
// Synapse's comment cites element-android#3725 for why they cannot be omitted,
// and its __bool__ explicitly excludes them. This is the rule whose absence
// produced the hot loop.
func e2eeIsNews(v *e2eeJSON) bool {
	return v.DeviceLists != nil &&
		(len(v.DeviceLists.Changed) > 0 || len(v.DeviceLists.Left) > 0)
}

func accountDataIsNews(v *accountDataJSON) bool {
	return len(v.Global) > 0 || len(v.Rooms) > 0
}

// receiptsIsNews and typingIsNews: the `rooms` object is always present, so its
// CONTENTS are the question.
func receiptsIsNews(v *receiptsJSON) bool { return len(v.Rooms) > 0 }
func typingIsNews(v *typingJSON) bool     { return len(v.Rooms) > 0 }

func threadSubscriptionsIsNews(v *threadSubscriptionsJSON) bool {
	return len(v.Subscribed) > 0 || len(v.Unsubscribed) > 0 || v.PrevBatch != nil
}

// stickyEventsIsNews: a room with an event the user may see, whether or not
// the timeline then carries it (StickyEventsExtension.__bool__).
func stickyEventsIsNews(v *stickyEventsJSON) bool { return len(v.Rooms) > 0 }

func set(out map[string]json.RawMessage, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	out[key] = body
	return nil
}

// relevantRooms resolves the reserved `lists` and `rooms` keys every
// room-scoped extension shares.
//
// `["*"]` -- the default -- means every list, or every room subscription. An
// EMPTY list means none, which is a different thing and the reason both are
// pointers in the request: a client that wants receipts for one list only sends
// `{"lists": ["that-one"], "rooms": []}`.
//
// Only rooms actually in the response are eligible. An extension that returned
// data for a room the client was not sent would be describing something it
// cannot place.
func relevantRooms(in extensionInputs, scope *ExtensionScope) map[string]bool {
	out := map[string]bool{}

	for _, roomID := range scope.Rooms {
		if roomID == Wildcard {
			for id := range in.Subscribed {
				out[id] = true
			}
			break
		}
		if in.Subscribed[roomID] {
			out[roomID] = true
		}
	}

	for _, listKey := range scope.Lists {
		if listKey == Wildcard {
			for _, l := range in.Lists {
				for _, op := range l.Ops {
					for _, roomID := range op.RoomIDs {
						out[roomID] = true
					}
				}
			}
			break
		}
		if l, ok := in.Lists[listKey]; ok {
			for _, op := range l.Ops {
				for _, roomID := range op.RoomIDs {
					out[roomID] = true
				}
			}
		}
	}
	return out
}

// --- to_device (MSC3885) ---

type toDeviceJSON struct {
	NextBatch string            `json:"next_batch"`
	Events    []json.RawMessage `json:"events"`
}

// toDeviceExtension serves and DELETES to-device messages.
//
// The deletion is the same one classic sync makes and the same package makes
// it: a worker that serves this section without deleting hands a device the
// same room keys for ever. With `to_device.enabled` false the section is
// omitted rather than served undeleted -- there is no third option.
func toDeviceExtension(
	ctx context.Context, d Deps, in extensionInputs, req *ToDeviceExtension,
) (*toDeviceJSON, error) {

	if in.DeviceID == "" || d.Inbox == nil {
		// Synapse answers with an empty section rather than omitting it, so a
		// client can tell "nothing for you" from "not supported".
		return &toDeviceJSON{
			NextBatch: strconv.FormatInt(in.Now.ToDevice, 10),
			Events:    []json.RawMessage{},
		}, nil
	}

	var since int64
	if req.Since != "" {
		n, err := strconv.ParseInt(req.Since, 10, 64)
		if err != nil {
			return nil, err
		}
		since = n
		if in.Now.ToDevice < since {
			// A `since` from the future, which Synapse warns about and answers
			// empty rather than treating as an error.
			return &toDeviceJSON{NextBatch: req.Since, Events: []json.RawMessage{}}, nil
		}
		// Everything at or below `since` has demonstrably reached the device.
		if _, err := d.Inbox.DeleteUpTo(ctx, in.UserID, in.DeviceID, since); err != nil {
			return nil, err
		}
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		// Synapse caps at 100 regardless of what was asked for.
		limit = 100
	}
	messages, next, err := d.Store.MessagesForDevice(
		ctx, in.UserID, in.DeviceID, since, in.Now.ToDevice, limit)
	if err != nil {
		return nil, err
	}
	events := make([]json.RawMessage, 0, len(messages))
	for _, m := range messages {
		events = append(events, m)
	}
	return &toDeviceJSON{NextBatch: strconv.FormatInt(next, 10), Events: events}, nil
}

// --- e2ee (MSC3884) ---

type e2eeJSON struct {
	DeviceOneTimeKeysCount   map[string]int   `json:"device_one_time_keys_count"`
	DeviceUnusedFallbackKeys []string         `json:"device_unused_fallback_key_types"`
	DeviceLists              *deviceListsJSON `json:"device_lists,omitempty"`
}

type deviceListsJSON struct {
	Changed []string `json:"changed"`
	Left    []string `json:"left"`
}

func e2eeExtension(ctx context.Context, d Deps, in extensionInputs) (*e2eeJSON, error) {
	out := &e2eeJSON{
		// Both are ALWAYS present, even when empty. Synapse's comment points at
		// element-android#3725: a client cannot tell "no keys" from "nothing
		// changed" if the field can be absent.
		DeviceOneTimeKeysCount:   map[string]int{},
		DeviceUnusedFallbackKeys: []string{},
	}

	if in.DeviceID != "" {
		counts, err := d.Store.DeviceKeyCounts(ctx, in.UserID, in.DeviceID)
		if err != nil {
			return nil, err
		}
		if counts.OneTimeKeys != nil {
			out.DeviceOneTimeKeysCount = counts.OneTimeKeys
		}
		if counts.UnusedFallbackKeyTypes != nil {
			out.DeviceUnusedFallbackKeys = counts.UnusedFallbackKeyTypes
		}
	}

	// Device list changes need a range, so an initial request has none. That is
	// not a gap: a client with no `pos` is about to fetch keys for everyone it
	// shares a room with anyway.
	if in.From != nil {
		lists, err := deviceListUpdates(ctx, d, in.UserID, *in.From, in.Now)
		if err != nil {
			return nil, err
		}
		out.DeviceLists = lists
	}
	return out, nil
}

// deviceListUpdates is DeviceHandler.get_user_ids_changed: whose keys the
// client should re-fetch, and whom it no longer shares a room with.
//
// It is scoped to EVERY room the user is joined to, not to the rooms this
// connection's lists happen to cover. The two differ completely for Element X,
// which asks for `e2ee` on a connection of its own with no lists at all: scoped
// to the lists, that connection was told about the caller's own devices and
// nobody else's, so the client never learned of a co-member's new device -- and
// cannot trust, or encrypt to, a device it has never fetched.
func deviceListUpdates(ctx context.Context, d Deps, userID string,
	from, now streamtoken.Token) (*deviceListsJSON, error) {

	joined, err := d.Store.RoomsForUser(ctx, userID, []string{"join"})
	if err != nil {
		return nil, err
	}
	joinedIDs := make([]string, 0, len(joined))
	joinedSet := make(map[string]bool, len(joined))
	for _, r := range joined {
		joinedIDs = append(joinedIDs, r.RoomID)
		joinedSet[r.RoomID] = true
	}
	sort.Strings(joinedIDs)

	roomFrom, roomTo := from.Room.MaxStreamPos(), now.Room.MaxStreamPos()

	// The caller's own joins and leaves. "Joinedness" is what counts: a join
	// following a join is a profile change.
	self, err := d.Store.SelfMemberDeltas(ctx, userID, roomFrom, roomTo)
	if err != nil {
		return nil, err
	}
	newlyJoinedRooms := map[string]bool{}
	newlyLeftRooms := map[string]bool{}
	for _, c := range self {
		if d.ExcludedRooms[c.RoomID] {
			continue
		}
		if c.Membership == "join" {
			if c.PrevMembership != "join" {
				newlyJoinedRooms[c.RoomID] = true
				delete(newlyLeftRooms, c.RoomID)
			}
		} else if c.PrevMembership == "join" {
			delete(newlyJoinedRooms, c.RoomID)
			newlyLeftRooms[c.RoomID] = true
		}
	}

	// Everybody else's, in the rooms we are in now.
	deltas, err := d.Store.MemberDeltasInRooms(ctx, joinedIDs, roomFrom, roomTo)
	if err != nil {
		return nil, err
	}
	present := func(m string) bool { return m == "join" || m == "invite" || m == "knock" }
	newUsers := map[string]bool{}
	leftUsers := map[string]bool{}
	for _, c := range deltas {
		membership := c.Membership
		if !c.HasEvent {
			membership = ""
		}
		if present(membership) {
			if !present(c.PrevMembership) {
				newUsers[c.UserID] = true
				delete(leftUsers, c.UserID)
			}
		} else if present(c.PrevMembership) {
			delete(newUsers, c.UserID)
			leftUsers[c.UserID] = true
		}
	}

	changed, err := d.Store.DeviceListChanges(ctx, userID, joinedIDs,
		from.DeviceList.Stream, now.DeviceList.MaxStreamPos())
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(changed))
	for _, u := range changed {
		set[u] = true
	}
	for u := range newUsers {
		set[u] = true
	}
	if len(newlyJoinedRooms) > 0 {
		members, err := d.Store.JoinedMembersOf(ctx, sortedKeys(newlyJoinedRooms))
		if err != nil {
			return nil, err
		}
		for _, u := range members {
			set[u] = true
		}
	}

	if len(newlyLeftRooms) > 0 {
		members, err := d.Store.JoinedMembersOf(ctx, sortedKeys(newlyLeftRooms))
		if err != nil {
			return nil, err
		}
		for _, u := range members {
			leftUsers[u] = true
		}
	}
	// Someone still in another room with us has not left our view; telling the
	// client otherwise makes it stop tracking keys it still needs.
	if len(leftUsers) > 0 {
		sharing, err := d.Store.UsersJoinedToAny(ctx, sortedKeys(leftUsers), joinedIDs)
		if err != nil {
			return nil, err
		}
		for u := range leftUsers {
			if sharing[u] {
				delete(leftUsers, u)
			}
		}
	}

	// A remote user with a non-compliant ID is dropped (#20338).
	for _, m := range []map[string]bool{set, leftUsers} {
		for u := range m {
			if !filter.SyncableUserID(u, d.ServerName) {
				delete(m, u)
			}
		}
	}
	return &deviceListsJSON{Changed: sortedKeys(set), Left: sortedKeys(leftUsers)}, nil
}

// --- typing (MSC3961) ---

type typingJSON struct {
	Rooms map[string]json.RawMessage `json:"rooms"`
}

// typingExtension reports who is typing, from the replication view.
//
// No connection tracking, deliberately, and Synapse says why: a typing
// notification times out in about thirty seconds, so a room that fell out of
// range for longer than that has nothing worth catching up on. Anything still
// live arrives when the room comes back.
func typingExtension(d Deps, in extensionInputs, scope *ExtensionScope) *typingJSON {
	out := &typingJSON{Rooms: map[string]json.RawMessage{}}
	if d.Replication == nil {
		return out
	}
	relevant := relevantRooms(in, scope)

	// Which rooms to report is a question about the typing STREAM, not about
	// who is typing now: Synapse asks its typing source for everything since
	// the client's typing key, and a room whose typists were cleared is a
	// change like any other. So a room reports `user_ids: []` -- nobody is
	// typing, and the client is being told so.
	//
	// An initial request asks from zero, which is every room that has ever had
	// a typist. Ours reported only rooms with someone typing NOW, and the
	// reference caught it on a request where two rooms should have carried an
	// empty list.
	var from int64
	if in.From != nil {
		from = in.From.Typing
	}
	changed := d.Replication.TypingChangedSince(from)

	for _, roomID := range changed {
		if !relevant[roomID] {
			continue
		}
		users := d.Replication.TypingIn(roomID)
		if users == nil {
			users = []string{}
		}
		body, err := json.Marshal(map[string]any{
			"type":    "m.typing",
			"content": map[string]any{"user_ids": users},
		})
		if err != nil {
			continue
		}
		out.Rooms[roomID] = body
	}
	return out
}

// --- account_data (MSC3959) ---

type accountDataJSON struct {
	Global []json.RawMessage            `json:"global"`
	Rooms  map[string][]json.RawMessage `json:"rooms"`
}

// accountDataExtension serves global and per-room account data.
//
// Per-room account data is tracked per connection, exactly as rooms are: a room
// the client has never been given gets ALL of its account data, one it has gets
// only what changed. The alternative -- sending everything every time -- is
// what makes an account-data extension expensive on a 654-room account.
func accountDataExtension(
	ctx context.Context, d Deps, in extensionInputs, scope *ExtensionScope,
) (*accountDataJSON, error) {

	out := &accountDataJSON{Global: []json.RawMessage{}, Rooms: map[string][]json.RawMessage{}}

	// Global. On an initial request everything; otherwise the changes.
	var global []store.AccountDataEntry
	var err error
	if in.From == nil {
		global, err = d.Store.GlobalAccountData(ctx, in.UserID, d.MSC3391Enabled)
	} else {
		global, err = d.Store.GlobalAccountDataSince(ctx, in.UserID,
			in.From.AccountData, in.Now.AccountData, d.MSC3391Enabled)
	}
	if err != nil {
		return nil, err
	}
	for _, e := range global {
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return nil, err
		}
		out.Global = append(out.Global, body)
	}

	// m.push_rules is synthesised rather than stored, and it belongs in the
	// global section on an initial request or whenever the rules changed.
	if in.From == nil || in.From.PushRules < in.Now.PushRules {
		rules, err := pushRulesEvent(ctx, d, in.UserID)
		if err != nil {
			return nil, err
		}
		if rules != nil {
			out.Global = append(out.Global, rules)
		}
	}

	relevant := relevantRooms(in, scope)
	if len(relevant) == 0 {
		return out, nil
	}

	// Split by what this connection has already been told.
	var initial []string
	previously := map[string]int64{}
	live := map[string]bool{}
	for _, roomID := range sortedKeys(relevant) {
		if in.From == nil {
			initial = append(initial, roomID)
			continue
		}
		status := in.Previous.AccountData.HaveSentRoom(roomID)
		switch status.Status {
		case slidingstore.FlagLive:
			live[roomID] = true
		case slidingstore.FlagPreviously:
			pos, err := strconv.ParseInt(status.LastToken, 10, 64)
			if err != nil {
				// An unreadable resume point: send everything rather than
				// resume from a position we cannot trust.
				initial = append(initial, roomID)
				continue
			}
			previously[roomID] = pos
		default:
			initial = append(initial, roomID)
		}
	}

	// Everything that changed since `from`, fetched once. It answers the live
	// rooms directly and tells us which rooms outside the window have pending
	// updates.
	var changedByRoom map[string][]store.AccountDataEntry
	if in.From != nil {
		changedByRoom, err = d.Store.RoomAccountDataSince(ctx, in.UserID,
			in.From.AccountData, in.Now.AccountData, d.MSC3391Enabled)
		if err != nil {
			return nil, err
		}
		for roomID := range live {
			if entries, ok := changedByRoom[roomID]; ok && len(entries) > 0 {
				if err := addRoomAccountData(out, roomID, entries); err != nil {
					return nil, err
				}
			}
		}
	}

	for roomID, from := range previously {
		entries, err := d.Store.RoomAccountDataSince(ctx, in.UserID, from, in.Now.AccountData, d.MSC3391Enabled)
		if err != nil {
			return nil, err
		}
		if e, ok := entries[roomID]; ok && len(e) > 0 {
			if err := addRoomAccountData(out, roomID, e); err != nil {
				return nil, err
			}
		}
	}

	if len(initial) > 0 {
		all, err := d.Store.AllRoomAccountData(ctx, in.UserID, d.MSC3391Enabled)
		if err != nil {
			return nil, err
		}
		for _, roomID := range initial {
			// An entry ALWAYS, even an empty one. A room being sent from
			// scratch is being described in full, and "this room has no
			// account data" is part of that description; a room merely
			// updated is omitted when nothing changed. Synapse draws the
			// line in the same place, and the reference returns `[]` here
			// where we first returned nothing at all.
			out.Rooms[roomID] = []json.RawMessage{}
			if entries, ok := all[roomID]; ok && len(entries) > 0 {
				if err := addRoomAccountData(out, roomID, entries); err != nil {
					return nil, err
				}
			}
		}
	}

	// Record what the connection now has.
	sent := append(append([]string{}, initial...), sortedKeys(boolSet(previously))...)
	sort.Strings(sent)
	in.New.AccountData.RecordSentRooms(sent)

	// And which rooms have account data we did NOT send, so they resume from
	// here rather than looking up to date when they next come into range.
	if in.From != nil {
		var missing []string
		for roomID := range changedByRoom {
			if !relevant[roomID] {
				missing = append(missing, roomID)
			}
		}
		sort.Strings(missing)
		in.New.AccountData.RecordUnsentRooms(missing, strconv.FormatInt(in.From.AccountData, 10))
	}
	return out, nil
}

// addRoomAccountData appends a room's account data, at most ONE entry per type.
//
// Synapse holds per-room account data as a map from type to content, so a type
// appears at most once. Ours is a list, so this is where that invariant is
// re-imposed. The store already drops a stored `m.tag` when it synthesises one
// (see AllRoomAccountData); this is the same guarantee applied to the merged
// result, since a room can be filled from more than one query here.
func addRoomAccountData(out *accountDataJSON, roomID string, entries []store.AccountDataEntry) error {
	seen := map[string]bool{}
	for _, existing := range out.Rooms[roomID] {
		seen[gjson.GetBytes(existing, "type").String()] = true
	}
	for _, e := range entries {
		if seen[e.Type] {
			continue
		}
		seen[e.Type] = true
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return err
		}
		out.Rooms[roomID] = append(out.Rooms[roomID], body)
	}
	return nil
}

func boolSet[V any](m map[string]V) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

// --- receipts (MSC3960) ---

type receiptsJSON struct {
	Rooms map[string]json.RawMessage `json:"rooms"`
}

// receiptsExtension serves read receipts, tracked per connection like account
// data.
//
// One rule is not about tracking at all: a room sent from scratch, or one whose
// timeline was expanded, has its receipts re-sent regardless of what the
// connection was told before. A client that has just been handed a screenful of
// timeline needs the read markers that go with it.
func receiptsExtension(
	ctx context.Context, d Deps, in extensionInputs, scope *ExtensionScope,
) (*receiptsJSON, error) {

	out := &receiptsJSON{Rooms: map[string]json.RawMessage{}}
	relevant := relevantRooms(in, scope)

	var initial []string
	previously := map[string]int64{}
	var live []string
	for _, roomID := range sortedKeys(relevant) {
		if in.From == nil {
			initial = append(initial, roomID)
			continue
		}
		if room := in.Rooms[roomID]; room != nil && (room.Initial || room.UnstableExpandedTimeline) {
			initial = append(initial, roomID)
			continue
		}
		status := in.Previous.Receipts.HaveSentRoom(roomID)
		switch status.Status {
		case slidingstore.FlagLive:
			live = append(live, roomID)
		case slidingstore.FlagPreviously:
			pos, err := strconv.ParseInt(status.LastToken, 10, 64)
			if err != nil {
				initial = append(initial, roomID)
				continue
			}
			previously[roomID] = pos
		default:
			initial = append(initial, roomID)
		}
	}

	byRoom := map[string][]store.ReceiptRow{}

	if len(live) > 0 && in.From != nil {
		rows, err := d.Store.ReceiptsSince(ctx, live,
			in.From.Receipt.Stream, in.Now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for roomID, r := range rows {
			byRoom[roomID] = append(byRoom[roomID], r...)
		}
	}
	for roomID, from := range previously {
		rows, err := d.Store.ReceiptsSince(ctx, []string{roomID}, from, in.Now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for id, r := range rows {
			byRoom[id] = append(byRoom[id], r...)
		}
	}
	if len(initial) > 0 {
		// A room sent from scratch gets receipts for the events IN ITS
		// TIMELINE, plus the user's own wherever they point. Not every receipt
		// in the room: that list grows with the membership, and sending all of
		// it beside a three-event timeline is most of the response for none of
		// the value. Synapse notes this is in the spec.
		byEvent := map[string][]string{}
		for _, roomID := range initial {
			room := in.Rooms[roomID]
			if room == nil {
				continue
			}
			for _, ev := range room.Timeline {
				if id := gjson.GetBytes(ev, "event_id").String(); id != "" {
					byEvent[roomID] = append(byEvent[roomID], id)
				}
			}
		}
		rows, err := d.Store.ReceiptsForEvents(ctx, byEvent)
		if err != nil {
			return nil, err
		}
		for roomID, r := range rows {
			byRoom[roomID] = append(byRoom[roomID], r...)
		}

		// The user's own read position, wherever it points -- without it a
		// client cannot draw its own unread marker.
		mine, err := d.Store.ReceiptsForUserInRooms(ctx, in.UserID, initial, in.Now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for roomID, r := range mine {
			byRoom[roomID] = append(byRoom[roomID], r...)
		}
	}

	// No receipt in a server announcement room is shared (Synapse 1.165).
	var announcement map[string]bool
	if d.AnnouncementRooms != nil && len(byRoom) > 0 {
		var err error
		if announcement, err = d.AnnouncementRooms(ctx); err != nil {
			return nil, err
		}
	}
	for _, roomID := range sortedKeys(boolSet(byRoom)) {
		// The same renderer classic sync uses, which is what applies the
		// private-receipt rule: a m.read.private receipt belongs to its owner
		// and nobody else.
		body, err := eventfilter.ReceiptEvent(roomID, byRoom[roomID], in.UserID, true, announcement[roomID])
		if err != nil {
			return nil, err
		}
		if body == nil {
			continue
		}
		// The extension carries type and content, without the room_id the
		// legacy shape has -- the room is the map key.
		var full map[string]json.RawMessage
		if err := json.Unmarshal(body, &full); err != nil {
			return nil, err
		}
		delete(full, "room_id")
		trimmed, err := json.Marshal(full)
		if err != nil {
			return nil, err
		}
		out.Rooms[roomID] = trimmed
	}

	sent := append(append([]string{}, initial...), sortedKeys(boolSet(previously))...)
	sort.Strings(sent)
	in.New.Receipts.RecordSentRooms(sent)

	// Only rooms we have previously sent receipts for need marking: a NEVER
	// room is handled correctly when it first comes into range, and a
	// PREVIOUSLY one already points where it should.
	if in.From != nil {
		var candidates []string
		for roomID, status := range in.Previous.Receipts.All() {
			if status.Status == slidingstore.FlagLive && !relevant[roomID] {
				candidates = append(candidates, roomID)
			}
		}
		sort.Strings(candidates)
		if len(candidates) > 0 {
			rows, err := d.Store.ReceiptsSince(ctx, candidates,
				in.From.Receipt.Stream, in.Now.Receipt.MaxStreamPos())
			if err != nil {
				return nil, err
			}
			changed := make([]string, 0, len(rows))
			for roomID := range rows {
				changed = append(changed, roomID)
			}
			sort.Strings(changed)
			in.New.Receipts.RecordUnsentRooms(changed, strconv.FormatInt(in.From.Receipt.Stream, 10))
		}
	}
	return out, nil
}

// --- thread subscriptions (MSC4308) ---

type threadSubscriptionsJSON struct {
	Subscribed   map[string]map[string]threadSubJSON   `json:"subscribed,omitempty"`
	Unsubscribed map[string]map[string]threadUnsubJSON `json:"unsubscribed,omitempty"`
	PrevBatch    *string                               `json:"prev_batch,omitempty"`
}

type threadSubJSON struct {
	Automatic bool  `json:"automatic"`
	BumpStamp int64 `json:"bump_stamp"`
}

type threadUnsubJSON struct {
	BumpStamp int64 `json:"bump_stamp"`
}

// threadSubscriptionsExtension reports which threads the user started or
// stopped following.
//
// Returns nil -- and so is omitted entirely -- when nothing changed, unlike the
// extensions above. That is Synapse's choice and worth matching: a client
// polling every thirty seconds would otherwise carry an empty object for ever.
func threadSubscriptionsExtension(
	ctx context.Context, d Deps, in extensionInputs, req *ThreadSubscriptionsExtension,
) (*threadSubscriptionsJSON, error) {

	var from int64
	if in.From != nil {
		from = in.From.ThreadSubscriptions
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	updates, err := d.Store.ThreadSubscriptionsSince(
		ctx, in.UserID, from, in.Now.ThreadSubscriptions, limit)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return nil, nil
	}

	out := &threadSubscriptionsJSON{}
	for _, u := range updates {
		if u.Subscribed {
			if out.Subscribed == nil {
				out.Subscribed = map[string]map[string]threadSubJSON{}
			}
			if out.Subscribed[u.RoomID] == nil {
				out.Subscribed[u.RoomID] = map[string]threadSubJSON{}
			}
			out.Subscribed[u.RoomID][u.ThreadRoot] = threadSubJSON{
				Automatic: u.Automatic, BumpStamp: u.StreamID,
			}
			continue
		}
		if out.Unsubscribed == nil {
			out.Unsubscribed = map[string]map[string]threadUnsubJSON{}
		}
		if out.Unsubscribed[u.RoomID] == nil {
			out.Unsubscribed[u.RoomID] = map[string]threadUnsubJSON{}
		}
		out.Unsubscribed[u.RoomID][u.ThreadRoot] = threadUnsubJSON{BumpStamp: u.StreamID}
	}

	// Filling the limit means there may be more behind, and the client is told
	// where to paginate from. The minus one is because the bound is inclusive
	// and the oldest returned update has already been seen.
	if len(updates) == limit {
		prev := "ts" + strconv.FormatInt(updates[0].StreamID-1, 10)
		out.PrevBatch = &prev
	}
	return out, nil
}

// --- sticky events (MSC4354 / MSC4480) ---

type stickyEventsJSON struct {
	Rooms map[string]stickyRoomJSON `json:"rooms"`
}

type stickyRoomJSON struct {
	Events []json.RawMessage `json:"events"`
}

// stickyEventsExtension is get_sticky_events_extension_response (Synapse
// 1.165, #20363 and #20431). It returns nil, and the extension is left out,
// when it is off or no room has a sticky event the user may see; it is
// called with req nil (off) too, because rooms that gain sticky events
// while out of scope are still tracked, so turning it back on, or a room
// coming back into scope, delivers what was missed.
//
// There is no `since` of its own: the connection position carries it. Each
// in-scope room is read from the start of the stream (its first time, a
// newly joined room, or one sent from scratch), from where it was left
// (PREVIOUSLY), or from the request's pos (LIVE). The limit is shared,
// rooms in groups by where they start; a group the budget runs out in is
// left PREVIOUSLY at the position it reached, so the next response goes on
// from there without holding the pos back.
//
// Events already in a room's timeline are dropped when serialising, and
// may leave a room with an empty list: the decision to send a room is made
// before that, as Synapse's servlet makes it after the handler.
func stickyEventsExtension(
	ctx context.Context, d Deps, in extensionInputs, req *StickyEventsExtension,
) (*stickyEventsJSON, error) {
	if req == nil || !req.Enabled {
		return nil, recordStickyOutOfScope(ctx, d, in, nil)
	}
	relevant := relevantRooms(in, &req.ExtensionScope)
	if len(relevant) == 0 {
		return nil, recordStickyOutOfScope(ctx, d, in, nil)
	}

	byRoom, err := selectStickyEvents(ctx, d, in, req, relevant)
	if err != nil {
		return nil, err
	}

	out := &stickyEventsJSON{Rooms: map[string]stickyRoomJSON{}}
	for _, roomID := range sortedKeys(boolSet(byRoom)) {
		ids := byRoom[roomID]
		version := ""
		if r, ok := in.Membership[roomID]; ok {
			version = r.RoomVersion
		}
		events, err := d.Store.EventsByID(ctx, ids, version)
		if err != nil {
			return nil, err
		}
		loaded := make([]store.TimelineEvent, 0, len(ids))
		for _, id := range ids {
			ev, ok := events[id]
			if !ok {
				// The sticky row outlived its event, which a purge can do.
				continue
			}
			loaded = append(loaded, store.TimelineEvent{
				Stored: ev.Stored, Sender: ev.Sender, StateKey: ev.StateKey,
			})
		}

		// The ordinary visibility filter, which since Synapse v1.164 exempts
		// a still-sticky event from history visibility itself (#20295):
		// "Any joined user is authorised to see sticky events for the
		// duration they remain sticky." It also works out the caller's
		// membership at each event, which MSC4115 puts in `unsigned`.
		// get_events_as_list without prev content: what a cached copy carries.
		for i := range loaded {
			d.Store.AttachCachedPrevContent(&loaded[i].Stored)
		}
		filtered, err := eventfilter.ForClient(ctx, d.Store, roomID, in.UserID,
			loaded, false, in.NowMS, d.MSC4354Enabled, nil)
		if err != nil {
			return nil, err
		}
		if len(filtered.Events) == 0 {
			// Nothing this user may see: the room is not sent at all.
			continue
		}

		// Already in the room's timeline: the client learns of them there.
		inTimeline := map[string]bool{}
		if room := in.Rooms[roomID]; room != nil {
			for _, ev := range room.Timeline {
				inTimeline[gjson.GetBytes(ev, "event_id").String()] = true
			}
		}
		redactions, err := d.Store.Redactions(ctx, ids)
		if err != nil {
			return nil, err
		}
		cfg := d.EventConfigAdmin(in.UserID, in.DeviceID, 0, filtered.AdminMetadata)
		rendered := []json.RawMessage{}
		for i, ev := range filtered.Events {
			if inTimeline[ev.EventID] {
				continue
			}
			stored := ev.Stored
			if i < len(filtered.Memberships) {
				stored.Membership = filtered.Memberships[i]
			}
			if err := eventfilter.AttachRedaction(&stored, redactions, in.NowMS, cfg); err != nil {
				return nil, err
			}
			// One that has stopped being sticky since it was selected is
			// left out (#20349).
			body, ok, err := clientevent.SerializeStickyOnly(stored, in.NowMS, cfg)
			if err != nil {
				return nil, err
			}
			if ok {
				rendered = append(rendered, body)
			}
		}
		out.Rooms[roomID] = stickyRoomJSON{Events: rendered}
	}

	if err := recordStickyOutOfScope(ctx, d, in, relevant); err != nil {
		return nil, err
	}
	if len(out.Rooms) == 0 {
		return nil, nil
	}
	return out, nil
}

// selectStickyEvents reads the sticky events of the in-scope rooms, by
// group and within the limit, and records in in.New how far each room was
// sent: the first half of get_sticky_events_extension_response.
func selectStickyEvents(ctx context.Context, d Deps, in extensionInputs,
	req *StickyEventsExtension, relevant map[string]bool) (map[string][]string, error) {

	var initial []string
	previously := map[string][]string{} // last token -> rooms
	var live []string
	for _, roomID := range sortedKeys(relevant) {
		if in.From == nil || in.NewlyJoined[roomID] {
			initial = append(initial, roomID)
			continue
		}
		if room := in.Rooms[roomID]; room != nil && room.Initial {
			initial = append(initial, roomID)
			continue
		}
		switch status := in.Previous.StickyEvents.HaveSentRoom(roomID); status.Status {
		case slidingstore.FlagLive:
			live = append(live, roomID)
		case slidingstore.FlagPreviously:
			previously[status.LastToken] = append(previously[status.LastToken], roomID)
		default:
			initial = append(initial, roomID)
		}
	}

	type group struct {
		rooms []string
		from  int64
		token string // as recorded
	}
	var groups []group
	if len(initial) > 0 {
		// StreamToken.START: every unexpired sticky event.
		groups = append(groups, group{initial, 0, "0"})
	}
	for _, token := range sortedKeys(boolSet(previously)) {
		from, err := stickyPosition(token)
		if err != nil {
			// A position we did not write: start the rooms over, which
			// re-sends rather than skips.
			groups = append(groups, group{previously[token], 0, token})
			continue
		}
		groups = append(groups, group{previously[token], from, token})
	}
	if len(live) > 0 {
		from := in.From.StickyEvents
		groups = append(groups, group{live, from, strconv.FormatInt(from, 10)})
	}

	remaining := min(req.Limit, stickyMaxEventsInSync)
	byRoom := map[string][]string{}
	for _, g := range groups {
		if remaining <= 0 {
			// Out of budget: these rooms resume where they would have
			// started.
			in.New.StickyEvents.RecordPartiallySentRooms(g.rooms, g.token)
			continue
		}
		readTo, rows, err := d.Store.StickyEvents(ctx, g.rooms, g.from, in.Now.StickyEvents, in.NowMS, remaining)
		if err != nil {
			return nil, err
		}
		read := 0
		for roomID, ids := range rows {
			byRoom[roomID] = append(byRoom[roomID], ids...)
			read += len(ids)
		}
		if read >= remaining {
			in.New.StickyEvents.RecordPartiallySentRooms(g.rooms, strconv.FormatInt(readTo, 10))
		} else {
			in.New.StickyEvents.RecordSentRooms(g.rooms)
		}
		remaining -= read
	}

	return byRoom, nil
}

// recordStickyOutOfScope is _record_sticky_events_in_out_of_scope_rooms:
// a LIVE room outside relevant that gained sticky events (expired ones
// awaiting cleanup included) since the request's pos goes PREVIOUSLY at
// that pos, to be caught up when it is back in scope. PREVIOUSLY rooms
// keep their position and NEVER rooms will get everything anyway.
func recordStickyOutOfScope(ctx context.Context, d Deps, in extensionInputs, relevant map[string]bool) error {
	if in.From == nil {
		return nil
	}
	var outOfScope []string
	for roomID, hs := range in.Previous.StickyEvents.All() {
		if hs.Status == slidingstore.FlagLive && !relevant[roomID] {
			outOfScope = append(outOfScope, roomID)
		}
	}
	sort.Strings(outOfScope)
	changed, err := d.Store.StickyRoomsBetween(ctx, outOfScope, in.From.StickyEvents, in.Now.StickyEvents)
	if err != nil {
		return err
	}
	in.New.StickyEvents.RecordUnsentRooms(sortedKeys(changed), strconv.FormatInt(in.From.StickyEvents, 10))
	return nil
}

// stickyPosition reads a sticky stream position as Synapse records it, a
// MultiWriterStreamToken: with one events writer, a bare integer. A vector
// clock (several writers) is read at its minimum, which can only re-send.
func stickyPosition(token string) (int64, error) {
	mw, err := streamtoken.ParseMultiWriter(token)
	if err != nil {
		return 0, err
	}
	return mw.Stream, nil
}

// stickyMaxEventsInSync is Synapse's StickyEvent.MAX_EVENTS_IN_SYNC.
const stickyMaxEventsInSync = 100

// pushRulesEvent renders the synthesised m.push_rules account data event.
func pushRulesEvent(ctx context.Context, d Deps, userID string) (json.RawMessage, error) {
	rules, enabled, err := d.Store.PushRules(ctx, userID)
	if err != nil {
		return nil, err
	}
	content, err := pushrules.Format(userID, rules, enabled, d.PushRuleFeatures)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"type": "m.push_rules", "content": content})
}
