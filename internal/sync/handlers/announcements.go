package handlers

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Server announcement rooms (Synapse 1.165, experimental_features.
// server_announcements): rooms main creates at start, world-readable and not
// federated, that every local user is shown in /sync as a synthetic join
// without being a member. What sync reproduces of it:
//
//   - legacy /sync delivers each room to every local user it is configured
//     for (_get_announcement_room_entries): once it has had an announcement
//     (a non-state event), with the user's own membership taken out of state
//     and timeline and a synthetic join put in the state, the m.server_notice
//     tag, and no unread counts;
//   - no receipt in such a room is shared, on any endpoint: a user sees only
//     their own (read_receipt_should_be_shared);
//   - a user who left one keeps reading its state as one who never joined
//     (check_user_in_room_or_world_readable).
//
// Tag writes still go to main (TagServlet's checks), and so does
// /rooms/{roomId}/initialSync of such a room by a user who is not joined.

// announcementSet is get_server_announcement_room_ids (all) and
// get_server_announcement_room_ids_for_user (forUser) for one request.
type announcementSet struct {
	all, forUser map[string]bool
}

// announcementRooms reads them: the rooms main has stored for the ids
// configured now, and of those the ones not excluding userID. Nothing is
// queried while no room is configured.
func announcementRooms(ctx context.Context, d Deps, userID string) (announcementSet, error) {
	var set announcementSet
	if d.Announcements == nil {
		return set, nil
	}
	cfg := d.Announcements()
	if !cfg.Configured() {
		return set, nil
	}
	byConfig, err := d.Store.AnnouncementRoomsByConfigID(ctx)
	if err != nil {
		return set, err
	}
	excluded := cfg.ExcludedConfigIDs(userID)
	set.all, set.forUser = map[string]bool{}, map[string]bool{}
	for _, id := range cfg.ConfigIDs() {
		roomID, ok := byConfig[id]
		if !ok {
			continue
		}
		set.all[roomID] = true
		if !excluded[id] {
			set.forUser[roomID] = true
		}
	}
	return set, nil
}

// syntheticJoinRooms is SyncResultBuilder.synthetic_join_announcement_room_ids:
// the announcement rooms delivered to the user that the filter keeps, less
// those they are joined to, those excluded from sync, and those whose last
// membership change in the window (lastMembership) is anything but a leave
// or a ban -- an invite or a knock is shown as itself.
func syntheticJoinRooms(d Deps, ann announcementSet, f *filter.Collection, joined map[string]bool,
	lastMembership map[string]string) map[string]bool {

	if len(ann.forUser) == 0 {
		return nil
	}
	ids := make([]string, 0, len(ann.forUser))
	for id := range ann.forUser {
		ids = append(ids, id)
	}
	out := map[string]bool{}
	for _, id := range f.FilterRooms(ids) {
		if joined[id] || d.ExcludedRooms[id] {
			continue
		}
		switch lastMembership[id] {
		case "", "leave", "ban":
			out[id] = true
		}
	}
	return out
}

// announcementEntry is a RoomSyncResultBuilder from _get_announcement_room_
// entries: a room to report as joined, with the events loaded for it and
// whether it gets its full state.
type announcementEntry struct {
	room      store.RoomForUser
	raw       []store.TimelineEvent
	loaded    bool // in room_to_events
	upto      streamtoken.Token
	fullState bool
}

// announcementEntries is _get_announcement_room_entries.
//
// A room shows up once it has had an announcement, so that creating it and
// joining its senders does not show it to every user. Without a since token
// (or with full_state) a room is included whenever it has ever had one; on
// an incremental sync, when it had one and there is something new for it:
// an event in the window, room account data or tags (adRooms), or ephemeral
// events for the user (ephRooms). Rooms the user left in the window (left)
// are included regardless, so the synthetic join replaces their real
// membership. An entry carries the full state without a since token, when
// its first announcement is after it, or when the user left; otherwise the
// changes since it, as a joined room's.
//
// Sending an announcement wakes nobody's sync: the room is not among the
// rooms a long poll waits on, so a waiting sync picks it up when it next
// runs, as Synapse's does.
func announcementEntries(ctx context.Context, d Deps, roomIDs map[string]bool, since *streamtoken.Token,
	now streamtoken.Token, f *filter.Collection, syncFullState bool,
	left, adRooms, ephRooms map[string]bool) ([]announcementEntry, error) {

	if len(roomIDs) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(roomIDs))
	versions := map[string]string{}
	for id := range roomIDs {
		info, err := d.Store.RoomInfo(ctx, id)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		versions[id] = info.RoomVersion
	}
	sort.Strings(ids)

	// limit=timeline_limit + 1, whatever the filter blocks: what was loaded
	// decides which rooms are included.
	lower := int64(minStreamOrdering)
	if since != nil {
		lower = since.Room.MaxStreamPos()
	}
	nowPos := now.Room.MaxStreamPos()
	loaded, err := d.Store.RoomTimelineSince(ctx, ids, versions, lower, nowPos, f.TimelineLimit()+1)
	if err != nil {
		return nil, err
	}

	include := map[string]bool{}
	for id, events := range loaded {
		for _, ev := range events {
			if !ev.IsState {
				include[id] = true
				break
			}
		}
	}
	for id := range left {
		include[id] = true
	}

	var check []string
	if since == nil || syncFullState {
		for _, id := range ids {
			if !include[id] {
				check = append(check, id)
			}
		}
	} else {
		for _, id := range ids {
			switch {
			case len(loaded[id]) > 0 && !left[id]:
				check = append(check, id)
			case !include[id] && (adRooms[id] || ephRooms[id]):
				check = append(check, id)
			}
		}
	}
	first, err := d.Store.FirstNonStateEventPositions(ctx, check, nowPos)
	if err != nil {
		return nil, err
	}
	for id := range first {
		include[id] = true
	}

	var instances map[string]int
	out := make([]announcementEntry, 0, len(include))
	for _, id := range ids {
		if !include[id] {
			continue
		}
		e := announcementEntry{room: store.RoomForUser{RoomID: id, RoomVersion: versions[id], Membership: "join"}}
		if events, ok := loaded[id]; ok && len(events) > 0 {
			e.raw, e.loaded = events, true
			e.upto = now.WithRoomKey(streamtoken.Live(events[0].StreamOrdering - 1))
		} else if since != nil {
			e.upto = *since
		} else {
			// Without a since token every room has an entry in
			// room_to_events; one whose events were all rejected is
			// paginated from the now token instead.
			e.upto = now
		}
		pos, ok := first[id]
		e.fullState = since == nil || !ok
		if ok && since != nil {
			if instances == nil {
				if instances, err = d.Store.InstanceIDs(ctx); err != nil {
					return nil, err
				}
			}
			// PersistedEventPosition.persisted_after: the token's position
			// for that writer, or its minimum for one it does not list.
			at := since.Room.Stream
			if iid, known := instances[pos.InstanceName]; known {
				at = since.Room.StreamPosForInstance(iid)
			}
			e.fullState = pos.Stream > at
		}
		out = append(out, e)
	}
	return out, nil
}

// minStreamOrdering bounds a backwards load with no since token from below:
// Synapse's to_key=None has no lower bound, backfilled (negative) events
// included.
const minStreamOrdering = -(1 << 62)

// announcementAccountData is a room's account data events for an
// announcement entry: the m.tag event first, its tags the user's own
// (tags: the current set on a full-state entry, those changed in the
// window otherwise, or nil) with m.server_notice ahead of them, as
// `{SERVER_NOTICE_ROOM_TAG: {}, **(tags or {})}` builds them; then the
// room's other account data.
func announcementAccountData(tags json.RawMessage, rest []store.AccountDataEntry) []store.AccountDataEntry {
	var b strings.Builder
	notice := `{}`
	if v := gjson.GetBytes(tags, `tags.m\.server_notice`); v.Exists() {
		notice = v.Raw
	}
	b.WriteString(`{"tags":{"m.server_notice":`)
	b.WriteString(notice)
	gjson.GetBytes(tags, "tags").ForEach(func(k, v gjson.Result) bool {
		if k.String() == "m.server_notice" {
			return true
		}
		key, _ := json.Marshal(k.String())
		b.WriteByte(',')
		b.Write(key)
		b.WriteByte(':')
		b.WriteString(v.Raw)
		return true
	})
	b.WriteString(`}}`)
	out := []store.AccountDataEntry{{Type: "m.tag", Content: json.RawMessage(b.String())}}
	for _, e := range rest {
		if e.Type != "m.tag" {
			out = append(out, e)
		}
	}
	return out
}

// withUpdatedTags is account_data_by_room with tags_by_room merged in, as
// _generate_room_entry assembles them: a room whose tags changed in the
// window gets an m.tag event (its whole current set, empty when the last tag
// was removed) ahead of its other room account data.
func withUpdatedTags(byRoom map[string][]store.AccountDataEntry,
	tags map[string]json.RawMessage) map[string][]store.AccountDataEntry {

	if len(tags) == 0 {
		return byRoom
	}
	out := make(map[string][]store.AccountDataEntry, len(byRoom)+len(tags))
	for id, entries := range byRoom {
		out[id] = entries
	}
	for id, t := range tags {
		merged := []store.AccountDataEntry{{Type: "m.tag", Content: t}}
		for _, e := range byRoom[id] {
			if e.Type != "m.tag" {
				merged = append(merged, e)
			}
		}
		out[id] = merged
	}
	return out
}

// fullStateAccountData is a full-state room's account data: its current
// tags (get_tags_for_room, none when it has none) instead of whatever the
// window said of them.
func fullStateAccountData(ctx context.Context, d Deps, userID, roomID string,
	entries []store.AccountDataEntry) ([]store.AccountDataEntry, error) {

	tags, err := d.Store.RoomTags(ctx, userID, roomID)
	if err != nil {
		return nil, err
	}
	out := make([]store.AccountDataEntry, 0, len(entries)+1)
	if tags != nil {
		out = append(out, store.AccountDataEntry{Type: "m.tag", Content: tags})
	}
	for _, e := range entries {
		if e.Type != "m.tag" {
			out = append(out, e)
		}
	}
	return out, nil
}

// announcementView is what an announcement entry changes in the room entry
// builders: the user's own membership taken out of state and timeline (the
// rest/client/sync.py encoder does it, after everything else was computed),
// the synthetic join added to the state, and no unread counts.
type announcementView struct {
	createTS int64
	// senders collects the senders of the timeline events taken out, which
	// Synapse's lazy_loading_members_user_ids still sees.
	senders *senderSet
	// stripped are the timeline events taken out. Synapse loaded them with
	// get_prev_content before its encoder dropped them, so they still go
	// through AttachPrevContent, for the event cache it fills (prevCache).
	stripped []*clientevent.Stored
}

// senderSet is a set of user IDs shared by rooms built concurrently.
type senderSet struct {
	mu  sync.Mutex
	ids []string
}

func (s *senderSet) add(id string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.ids = append(s.ids, id)
	s.mu.Unlock()
}

func (s *senderSet) list() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ids...)
}

// newAnnouncementView reads what a room's view needs.
func newAnnouncementView(ctx context.Context, d Deps, roomID string, senders *senderSet) (*announcementView, error) {
	ts, err := d.Store.CreateEventTS(ctx, roomID)
	if err != nil {
		return nil, err
	}
	return &announcementView{createTS: ts, senders: senders}, nil
}

// stripOwnMembership removes the user's membership from the timeline (and
// its memberships, index for index) and the state map.
func (v *announcementView) stripOwnMembership(userID string, messages []store.TimelineEvent,
	memberships []string, state map[store.StateKey]string) ([]store.TimelineEvent, []string) {

	if v == nil {
		return messages, memberships
	}
	delete(state, store.StateKey{Type: memberType, StateKey: userID})
	keptM := make([]store.TimelineEvent, 0, len(messages))
	keptS := make([]string, 0, len(memberships))
	for i, ev := range messages {
		if ev.Type == memberType && ev.IsState && ev.StateKey == userID {
			v.senders.add(ev.Sender)
			stored := ev.Stored
			v.stripped = append(v.stripped, &stored)
			continue
		}
		keptM = append(keptM, ev)
		if i < len(memberships) {
			keptS = append(keptS, memberships[i])
		}
	}
	return keptM, keptS
}

// strippedEvents are the timeline events taken out, nil for no view.
func (v *announcementView) strippedEvents() []*clientevent.Stored {
	if v == nil {
		return nil
	}
	return v.stripped
}

// syntheticJoin is the serialised synthetic join for the state block.
func (v *announcementView) syntheticJoin(roomID, userID string, cfg clientevent.Config) (json.RawMessage, error) {
	return clientevent.SerializeSynthetic(clientevent.SyntheticJoin(roomID, userID, v.createTS), cfg)
}

// ownReceipts keeps the user's own receipt rows: the announcement receipts
// of _generate_sync_entry_for_ephemeral (get_linearized_receipts_for_user_in_
// rooms).
func ownReceipts(rows []store.ReceiptRow, userID string) []store.ReceiptRow {
	var out []store.ReceiptRow
	for _, r := range rows {
		if r.UserID == userID {
			out = append(out, r)
		}
	}
	return out
}
