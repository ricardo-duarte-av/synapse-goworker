package replication

import (
	"encoding/json"
	"sort"

	"github.com/tidwall/gjson"
)

// rowSubjects works out which rooms and users a replication row concerns, so a
// waiting sync can be woken only when something it cares about happens.
//
// The rows are positional JSON arrays, one shape per stream
// (synapse/replication/tcp/streams/*.py). Getting a shape wrong costs a missed
// wakeup rather than a wrong answer -- the sync recomputes from the database
// either way -- but a missed wakeup is a client that hangs until its timeout.
func rowSubjects(stream, row string) (roomIDs, userIDs []string) {
	d := rowDetails(stream, row)
	return d.RoomIDs, d.UserIDs
}

// RowDetail is what a replication row says, including the parts the notifier
// does not need.
//
// The notifier only ever asks "who should be woken", for which a room id is
// enough. Cache invalidation needs more: dropping a user's room list requires
// knowing that this was an m.room.member event and WHOSE, which is in the row
// and was previously discarded.
type RowDetail struct {
	RoomIDs []string
	UserIDs []string
	// Type and StateKey are set for state events on the events stream, empty
	// otherwise.
	Type     string
	StateKey string
}

// rowDetails parses one replication row.
//
// Kept as the single parser with rowSubjects as a thin wrapper, so the
// notifier's behaviour is provably unchanged by anything added here.
func rowDetails(stream, row string) RowDetail {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return RowDetail{}
	}
	a := r.Array()
	get := func(i int) string {
		if i < len(a) {
			return a[i].String()
		}
		return ""
	}

	switch stream {
	case StreamEvents:
		// The events stream merges three sources into one, and each has its own
		// row shape and its own leading type id (EventsStream._update_function,
		// replication/tcp/streams/events.py). Only "ev" used to be parsed, so
		// the other two named no room and therefore woke EVERY parked client --
		// and a state change emits one of them alongside every "ev" row it
		// accompanies, so a single join woke the world twice.
		if len(a) < 2 {
			break
		}
		inner := a[1].Array()
		at := func(i int) string {
			if i < len(inner) {
				return inner[i].String()
			}
			return ""
		}
		switch a[0].String() {
		case "ev":
			// [event_id, room_id, type, state_key, redacts, relates_to,
			//  membership, rejected, outlier]
			if len(inner) >= 2 {
				d := RowDetail{
					RoomIDs:  []string{inner[1].String()},
					Type:     at(2),
					StateKey: at(3),
				}
				// A membership event also names its target, because the target
				// is not yet in the room and so is not waiting on it: without
				// this, creating a room, being invited, or joining parks the
				// client until something unrelated wakes it. Synapse's
				// extra_users (replication/tcp/client.py, on_rdata). Only "ev"
				// rows: Synapse passes nothing for "state" ones.
				if d.Type == "m.room.member" && d.StateKey != "" {
					d.UserIDs = []string{d.StateKey}
				}
				return d
			}
		case "state":
			// [room_id, type, state_key, event_id] -- a current-state delta.
			// Carries type and state key like an "ev" row, and deliberately so:
			// a state reset moves a membership with no event of its own, and
			// this is the only row that says as much.
			if len(inner) >= 1 {
				return RowDetail{
					RoomIDs:  []string{inner[0].String()},
					Type:     at(1),
					StateKey: at(2),
				}
			}
		case "state-all":
			// [room_id] -- stands in for a burst of state deltas too large to
			// send individually. It names no type, which is the point: the
			// listener is being told to treat the room's whole state as changed.
			if len(inner) >= 1 {
				return RowDetail{RoomIDs: []string{inner[0].String()}}
			}
		}
	case StreamTyping:
		// [room_id, [user_id, ...]]
		return RowDetail{RoomIDs: []string{get(0)}}
	case StreamReceipts:
		// [room_id, receipt_type, user_id, event_id, thread_id, data]
		return RowDetail{RoomIDs: []string{get(0)}, UserIDs: []string{get(2)}}
	case StreamPresence:
		// [user_id, state, ...]
		return RowDetail{UserIDs: []string{get(0)}}
	case StreamAccountData:
		// [user_id, room_id, account_data_type]
		if room := get(1); room != "" {
			return RowDetail{RoomIDs: []string{room}, UserIDs: []string{get(0)}}
		}
		return RowDetail{UserIDs: []string{get(0)}}
	case StreamToDevice, StreamDeviceLists, StreamPushRules:
		// [user_id, ...] for all three.
		return RowDetail{UserIDs: []string{get(0)}}
	case StreamStickyEvents:
		// [room_id, event_id]. Sticky events ARE served in /sync, so this is
		// the one of the four below that has to wake anybody at all.
		return RowDetail{RoomIDs: []string{get(0)}}
	case StreamThreadSubscriptions:
		// [user_id, room_id, event_id]
		return RowDetail{UserIDs: []string{get(0)}, RoomIDs: []string{get(1)}}
	case StreamUnPartialStatedRoom:
		// [room_id]. A room finishing a faster-remote-join changes what a
		// non-lazy incremental sync owes it -- Synapse's
		// forced_newly_joined_room_ids.
		return RowDetail{RoomIDs: []string{get(0)}}
	case StreamProfileUpdates:
		// [user_id, action, affected_fields]. The profile table, not the
		// m.room.member events a display name change also produces -- those
		// arrive on the events stream and wake their own rooms.
		return RowDetail{UserIDs: []string{get(0)}}
	}

	// Everything else, quarantined_media included, keeps the conservative
	// answer: no subjects, which the notifier reads as "wake everyone".
	//
	// quarantined_media is deliberately left there even though its row --
	// [origin, media_id, quarantined] -- names neither a room nor a user, and
	// its position only ever reaches a token. Silencing it would be a change
	// to when clients are woken that nothing in Synapse's source justifies,
	// and an over-wake costs a recomputed sync while an under-wake costs a
	// client hanging until its timeout. Those are not symmetric.
	return RowDetail{}
}

// updateTyping replaces a room's typist list.
//
// The row carries the WHOLE current set for that room, not a delta, so a user
// who stopped typing simply is not in it. Merging instead of replacing would
// leave people typing forever.
// TypingChangedSince returns the rooms whose typists changed after a typing
// stream position, oldest change first.
//
// Only rooms this process has SEEN change since it started: the serials are
// built from the replication stream, and a room that has been quiet since
// startup has no entry at all. That is the same limitation typing has
// everywhere here -- there is nothing in the database to fall back on -- and it
// resolves itself as soon as anyone types.
func (s *Subscriber) TypingChangedSince(pos int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.live {
		return nil
	}
	type change struct {
		roomID string
		serial int64
	}
	var changed []change
	for roomID, serial := range s.typingSerial {
		if serial > pos {
			changed = append(changed, change{roomID, serial})
		}
	}
	sort.Slice(changed, func(i, j int) bool {
		if changed[i].serial != changed[j].serial {
			return changed[i].serial < changed[j].serial
		}
		return changed[i].roomID < changed[j].roomID
	})
	out := make([]string, 0, len(changed))
	for _, c := range changed {
		out = append(out, c.roomID)
	}
	return out
}

func (s *Subscriber) updateTyping(row string, pos int64) {
	var parsed []json.RawMessage
	if err := json.Unmarshal([]byte(row), &parsed); err != nil || len(parsed) < 2 {
		return
	}
	var roomID string
	if err := json.Unmarshal(parsed[0], &roomID); err != nil {
		return
	}
	var users []string
	if err := json.Unmarshal(parsed[1], &users); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// A batched row carries the literal token "batch" rather than a position,
	// so pos is 0 for all but the last of a batch. The stream position already
	// held is the best available answer, and it never goes backwards.
	serial := pos
	if serial <= 0 {
		serial = s.positions[StreamTyping]
	}
	if serial > s.typingSerial[roomID] {
		s.typingSerial[roomID] = serial
	}

	if len(users) == 0 {
		delete(s.typing, roomID)
		return
	}
	s.typing[roomID] = users
}
