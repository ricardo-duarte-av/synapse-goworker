package slidingsync

import (
	"context"
	"sort"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Making a CURRENT room list answer for a PAST token.
//
// `sliding_sync_membership_snapshots` holds a user's memberships as they are
// now. A sync answers as of its token, which is usually now but need not be --
// a client polling from an old `pos`, or a request that raced a membership
// change, both land behind. Two corrections follow, and they are different
// questions:
//
//   - The REWIND undoes membership changes that happened after the token, so a
//     room the user joined ten seconds ago does not appear in a sync answering
//     as of twenty seconds ago.
//   - NEWLY JOINED / NEWLY LEFT names the rooms whose membership changed
//     INSIDE the token range, which the response has to treat differently: a
//     newly joined room is sent in full rather than as a delta, and a newly
//     left one is kept in the list so the user's own leave is the last thing
//     they see.
//
// Ported from _get_rewind_changes_to_current_membership_to_token and
// _get_newly_joined_and_left_rooms.

// membershipRewind is what one room's membership should be rewound to.
//
// A nil entry means the room must be dropped entirely: the user joined it after
// the token, or its previous membership cannot be determined.
type membershipRewind struct {
	drop bool
	room store.SlidingRoom
}

// rewindToToken computes the corrections needed to make a current room list
// describe the memberships at `to`.
func rewindToToken(
	ctx context.Context, d Deps, userID string,
	rooms map[string]store.SlidingRoom, to streamtoken.Token,
) (map[string]membershipRewind, error) {

	if len(rooms) == 0 {
		return nil, nil
	}

	// The point the snapshot was taken at, as a room key.
	//
	// It is a VECTOR CLOCK, not a single position, and collapsing it to one
	// loses changes. Each event persister contributes its own highest position;
	// the key's base is the lowest of those -- a clock is only as advanced as
	// its slowest writer -- and every writer ahead of that base is listed
	// individually. This deployment has six persisters in active use, so a
	// single-position key would stop the rewind at the slowest of them and miss
	// everything the other five did above it.
	byInstance := map[string]int64{}
	for _, r := range rooms {
		instance := r.EventInstance
		if instance == "" {
			instance = "master"
		}
		if r.EventStream > byInstance[instance] {
			byInstance[instance] = r.EventStream
		}
	}
	var minPos int64
	first := true
	for _, pos := range byInstance {
		if first || pos < minPos {
			minPos, first = pos, false
		}
	}

	instanceIDs, err := d.Store.InstanceIDs(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := streamtoken.Live(minPos)
	names := make([]string, 0, len(byInstance))
	for name := range byInstance {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pos := byInstance[name]
		if pos <= minPos {
			// Instances lists only writers AHEAD of the base, matching how the
			// token is serialised.
			continue
		}
		id, known := instanceIDs[name]
		if !known {
			// A writer with no instance_map row cannot be addressed in a token.
			// Its position folds into the base, which is the conservative
			// direction: the rewind then looks at a wider range than needed.
			continue
		}
		snapshot.Instances = append(snapshot.Instances, streamtoken.InstancePos{ID: id, Pos: pos})
	}

	// If the snapshot is at or behind the token, nothing happened after it and
	// there is nothing to undo. This is the ordinary case.
	if snapshot.MaxStreamPos() <= to.Room.MaxStreamPos() {
		return nil, nil
	}

	changes, err := d.Store.CurrentStateDeltaMembershipChanges(
		ctx, userID, to.Room, snapshot, d.ExcludedRooms)
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 {
		return nil, nil
	}

	// Only the FIRST change after the token matters per room: stepping back
	// from it gives the membership that was in force at the token, and any
	// later change is something the token cannot see.
	firstAfter := map[string]store.DeltaMembershipChange{}
	for _, c := range changes {
		if _, seen := firstAfter[c.RoomID]; !seen {
			firstAfter[c.RoomID] = c
		}
	}

	out := map[string]membershipRewind{}
	for roomID, c := range firstAfter {
		if c.PrevEventID == "" {
			// No previous membership: the user's first membership in this room
			// is after the token, so at the token they were not in it at all.
			out[roomID] = membershipRewind{drop: true}
			continue
		}
		if c.PrevMembership == "" || c.PrevSender == "" {
			// We have a previous event id but cannot read what it said -- it
			// may have been culled. Dropping the room is the safe direction:
			// showing it with a membership we are guessing at is worse than
			// omitting it, and it comes back on the next sync.
			out[roomID] = membershipRewind{drop: true}
			continue
		}

		existing, known := rooms[roomID]
		rewound := store.SlidingRoom{
			RoomID:     roomID,
			Sender:     c.PrevSender,
			Membership: c.PrevMembership,
			EventID:    c.PrevEventID,
			// The room's own metadata is NOT rewound. Synapse keeps it too:
			// the room type and encryption flag as of an old token are not
			// worth a state resolution per room, and neither changes often.
			RoomVersion:   existing.RoomVersion,
			HasKnownState: existing.HasKnownState,
			RoomType:      existing.RoomType,
			IsEncrypted:   existing.IsEncrypted,
			EventInstance: c.PrevInstance,
			EventStream:   c.PrevStreamPos,
		}
		if !known {
			// A room the user is no longer in at all. The version has to come
			// from the room itself.
			info, err := d.Store.RoomInfo(ctx, roomID)
			if err != nil {
				// A room we cannot describe is one we cannot serve.
				out[roomID] = membershipRewind{drop: true}
				continue
			}
			rewound.RoomVersion = info.RoomVersion
		}
		out[roomID] = membershipRewind{room: rewound}
	}
	return out, nil
}

// newlyJoinedAndLeft names the rooms whose membership changed inside the token
// range.
//
// Both sets change how the response is built rather than merely what is in it.
// A newly joined room is sent in full even on an incremental sync -- a client
// that has just joined has nothing to apply a delta to. A newly left one stays
// in the list although a leave would normally remove it, so that the user's own
// leave event is the last thing they are shown.
func newlyJoinedAndLeft(
	ctx context.Context, d Deps, userID string, from *streamtoken.Token, to streamtoken.Token,
) (joined map[string]bool, left map[string]store.SlidingMembershipChange, err error) {

	joined = map[string]bool{}
	left = map[string]store.SlidingMembershipChange{}
	if from == nil {
		// An initial sync has no range, so nothing is newly anything.
		return joined, left, nil
	}

	changes, err := d.Store.SlidingMembershipChanges(ctx, userID, from.Room, to.Room, d.ExcludedRooms)
	if err != nil {
		return nil, nil, err
	}
	for roomID, c := range changes {
		switch c.Membership {
		case "join":
			joined[roomID] = true
		case "leave":
			left[roomID] = c
		}
	}
	return joined, left, nil
}
