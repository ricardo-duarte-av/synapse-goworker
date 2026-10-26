package slidingsync

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// The rewind and the newly-joined/left computation are the two corrections that
// make a CURRENT room list answer for a PAST token. They are hard to see: on a
// sync whose token is current -- almost every sync -- both are no-ops, so a
// build with either one removed compares clean against Synapse all day.
//
// So they are tested against the database directly, by picking a token from
// before a membership change that really happened and asserting the functions
// notice it.
//
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	  go test ./internal/slidingsync/ -run LiveRewind -v

// A room joined AFTER the token must not appear in a list answering as of it.
// Without the rewind the client is shown a room it had not joined yet, whose
// timeline it cannot legitimately see.
func TestLiveRewindDropsRoomsJoinedAfterTheToken(t *testing.T) {
	d, _, now, ctx := liveDeps(t)

	// A GENUINE first join: `prev_event_id IS NULL` in the delta stream, so
	// there is no earlier membership at all. The snapshot table's newest
	// membership row is not good enough -- it is often a display-name rewrite,
	// which is a join whose predecessor is also a join, and rewinding past it
	// correctly changes nothing.
	var userID, roomID string
	var joinPos int64
	err := d.Store.Pool().QueryRow(ctx, `
		SELECT s.state_key, s.room_id, s.stream_id
		  FROM current_state_delta_stream s
		  JOIN room_memberships m ON m.event_id = s.event_id
		  JOIN local_current_membership lcm
		    ON lcm.room_id = s.room_id AND lcm.user_id = s.state_key
		 WHERE s.type = 'm.room.member' AND m.membership = 'join'
		   AND s.prev_event_id IS NULL AND lcm.membership = 'join'
		 ORDER BY s.stream_id DESC LIMIT 1`).Scan(&userID, &roomID, &joinPos)
	if err != nil {
		t.Skipf("no first-joins to test against: %v", err)
	}
	t.Logf("%s joined %s at %d", userID, roomID, joinPos)

	// As of now, the room is there.
	rooms, err := d.Store.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rooms[roomID]; !ok {
		t.Fatalf("%s is not in the current room list at all", roomID)
	}

	// As of just before the join, it must not be.
	before := now
	before.Room = streamtoken.Live(joinPos - 1)

	rewind, err := rewindToToken(ctx, d, userID, rooms, before)
	if err != nil {
		t.Fatal(err)
	}
	change, found := rewind[roomID]
	if !found {
		t.Fatalf("the rewind said nothing about %s, which was joined at %d, "+
			"for a token at %d -- a client syncing from that token would be shown "+
			"a room it had not joined", roomID, joinPos, joinPos-1)
	}
	if !change.drop {
		t.Errorf("the rewind kept %s with membership %q; the user had no membership "+
			"at all before their join", roomID, change.room.Membership)
	}
}

// A membership that CHANGED after the token must be rewound to what it was,
// not dropped. Leaving a room and being shown as still in it is the failure the
// rewind's second half exists for.
func TestLiveRewindRestoresThePreviousMembership(t *testing.T) {
	d, _, now, ctx := liveDeps(t)

	// A user whose membership in some room has a previous membership behind it
	// -- a leave after a join, or a join after a leave.
	var userID, roomID, prevMembership string
	var pos int64
	err := d.Store.Pool().QueryRow(ctx, `
		SELECT s.state_key, s.room_id, m_prev.membership, s.stream_id
		  FROM current_state_delta_stream s
		  JOIN room_memberships m_prev ON m_prev.event_id = s.prev_event_id
		  JOIN room_memberships m ON m.event_id = s.event_id
		 WHERE s.type = 'm.room.member'
		   AND m_prev.membership != m.membership
		   AND s.state_key LIKE '%:aguiarvieira.pt'
		 ORDER BY s.stream_id DESC LIMIT 1`).Scan(&userID, &roomID, &prevMembership, &pos)
	if err != nil {
		t.Skipf("no membership transitions to test against: %v", err)
	}
	t.Logf("%s in %s changed from %q at %d", userID, roomID, prevMembership, pos)

	rooms, err := d.Store.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}

	before := now
	before.Room = streamtoken.Live(pos - 1)

	rewind, err := rewindToToken(ctx, d, userID, rooms, before)
	if err != nil {
		t.Fatal(err)
	}
	change, found := rewind[roomID]
	if !found {
		t.Fatalf("the rewind said nothing about %s, whose membership changed at %d", roomID, pos)
	}
	if change.drop {
		// Legitimate only if there was no readable previous membership, which
		// the query above already ruled out.
		t.Fatalf("the rewind dropped %s, but it had membership %q before the change",
			roomID, prevMembership)
	}
	if change.room.Membership != prevMembership {
		t.Errorf("rewound to %q, want %q", change.room.Membership, prevMembership)
	}
	if change.room.RoomVersion == "" {
		t.Error("the rewound room has no room version; nothing can be serialised for it")
	}
}

// A token that is already current has nothing to undo, and the rewind must do
// no work at all -- it runs on every request.
func TestLiveRewindIsANoOpAtTheCurrentToken(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	rooms, err := d.Store.SlidingRoomsForUser(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	rewind, err := rewindToToken(ctx, d, userID, rooms, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(rewind) != 0 {
		t.Errorf("the rewind found %d changes at the current token; there is nothing "+
			"after it to undo", len(rewind))
	}
}

// newly_joined and newly_left are what make a room be sent in full, or kept in
// the list at all. A range containing a real join must report it.
func TestLiveNewlyJoinedNamesARealJoin(t *testing.T) {
	d, _, now, ctx := liveDeps(t)

	var userID, roomID string
	var joinPos int64
	err := d.Store.Pool().QueryRow(ctx, `
		SELECT s.state_key, s.room_id, s.stream_id
		  FROM current_state_delta_stream s
		  JOIN room_memberships m ON m.event_id = s.event_id
		 WHERE s.type = 'm.room.member' AND m.membership = 'join'
		   AND s.prev_event_id IS NULL
		 ORDER BY s.stream_id DESC LIMIT 1`).Scan(&userID, &roomID, &joinPos)
	if err != nil {
		t.Skipf("no first-joins: %v", err)
	}

	from := now
	from.Room = streamtoken.Live(joinPos - 1)
	joined, _, err := newlyJoinedAndLeft(ctx, d, userID, &from, now)
	if err != nil {
		t.Fatal(err)
	}
	if !joined[roomID] {
		t.Errorf("%s was joined at %d but is not reported newly joined for a range "+
			"starting at %d -- it would be sent as a delta to a client with no base "+
			"to apply it to", roomID, joinPos, joinPos-1)
	}

	// And a range that starts after the join must not report it.
	after := now
	after.Room = streamtoken.Live(joinPos)
	joined, _, err = newlyJoinedAndLeft(ctx, d, userID, &after, now)
	if err != nil {
		t.Fatal(err)
	}
	if joined[roomID] {
		t.Errorf("%s is reported newly joined for a range starting AT the join; the "+
			"range is exclusive at its lower bound", roomID)
	}
}

func TestLiveNewlyLeftNamesARealLeave(t *testing.T) {
	d, _, now, ctx := liveDeps(t)

	var userID, roomID string
	var pos int64
	err := d.Store.Pool().QueryRow(ctx, `
		SELECT s.user_id, s.room_id, s.event_stream_ordering
		  FROM sliding_sync_membership_snapshots s
		 WHERE s.membership = 'leave' AND s.forgotten = 0
		 ORDER BY s.event_stream_ordering DESC LIMIT 1`).Scan(&userID, &roomID, &pos)
	if err != nil {
		t.Skipf("no leaves: %v", err)
	}

	from := now
	from.Room = streamtoken.Live(pos - 1)
	_, left, err := newlyJoinedAndLeft(ctx, d, userID, &from, now)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := left[roomID]
	if !ok {
		t.Fatalf("%s was left at %d but is not reported newly left; the user would "+
			"never be told they left", roomID, pos)
	}
	if entry.Membership != "leave" {
		t.Errorf("newly left entry has membership %q", entry.Membership)
	}
}

// An initial sync has no range, so nothing can be newly anything in it.
func TestLiveNothingIsNewlyAnythingOnAnInitialSync(t *testing.T) {
	d, userID, now, ctx := liveDeps(t)

	joined, left, err := newlyJoinedAndLeft(ctx, d, userID, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(joined) != 0 || len(left) != 0 {
		t.Errorf("an initial sync reported %d newly joined and %d newly left",
			len(joined), len(left))
	}
}

// A display-name change rewrites the membership event without changing the
// membership, and Synapse's docstring says such changes "will be filtered out
// since they result in no meaningful change". Whether they actually are depends
// on data, and both outcomes are Synapse's -- see synapse-notes.md.
//
// The filter compares the membership against the PREVIOUS one, and the union
// has two sources with different ideas of "previous":
//
//   - current_state_delta_stream knows its own prev_event_id, so it always
//     filters correctly.
//   - sliding_sync_membership_snapshots has to reach the predecessor through
//     event_edges, which gives DAG PARENTS rather than state predecessors.
//     Measured here: it yields a membership at all only 51.7% of the time
//     (1,056 of 2,044 rows), and of those, 62.3% belong to a DIFFERENT USER.
//
// A row that filters is skipped, and nothing removes an entry another row
// already added, so a change is suppressed only when BOTH branches agree it is
// not one. That makes the error safe in the direction that matters: a genuine
// join always has a delta row with no predecessor, so it is always reported,
// while a display-name change is reported about half the time. Over-reporting
// costs one room sent in full; under-reporting would send a delta to a client
// with no base for it.
//
// Both tests below assert the behaviour, not the docstring.
func TestLiveADisplayNameChangeIsFilteredWhenThePredecessorIsVisible(t *testing.T) {
	d, _, now, ctx := liveDeps(t)

	// BOTH branches must agree it is not a change, or the one that disagrees
	// puts the room back. So: a delta row whose own predecessor is a join (a
	// real display-name change) AND a snapshot row whose event_edges parent is
	// also a join.
	var userID, roomID string
	var pos int64
	err := d.Store.Pool().QueryRow(ctx, `
		SELECT s.state_key, s.room_id, s.stream_id
		  FROM current_state_delta_stream s
		  JOIN room_memberships m ON m.event_id = s.event_id
		  JOIN room_memberships m_prev ON m_prev.event_id = s.prev_event_id
		  JOIN sliding_sync_membership_snapshots snap
		    ON snap.room_id = s.room_id AND snap.user_id = s.state_key
		   AND snap.event_stream_ordering = s.stream_id
		  JOIN event_edges e ON e.event_id = snap.membership_event_id
		  JOIN room_memberships snap_prev ON snap_prev.event_id = e.prev_event_id
		 WHERE s.type = 'm.room.member' AND m.membership = m_prev.membership
		   AND snap_prev.membership = m.membership
		 ORDER BY s.stream_id DESC LIMIT 1`).Scan(&userID, &roomID, &pos)
	if err != nil {
		t.Skipf("no join-to-join both branches can see: %v", err)
	}
	t.Logf("%s rewrote their membership in %s at %d, predecessor visible", userID, roomID, pos)

	from, to := now, now
	from.Room = streamtoken.Live(pos - 1)
	to.Room = streamtoken.Live(pos)

	joined, left, err := newlyJoinedAndLeft(ctx, d, userID, &from, to)
	if err != nil {
		t.Fatal(err)
	}
	if joined[roomID] {
		t.Errorf("%s is reported newly joined for a display name change whose "+
			"predecessor is a join; it would be re-sent in full", roomID)
	}
	if _, ok := left[roomID]; ok {
		t.Errorf("%s is reported newly left for a display name change", roomID)
	}
}

// The other half, asserted so the divergence is visible if Synapse ever fixes
// it: when the snapshot branch cannot reach the predecessor, the change IS
// reported. Matching Synapse here is the point -- a client is sent one room in
// full that it did not strictly need, which is wasteful and not wrong.
func TestLiveADisplayNameChangeIsReportedWhenThePredecessorIsHidden(t *testing.T) {
	d, _, now, ctx := liveDeps(t)

	var userID, roomID string
	var pos int64
	err := d.Store.Pool().QueryRow(ctx, `
		SELECT s.state_key, s.room_id, s.stream_id
		  FROM current_state_delta_stream s
		  JOIN room_memberships m_prev ON m_prev.event_id = s.prev_event_id
		  JOIN room_memberships m ON m.event_id = s.event_id
		  JOIN sliding_sync_membership_snapshots snap
		    ON snap.room_id = s.room_id AND snap.user_id = s.state_key
		   AND snap.event_stream_ordering = s.stream_id
		  LEFT JOIN event_edges e ON e.event_id = snap.membership_event_id
		  LEFT JOIN room_memberships snap_prev ON snap_prev.event_id = e.prev_event_id
		 WHERE s.type = 'm.room.member'
		   AND m_prev.membership = m.membership
		   AND snap_prev.membership IS NULL
		 ORDER BY s.stream_id DESC LIMIT 1`).Scan(&userID, &roomID, &pos)
	if err != nil {
		t.Skipf("no join-to-join with a hidden predecessor: %v", err)
	}
	t.Logf("%s rewrote their membership in %s at %d, predecessor NOT visible to the "+
		"snapshot branch", userID, roomID, pos)

	from, to := now, now
	from.Room = streamtoken.Live(pos - 1)
	to.Room = streamtoken.Live(pos)

	joined, _, err := newlyJoinedAndLeft(ctx, d, userID, &from, to)
	if err != nil {
		t.Fatal(err)
	}
	if !joined[roomID] {
		t.Errorf("%s is NOT reported newly joined; Synapse reports it here, because "+
			"the snapshot branch cannot see that the previous membership was also a "+
			"join. If this now passes, Synapse has changed and so should we.", roomID)
	}
}
