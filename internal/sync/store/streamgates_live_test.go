package store

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"
)

// The stream-change gates cannot be tested by cmd/syncdiff, and the two gates
// fail its reach for opposite reasons. Both were established by measurement on
// 2026-09-03 rather than assumed:
//
//   - The presence gate is invisible on the test account, which has 9 rooms and
//     sees ZERO presence events in an incremental sync -- so a gate hard-wired
//     to "nothing changed" compared clean. On the main account, with 189
//     presence events in one sync, the UNMUTATED build already mismatches:
//     presence is live and unpinnable, which is why CLAUDE.md's large-account
//     recipe blocks it with not_types ["*"] in the first place.
//   - The timeline-gap gate needs a room with an actual gap in the compared
//     window, which no ordinary account reliably has.
//
// So a comparator that says "ok" says nothing here. These tests check the
// property directly instead: for any range the cache claims to know, its answer
// must agree with the query it is there to skip. A gate may over-report freely
// -- that only costs a query -- but must never under-report.
//
// What they catch, verified by mutation on 2026-09-03: a gate wired to
// "unchanged" (both), and a gate that narrows a room list to nothing.
//
// What they do NOT catch, and where it is caught instead:
//
//   - Eviction failing to raise the horizon. The presence gate reads only the
//     cache's maximum position, which evicting the OLDEST entries cannot
//     change; and the events cache holds ~366 rooms against a bound of 10,000,
//     so it never evicts here at all. Covered by TestEvictionRaisesHorizon and
//     TestNoFalseNegatives in internal/streamcache, which run with a bound
//     small enough that eviction is constant.
//   - Dropping the "+1" in CacheDict's horizon. That guard exists for a table
//     whose LIMIT can cut through several rows sharing one stream id, and
//     neither events.stream_ordering nor presence_stream.stream_id does that.
//     It will matter for receipts_linearized, which is not gated on yet.
//
// Run against the live deployment:
//
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	  go test ./internal/store/ -run Live -v
func liveStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("GOSYNC_TEST_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	db, err := Open(ctx, Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)
	return db, ctx
}

// livePrefill arms the caches from the live database and returns the positions
// they were armed at.
//
// Bounding every later question at these positions is what makes the test
// deterministic: rows persisted after the prefill are invisible to a cache that
// nothing is feeding, and would look like false negatives that in production
// replication would have prevented.
func livePrefill(t *testing.T, db *Store, ctx context.Context) map[string]int64 {
	t.Helper()
	positions, err := db.StreamPositions(ctx)
	if err != nil {
		t.Fatalf("stream positions: %v", err)
	}
	if err := db.PrefillStreamCaches(ctx, positions); err != nil {
		t.Fatalf("prefill: %v", err)
	}
	return positions
}

func TestLivePresenceGateNeverUnderReports(t *testing.T) {
	db, ctx := liveStore(t)
	positions := livePrefill(t, db, ctx)

	now := positions["presence"]
	earliest := db.streams.presence.EarliestKnownPosition()
	if now <= earliest {
		t.Skipf("presence horizon %d is not below the current position %d", earliest, now)
	}
	t.Logf("presence: horizon %d, now %d, covering %d positions", earliest, now, now-earliest)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	skipped := 0
	for i := 0; i < 60; i++ {
		// Sample across the covered range, and always include both ends.
		var pos int64
		switch i {
		case 0:
			pos = earliest
		case 1:
			pos = now
		default:
			pos = earliest + rng.Int63n(now-earliest)
		}

		gate := db.AnyPresenceSince(pos)

		var actually bool
		err := db.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM presence_stream WHERE stream_id > $1 AND stream_id <= $2)`,
			pos, now).Scan(&actually)
		if err != nil {
			t.Fatalf("probe at %d: %v", pos, err)
		}

		if actually && !gate {
			t.Fatalf("false negative: presence moved after %d, gate says nothing did "+
				"(horizon %d, now %d) -- this silently drops the presence section from a sync",
				pos, earliest, now)
		}
		if !actually && gate {
			skipped++
		}
	}
	// Over-reporting is allowed but is the whole cost of the cache being
	// useless, so report it rather than hiding it behind a pass.
	t.Logf("over-reported (query would have run for nothing) on %d of 60 samples", skipped)
}

func TestLiveEventsGateNeverUnderReports(t *testing.T) {
	db, ctx := liveStore(t)
	positions := livePrefill(t, db, ctx)

	now := positions["events"]
	earliest := db.streams.events.EarliestKnownPosition()
	if now <= earliest {
		t.Skipf("events horizon %d is not below the current position %d", earliest, now)
	}
	t.Logf("events: horizon %d, now %d, covering %d positions", earliest, now, now-earliest)

	// Every room the cache knows about, plus rooms it does not, so that the
	// "unknown entity means unchanged" reading is exercised rather than assumed.
	var rooms []string
	rows, err := db.pool.Query(ctx,
		`SELECT room_id FROM rooms ORDER BY room_id LIMIT 400`)
	if err != nil {
		t.Fatalf("room list: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("room list: %v", err)
		}
		rooms = append(rooms, id)
	}
	rows.Close()
	if len(rooms) == 0 {
		t.Skip("no rooms")
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 25; i++ {
		pos := earliest + rng.Int63n(now-earliest)

		got := map[string]bool{}
		for _, r := range db.RoomsWithEventsSince(rooms, pos) {
			got[r] = true
		}

		probe, err := db.pool.Query(ctx,
			`SELECT DISTINCT room_id FROM events
			  WHERE room_id = ANY($1) AND stream_ordering > $2 AND stream_ordering <= $3`,
			rooms, pos, now)
		if err != nil {
			t.Fatalf("probe at %d: %v", pos, err)
		}
		var missing []string
		for probe.Next() {
			var id string
			if err := probe.Scan(&id); err != nil {
				t.Fatalf("probe at %d: %v", pos, err)
			}
			if !got[id] {
				missing = append(missing, id)
			}
		}
		probe.Close()

		if len(missing) > 0 {
			t.Fatalf("false negative at %d: %d rooms had events the gate omitted, e.g. %s "+
				"(horizon %d, now %d) -- this drops the timeline-gap lookup for a room that needs it",
				pos, len(missing), missing[0], earliest, now)
		}
	}
}

// The horizon is the whole safety argument: below it the cache must claim
// nothing. A gate that answered from an empty cache below its own horizon would
// report "unchanged" for the entire history.
func TestLiveGatesSayChangedBelowTheHorizon(t *testing.T) {
	db, ctx := liveStore(t)
	livePrefill(t, db, ctx)

	if !db.AnyPresenceSince(0) {
		t.Error("presence gate answered 'unchanged' for all of history")
	}
	if !db.AnyPresenceSince(db.streams.presence.EarliestKnownPosition() - 1) {
		t.Error("presence gate answered below its own horizon")
	}

	rooms := []string{"!nonexistent:example.com"}
	if got := db.RoomsWithEventsSince(rooms, 0); len(got) != 1 {
		t.Errorf("events gate narrowed a list below the horizon to %v; it must return all of it", got)
	}
}
