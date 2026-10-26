package store

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamcache"
)

// Stream-change caches: "has this room/user changed since position P?"
//
// Distinct from the derived caches in derived.go, and the difference is worth
// keeping straight. A derived cache holds an ANSWER and must be invalidated
// when the answer changes; get that wrong and we serve something stale. A
// stream cache holds no answer at all -- only whether asking is worth it -- and
// is kept warm by replication rather than invalidated. Get that wrong in one
// direction and we run a query we did not need; in the other, a client silently
// misses an event. See internal/streamcache.
//
// The entity is a room for the events and receipts streams and a user for the
// rest, matching Synapse's choice of key for each.

// streamCaches holds one cache per stream we gate on.
type streamCaches struct {
	events      *streamcache.Cache // room
	membership  *streamcache.Cache // user, positioned in the events stream
	receipts    *streamcache.Cache // room
	accountData *streamcache.Cache // user
	toDevice    *streamcache.Cache // user
	presence    *streamcache.Cache // user
}

// StreamCacheSizes bounds each cache, in distinct stream positions rather than
// entities: one position can name many rooms. Zero takes the default; negative
// disables that cache, which makes "is this cache hiding a bug?" answerable.
type StreamCacheSizes struct {
	Events      int
	Membership  int
	Receipts    int
	AccountData int
	ToDevice    int
	Presence    int
}

const defaultStreamCacheEntries = 10000

func sizeOr(n int) int {
	if n == 0 {
		return defaultStreamCacheEntries
	}
	return n
}

func newStreamCaches(sizes StreamCacheSizes) *streamCaches {
	return &streamCaches{
		events:      streamcache.New("events", 0, sizeOr(sizes.Events)),
		membership:  streamcache.New("membership", 0, sizeOr(sizes.Membership)),
		receipts:    streamcache.New("receipts", 0, sizeOr(sizes.Receipts)),
		accountData: streamcache.New("account_data", 0, sizeOr(sizes.AccountData)),
		toDevice:    streamcache.New("to_device", 0, sizeOr(sizes.ToDevice)),
		presence:    streamcache.New("presence", 0, sizeOr(sizes.Presence)),
	}
}

// byName maps a replication stream to the cache it feeds, plus whether that
// cache is keyed by room or by user.
//
// Membership is absent because it is fed from the events stream by the row's
// type, not by a stream of its own.
func (c *streamCaches) byName(stream string) (cache *streamcache.Cache, byRoom bool, ok bool) {
	if c == nil {
		return nil, false, false
	}
	switch stream {
	case "events":
		return c.events, true, true
	case "receipts":
		return c.receipts, true, true
	case "account_data":
		return c.accountData, false, true
	case "to_device":
		return c.toDevice, false, true
	case "presence":
		return c.presence, false, true
	}
	return nil, false, false
}

func (c *streamCaches) all() map[string]*streamcache.Cache {
	if c == nil {
		return nil
	}
	return map[string]*streamcache.Cache{
		"events":       c.events,
		"membership":   c.membership,
		"receipts":     c.receipts,
		"account_data": c.accountData,
		"to_device":    c.toDevice,
		"presence":     c.presence,
	}
}

// StreamCacheChanged records a change from a replication row.
//
// The caller supplies the entity already resolved to a room or a user, because
// deciding which is a property of the row and belongs with the row parser.
func (s *Store) StreamCacheChanged(stream, entity string, pos int64) {
	cache, _, ok := s.streams.byName(stream)
	if !ok {
		return
	}
	cache.EntityHasChanged(entity, pos)
}

// StreamCacheIsRoomKeyed says whether a stream's cache is keyed by room, so the
// feeder knows which of a row's subjects to hand over.
func (s *Store) StreamCacheIsRoomKeyed(stream string) (byRoom, known bool) {
	_, byRoom, known = s.streams.byName(stream)
	return byRoom, known
}

// MembershipChanged records that a user's membership moved, from an
// `m.room.member` row on the events stream.
//
// Synapse keeps this in the events stream's position space too
// (`_membership_stream_cache`, seeded from `events_max`), so the position here
// is a stream ordering, not a membership id of its own.
func (s *Store) MembershipChanged(userID string, pos int64) {
	if s.streams == nil {
		return
	}
	s.streams.membership.EntityHasChanged(userID, pos)
}

// StreamCacheReset says a stream jumped without us seeing the rows in between.
//
// Everything the cache holds for that stream is discarded and its horizon moves
// to the new position. Anything else -- keeping the entries, or keeping the
// horizon -- would leave the cache claiming to know a range it never saw.
func (s *Store) StreamCacheReset(stream string, pos int64) {
	if cache, _, ok := s.streams.byName(stream); ok {
		cache.AllEntitiesChanged(pos)
	}
	if stream == "events" && s.streams != nil {
		s.streams.membership.AllEntitiesChanged(pos)
	}
}

// PrefillStreamCaches seeds the caches from one query each and arms them.
//
// Called at startup and on every replication reconnect. Without it each
// horizon sits at "now", every question falls below it, and every gate answers
// "changed" -- correct, useless, and with no symptom beyond queries that never
// went away. Hence gosync_stream_cache_earliest_position.
//
// Which caches are prefilled and how deeply is Synapse's choice in each case,
// not a uniform rule, and the differences are deliberate:
//
//   - to_device scans 1,000 rows, not 100,000. device_inbox is drained as
//     clients acknowledge messages, so a deep scan buys history that has
//     already been deleted (deviceinbox.py).
//   - receipts scans 10,000 (receipts.py).
//   - account_data is NOT prefilled at all, and the obvious version of it is
//     actively wrong. Its position space is shared by three tables --
//     account_data, room_account_data and room_tags_revisions -- so seeding
//     from any one of them yields a horizon far below what that table alone
//     can account for, and the cache then answers "unchanged" for users whose
//     room account data moved. Measured here on 2026-09-03: prefilling from
//     `account_data` alone produced a horizon of 47 against a current position
//     of 1,530,013. Synapse constructs this cache empty at the current
//     position and lets live traffic fill it, which is the safe direction.
//   - membership has no table to prefill from: it is fed by `m.room.member`
//     rows on the events stream, and reconstructing it would be a state lookup
//     per user rather than one scan. Synapse starts it empty too.
func (s *Store) PrefillStreamCaches(ctx context.Context, positions map[string]int64) error {
	if s.streams == nil {
		return nil
	}
	specs := []struct {
		cache        *streamcache.Cache
		stream       string
		table        string
		entityColumn string
		streamColumn string
		limit        int
	}{
		{s.streams.events, "events", "events", "room_id", "stream_ordering", 100000},
		{s.streams.receipts, "receipts", "receipts_linearized", "room_id", "stream_id", 10000},
		{s.streams.presence, "presence", "presence_stream", "user_id", "stream_id", 100000},
		{s.streams.toDevice, "to_device", "device_inbox", "user_id", "stream_id", 1000},
	}
	for _, sp := range specs {
		entries, minPos, err := s.CacheDict(
			ctx, sp.table, sp.entityColumn, sp.streamColumn, positions[sp.stream], sp.limit)
		if err != nil {
			return err
		}
		sp.cache.Arm(minPos)
		sp.cache.Prefill(entries, minPos)
	}

	// Armed at the current position with nothing in them: they know nothing
	// below it, which is exactly true.
	s.streams.accountData.Arm(positions["account_data"])
	s.streams.membership.Arm(positions["events"])
	return nil
}

// DisarmStreamCaches makes every gate answer "changed".
//
// Called when replication drops. While it is down we cannot see changes
// happening, so the only safe answer is the one that costs a query -- which is
// exactly the behaviour the worker had before these caches existed.
func (s *Store) DisarmStreamCaches() {
	for _, c := range s.streams.all() {
		c.Disarm()
	}
}

// StreamCacheStats reports each cache for the metrics collector.
func (s *Store) StreamCacheStats() map[string]streamcache.Stats {
	all := s.streams.all()
	if all == nil {
		return nil
	}
	out := make(map[string]streamcache.Stats, len(all))
	for name, c := range all {
		out[name] = c.Stats()
	}
	return out
}

// RoomsWithEventsSince narrows a room list to those that may have had an event
// in (from, to].
//
// Returning every room is always correct, and is what happens below the
// horizon or with the cache disarmed.
func (s *Store) RoomsWithEventsSince(roomIDs []string, from int64) []string {
	if s.streams == nil {
		return roomIDs
	}
	return s.streams.events.EntitiesChanged(roomIDs, from)
}

// AnyPresenceSince reports whether any presence at all moved after pos.
//
// The gate for the single most expensive query on the sync path: a correlated
// subquery over presence_stream and current_state_events, 25ms mean and 430ms
// cold, run once per sync whether or not anybody's presence moved. Synapse
// gates it the same way.
func (s *Store) AnyPresenceSince(pos int64) bool {
	if s.streams == nil {
		return true
	}
	return s.streams.presence.HasAnyEntityChanged(pos)
}

// LastEventPosition returns an upper bound on the position of a room's most
// recent event, if the cache knows one.
//
// Sliding sync's room-list ordering is the caller. It is an upper bound because
// the cache records when an event was persisted, not whether the asking user
// may see it, so a caller that must not over-report has to re-check against its
// own token.
func (s *Store) LastEventPosition(roomID string) (int64, bool) {
	if s.streams == nil {
		return 0, false
	}
	return s.streams.events.MaxPosOfLastChange(roomID)
}
