package store

import (
	"context"
	"sync"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lru"
)

// Derived caches hold answers that survive a request.
//
// Sizes are bounds on ENTRIES, not bytes, and what an entry costs differs by a
// factor of thirty-five across these caches -- an access token id is a few
// dozen bytes, a 654-room user's room list is 139KB. So the entry counts below
// are not comparable with each other, and the one to think about before
// raising is rooms_for_user. A byte bound would express this better; it is not
// here yet.
//
// Two kinds live here, and the difference is the whole design:
//
//   - Immutable by construction. A filter is never edited (`user_filters` is
//     append-only), a room's version cannot change (an upgrade makes a new
//     room), and an access token's id is fixed for as long as the token
//     exists. Nothing has to invalidate these; they only have to be dropped if
//     the underlying row can be DELETED, which is what the `caches` stream and
//     the replication-drop purge are for.
//
//   - Invalidated over replication. A room summary, a room's history
//     visibility, a user's ignore list and their room list all change, and the
//     only signal we get is the replication stream.
//
// The second kind carries a hazard the first does not, and it is not the
// obvious one. It is not that an entry might be a few milliseconds stale --
// it is that our now token is built from max(database, replication), so it can
// include an event whose RDATA row has not arrived. Serve a stale cache under
// a token that already covers the change and the client's next `since` starts
// after it: the change is not late, it is *lost*, and no later sync will
// mention it because every window is bounded by a token the client already
// holds.
//
// So a cached entry is only served when replication has delivered every
// invalidation up to the token's position for the relevant stream. When it has
// not -- the window between a commit and its row -- the guard fails and we
// query. A skew becomes a cache miss instead of a missing room.

// Horizon is the now token's position on each stream that invalidates a cache,
// carried on the context because it is a property of the sync pass rather than
// of any one lookup.
//
// A context without one disables every guarded cache, which is the right
// default for the paths that have no token to check against.
type Horizon struct {
	Events      int64
	AccountData int64
}

type horizonKey struct{}

// WithHorizon attaches the now token's positions for this pass.
func WithHorizon(ctx context.Context, h Horizon) context.Context {
	return context.WithValue(ctx, horizonKey{}, h)
}

func horizonFrom(ctx context.Context) (Horizon, bool) {
	h, ok := ctx.Value(horizonKey{}).(Horizon)
	return h, ok
}

// Stream names used by the guard. These match the replication stream names.
const (
	streamEvents      = "events"
	streamAccountData = "account_data"
)

type derivedCaches struct {
	// Immutable: no invalidation needed, only purging.
	userFilter  *lru.Cache[string, []byte]
	roomInfo    *lru.Cache[string, RoomInfo]
	accessToken *lru.Cache[string, int64]

	// Invalidated over replication, and guarded by the horizon.
	roomSummary  *lru.Cache[string, MemberSummary]
	historyVis   *lru.Cache[string, string]
	ignoredUsers *lru.Cache[string, map[string]bool]
	// Keyed by user, with the membership set inside: invalidation is per user,
	// and a composite key could not be found by a user-keyed Remove.
	roomsForUser *lru.Cache[string, map[string][]RoomForUser]

	// applied is the position, per stream, up to which every invalidation has
	// been performed. Written only by the replication listener.
	//
	// Deliberately NOT the subscriber's own stream position: that advances
	// when a row arrives, this advances when the row has been acted on. The
	// gap between them is exactly the window the guard exists to refuse.
	mu      sync.RWMutex
	applied map[string]int64
	armed   bool
}

func newDerivedCaches(cfg Config) *derivedCaches {
	size := func(n, def int) int {
		if n == 0 {
			return def
		}
		return n
	}
	return &derivedCaches{
		userFilter:   lru.New[string, []byte](size(cfg.UserFilterCacheEntries, 5000)),
		roomInfo:     lru.New[string, RoomInfo](size(cfg.RoomInfoCacheEntries, 20000)),
		accessToken:  lru.New[string, int64](size(cfg.AccessTokenCacheEntries, 20000)),
		roomSummary:  lru.New[string, MemberSummary](size(cfg.RoomSummaryCacheEntries, 20000)),
		historyVis:   lru.New[string, string](size(cfg.HistoryVisibilityCacheEntries, 20000)),
		ignoredUsers: lru.New[string, map[string]bool](size(cfg.IgnoredUsersCacheEntries, 5000)),
		// 1000, not 5000 like its neighbours, because its entries are the
		// only large ones here: measured, a 20-room user costs ~3.9KB, a
		// 100-room user ~21KB and a 654-room user ~139KB. At 5000 that is
		// two thirds of a gigabyte for an account like the one this was
		// built against, in a worker that otherwise runs in 20MB.
		roomsForUser: lru.New[string, map[string][]RoomForUser](size(cfg.RoomsForUserCacheEntries, 1000)),
		applied:      map[string]int64{},
	}
}

// fresh reports whether a guarded cache may answer for this stream.
//
// False whenever there is no horizon, the caches are disarmed, or replication
// has not yet caught up to the token. All three are "ask the database", which
// is never wrong -- only slower.
func (d *derivedCaches) fresh(ctx context.Context, stream string) bool {
	if d == nil {
		return false
	}
	h, ok := horizonFrom(ctx)
	if !ok {
		return false
	}
	var want int64
	switch stream {
	case streamEvents:
		want = h.Events
	case streamAccountData:
		want = h.AccountData
	default:
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if !d.armed {
		return false
	}
	return d.applied[stream] >= want
}

// Applied records that every invalidation up to pos on a stream has been done.
//
// Called by the replication listener AFTER it has invalidated, never before:
// the ordering is what makes the guard mean anything.
func (s *Store) Applied(stream string, pos int64) {
	d := s.derived
	if d == nil || pos <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if pos > d.applied[stream] {
		d.applied[stream] = pos
	}
}

// ArmDerivedCaches enables the derived caches, seeding the applied positions.
//
// Seeded rather than started at zero: a stream that never carries a row --
// account_data on a quiet account -- would otherwise keep its horizon at zero
// for ever and its cache would never once be allowed to answer. The caches are
// empty at this moment, so seeding from the current positions claims only that
// nothing cached predates them, which is true of nothing.
func (s *Store) ArmDerivedCaches(positions map[string]int64) {
	d := s.derived
	if d == nil {
		return
	}
	d.mu.Lock()
	d.applied = map[string]int64{}
	for stream, pos := range positions {
		d.applied[stream] = pos
	}
	d.armed = true
	d.mu.Unlock()

	d.setArmed(true)
}

// DisarmDerivedCaches stops the derived caches answering, and empties them.
//
// While replication is down we cannot see a room being purged, a summary
// changing or a user editing their ignore list, so every one of these becomes
// a guess. A guess is not a fallback.
func (s *Store) DisarmDerivedCaches() {
	d := s.derived
	if d == nil {
		return
	}
	d.mu.Lock()
	d.armed = false
	d.applied = map[string]int64{}
	d.mu.Unlock()

	d.setArmed(false)
}

func (d *derivedCaches) setArmed(armed bool) {
	d.userFilter.SetArmed(armed)
	d.roomInfo.SetArmed(armed)
	d.accessToken.SetArmed(armed)
	d.roomSummary.SetArmed(armed)
	d.historyVis.SetArmed(armed)
	d.ignoredUsers.SetArmed(armed)
	d.roomsForUser.SetArmed(armed)
}

// InvalidateRoom drops everything cached about one room.
//
// Conservative on purpose: any event in a room drops that room's summary and
// history visibility rather than working out whether this particular event
// could have changed them. Both are cheap map deletes, and the alternative is
// a table of which event types affect which cache -- a table that is wrong the
// moment Synapse adds a state event type.
func (s *Store) InvalidateRoom(roomID string) {
	d := s.derived
	if d == nil || roomID == "" {
		return
	}
	d.roomSummary.Remove(roomID)
	d.historyVis.Remove(roomID)
}

// InvalidateUserMembership drops what a membership change makes stale.
func (s *Store) InvalidateUserMembership(userID string) {
	d := s.derived
	if d == nil || userID == "" {
		return
	}
	d.roomsForUser.Remove(userID)
}

// InvalidateUserAccountData drops what an account data change makes stale.
func (s *Store) InvalidateUserAccountData(userID string) {
	d := s.derived
	if d == nil || userID == "" {
		return
	}
	d.ignoredUsers.Remove(userID)
}

// PurgeDerivedCaches empties every derived cache.
//
// For the `caches` replication stream, whose rows say that something we
// believed immutable has been deleted, and for anything we could not parse.
func (s *Store) PurgeDerivedCaches() {
	d := s.derived
	if d == nil {
		return
	}
	d.userFilter.Purge()
	d.roomInfo.Purge()
	d.accessToken.Purge()
	d.roomSummary.Purge()
	d.historyVis.Purge()
	d.ignoredUsers.Purge()
	d.roomsForUser.Purge()
}

// DerivedCacheStats reports each cache for the metrics collector.
func (s *Store) DerivedCacheStats() map[string]lru.Stats {
	d := s.derived
	if d == nil {
		return nil
	}
	return map[string]lru.Stats{
		"user_filter":        d.userFilter.Stats(),
		"room_info":          d.roomInfo.Stats(),
		"access_token":       d.accessToken.Stats(),
		"room_summary":       d.roomSummary.Stats(),
		"history_visibility": d.historyVis.Stats(),
		"ignored_users":      d.ignoredUsers.Stats(),
		"rooms_for_user":     d.roomsForUser.Stats(),
	}
}

// Copies on the way out.
//
// A cached map or slice handed to a caller is shared with every later caller,
// so one caller writing to it corrupts the cache for everyone -- a bug that
// appears only on the second request and looks like data corruption rather
// than a cache fault. Copying is cheap next to the round trip it replaces, and
// removes the question entirely.
//
// The value types below hold only strings and scalars, so a shallow copy is a
// complete one.

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func copyStringSet(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyRoomsForUser(rooms []RoomForUser) []RoomForUser {
	if rooms == nil {
		return nil
	}
	out := make([]RoomForUser, len(rooms))
	copy(out, rooms)
	return out
}

func copyMemberSummary(s MemberSummary) MemberSummary {
	out := MemberSummary{}
	if s.Counts != nil {
		out.Counts = make(map[string]int, len(s.Counts))
		for k, v := range s.Counts {
			out.Counts[k] = v
		}
	}
	if s.Members != nil {
		out.Members = make([]SummaryMember, len(s.Members))
		copy(out.Members, s.Members)
	}
	return out
}

// addRoomsForUser stores one membership set's answer without mutating a map
// another caller may be reading.
//
// The cached value is replaced rather than written into: a reader that took
// the map out of the cache a moment ago still holds the old one, and writing
// into it would be a data race no test would reproduce.
func (d *derivedCaches) addRoomsForUser(userID, set string, rooms []RoomForUser) {
	bySet := map[string][]RoomForUser{}
	if existing, ok := d.roomsForUser.Get(userID); ok {
		for k, v := range existing {
			bySet[k] = v
		}
	}
	bySet[set] = rooms
	d.roomsForUser.Add(userID, bySet)
}
