package app

import (
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// cacheInvalidator turns replication rows into cache invalidations.
//
// It lives here rather than in internal/store because it is the one place that
// knows both halves: which streams Synapse writes, and what this worker keeps.
type cacheInvalidator struct {
	db  *store.Store
	log zerolog.Logger
}

// OnRow drops what this row makes stale, then records the position as applied.
//
// The order is the contract. A cached answer is only served when the applied
// position has reached the sync's token, so recording the position before
// doing the work would open exactly the window the guard exists to close.
func (c *cacheInvalidator) OnRow(stream string, pos int64, d replication.RowDetail) {
	switch stream {
	case replication.StreamEvents:
		for _, roomID := range d.RoomIDs {
			c.db.InvalidateRoom(roomID)
		}
		// A membership event changes whose room list is wrong, and the state
		// key is who. Not the sender: an invite is sent by one user about
		// another, and it is the target's room list that changes.
		if d.Type == "m.room.member" && d.StateKey != "" {
			c.db.InvalidateUserMembership(d.StateKey)
			// The membership stream cache is fed from here rather than from
			// streamFeeder, because it is the only consumer that sees a row's
			// type and state key. Synapse keeps membership in the events
			// stream's position space too, so `pos` is a stream ordering.
			c.db.MembershipChanged(d.StateKey, pos)
		}

	case replication.StreamAccountData:
		for _, userID := range d.UserIDs {
			c.db.InvalidateUserAccountData(userID)
		}

	default:
		// Every other stream -- typing, receipts, presence, to_device,
		// device_lists, push_rules -- feeds nothing that is cached here. They
		// are listed as a default rather than enumerated so that a stream
		// added to Synapse does not silently start invalidating nothing while
		// looking like it was considered.
		return
	}

	c.db.Applied(stream, pos)
}

// OnRoomInvalidated drops one room's derived entries.
//
// The state caches are deliberately NOT touched: they are keyed by state
// group, which is immutable, so a room's current state moving on does not make
// any group we hold wrong -- it only means a newer group exists.
func (c *cacheInvalidator) OnRoomInvalidated(roomID string) {
	c.db.InvalidateRoom(roomID)
}

// OnPurge drops everything.
func (c *cacheInvalidator) OnPurge(reason string) {
	c.db.PurgeCaches()
	c.db.PurgeDerivedCaches()
	c.log.Info().Str("reason", reason).Msg("dropped caches")
}
