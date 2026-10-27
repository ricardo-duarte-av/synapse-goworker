package app

import (
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// streamFeeder keeps the stream-change caches warm from replication.
//
// It is a replication.Listener rather than part of the notifier or the
// invalidator, because all three read the same row and want different things
// from it:
//
//   - The notifier wakes whoever the row names, and a row naming nobody wakes
//     everybody. Over-waking costs one recomputation.
//   - The invalidator drops the cached answers the row makes wrong, and decides
//     per stream what a row naming nobody means.
//   - This records "entity E changed at position P". A row naming nobody means
//     something changed and we cannot say what, which is the one case a stream
//     cache cannot shrug off: the horizon has to move rather than the entry.
//
// Folding any two of them together would force one of those defaults onto the
// other, and the wrong default here is a client silently missing an event.
type streamFeeder struct {
	db *store.Store
}

// OnStreamAdvance records a row, or a jump.
func (f *streamFeeder) OnStreamAdvance(stream string, pos int64, roomIDs, userIDs []string) {
	byRoom, known := f.db.StreamCacheIsRoomKeyed(stream)
	if !known {
		return
	}

	// A POSITION command carries a position and no subjects: the stream moved
	// and we did not see the rows. So did an RDATA row we could not parse.
	// Either way the cache no longer covers the range it claims to, and the
	// only honest response is to forget what it holds and move the horizon up.
	// Keeping the entries would leave it answering "unchanged" for rooms whose
	// changes it never saw.
	subjects := roomIDs
	if !byRoom {
		subjects = userIDs
	}
	if len(subjects) == 0 {
		f.db.StreamCacheReset(stream, pos)
		return
	}

	for _, entity := range subjects {
		f.db.StreamCacheChanged(stream, entity, pos)
	}
}
