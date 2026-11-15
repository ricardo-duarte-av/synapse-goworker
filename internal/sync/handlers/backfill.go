package handlers

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Backfiller is maybe_backfill where this process can do it (inbound's
// write mode).
type Backfiller interface {
	// MaybeBackfill is maybe_backfill(room, currentDepth, limit): whether
	// it tried to backfill something. ErrBackfillHandBack means it reached
	// what it does not do as Synapse would.
	MaybeBackfill(ctx context.Context, roomID string, currentDepth int64, limit int) (bool, error)
}

// ErrBackfillHandBack is a backfill left to Synapse.
var ErrBackfillHandBack = errors.New("backfill left to Synapse")

// handBackFor is how long a room whose backfill was left to Synapse has
// its pages that would backfill handed to Synapse whole, so that its
// backfill happens there.
const handBackFor = 10 * time.Minute

// BackfillHook holds the backfiller, set once it is known (inbound starts
// after sync). Nil, or empty, leaves every page that would backfill to
// Synapse.
type BackfillHook struct {
	b atomic.Pointer[Backfiller]

	mu     sync.Mutex
	toMain map[string]time.Time
}

// Set installs the backfiller.
func (h *BackfillHook) Set(b Backfiller) { h.b.Store(&b) }

func (h *BackfillHook) get() Backfiller {
	if h == nil {
		return nil
	}
	if b := h.b.Load(); b != nil {
		return *b
	}
	return nil
}

// handedBack reports a room whose backfill was left to Synapse lately.
func (h *BackfillHook) handedBack(room string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	until, ok := h.toMain[room]
	if ok && time.Now().After(until) {
		delete(h.toMain, room)
		return false
	}
	return ok
}

func (h *BackfillHook) handBack(room string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.toMain == nil {
		h.toMain = map[string]time.Time{}
	}
	now := time.Now()
	for r, until := range h.toMain {
		if now.After(until) {
			delete(h.toMain, r)
		}
	}
	h.toMain[room] = now.Add(handBackFor)
}

// backfillBecauseTooManyGaps is BACKFILL_BECAUSE_TOO_MANY_GAPS_THRESHOLD.
const backfillBecauseTooManyGaps = 3

// backfillInForeground is get_messages' test on a backwards page: a gap
// of more than two in its depths, more than three gaps of two, or fewer
// events than asked for, and the client waits for the backfill.
func backfillInForeground(depths []int64, limit int) bool {
	set := map[int64]bool{}
	for _, d := range depths {
		set[d] = true
	}
	sorted := make([]int64, 0, len(set))
	for d := range set {
		sorted = append(sorted, d)
	}
	slices.Sort(sorted)
	gaps := 0
	prev := int64(0)
	if len(sorted) > 0 {
		prev = sorted[0]
	}
	for _, d := range sorted {
		gap := d - prev
		if gap < 0 {
			gap = -gap
		}
		if gap > 1 {
			gaps++
		}
		if gap > 2 {
			return true
		}
		prev = d
	}
	return gaps > backfillBecauseTooManyGaps || len(depths) < limit
}
