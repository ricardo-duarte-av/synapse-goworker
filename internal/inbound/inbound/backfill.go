package inbound

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"maunium.net/go/mautrix"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedfetch"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
)

var (
	backfills = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goinbound_backfills_total",
		Help: "maybe_backfill runs for /messages, by outcome: backfilled, nothing (no point near enough or visible, or no server gave anything), handed_back (left to Synapse), error.",
	}, []string{"outcome"})
	backfillTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "goinbound_backfill_seconds",
		Help:    "Time maybe_backfill took, its room's queue and lock included.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})
	backfilledEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goinbound_backfilled_events_total",
		Help: "Events received from /backfill, checked, before processing.",
	})
)

// Backfill: FederationHandler.maybe_backfill and _maybe_backfill_inner
// (handlers/federation.py), FederationEventHandler.backfill
// (handlers/federation_event.py), for /messages.

// ErrBackfillNotReproduced is a backfill that reached what write mode does
// not do as Synapse would. What it persisted on the way is what Synapse
// would have persisted too; the rest is Synapse's.
var ErrBackfillNotReproduced = errors.New("inbound: backfill not reproducible")

// Backfilling pulls a room's history from a server.
type Backfilling interface {
	Backfill(ctx context.Context, dest, roomID string, rv *pdu.RoomVersion, extremities []string, limit int) ([]*fedfetch.Pulled, error)
}

// BackfillDeps enables backfill.
type BackfillDeps struct {
	Fetch Backfilling
	Store *pdustore.Store
	// VisibleToSelf is filter_events_for_server for our own server, with
	// nothing erased filtered out, for one event.
	VisibleToSelf func(ctx context.Context, eventID string) (bool, error)
}

// Constants of backfill (handlers/federation.py, pagination.py,
// api/constants.py).
const (
	// numberOfEventsToBackfill is NUMBER_OF_EVENTS_TO_BACKFILL: what a
	// /backfill asks for, and the padding on the depth points are looked
	// for near.
	numberOfEventsToBackfill = 100
	// MaxDepth is MAX_DEPTH (CANONICALJSON_MAX_INT).
	MaxDepth = int64(1)<<53 - 1
	// lockPurgePagination is PURGE_PAGINATION_LOCK_NAME.
	lockPurgePagination = "purge_pagination_lock"
	// maxBackfillDenied is try_backfill's max_denied_count.
	maxBackfillDenied = 5
	// maxExtremitiesToRequest is how many backfill points one /backfill
	// names.
	maxExtremitiesToRequest = 5
)

// EnableBackfill turns backfill on; it needs missing events on too.
func (p *Processor) EnableBackfill(b *BackfillDeps) { p.backfill = b }

// CanBackfill reports whether MaybeBackfill can run.
func (p *Processor) CanBackfill() bool { return p != nil && p.backfill != nil && p.missing != nil }

// roomQueue is maybe_backfill's _room_backfill Linearizer: one backfill of
// a room at a time in this process.
type roomQueue struct {
	mu    sync.Mutex
	rooms map[string]*roomSlot
}

type roomSlot struct {
	ch    chan struct{}
	users int
}

func (q *roomQueue) acquire(ctx context.Context, room string) (func(), error) {
	q.mu.Lock()
	if q.rooms == nil {
		q.rooms = map[string]*roomSlot{}
	}
	s := q.rooms[room]
	if s == nil {
		s = &roomSlot{ch: make(chan struct{}, 1)}
		q.rooms[room] = s
	}
	s.users++
	q.mu.Unlock()
	done := func() {
		q.mu.Lock()
		if s.users--; s.users == 0 {
			delete(q.rooms, room)
		}
		q.mu.Unlock()
	}
	select {
	case s.ch <- struct{}{}:
		return func() { <-s.ch; done() }, nil
	case <-ctx.Done():
		done()
		return nil, ctx.Err()
	}
}

// MaybeBackfill is maybe_backfill(room, currentDepth, limit): whether it
// tried to backfill something. ErrBackfillNotReproduced leaves the rest to
// Synapse. It runs to the end whatever happens to ctx's caller, as
// Synapse's /messages does not stop a backfill when its client goes.
func (p *Processor) MaybeBackfill(ctx context.Context, roomID string, currentDepth int64, limit int) (bool, error) {
	if !p.CanBackfill() {
		return false, ErrBackfillNotReproduced
	}
	ctx = context.WithoutCancel(ctx)
	rvID, err := p.read.RoomVersion(ctx, roomID)
	if err != nil {
		return false, fmt.Errorf("%w: room version: %v", ErrBackfillNotReproduced, err)
	}
	if strings.HasPrefix(rvID, "org.matrix.msc4242.") {
		// "we currently don't support backfilling in MSC4242 rooms".
		return false, nil
	}
	rv, ok := pdu.LookupRoomVersion(rvID)
	if !ok {
		return false, fmt.Errorf("%w: room version %q", ErrBackfillNotReproduced, rvID)
	}

	release, err := p.backfills.acquire(ctx, roomID)
	if err != nil {
		return false, err
	}
	defer release()
	lock, err := p.locks.AcquireReadWrite(ctx, lockPurgePagination, roomID, false)
	if err != nil {
		return false, fmt.Errorf("%w: purge lock: %v", ErrBackfillNotReproduced, err)
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = lock.Release(rctx)
		cancel()
	}()
	start := time.Now()
	did, err := p.maybeBackfillInner(ctx, roomID, rv, currentDepth, limit)
	outcome := "nothing"
	switch {
	case errors.Is(err, ErrBackfillNotReproduced):
		outcome = "handed_back"
	case err != nil:
		outcome = "error"
	case did:
		outcome = "backfilled"
	}
	backfills.WithLabelValues(outcome).Inc()
	backfillTime.Observe(time.Since(start).Seconds())
	return did, err
}

func (p *Processor) maybeBackfillInner(ctx context.Context, roomID string, rv *pdu.RoomVersion, currentDepth int64, limit int) (bool, error) {
	b := p.backfill
	now := time.Now().UnixMilli()
	points, err := b.Store.BackfillPoints(ctx, roomID, currentDepth+numberOfEventsToBackfill, now, 50)
	if err != nil {
		return false, err
	}
	// Closest to the current depth first, those in the past before those
	// after; sorted is stable, so the query's order breaks ties.
	sort.SliceStable(points, func(i, j int) bool {
		di, dj := abs64(currentDepth-points[i].Depth), abs64(currentDepth-points[j].Depth)
		if di != dj {
			return di < dj
		}
		return after(currentDepth, points[i].Depth) < after(currentDepth, points[j].Depth)
	})

	if len(points) == 0 && currentDepth != MaxDepth {
		later, err := b.Store.BackfillPoints(ctx, roomID, MaxDepth, now, 1)
		if err != nil {
			return false, err
		}
		if len(later) == 0 {
			return false, nil
		}
		// _maybe_backfill_inner_anyway_with_max_depth, in the background.
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			if _, err := p.MaybeBackfill(p.ctx, roomID, MaxDepth, limit); err != nil {
				p.log.Info().Err(err).Str("room_id", roomID).Msg("Backfill at MAX_DEPTH")
			}
		}()
		return false, nil
	}
	if len(points) == 0 {
		return false, nil
	}
	if currentDepth-2*int64(limit) > points[0].Depth {
		return false, nil
	}

	var extremities []string
	for _, bp := range points {
		if len(extremities) >= maxExtremitiesToRequest {
			break
		}
		succ, err := b.Store.Successors(ctx, bp.EventID)
		if err != nil {
			return false, err
		}
		visible := false
		for _, id := range succ {
			v, err := b.VisibleToSelf(ctx, id)
			if err != nil {
				return false, err
			}
			if v {
				visible = true
				break
			}
		}
		if visible {
			extremities = append(extremities, bp.EventID)
		}
	}
	if len(extremities) == 0 {
		return false, nil
	}

	domains, err := b.Store.HostsOrdered(ctx, roomID)
	if err != nil {
		return false, err
	}
	return p.tryBackfill(ctx, roomID, rv, domains, extremities)
}

// tryBackfill is try_backfill: each server in turn until one gives
// something, giving up after five refusals with a 4xx.
func (p *Processor) tryBackfill(ctx context.Context, roomID string, rv *pdu.RoomVersion, domains, extremities []string) (bool, error) {
	denied := 0
	for _, dom := range domains {
		if dom == p.server {
			continue
		}
		log := p.log.With().Str("room_id", roomID).Str("destination", dom).Logger()
		err, perr := p.backfillFrom(ctx, dom, roomID, rv, extremities)
		if perr != nil {
			// What escapes _process_pulled_events: Synapse's own failures
			// (errFailed) are logged and the next server tried; ours are
			// not Synapse's to have.
			if errors.Is(perr, errFailed) {
				log.Info().Err(perr).Msg("Failed to backfill")
				continue
			}
			return false, fmt.Errorf("%w: %v", ErrBackfillNotReproduced, perr)
		}
		if err == nil {
			// "If this succeeded then we probably already have the
			// appropriate stuff."
			return true, nil
		}
		var nr *backoff.NotRetrying
		var he mautrix.HTTPError
		switch {
		case errors.Is(err, ErrBackfillNotReproduced), errors.Is(err, fedfetch.ErrUncertain), errors.Is(err, errUncertain):
			return false, fmt.Errorf("%w: %v", ErrBackfillNotReproduced, err)
		case errors.As(err, &nr):
			log.Info().Err(err).Msg("_maybe_backfill_inner")
		case errors.As(err, &he) && he.Response != nil && he.Response.StatusCode >= 400 && he.Response.StatusCode < 500:
			log.Warn().Err(err).Int("denied", denied).Int("max", maxBackfillDenied).Msg("Backfill denied")
			denied++
			if denied >= maxBackfillDenied {
				return false, nil
			}
		case ctx.Err() != nil:
			return false, err
		default:
			// InvalidResponseError, RequestSendFailed, a 5xx, and anything
			// else try_backfill logs and moves on from.
			log.Info().Err(err).Msg("Failed to backfill")
		}
	}
	return false, nil
}

// backfillFrom is FederationEventHandler.backfill from one server: err is
// the pull's, perr what processing the events raised.
func (p *Processor) backfillFrom(ctx context.Context, dest, roomID string, rv *pdu.RoomVersion, extremities []string) (err, perr error) {
	events, err := p.backfill.Fetch.Backfill(ctx, dest, roomID, rv, extremities, numberOfEventsToBackfill)
	if err != nil {
		return err, nil
	}
	if len(events) == 0 {
		return nil, nil
	}
	for _, e := range events {
		if e.Event.RoomID != roomID {
			return fmt.Errorf("%w: Remote server %s returned event %s which is in room %s, when we were backfilling in %s",
				fedfetch.ErrBackfillFailed, dest, e.ID, e.Event.RoomID, roomID), nil
		}
	}
	backfilledEvents.Add(float64(len(events)))
	return nil, p.processPulledEvents(ctx, dest, rv, events, true)
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// after is the tie-break: 0 for a point at or before the current depth.
func after(current, depth int64) int {
	if current >= depth {
		return 0
	}
	return 1
}
