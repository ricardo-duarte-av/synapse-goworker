// Package originup does what Synapse's inbound worker does when a server we
// had backed off sends us a signed request: it marks the server as up
// (Authenticator.reset_retry_timings, transport/server/_base.py:143).
//
// That is, in order:
//
//  1. set_destination_retry_timings(origin, None, 0, 0): the destinations
//     upsert, plus a row on the caches stream invalidating every process's
//     get_destination_retry_timings for the origin, under the worker's own
//     instance name (_invalidate_cache_and_stream);
//  2. the caches stream's RDATA for that row on the replication bus, which
//     is how every other process learns of it;
//  3. REMOTE_SERVER_UP, so every process (the federation sender above all)
//     wakes the destination.
//
// Every Synapse process writes the caches stream, the inbound worker
// included: its MultiWriterIdGenerator has writers=[], and a writer that
// then goes quiet holds nothing back ("nothing cares if a given worker's
// caches stream position lags", replication/tcp/resource.py).
package originup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/inbound"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
)

var resets = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goinbound_origin_up_total",
	Help: "Origins in backoff marked as up, by outcome: reset, db_error, publish_error.",
}, []string{"outcome"})

// Store writes retry timings (inbound.DB).
type Store interface {
	SetRetryTimings(ctx context.Context, dest string, failureTS *int64, lastTS, interval int64) (inbound.CacheInvalidation, error)
}

// Publisher publishes one replication command line on the bus.
type Publisher interface {
	Publish(ctx context.Context, line string) error
}

// Options builds a Resetter.
type Options struct {
	Store     Store
	Publisher Publisher
	// Caches is the process's caches stream. Nil makes one of our own on
	// Publisher, for a process where nothing else writes it.
	Caches *cachestream.Writer
	// Instance is our instance name on the caches stream.
	Instance string
	Log      zerolog.Logger
}

// Resetter marks origins as up.
type Resetter struct {
	store    Store
	pub      Publisher
	caches   *cachestream.Writer
	instance string
	log      zerolog.Logger
	now      func() time.Time
	// retryDelay is between publish attempts; shortened in tests.
	retryDelay time.Duration

	mu       sync.Mutex
	inflight map[string]bool
	failedAt time.Time
}

// failureHold is how long after a failed reset Ready says no, so that while
// Synapse's worker exists, the transactions go to it instead.
const failureHold = 5 * time.Minute

const (
	publishAttempts = 3
	stepTimeout     = 10 * time.Second
)

// New builds a Resetter.
func New(o Options) *Resetter {
	caches := o.Caches
	if caches == nil {
		caches = cachestream.New(cachestream.Options{Publisher: o.Publisher, Instance: o.Instance, Log: o.Log})
	}
	return &Resetter{store: o.Store, pub: o.Publisher, caches: caches, instance: o.Instance, log: o.Log,
		now: time.Now, retryDelay: time.Second, inflight: map[string]bool{}}
}

// SetRetryDelay shortens the wait between publish attempts, for tests.
func (r *Resetter) SetRetryDelay(d time.Duration) {
	r.retryDelay = d
	r.caches.RetryDelay = d
}

// Ready says whether origins in backoff can be served here: false for a
// while after a reset failed.
func (r *Resetter) Ready() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failedAt.IsZero() || r.now().Sub(r.failedAt) >= failureHold
}

// Reset marks origin as up, in the background, as Synapse does: the request
// that showed the origin is alive does not wait for it. A reset already
// running for origin is not repeated. ip is the address the request came
// from, for the log.
func (r *Resetter) Reset(origin, ip string) {
	r.mu.Lock()
	if r.inflight[origin] {
		r.mu.Unlock()
		return
	}
	r.inflight[origin] = true
	r.mu.Unlock()

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.inflight, origin)
			r.mu.Unlock()
		}()
		outcome, err := r.reset(origin)
		resets.WithLabelValues(outcome).Inc()
		if err != nil {
			r.mu.Lock()
			r.failedAt = r.now()
			r.mu.Unlock()
			r.log.Warn().Err(err).Str("origin", origin).Str("ip", ip).Str("outcome", outcome).Msg("Could not mark an origin as up")
			return
		}
		r.log.Info().Str("origin", origin).Str("ip", ip).Msg("Marked origin as up")
	}()
}

func (r *Resetter) reset(origin string) (string, error) {
	return r.storeTimings(origin, nil, 0, 0, true)
}

var timingWrites = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goinbound_retry_timings_total",
	Help: "Destination retry timings stored after our own requests (store_retry_timings), by kind (backoff, cleared) and outcome.",
}, []string{"kind", "outcome"})

// SetTimings is RetryDestinationLimiter's store_retry_timings, run in the
// background as Synapse runs it: the timings, their caches row and RDATA,
// and, when up, REMOTE_SERVER_UP (a destination that was failing and no
// longer is).
func (r *Resetter) SetTimings(dest string, failureTS *int64, lastTS, interval int64, up bool) {
	kind := "backoff"
	if interval == 0 {
		kind = "cleared"
	}
	go func() {
		outcome, err := r.storeTimings(dest, failureTS, lastTS, interval, up)
		timingWrites.WithLabelValues(kind, outcome).Inc()
		if err != nil {
			r.mu.Lock()
			r.failedAt = r.now()
			r.mu.Unlock()
			r.log.Warn().Err(err).Str("destination", dest).Str("outcome", outcome).Msg("Failed to store destination_retry_timings")
		}
	}()
}

func (r *Resetter) storeTimings(dest string, failureTS *int64, lastTS, interval int64, up bool) (string, error) {
	err := r.caches.Do(func() ([]cachestream.Row, error) {
		ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
		defer cancel()
		inv, err := r.store.SetRetryTimings(ctx, dest, failureTS, lastTS, interval)
		if err != nil {
			return nil, err
		}
		return []cachestream.Row{inv}, nil
	})
	var perr *cachestream.PublishError
	switch {
	case errors.As(err, &perr):
		return "publish_error", err
	case err != nil:
		return "db_error", err
	}
	if !up {
		return "stored", nil
	}
	if err := r.publish("REMOTE_SERVER_UP " + dest); err != nil {
		return "publish_error", err
	}
	return "reset", nil
}

func (r *Resetter) publish(line string) error {
	var err error
	for i := range publishAttempts {
		if i > 0 {
			time.Sleep(r.retryDelay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
		err = r.pub.Publish(ctx, line)
		cancel()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("originup: publishing %.40q: %w", line, err)
}

// OnReplicate answers a REPLICATE with our caches position
// (cachestream.Writer.OnReplicate).
func (r *Resetter) OnReplicate() { r.caches.OnReplicate() }
