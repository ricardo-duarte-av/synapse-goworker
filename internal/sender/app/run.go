package app

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/sender"
)

func (w *Worker) registerMetrics() {
	metrics.MustRegister()
	metrics.BuildInfo.WithLabelValues(w.cfg.Tag, w.cfg.Commit, w.cfg.BuildTime).Set(1)
	w.recordRole(w.role.Load())
}

// Run starts every long-lived task and returns when the first one stops.
func (w *Worker) Run(ctx context.Context) error {
	scfg := w.syn.Load()
	key, _ := scfg.SigningKey()
	r := w.role.Load()
	w.log.Info().
		Str("server_name", scfg.ServerName).
		Str("instance_name", w.cfg.InstanceName).
		Bool("sending", r.active).
		Strs("senders", r.shard.Instances()).
		Bool("presence", scfg.TrackPresence).
		Str("signing_key", key.ID()).
		Msg("starting the federation sender")
	if !r.active {
		w.log.Info().Msg("not named in federation_sender_instances; following the " +
			"replication stream without sending until homeserver.yaml names this instance")
	}

	// Bind the queues to the WORKER's lifetime, not to any batch's. A
	// transmission loop waits on remote servers long after the batch that
	// queued its work has finished.
	w.queues.Start(ctx)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		w.sub.Run(gctx)
		return gctx.Err()
	})
	g.Go(func() error { return w.sender.Run(gctx) })
	g.Go(func() error { return w.catchup.Run(gctx) })
	g.Go(func() error { return w.sampleGauges(gctx) })
	g.Go(func() error { return w.typingKeepAlive(gctx) })
	// The typing pushes, one at a time and in the order the diffs produced
	// them. See internal/sender.Typing.
	g.Go(func() error { return w.typing.Run(gctx) })

	err := g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// typingKeepAlive re-announces users who are still typing.
//
// A remote server expires a typing indicator after Synapse's FederationTimeout
// of one minute, so a user composing a long message would appear to stop typing
// mid-sentence. Synapse re-pokes on a wheel timer keyed per member
// (typing.py:143); a ticker is the same thing at this scale, since the work is
// bounded by the number of people typing right now and is usually zero.
//
// The tick is deliberately shorter than FederationPingInterval: a ticker that
// matched it exactly would drift into announcing at 40s + one tick, which is
// past the point the remote gives up.
func (w *Worker) typingKeepAlive(ctx context.Context) error {
	ticker := time.NewTicker(sender.FederationPingInterval / 4)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.typing.KeepAlive()
		}
	}
}

// samplePool records one connection pool's saturation.
//
// Sampled rather than tracked, like the queue depths and for the same reason:
// the pool keeps these counters itself, so reading them is both cheaper and
// harder to get wrong than maintaining a parallel set.
func (w *Worker) samplePool(name string, st *pgxpool.Stat) {
	if st == nil {
		return
	}
	metrics.DBPoolConns.WithLabelValues(name, "acquired").Set(float64(st.AcquiredConns()))
	metrics.DBPoolConns.WithLabelValues(name, "idle").Set(float64(st.IdleConns()))
	metrics.DBPoolConns.WithLabelValues(name, "total").Set(float64(st.TotalConns()))
	metrics.DBPoolConns.WithLabelValues(name, "max").Set(float64(st.MaxConns()))
	metrics.DBPoolAcquireWaitSeconds.WithLabelValues(name).Set(st.EmptyAcquireWaitTime().Seconds())
	metrics.DBPoolEmptyAcquires.WithLabelValues(name).Set(float64(st.EmptyAcquireCount()))
}

// sampleGauges refreshes the values that are cheaper to sample than to track.
//
// Sampling the queue depths rather than incrementing counters means a leak in
// the queues shows up even when the counters that should have caught it are
// themselves the buggy part.
func (w *Worker) sampleGauges(ctx context.Context) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			dests, pdus, edus := w.queues.PendingTotals()
			metrics.QueuedDestinations.Set(float64(dests))
			metrics.QueuedPDUs.Set(float64(pdus))
			metrics.QueuedEDUs.Set(float64(edus))
			metrics.KnownDestinations.Set(float64(w.queues.Count()))
			if w.hosts != nil {
				metrics.StateGroupCacheEntries.Set(float64(w.hosts.Len()))
			}
			w.samplePool("synapse", w.db.Stat())
			if w.writer != nil {
				w.samplePool("synapse_write", w.writer.Stat())
			}
			if w.cursors != nil {
				w.samplePool("state", w.cursors.Stat())
			}
			metrics.DestinationsBackingOff.Set(float64(w.limiter.Backoff()))

			if w.sub.Live() {
				metrics.ReplicationLive.Set(1)
			} else {
				metrics.ReplicationLive.Set(0)
			}
			for stream, pos := range w.sub.Positions() {
				metrics.StreamPosition.WithLabelValues(stream).Set(float64(pos))
			}

		}
	}
}
