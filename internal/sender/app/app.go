// Package app assembles the outbound federation sender.
//
// The sender always runs: it follows the replication stream and keeps its
// typing state current whether or not it is sending. Whether it SENDS is
// decided by homeserver.yaml alone -- this process is a federation sender
// while federation_sender_instances names it (and send_federation is on), and
// takes its share of destinations by Synapse's own hashing when several
// senders are named. SetSynapse re-applies that decision whenever
// homeserver.yaml is re-read, so listing or unlisting this worker and
// restarting main is enough to give it the job or take it away.
package app

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/destinations"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/queue"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/retry"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/sender"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/sharding"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/sink"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/state"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/txn"
)

// Pool sizes one database connection pool.
type Pool struct {
	DSN      string
	MaxConns int32
}

// Settings is everything the sender runs on.
type Settings struct {
	// InstanceName is this process's name: what federation_sender_instances
	// must list for it to send, what its cursors are keyed by, and what it
	// calls itself on the replication bus.
	InstanceName string
	// Synapse is the parsed homeserver.yaml.
	Synapse *synapsecfg.Config
	// Sender is the `sender:` block.
	Sender config.Config

	// Read is the SELECT-only pool, Write the one that does a sender's
	// bookkeeping in Synapse's tables, State the one for gofederation.*.
	Read, Write, State Pool
	ConnectTimeout     time.Duration

	// MaxConcurrentDestinations bounds the per-destination transmission
	// loops, and CatchUpConcurrency the destinations caught up at once.
	MaxConcurrentDestinations int
	CatchUpConcurrency        int

	// Replication is the sender's view of the worker's one subscription.
	// Nil without redis: no new events are seen.
	Replication *bus.Consumer

	// EventVisibleToServer is filter_events_for_server for one local event
	// (the federation reads' port), which the MSC4354 sticky event backlog
	// is filtered with. Nil: the backlog is not sent (its rows stay).
	EventVisibleToServer func(ctx context.Context, eventID, server string) (bool, error)

	// UserAgent is sent on every federation request.
	UserAgent string
	// Tag, Commit and BuildTime are stamped into the build info metric.
	Tag, Commit, BuildTime string

	Log zerolog.Logger
}

// role is whether this process is a federation sender, and its shard.
type role struct {
	active bool
	shard  *sharding.Config
}

// Worker is the sender with everything that has a lifetime.
type Worker struct {
	cfg Settings
	log zerolog.Logger

	// syn is the current homeserver.yaml; role is what it says about us.
	syn  atomic.Pointer[synapsecfg.Config]
	role atomic.Pointer[role]
	// onReplicate is told when a Synapse process connects to the bus.
	onReplicate atomic.Pointer[func()]

	// msc4354 is msc4354_enabled when the sender started: whether the sticky
	// event backlog is sent.
	msc4354 bool

	db      *store.Store
	writer  *store.Writer
	cursors *state.Store
	// limiter is the per-destination backoff, shared by the routing filter and
	// the gate in front of the transmission loop. One instance, because two
	// implementations of a backoff would disagree under exactly the conditions
	// that make a backoff matter.
	limiter   *retry.Limiter
	queues    *queue.Manager
	sender    *sender.Sender
	devices   *sender.Devices
	catchup   *sender.CatchUp
	hosts     *destinations.CachedRoomHosts
	ephemeral *sender.Ephemeral
	typing    *sender.Typing
	sub       *replication.Subscriber
}

// New connects and assembles the sender. It does not start sending; Run
// does, and only while homeserver.yaml names this instance.
func New(ctx context.Context, cfg Settings) (*Worker, error) {
	if cfg.InstanceName == "" {
		return nil, fmt.Errorf("sender: instance_name is required")
	}
	if err := cfg.Sender.Validate(); err != nil {
		return nil, err
	}
	scfg := cfg.Synapse
	w := &Worker{cfg: cfg, log: cfg.Log}
	w.syn.Store(scfg)
	w.role.Store(w.roleFor(scfg))
	log := cfg.Log

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var err error
	if w.db, err = store.Open(openCtx, store.Config{
		OnQuery:        dbObserver("synapse"),
		DSN:            cfg.Read.DSN,
		MaxConns:       cfg.Read.MaxConns,
		ConnectTimeout: cfg.ConnectTimeout,
	}); err != nil {
		return nil, err
	}

	// Opened even while not a sender, so a role that cannot write is a
	// startup warning rather than a silent failure on the day this process is
	// named in homeserver.yaml.
	if w.writer, err = store.OpenWriter(openCtx, store.Config{
		OnQuery:        dbObserver("synapse_write"),
		DSN:            cfg.Write.DSN,
		MaxConns:       cfg.Write.MaxConns,
		ConnectTimeout: cfg.ConnectTimeout,
	}); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.writer.CanWrite(openCtx); err != nil {
		w.Close()
		return nil, err
	}

	if w.cursors, err = state.Open(openCtx, state.Config{
		OnQuery:        dbObserver("state"),
		DSN:            cfg.State.DSN,
		Table:          "gofederation.stream_positions",
		RetryTable:     "gofederation.destination_retry",
		InstanceName:   cfg.InstanceName,
		MaxConns:       cfg.State.MaxConns,
		ConnectTimeout: cfg.ConnectTimeout,
	}); err != nil {
		w.Close()
		return nil, err
	}
	if created := w.cursors.Created(); len(created) > 0 {
		log.Info().Strs("tables", created).Msg("created the sender's state tables")
	}

	key, err := scfg.SigningKey()
	if err != nil {
		w.Close()
		return nil, err
	}
	signer, err := txn.NewSigner(scfg.ServerName,
		fmt.Sprintf("%s %s %s", key.Algorithm, key.Version, encodeSeed(key.Seed)))
	if err != nil {
		w.Close()
		return nil, err
	}

	live := sink.NewHTTP(sink.HTTPConfig{
		Log:       log,
		UserAgent: cfg.UserAgent,
		Timeout:   scfg.ClientTimeout,
		Retries:   scfg.MaxLongRetries,
		MaxDelay:  scfg.MaxLongRetryDelay,
	})
	live.SetOnSent(w.countTransaction)
	live.SetOnEDUTypes(countEDUTypes)
	live.SetObservers(
		func(outcome string, took time.Duration) {
			metrics.SendDuration.WithLabelValues(outcome).Observe(took.Seconds())
		},
		func(delta int) { metrics.InFlightSends.Add(float64(delta)) },
	)

	// The persistent backoff, seeded from Synapse's destinations table and
	// then ours, so a restart does not forget that a quarter of the
	// destination list has been dead for years.
	w.limiter = retry.New(
		retry.Config{
			MinInterval: time.Duration(scfg.Backoff.MinMS) * time.Millisecond,
			Multiplier:  scfg.Backoff.Multiplier,
			MaxInterval: time.Duration(scfg.Backoff.MaxMS) * time.Millisecond,
		},
		w.loadTimings,
		w.persistTimings,
	)
	w.limiter.SetOnRecovered(func(destination string) {
		log.Info().Str("destination", destination).Msg("destination recovered; backoff cleared")
	})

	// The MSC4354 sticky event backlog, drained by every destination's
	// queue between live transactions.
	var sticky queue.StickySource
	w.msc4354 = scfg.Flag("msc4354_enabled", false) && cfg.EventVisibleToServer != nil
	if w.msc4354 {
		sticky = &sender.StickyBacklog{
			Store: w.writer, Events: w.db, Visible: cfg.EventVisibleToServer,
			ServerName: scfg.ServerName, Limit: cfg.Sender.MaxPDUsPerTransaction,
		}
	}

	w.queues = queue.NewManager(queue.ManagerConfig{
		Sticky: sticky,
		// Presence only, and never typing or receipts; see queue.BatchConfig.
		Batch: queue.BatchConfig{
			MaxStates: cfg.Sender.PresenceBatchStates,
			MaxWait:   cfg.Sender.PresenceBatchMaxWait(),
		},
		Limits: queue.Limits{
			MaxPDUs: cfg.Sender.MaxPDUsPerTransaction,
			MaxEDUs: cfg.Sender.MaxEDUsPerTransaction,
		},
		Signer:        signer,
		IDs:           txn.NewIDGenerator(txn.DefaultIDPrefix),
		Sink:          live,
		Log:           log,
		MaxConcurrent: cfg.MaxConcurrentDestinations,
		OnSuccess:     w.onDelivered,
		OnEDUsSent:    w.onEDUsDelivered,
		OnOutcome:     w.onSendOutcome,
		Due: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.shouldHandle(destination) && w.limiter.Due(ctx, destination)
		},
		// A destination that will not be tried again for an hour is in a real
		// outage, not a blip, and holding its presence and receipts until it
		// returns is both useless and unbounded.
		LongBackoff: func(destination string) bool {
			t := w.limiter.Timings(destination)
			return time.Duration(t.RetryInterval)*time.Millisecond > queue.CatchUpRetryInterval
		},
		OnEDUsDropped: func(destination string, n int) {
			metrics.EDUsDropped.WithLabelValues(destination).Add(float64(n))
		},
		// A 429 throttles US; it does not condemn the host. See
		// retry.Limiter.RateLimited.
		OnRateLimited: func(destination string, after time.Duration) time.Duration {
			metrics.RateLimited.WithLabelValues(destination).Inc()
			wait := w.limiter.RateLimited(destination, after)
			log.Info().Str("destination", destination).Dur("wait", wait).
				Msg("destination asked us to slow down; backing our own rate off, not the destination")
			return wait
		},
	})

	// The joined-host lookup is the most expensive thing in the per-event path
	// and its answer cannot change: a state group is immutable, so a new state
	// creates a new group rather than editing one. Caching it needs no
	// invalidation and turns the query into once per state change rather than
	// once per event.
	hosts := destinations.NewCachedRoomHosts(w.db, cfg.Sender.HostCacheEntries)
	hosts.SetOnLookup(func(hit bool) {
		result := "miss"
		if hit {
			result = "hit"
		}
		metrics.StateGroupCache.WithLabelValues(result).Inc()
	})
	w.hosts = hosts

	w.sender = sender.New(sender.Config{
		Store:          w.db,
		Cursors:        w.cursors,
		Resolver:       destinations.NewResolver(hosts, scfg.ServerName, scfg.DomainWhitelist),
		Queues:         w.queues,
		Observer:       &observer{},
		Log:            log,
		ServerName:     scfg.ServerName,
		ShouldHandle:   w.shouldHandle,
		Active:         w.Active,
		BatchLimit:     cfg.Sender.EventBatchLimit,
		RecordRoutes:   w.recordRoutes,
		RecordPosition: w.recordPosition,
	})

	w.devices = sender.NewDevices(sender.DevicesConfig{
		Store:                 w.db,
		Log:                   log,
		Cursors:               w.cursors,
		Queues:                w.queues,
		ShouldHandle:          w.shouldHandle,
		AllowDeviceNameLookup: scfg.AllowDeviceNameLookup,
		// The fourth of Synapse's retry-filtered senders, with the same hour of
		// slack as the rest (federation/sender/__init__.py:1074).
		DueWithin: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.limiter.DueWithin(ctx, destination, queue.CatchUpRetryInterval)
		},
	})

	w.ephemeral = sender.NewEphemeral(sender.EphemeralConfig{
		Store:         w.db,
		Queues:        w.queues,
		Log:           log,
		ServerName:    scfg.ServerName,
		ShouldHandle:  w.shouldHandle,
		TrackPresence: scfg.TrackPresence,
		// The hour of slack is Synapse's retry_due_within_ms, passed as
		// CATCHUP_RETRY_INTERVAL wherever it decides whether an ephemeral EDU
		// is worth building at all.
		DueWithin: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.limiter.DueWithin(ctx, destination, queue.CatchUpRetryInterval)
		},
	})

	w.typing = sender.NewTyping(sender.TypingConfig{
		Hosts:        w.db,
		Queues:       w.queues,
		Log:          log,
		ServerName:   scfg.ServerName,
		ShouldHandle: w.shouldHandle,
		Due: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.limiter.Due(ctx, destination)
		},
	})

	w.catchup = sender.NewCatchUp(sender.CatchUpConfig{
		Store:        w.db,
		Sticky:       w.writer,
		ServerName:   scfg.ServerName,
		Queues:       w.queues,
		Log:          log,
		ShouldHandle: w.shouldHandle,
		Active:       w.Active,
		Concurrency:  cfg.CatchUpConcurrency,
		Due: func(destination string) bool {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return w.limiter.Due(ctx, destination)
		},
	})

	w.sub = replication.New(replication.Config{
		Bus:          cfg.Replication,
		InstanceName: cfg.InstanceName,
	}, log, &handler{worker: w, log: log})

	w.registerMetrics()
	return w, nil
}

// roleFor reads what homeserver.yaml says about this process.
//
// Synapse sends from main when send_federation is on and no instances are
// listed; synapsecfg reports that as a SenderInstances of ["master"], so an
// empty list here means nobody sends and this process certainly does not.
func (w *Worker) roleFor(scfg *synapsecfg.Config) *role {
	return &role{
		active: slices.Contains(scfg.SenderInstances, w.cfg.InstanceName),
		shard:  sharding.New(scfg.SenderInstances),
	}
}

// SetOnReplicate registers what to do when a Synapse process (re)connects
// to the replication bus, which is when Synapse itself picks up a changed
// homeserver.yaml. It must not block.
func (w *Worker) SetOnReplicate(f func()) { w.onReplicate.Store(&f) }

// Active reports whether this process is currently a federation sender.
func (w *Worker) Active() bool { return w.role.Load().active }

// shouldHandle reports whether this process owns a destination right now:
// it is a sender, and the destination hashes to it.
func (w *Worker) shouldHandle(destination string) bool {
	r := w.role.Load()
	return r.active && r.shard.ShouldHandle(w.cfg.InstanceName, destination)
}

// SetSynapse applies a re-read homeserver.yaml. Only the sender role and the
// shard change at runtime; everything else needs a restart, and a change to
// it is logged as such.
func (w *Worker) SetSynapse(scfg *synapsecfg.Config) {
	prev := w.syn.Swap(scfg)
	before := w.role.Load()
	after := w.roleFor(scfg)
	w.role.Store(after)
	w.recordRole(after)

	if prev.ServerName != scfg.ServerName || prev.Redis != scfg.Redis {
		w.log.Warn().Msg("homeserver.yaml changed server_name or redis; the sender keeps " +
			"the old values until this process restarts")
	}
	if before.active == after.active &&
		slices.Equal(before.shard.Instances(), after.shard.Instances()) {
		return
	}
	w.log.Warn().
		Bool("was_sending", before.active).
		Bool("sending", after.active).
		Strs("senders", after.shard.Instances()).
		Msg("federation sender role changed by homeserver.yaml")

	if after.active {
		// While we were not sending, someone else was keeping the backoffs;
		// ours are stale. Then resume: the pickup loop restarts from our own
		// cursor, or from where Synapse's senders had got to, and catch-up
		// reconciles every destination that is owed something.
		w.limiter.Forget()
		max, err := w.db.MaxStreamOrdering(context.Background())
		if err != nil {
			w.log.Error().Err(err).Msg("could not read the event stream position; " +
				"sending resumes on the next event")
			return
		}
		w.sender.NotifyNewEvents(max)
		w.catchup.Kick()
	}
}

func (w *Worker) recordRole(r *role) {
	if r.active {
		metrics.SenderActive.Set(1)
	} else {
		metrics.SenderActive.Set(0)
	}
	metrics.SenderShards.Set(float64(len(r.shard.Instances())))
}

// Close releases the connection pools.
func (w *Worker) Close() {
	if w.cursors != nil {
		w.cursors.Close()
	}
	if w.writer != nil {
		w.writer.Close()
	}
	if w.db != nil {
		w.db.Close()
	}
}

func encodeSeed(seed []byte) string {
	return base64RawStd.EncodeToString(seed)
}
