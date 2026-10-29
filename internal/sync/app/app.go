// Package app assembles client sync: /sync, simplified sliding sync
// (MSC4186), /events, /initialSync and /rooms/{id}/initialSync.
package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/limits"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/deviceinbox"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/handlers"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lazyload"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/notifier"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/presence"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// Pool sizes one database connection pool.
type Pool struct {
	DSN      string
	MaxConns int32
}

// Settings is everything the sync path runs on.
type Settings struct {
	// Synapse is homeserver.yaml. It is reloaded for the presence writer's
	// address and the replication secret whenever a relay fails in a way it
	// could explain.
	Synapse *synapsecfg.Source
	Sync    config.Config

	// Read is the SELECT-only pool; Inbox the one that deletes from
	// device_inbox; Sliding the one that owns the gosync schema.
	Read, Inbox, Sliding Pool
	ConnectTimeout       time.Duration

	// Auth authenticates every request (platform/clientauth).
	Auth *clientauth.Authenticator
	// Fallback is Synapse's main process, for a request a handler hands
	// back.
	Fallback http.Handler

	// States is the worker's one state cache; sync uses its event -> state
	// group entries.
	States *statecache.Cache
	// Replication is sync's view of the worker's one subscription. Nil
	// without redis: positions are then approximated from the database.
	Replication *bus.Consumer
	// Requests bounds concurrent requests, refused past it; InitialSyncs
	// bounds syncs without a since token, which wait for a slot.
	Requests, InitialSyncs *limits.Gate
	Tag                    string
	Log                    zerolog.Logger
}

// Sync is the assembled client sync path.
type Sync struct {
	cfg     Settings
	log     zerolog.Logger
	db      *store.Store
	inbox   *deviceinbox.Deleter
	sliding *slidingstore.Store
	sub     *replication.Subscriber
	routes  server.Routes
}

// New connects and assembles the sync path.
func New(ctx context.Context, cfg Settings) (*Sync, error) {
	if err := cfg.Sync.Validate(); err != nil {
		return nil, err
	}
	log, c := cfg.Log, cfg.Sync.Caches
	s := &Sync{cfg: cfg, log: log}
	metrics.BuildInfo.WithLabelValues(cfg.Tag).Set(1)

	openCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout+20*time.Second)
	defer cancel()

	var err error
	s.db, err = store.Open(openCtx, store.Config{
		DSN:                       cfg.Read.DSN,
		MaxConns:                  cfg.Read.MaxConns,
		ConnectTimeout:            cfg.ConnectTimeout,
		States:                    cfg.States,
		FilteredStateCacheEntries: c.FilteredStateCacheSize,

		UserFilterCacheEntries:        c.UserFilterCacheSize,
		RoomInfoCacheEntries:          c.RoomInfoCacheSize,
		RoomSummaryCacheEntries:       c.RoomSummaryCacheSize,
		HistoryVisibilityCacheEntries: c.HistoryVisibilityCacheSize,
		IgnoredUsersCacheEntries:      c.IgnoredUsersCacheSize,
		RoomsForUserCacheEntries:      c.RoomsForUserCacheSize,

		StreamCaches: store.StreamCacheSizes{
			Events:      c.EventsStreamCacheSize,
			Membership:  c.MembershipStreamCacheSize,
			Receipts:    c.ReceiptsStreamCacheSize,
			AccountData: c.AccountDataStreamCacheSize,
			ToDevice:    c.ToDeviceStreamCacheSize,
			Presence:    c.PresenceStreamCacheSize,
		},
	})
	if err != nil {
		return nil, err
	}
	metrics.RegisterCaches(s.db.DerivedCacheStats)
	metrics.RegisterStreamCaches(s.db.StreamCacheStats)

	// Reported, not enforced: through pgcat the role is Synapse's own, and
	// that the read path only reads is held by the code and its tests
	// (every store query is a SELECT), not by a grant.
	readOnly, err := s.db.IsReadOnly(openCtx)
	if err != nil {
		s.Close()
		return nil, err
	}
	if readOnly {
		metrics.DatabaseReadOnly.Set(1)
	} else {
		metrics.DatabaseReadOnly.Set(0)
	}

	// The one writing connection of classic sync: it deletes acknowledged
	// to-device messages, and nothing else (package deviceinbox).
	// Unavailable, they are left for Synapse to deliver, which is lossless.
	if cfg.Sync.ToDevice {
		s.inbox, err = deviceinbox.Open(openCtx, deviceinbox.Config{
			DSN:            cfg.Inbox.DSN,
			MaxConns:       cfg.Inbox.MaxConns,
			ConnectTimeout: cfg.ConnectTimeout,
		})
		if err != nil {
			log.Warn().Err(err).Msg("sync: to_device unavailable; messages are left for Synapse to deliver")
		} else {
			log.Info().Msg("sync: to_device enabled; acknowledged messages are deleted")
		}
	}

	presenceClient, err := s.openPresence(log)
	if err != nil {
		s.Close()
		return nil, err
	}

	notif := notifier.New()
	s.sub = s.subscribe(ctx, notif)

	if cfg.Sync.SlidingSync.Enabled {
		if err := s.openSliding(ctx, openCtx); err != nil {
			// Degraded: both sliding sync paths stay unregistered, so the
			// worker's catch-all sends them to Synapse.
			log.Warn().Err(err).Msg("sync: sliding sync unavailable; its requests go to Synapse")
			if s.sliding != nil {
				s.sliding.Close()
				s.sliding = nil
			}
		}
	}

	flag := cfg.Synapse.Current().Flag
	deps := handlers.Deps{
		Store:   s.db,
		Sliding: s.sliding,
		// Synapse's msc3575_enabled defaults to TRUE, unlike its neighbours.
		MSC3575Enabled:      flag("msc3575_enabled", true),
		MSC4308Enabled:      flag("msc4308_enabled", false),
		ServerName:          cfg.Synapse.Current().ServerName,
		Auth:                cfg.Auth,
		Fallback:            cfg.Fallback,
		Log:                 log,
		MSC4354Enabled:      flag("msc4354_enabled", false),
		MSC3391Enabled:      flag("msc3391_enabled", false),
		MSC3773Enabled:      flag("msc3773_enabled", false),
		MSC3874Enabled:      flag("msc3874_enabled", false),
		FilterTimelineLimit: cfg.Sync.TimelineLimitCap(),
		Replication:         s.sub,
		Notifier:            notif,
		LazyLoad:            lazyload.New(c.LazyLoadMembersCacheSize, c.LazyLoadMembersCacheTTL),
		Inbox:               s.inbox,
		Presence:            presenceClient,
		PushRuleFeatures: pushrules.Features{
			MSC1767Enabled:             flag("msc1767_enabled", false),
			MSC3381PollsEnabled:        flag("msc3381_polls_enabled", false),
			MSC3664Enabled:             flag("msc3664_enabled", false),
			MSC4028PushEncryptedEvents: flag("msc4028_push_encrypted_events", false),
			MSC4210Enabled:             flag("msc4210_enabled", true), // Synapse 1.164: on unless set off
			MSC4306Enabled:             flag("msc4306_enabled", false),
		},
	}
	s.routes = server.Routes{
		RoomInitialSync: handlers.RoomInitialSync(deps),
		InitialSync:     handlers.InitialSync(deps),
		Events:          handlers.Events(deps),
		Sync:            initialSyncGate(cfg.InitialSyncs, handlers.Sync(deps)),
		RoomState:       handlers.RoomState(deps),
	}
	if s.sliding != nil {
		s.routes.SlidingSync = handlers.SlidingSync(deps)
	}
	return s, nil
}

// Register adds the sync routes to mux. CORS sits inside the request log, so
// a preflight is still logged.
func (s *Sync) Register(mux *http.ServeMux) {
	server.Register(mux, s.routes, func(h http.Handler) http.Handler {
		return server.WithRequestLog(s.log, server.WithCORS(requestGate(s.cfg.Requests, h)))
	})
}

// requestGate refuses a request past the bound, with 503 and Retry-After: a
// client backs off and retries, which beats holding one more long-poll.
func requestGate(g *limits.Gate, next http.Handler) http.Handler {
	if g == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.TryAcquire() {
			limits.Busy(w, http.StatusServiceUnavailable, 5*time.Second)
			return
		}
		defer g.Release()
		next.ServeHTTP(w, r)
	})
}

// initialSyncGate makes a sync without a since token wait for a slot. It is
// the one request whose memory scales with the whole account, so a burst of
// them (clients after a restart) must be taken a few at a time.
func initialSyncGate(g *limits.Gate, next http.Handler) http.Handler {
	if g == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("since") != "" {
			next.ServeHTTP(w, r)
			return
		}
		if !g.Acquire(r.Context()) {
			return // the client went away while waiting
		}
		defer g.Release()
		next.ServeHTTP(w, r)
	})
}

// Run follows the replication stream and reaps sliding sync connections,
// until ctx ends.
func (s *Sync) Run(ctx context.Context) error {
	if s.sliding != nil {
		go reapSlidingConnections(ctx, s.sliding, s.cfg.Sync.SlidingSync.ReapIntervalMinutes, s.log)
	}
	s.sub.Run(ctx)
	return ctx.Err()
}

// Close releases the connection pools.
func (s *Sync) Close() {
	if s.sliding != nil {
		s.sliding.Close()
	}
	if s.inbox != nil {
		s.inbox.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
}

// openPresence builds the presence relay out of homeserver.yaml.
//
// This is the one thing classic sync TELLS the homeserver. Without it every
// account it serves appears permanently offline, because Synapse's /sync
// calls user_syncing() and ours would say nothing.
func (s *Sync) openPresence(log zerolog.Logger) (*presence.Client, error) {
	sc := s.cfg.Synapse.Current()
	if !sc.PresenceEnabled {
		log.Info().Msg("sync: presence is disabled on this homeserver; not relaying")
		return nil, nil
	}
	writer, err := sc.PresenceWriter()
	if err != nil {
		return nil, err
	}
	interval := presence.DeriveRelayInterval(sc.SyncOnlineTimeout, sc.LastActiveGranularity)
	pc, err := presence.New(presence.Config{
		Socket:        writer.Socket,
		URL:           writer.URL,
		Secret:        sc.ReplicationSecret,
		RelayInterval: interval,
		// Read again whenever a relay fails in a way the file could
		// explain: the writer moves and the secret rotates, silently.
		Resolve: resolvePresence(s.cfg.Synapse),
	}, log)
	if err != nil {
		return nil, err
	}
	metrics.RegisterPresence(pc.Tracked)
	log.Info().Str("writer", writer.Name).
		Str("address", writer.Socket+writer.URL).
		Dur("relay_interval", interval).Msg("sync: presence relaying enabled")
	return pc, nil
}

// subscribe builds the replication subscriber that wakes long-polls and
// keeps the caches honest.
func (s *Sync) subscribe(ctx context.Context, notif *notifier.Notifier) *replication.Subscriber {
	db, log := s.db, s.log
	sub := replication.New(replication.Config{Bus: s.cfg.Replication}, log, notif)

	// The stream-change caches ride alongside the notifier rather than inside
	// it. Both read every row; see streamfeed.go for why they cannot share a
	// reading of one.
	sub.AddListener(&streamFeeder{db: db})
	metrics.RegisterStreamPositions(sub.Positions)

	// The state caches hold data that is immutable but not permanent: a
	// purged room's state groups stay valid right up until they stop
	// existing. While we follow the stream that deletion is visible; while we
	// do not, it is not -- so a lost connection discards them, and disarms
	// the derived and stream caches, whose invalidations arrive over the
	// connection that has just gone.
	sub.SetOnDrop(func() {
		groups, filtered := db.CacheLen()
		db.PurgeCaches()
		db.DisarmDerivedCaches()
		db.DisarmStreamCaches()
		log.Warn().Int("state_groups", groups).Int("filtered_state", filtered).
			Msg("sync: replication dropped; discarded state caches, disarmed derived and stream caches")
	})
	sub.SetOnConnect(func(positions map[string]int64) {
		db.ArmDerivedCaches(positions)
		// Prefill on every connect: arming without it leaves each horizon at
		// "now", so every gate says "changed" and the queries never go away.
		// If it fails the caches stay disarmed rather than armed-and-empty.
		prefillCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := db.PrefillStreamCaches(prefillCtx, positions)
		cancel()
		if err != nil {
			db.DisarmStreamCaches()
			log.Error().Err(err).Msg("sync: replication live; stream caches left disarmed")
			return
		}
		log.Info().Msg("sync: replication live; derived and stream caches armed")
	})
	sub.SetInvalidator(&cacheInvalidator{db: db, log: log})

	// Seeded from the database so there is an answer before any traffic.
	// Every seeded value is a lower bound the first row on that stream
	// replaces.
	if seed, err := db.StreamPositions(ctx); err != nil {
		log.Warn().Err(err).Msg("sync: could not seed stream positions")
	} else {
		sub.Seed(seed)
	}
	return sub
}

// openSliding opens sliding sync's per-connection state: a write, in its own
// schema behind its own role.
func (s *Sync) openSliding(ctx, openCtx context.Context) error {
	var err error
	s.sliding, err = slidingstore.Open(openCtx, slidingstore.Config{
		DSN:            s.cfg.Sliding.DSN,
		MaxConns:       s.cfg.Sliding.MaxConns,
		ConnectTimeout: s.cfg.ConnectTimeout,
	})
	if err != nil {
		return err
	}
	if s.sliding.Created() {
		s.log.Info().Msg("sync: created the gosync schema's tables")
	}

	// The materialised tables sliding sync reads are maintained by Synapse's
	// event persister. Incomplete, a room silently missing from a client's
	// list is the failure nobody reports as a bug, so we refuse.
	ready, why, err := s.db.SlidingSyncTablesReady(openCtx)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("sliding sync tables not ready: %s", why)
	}

	sliding := s.sliding
	metrics.RegisterSlidingSyncStore(func() (metrics.SlidingStoreCounts, error) {
		// A scrape must not hang on the database.
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		c, err := sliding.Count(cctx)
		if err != nil {
			return metrics.SlidingStoreCounts{}, err
		}
		return metrics.SlidingStoreCounts{
			Connections: c.Connections, Positions: c.Positions, Rows: c.Rows,
		}, nil
	})
	s.log.Info().Msg("sync: sliding sync enabled")
	return nil
}
