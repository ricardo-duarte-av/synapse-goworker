// Package app assembles the federation read endpoints: /event, /state,
// /state_ids and /get_missing_events.
package app

import (
	"context"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/cache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/fedapi"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/limits"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
)

// Config is the `federation_reads:` block.
type Config struct {
	// NativeTimeoutSeconds bounds a non-streamed answer before falling back
	// to Synapse. Defaults to 5.
	NativeTimeoutSeconds int `yaml:"native_timeout_seconds"`
	// StreamTimeoutSeconds is the absolute backstop on a streamed /state;
	// StreamIdleTimeoutSeconds bounds time without progress, which is the
	// real control. Default 900 and 60.
	StreamTimeoutSeconds     int `yaml:"stream_timeout_seconds"`
	StreamIdleTimeoutSeconds int `yaml:"stream_idle_timeout_seconds"`
	// RCFederation is Synapse's rc_federation, applied to what we answer.
	RCFederation ratelimit.FederationSettings `yaml:"rc_federation"`
	// Caches bounds the in-process caches of immutable data, in MB.
	Caches cache.Settings `yaml:"caches"`
}

// Validate refuses values the endpoints cannot run with.
func (c Config) Validate() error {
	if err := c.Caches.Validate(); err != nil {
		return err
	}
	return c.RCFederation.Validate()
}

// Redis is where cache invalidations are read.
type Redis struct {
	Enabled  bool
	Address  string
	Password string
	DB       int
}

// Settings is everything the read endpoints run on.
type Settings struct {
	ServerName        string
	TrustedKeyServers []string
	FederationReads   Config
	DSN               string
	MaxConns          int32
	ConnectTimeout    time.Duration
	Redis             Redis
	// Upstream is Synapse's main process, the fallback for anything not
	// answered natively.
	Upstream *proxy.Proxy
	// Reads bounds concurrent non-streamed answers; StateStreams the
	// streamed /state ones.
	Reads, StateStreams *limits.Gate
	Log                 zerolog.Logger
}

// FedRead is the assembled read endpoints.
type FedRead struct {
	handler *fedapi.Handler
	reads   *limits.Gate
	streams *limits.Gate
	db      *store.Store
	sub     *replication.Subscriber
	log     zerolog.Logger
}

// New connects and assembles the read endpoints.
func New(ctx context.Context, cfg Settings) (*FedRead, error) {
	fc := cfg.FederationReads
	if err := fc.Validate(); err != nil {
		return nil, err
	}
	log := cfg.Log
	connectCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout+10*time.Second)
	defer cancel()
	db, err := store.Open(connectCtx, store.Config{
		DSN:            cfg.DSN,
		MaxConns:       cfg.MaxConns,
		ConnectTimeout: cfg.ConnectTimeout,
		Cache:          fc.Caches,
	})
	if err != nil {
		return nil, err
	}
	f := &FedRead{db: db, log: log, reads: cfg.Reads, streams: cfg.StateStreams}
	metrics.RegisterCaches(db.CacheStats)

	readOnly, err := db.IsReadOnly(connectCtx)
	if err != nil {
		db.Close()
		return nil, err
	}
	if !readOnly {
		log.Warn().Msg("federation reads: database role is NOT read-only")
	}

	// Caching is safe against deletions only while the cache-invalidation
	// stream is followed; without it the caches do not run.
	if cfg.Redis.Enabled {
		f.sub, err = replication.New(replication.Config{
			Enabled:  true,
			Address:  cfg.Redis.Address,
			Channel:  cfg.ServerName,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
		}, db, log)
		if err != nil {
			db.SetCachesArmed(false)
			log.Error().Err(err).Msg("federation reads: replication misconfigured; caches disabled")
		}
	} else {
		db.SetCachesArmed(false)
		log.Warn().Msg("federation reads: no redis in homeserver.yaml; caches disabled")
	}

	verifier := fedauth.New(cfg.ServerName, fedauth.Options{
		KeyRefetchDelay: time.Hour,
		Timeout:         30 * time.Second,
		Notaries:        cfg.TrustedKeyServers,
		DB:              db,
		Log:             log,
	})
	f.handler = fedapi.New(cfg.ServerName, fc.RCFederation, cfg.Upstream, log,
		fedapi.WithNative(matrixstate.NewResolver(db), verifier,
			time.Duration(fc.NativeTimeoutSeconds)*time.Second),
		fedapi.WithStreamTimeout(time.Duration(fc.StreamTimeoutSeconds)*time.Second),
		fedapi.WithStreamIdleTimeout(time.Duration(fc.StreamIdleTimeoutSeconds)*time.Second))
	metrics.RegisterRateLimitHosts(f.handler.Limiter().Hosts)

	s := fc.Caches.WithDefaults()
	log.Info().
		Int("state_groups_mb", s.StateGroupsMB).
		Int("events_mb", s.EventsMB).
		Int("event_state_groups_mb", s.EventStateGroupsMB).
		Int("auth_chains_mb", s.AuthChainsMB).
		Msg("federation reads: caching immutable data in process")
	return f, nil
}

// Handler serves the read endpoints. Paths it does not route are answered
// 404; the worker's router only hands it the four it serves (fedapi.Match).
//
// A streamed /state waits for a slot: it is rare, large, and a remote server
// asking for one will wait. Everything else waits briefly, then gets
// Synapse's 429, which federation backs off on.
func (f *FedRead) Handler() http.Handler {
	if f.reads == nil || f.streams == nil {
		return f.handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, _ := fedapi.Match(r.URL.EscapedPath())
		if route.Endpoint.Streams() {
			if !f.streams.Acquire(r.Context()) {
				return // the remote server went away while waiting
			}
			defer f.streams.Release()
			f.handler.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), readWait)
		ok := f.reads.Acquire(ctx)
		cancel()
		if !ok {
			limits.Busy(w, http.StatusTooManyRequests, time.Second)
			return
		}
		defer f.reads.Release()
		f.handler.ServeHTTP(w, r)
	})
}

// readWait is how long a federation read waits for a slot before 429.
const readWait = 5 * time.Second

// Run follows the invalidation stream and expires idle rate-limit state,
// until ctx ends.
func (f *FedRead) Run(ctx context.Context) error {
	if f.sub != nil {
		go f.sub.Run(ctx)
	}
	// Discard state for servers we have not heard from, so a one-off contact
	// does not occupy memory indefinitely.
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if n := f.handler.Limiter().Cleanup(time.Hour); n > 0 {
				f.log.Debug().Int("removed", n).Msg("Cleaned up idle rate limit state")
			}
		}
	}
}

// Close releases the pool.
func (f *FedRead) Close() { f.db.Close() }
