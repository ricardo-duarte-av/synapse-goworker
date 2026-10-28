// Command goworker is one Go process for the Synapse work that
// gosync-worker, goinbound-worker, gofederation-worker, gopro-worker and
// goevent-creator did separately: client /sync, inbound federation /send,
// the federation read endpoints, the outbound federation sender, and the
// client endpoints that create room events.
//
// It has no switches for which of those it does. It serves whatever nginx
// routes to it, proxying anything else to Synapse's main process, and it
// sends federation only while homeserver.yaml names it in
// federation_sender_instances.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	creatorapp "github.com/ricardo-duarte-av/synapse-goworker/internal/creator/app"
	fedreadapp "github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/app"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/fedapi"
	inboundapp "github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/app"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/httpserve"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/limits"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	senderapp "github.com/ricardo-duarte-av/synapse-goworker/internal/sender/app"
	syncapp "github.com/ricardo-duarte-av/synapse-goworker/internal/sync/app"
)

// Stamped by the build. The same three names as the workers this replaces.
var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	var (
		configPath  = flag.String("config", "goworker.yaml", "path to the configuration file")
		check       = flag.Bool("check", false, "connect everything, report what this process would do, then exit")
		healthcheck = flag.Bool("healthcheck", false, "probe a running worker's /health and exit")
		showVersion = flag.Bool("version", false, "print build information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("goworker %s (%s, built %s, %s)\n", tag, commit, buildTime, runtime.Version())
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "goworker: %v\n", err)
		os.Exit(1)
	}
	if *healthcheck {
		if err := probe(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "goworker: %v\n", err)
			os.Exit(1)
		}
		return
	}

	log := newLogger(cfg.Log)
	if err := run(cfg, log, *check); err != nil {
		log.Error().Err(err).Msg("stopped")
		os.Exit(1)
	}
}

func run(cfg *config.Config, log zerolog.Logger, checkOnly bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.Memory.LimitMB > 0 {
		debug.SetMemoryLimit(int64(cfg.Memory.LimitMB) << 20)
	}
	setRuntimeMetrics(tag, commit, buildTime, cfg.Memory.LimitMB)

	// The one copy of homeserver.yaml every part reads.
	src, err := synapsecfg.Open(cfg.HomeserverYAML)
	if err != nil {
		return err
	}
	hs := src.Current()
	log.Info().Str("version", tag).Str("commit", commit).
		Str("server_name", hs.ServerName).Str("instance_name", cfg.InstanceName).
		Int("memory_limit_mb", cfg.Memory.LimitMB).Msg("starting goworker")

	upstream, err := proxy.New(cfg.Upstream, componentLogger(log, cfg.Log, "proxy"))
	if err != nil {
		return err
	}

	db, lim, pools := cfg.Database, cfg.Limits, cfg.Database.Pools
	// Every pool connects as Synapse does: homeserver.yaml's database block,
	// through its pooler (pgcat here), unless database.dsn overrides it.
	dsn := db.DSNOr(hs.Database.DSN())
	if dsn == "" {
		return fmt.Errorf("no database: homeserver.yaml has no usable database block and database.dsn is unset")
	}
	log.Info().Str("database", redactDSN(dsn)).Msg("connecting every pool as homeserver.yaml does")
	// The one subscription to Synapse's replication stream; every part
	// reads it through a consumer of its own.
	var stream *bus.Bus
	consumer := func(name string) *bus.Consumer { return nil }
	if hs.Redis.Enabled {
		stream = bus.New(bus.Config{
			Address:  redisAddress(hs.Redis),
			Username: hs.Redis.Username,
			Password: hs.Redis.Password,
			DB:       hs.Redis.DBID,
			Channel:  hs.ServerName,
		}, componentLogger(log, cfg.Log, "bus"))
		consumer = func(name string) *bus.Consumer { return stream.Consumer(name, 0) }
	} else {
		log.Warn().Msg("homeserver.yaml has no redis: nothing wakes long-polls, invalidates caches or shows the sender new events")
	}

	whoamiSocket, whoamiURL := "", ""
	if len(cfg.Upstream.Sockets) > 0 {
		whoamiSocket = cfg.Upstream.Sockets[0]
	} else {
		whoamiURL = "http://" + cfg.Upstream.Addrs[0]
	}

	// The one cache of state groups' full state, read by the federation
	// reads and inbound write mode alike.
	states := statecache.New(int64(cfg.Memory.StateCacheMB)<<20, int64(cfg.Memory.EventGroupCacheMB)<<20)
	// The one cache of other servers' published keys, which /send and the
	// federation reads verify requests with. It reads Synapse's
	// server_keys_json through whichever of the two opens a pool first.
	keys := fedauth.NewKeys(hs.ServerName,
		inboundapp.KeyOptions(cfg.Inbound, hs.TrustedKeyServers, componentLogger(log, cfg.Log, "inbound")))

	var closers []func()
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}()

	// Access tokens, checked as Synapse's InternalAuth does, with main's
	// /whoami for what is not reproduced (platform/clientauth).
	var userIPs clientauth.Publisher
	if hs.Redis.Enabled {
		r := clientauth.NewRedis(hs.Redis)
		closers = append(closers, func() { _ = r.Close() })
		userIPs = r
	}
	clientAuth, err := clientauth.New(ctx, clientauth.Config{
		ServerName:      hs.ServerName,
		Synapse:         hs.ClientAuth,
		AppServiceFiles: hs.AppServiceFiles,
		DSN:             dsn,
		MaxConns:        pools.ClientAuth,
		ConnectTimeout:  db.ConnectTimeout(),
		Whoami: clientauth.WhoamiConfig{
			Socket:      whoamiSocket,
			URL:         whoamiURL,
			PositiveTTL: cfg.Sync.Auth.Positive(),
			NegativeTTL: cfg.Sync.Auth.Negative(),
			MaxEntries:  cfg.Sync.Auth.Entries(),
			Timeout:     cfg.Sync.Auth.Timeout(),
		},
		TTL:         cfg.Sync.Auth.Positive(),
		Replication: consumer("client_auth"),
		Publisher:   userIPs,
		Log:         componentLogger(log, cfg.Log, "client_auth"),
	})
	if err != nil {
		return fmt.Errorf("client auth: %w", err)
	}
	closers = append(closers, clientAuth.Close)

	clientSync, err := syncapp.New(ctx, syncapp.Settings{
		Synapse:        src,
		Sync:           cfg.Sync,
		Read:           syncapp.Pool{DSN: dsn, MaxConns: pools.SyncRead},
		Inbox:          syncapp.Pool{DSN: dsn, MaxConns: pools.ToDevice},
		Sliding:        syncapp.Pool{DSN: dsn, MaxConns: pools.SlidingSync},
		ConnectTimeout: db.ConnectTimeout(),
		Auth:           clientAuth,
		Fallback:       fallback(upstream),
		Replication:    consumer("sync"),
		States:         states,
		Requests:       limits.NewGate("sync_requests", lim.SyncRequests),
		InitialSyncs:   limits.NewGate("initial_syncs", lim.InitialSyncs),
		Tag:            tag,
		Log:            componentLogger(log, cfg.Log, "sync"),
	})
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	closers = append(closers, clientSync.Close)

	reads, err := fedreadapp.New(ctx, fedreadapp.Settings{
		ServerName:        hs.ServerName,
		TrustedKeyServers: hs.TrustedKeyServers,
		FederationReads:   cfg.FederationReads,
		DSN:               dsn,
		MaxConns:          pools.FederationReadsRead,
		ConnectTimeout:    db.ConnectTimeout(),
		Replication:       consumer("federation_reads"),
		Upstream:          upstream,
		States:            states,
		Keys:              keys,
		Reads:             limits.NewGate("federation_reads", lim.FederationReads),
		StateStreams:      limits.NewGate("state_streams", lim.StateStreams),
		Log:               componentLogger(log, cfg.Log, "federation_reads"),
	})
	if err != nil {
		return fmt.Errorf("federation reads: %w", err)
	}
	closers = append(closers, reads.Close)

	inbound, err := inboundapp.New(ctx, inboundapp.Settings{
		InstanceName:   cfg.InstanceName,
		Synapse:        hs,
		Inbound:        cfg.Inbound,
		Upstream:       upstream,
		DSN:            dsn,
		ReadMaxConns:   pools.InboundRead,
		PDUMaxConns:    pools.InboundPDU,
		WriteMaxConns:  pools.InboundWrite,
		ConnectTimeout: db.ConnectTimeout(),
		Transactions:   limits.NewGate("inbound_transactions", lim.InboundTransactions),
		Replication:    consumer("inbound"),
		States:         states,
		Keys:           keys,
		Log:            componentLogger(log, cfg.Log, "inbound"),
	})
	var send http.Handler
	if err != nil {
		// Degraded, not down: /send then goes to main, as it did before
		// goinbound.
		degraded.WithLabelValues("inbound").Set(1)
		log.Warn().Err(err).Msg("inbound /send unavailable; it is proxied to main")
	} else {
		closers = append(closers, inbound.Close)
		send = inbound.Handler()
	}

	creator, err := creatorapp.New(ctx, creatorapp.Settings{
		Synapse:        hs,
		Upstream:       upstream,
		Auth:           clientAuth,
		DSN:            dsn,
		ReadMaxConns:   pools.CreatorRead,
		WriteMaxConns:  pools.CreatorWrite,
		ConnectTimeout: db.ConnectTimeout(),
		Log:            componentLogger(log, cfg.Log, "creator"),
	})
	var createEvents http.Handler
	if err != nil {
		// Degraded, not down: the event-creation paths then go to main.
		degraded.WithLabelValues("creator").Set(1)
		log.Warn().Err(err).Msg("event creator unavailable; its paths are proxied to main")
	} else {
		closers = append(closers, creator.Close)
		createEvents = creator.Handler()
	}

	sender, err := senderapp.New(ctx, senderapp.Settings{
		InstanceName:              cfg.InstanceName,
		Synapse:                   hs,
		Sender:                    cfg.Sender,
		Read:                      senderapp.Pool{DSN: dsn, MaxConns: pools.SenderRead},
		Write:                     senderapp.Pool{DSN: dsn, MaxConns: pools.SenderWrite},
		State:                     senderapp.Pool{DSN: dsn, MaxConns: pools.SenderState},
		ConnectTimeout:            db.ConnectTimeout(),
		MaxConcurrentDestinations: lim.SenderDestinations,
		CatchUpConcurrency:        lim.SenderCatchUp,
		Replication:               consumer("sender"),
		UserAgent:                 "synapse-goworker/" + tag,
		Tag:                       tag, Commit: commit, BuildTime: buildTime,
		Log: componentLogger(log, cfg.Log, "sender"),
	})
	if err != nil {
		return fmt.Errorf("sender: %w", err)
	}
	closers = append(closers, sender.Close)

	if checkOnly {
		log.Info().Bool("sending_federation", sender.Active()).
			Msg("configuration, homeserver.yaml and database access are usable")
		return nil
	}

	client := http.NewServeMux()
	clientSync.Register(client)
	router := &httpserve.Router{
		Send:         send,
		Reads:        reads.Handler(),
		ReadsMatch:   func(p string) bool { _, ok := fedapi.Match(p); return ok },
		Creator:      createEvents,
		CreatorMatch: creatorapp.Match,
		Client:       client,
		Fallback:     fallback(upstream),
	}

	mode, _ := cfg.Listen.ParsedSocketMode() // validated at load
	spec := httpserve.ListenSpec{Socket: cfg.Listen.Socket, Addr: cfg.Listen.Addr, SocketMode: mode}
	listener, err := httpserve.Listen(spec)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: router,
		// Remote servers and clients over the open internet: bound the
		// headers, but no write timeout -- a /sync long-poll and a streamed
		// /state both hold a connection open legitimately.
		ReadHeaderTimeout: 20 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	metricsSrv := metricsServer(cfg.Listen.Metrics)

	g, gctx := errgroup.WithContext(ctx)
	if stream != nil {
		g.Go(func() error { stream.Run(gctx); return gctx.Err() })
	}
	g.Go(func() error { clientAuth.Run(gctx); return gctx.Err() })
	g.Go(func() error { return clientSync.Run(gctx) })
	g.Go(func() error { return reads.Run(gctx) })
	g.Go(func() error { return sender.Run(gctx) })
	if creator != nil {
		g.Go(func() error { return creator.Run(gctx) })
	}
	g.Go(func() error {
		watchHomeserver(gctx, src, sender, log)
		return gctx.Err()
	})
	g.Go(func() error {
		log.Info().Str("listen", httpserve.Describe(spec)).Msg("serving")
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listener: %w", err)
		}
		return nil
	})
	if metricsSrv != nil {
		g.Go(func() error {
			log.Info().Str("addr", metricsSrv.Addr).Msg("serving metrics")
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("metrics listener: %w", err)
			}
			return nil
		})
	}
	g.Go(func() error {
		<-gctx.Done()
		log.Info().Msg("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(shutdownCtx)
		}
		return srv.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// fallback forwards to Synapse's main process whatever the worker does not
// serve.
func fallback(p *proxy.Proxy) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.Forward(w, r, 0)
	})
}

// redactDSN hides a DSN's password for the log.
func redactDSN(dsn string) string {
	return passwordRE.ReplaceAllString(dsn, "password=REDACTED")
}

var passwordRE = regexp.MustCompile(`password=('[^']*'|\S+)`)

// redisAddress is homeserver.yaml's redis as a dial address: a unix socket
// path, or host:port.
func redisAddress(r synapsecfg.Redis) string {
	if r.Socket != "" {
		return r.Socket
	}
	return r.Addr
}

func newLogger(cfg config.Log) zerolog.Logger {
	l := zerolog.New(os.Stdout)
	if cfg.Pretty {
		l = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"})
	}
	return l.Level(cfg.LevelFor("")).With().Timestamp().Logger()
}

// componentLogger is one part's logger: its lines carry "component", and its
// level is log.components' for that part, or log.level. A logger's level is
// its own, so a part may be noisier or quieter than the process.
func componentLogger(log zerolog.Logger, cfg config.Log, component string) zerolog.Logger {
	return log.With().Str("component", component).Logger().Level(cfg.LevelFor(component))
}
