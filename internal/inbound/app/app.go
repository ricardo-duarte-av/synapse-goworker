// Package app assembles the inbound federation front door:
// PUT /_matrix/federation/v1/send/{txnId}.
//
// EDU-only transactions are answered by forwarding each EDU to its stream
// writer. Transactions with PDUs are answered by write mode, which persists
// them through the event persisters as Synapse's inbound worker would.
// Anything not reproduced exactly goes to Synapse's main process.
//
// Missing capabilities degrade rather than refuse: without a write role,
// transactions with PDUs are proxied; without the replication bus, those
// from origins in backoff are.
package app

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/acl"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/edu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedfetch"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/inbound"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/keyring"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/pduverify"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/policysign"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/stateres"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/synapseauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/txn"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/limits"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Settings is everything the inbound path runs on.
type Settings struct {
	// InstanceName is our name in Synapse's lock tables and on the caches
	// stream.
	InstanceName string
	Synapse      *synapsecfg.Config
	Inbound      config.Config
	// Upstream is Synapse's main process.
	Upstream *proxy.Proxy

	// DSN is homeserver.yaml's database, normally through pgcat. The /send
	// path reads (store, pdustore) and writes what Synapse's own inbound
	// worker writes (inbound.DB) through three pools on it.
	DSN                                      string
	ReadMaxConns, PDUMaxConns, WriteMaxConns int32
	ConnectTimeout                           time.Duration
	// Transactions bounds /send transactions handled at once; past it they
	// wait for a slot.
	Transactions *limits.Gate

	Log zerolog.Logger
}

// Inbound is the assembled /send path.
type Inbound struct {
	handler *txn.Handler
	gate    *limits.Gate
	closers []func()
}

// Handler serves /send. Anything else it is given is proxied.
func (i *Inbound) Handler() http.Handler {
	if i.gate == nil {
		return i.handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !i.gate.Acquire(r.Context()) {
			return // the remote server went away while waiting
		}
		defer i.gate.Release()
		i.handler.ServeHTTP(w, r)
	})
}

// Close stops background work and releases the pools, newest first.
func (i *Inbound) Close() {
	for j := len(i.closers) - 1; j >= 0; j-- {
		i.closers[j]()
	}
}

// New connects and assembles the /send path.
func New(ctx context.Context, cfg Settings) (*Inbound, error) {
	if err := cfg.Inbound.Validate(); err != nil {
		return nil, err
	}
	hs, log, ic := cfg.Synapse, cfg.Log, cfg.Inbound
	out := &Inbound{gate: cfg.Transactions}
	fail := func(err error) (*Inbound, error) {
		out.Close()
		return nil, err
	}

	openCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout+20*time.Second)
	defer cancel()
	db, err := store.Open(openCtx, store.Config{
		DSN:            cfg.DSN,
		MaxConns:       orDefault(cfg.ReadMaxConns, 4),
		ConnectTimeout: cfg.ConnectTimeout,
		QueryTimeout:   5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	out.closers = append(out.closers, db.Close)

	notaries := hs.TrustedKeyServers
	verifier := fedauth.New(hs.ServerName, fedauth.Options{
		KeyRefetchDelay: time.Duration(ic.KeyRefetchMinutes) * time.Minute,
		Timeout:         time.Duration(ic.KeyTimeoutSeconds) * time.Second,
		Notaries:        notaries,
		DB:              db,
		MaxBodySize:     ic.MaxBodyBytes(),
		Log:             log,
	})

	router := edu.NewRouter(hs.Writers)
	repl := replication.NewClient(hs)
	if err := router.Validate(repl.Has); err != nil {
		return fail(err)
	}
	acls := acl.NewChecker(db, 0)
	processor := edu.NewProcessor(router, acls, repl, hs.TrackPresence, log)

	log.Info().
		Str("server_name", hs.ServerName).
		Bool("track_presence", hs.TrackPresence).
		Strs("typing", hs.Writers.Typing).
		Strs("receipts", hs.Writers.Receipts).
		Strs("to_device", hs.Writers.ToDevice).
		Strs("presence", hs.Writers.Presence).
		Strs("device_lists", hs.Writers.DeviceLists).
		Strs("notaries", notaries).
		Bool("replication_secret", hs.ReplicationSecret != "").
		Msg("Inbound: EDU routing from homeserver.yaml")

	opts := txn.Options{
		Verifier:        verifier,
		Upstream:        cfg.Upstream,
		Store:           db,
		EDUs:            processor,
		DomainWhitelist: hs.DomainWhitelist,
		MaxBody:         ic.MaxBodyBytes(),
		ForwardTimeout:  ic.EDUForwardTimeout(),
		Log:             log,
	}

	var handler atomic.Pointer[txn.Handler]
	var limiter *backoff.Limiter
	wdb, err := inbound.Open(openCtx, cfg.DSN, cfg.InstanceName, orDefault(cfg.WriteMaxConns, 4))
	if err != nil {
		// Degraded, not down: EDUs are still ours, PDUs go to main.
		log.Warn().Err(err).Msg("Inbound: write mode unavailable; transactions with PDUs go to Synapse")
	} else {
		out.closers = append(out.closers, wdb.Close)
		stop, l, err := writeMode(ctx, cfg, wdb, repl, acls, &opts, &handler)
		if err != nil {
			return fail(err)
		}
		limiter = l
		out.closers = append(out.closers, stop)
	}

	if ic.MarksOriginsUp() && wdb != nil {
		bus, r, err := openOriginUp(ctx, cfg.InstanceName, hs, wdb, log)
		if err != nil {
			log.Warn().Err(err).Msg("Inbound: origins in backoff cannot be marked up; their transactions go to Synapse")
		} else {
			busCtx, stopBus := context.WithCancel(context.Background())
			go bus.Listen(busCtx, r.OnReplicate)
			opts.OriginUp = r
			if limiter != nil {
				// Write mode's backoff timings are stored once the bus is up.
				limiter.SetWriter(r)
			}
			out.closers = append(out.closers, func() { stopBus(); bus.Close() })
		}
	}

	out.handler = txn.NewHandler(opts)
	handler.Store(out.handler)
	return out, nil
}

// writeMode builds the PDU path and puts it into opts. It returns what stops
// it, and the destination backoff its outbound requests are under.
func writeMode(ctx context.Context, cfg Settings, wdb *inbound.DB, repl *replication.Client,
	acls *acl.Checker, opts *txn.Options, handler *atomic.Pointer[txn.Handler]) (func(), *backoff.Limiter, error) {
	hs, log, ic := cfg.Synapse, cfg.Log, cfg.Inbound

	pst, err := pdustore.OpenAs(ctx, cfg.DSN, "", "", orDefault(cfg.PDUMaxConns, 4))
	if err != nil {
		return nil, nil, fmt.Errorf("inbound: %w", err)
	}
	pv := pduverify.New(pst, stateres.Options{})
	if ic.StateCacheEntries > 0 {
		pv.SetStateCacheEntries(ic.StateCacheEntries)
	}
	pc, err := pduverify.PushConfigFor(hs)
	if err != nil {
		pst.Close()
		return nil, nil, fmt.Errorf("inbound: push rules: %w", err)
	}
	pv.EnablePush(pc)

	proc := inbound.NewProcessor(wdb, pv, persister{repl, hs.Writers}, pst, hs.ServerName, log)
	if ic.Fallback == config.FallbackNone {
		proc.WithoutFallback()
		log.Warn().Msg("inbound.fallback is none: an event write mode cannot handle is retried, then dropped, as Synapse drops one it fails to handle")
	}
	// Every request write mode makes to another server is under Synapse's
	// destination retry limiter.
	limiter := backoff.New(pst, nil, hs.Backoff)
	// Keys Synapse does not hold yet, found as its Keyring would.
	keys := keyring.New(hs.ServerName, pst.VerifyKeys, hs.TrustedKeyServers)
	keys.Backoff = limiter
	// Requests our verifier refuses are judged as Synapse would, with this
	// keyring, and its refusals answered here.
	opts.SynapseAuth = synapseauth.New(hs.ServerName, hs.DomainWhitelist, keys.Fetch)

	var allow func(string) bool
	if hs.DomainWhitelist != nil {
		allow = func(s string) bool { return hs.DomainWhitelist[s] }
	}
	line, err := hs.SigningKeyLine()
	if err != nil {
		proc.Close()
		pst.Close()
		return nil, nil, fmt.Errorf("inbound: signing key: %w", err)
	}
	// Policy servers decide soft-fail on receipt (MSC4284): without asking,
	// spam Synapse soft-fails gets through.
	asker, err := policysign.New(hs.ServerName, line, allow)
	if err != nil {
		proc.Close()
		pst.Close()
		return nil, nil, fmt.Errorf("inbound: policy server client: %w", err)
	}
	asker.Backoff = limiter

	if ic.FetchesMissingEvents() {
		// An outer bound only: fedfetch times each attempt as Synapse does.
		fc, err := fedclient.New(hs.ServerName, line, allow, 10*time.Minute)
		if err != nil {
			proc.Close()
			pst.Close()
			return nil, nil, err
		}
		fetcher := fedfetch.New(fc, pst, pv, asker, hs.ServerName, keys)
		fetcher.Backoff = limiter
		fetcher.OnFailure = func(ctx context.Context, roomID, eventID, cause string) {
			if err := wdb.RecordFailedPull(ctx, roomID, eventID, cause); err != nil {
				log.Warn().Err(err).Str("event_id", eventID).Msg("Could not record a failed pull attempt")
			}
		}
		proc.EnableMissingEvents(&inbound.MissingDeps{Fetch: fetcher, Store: pst, Verifier: pv})
	}

	fd := inbound.NewFrontDoor(inbound.FrontDoorDeps{
		Store: pst, Verifier: pv, ACL: acls, Policy: asker, Processor: proc, Server: hs.ServerName, Keys: keys,
	})
	opts.PDUServer, opts.Responses = frontDoor{fd}, wdb
	// An event left to Synapse needs a Synapse process sweeping staged
	// events; the handler nudges the upstream into it.
	proc.OnDefer(func() {
		if h := handler.Load(); h != nil {
			h.NudgeSweep()
		}
	})
	proc.Start()
	log.Info().Str("instance", cfg.InstanceName).Bool("fetch_missing", ic.FetchesMissingEvents()).
		Msg("Inbound: write mode on; PDUs are persisted here")
	return func() { proc.Close(); pst.Close() }, limiter, nil
}

func orDefault(v, def int32) int32 {
	if v > 0 {
		return v
	}
	return def
}
