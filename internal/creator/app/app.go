// Package app assembles the event creator: the client endpoints that create
// room events (rooms/{id}/send, /state, /redact, the membership verbs, join/,
// knock/ and profile/), in place of Synapse's event_creator worker.
//
// It builds and signs the event, auths it, computes its state group and push
// actions, writes what Synapse's event creator writes itself, and hands the
// event to the room's events writer over send_events (package creator).
// Whatever it does not reproduce exactly goes to Synapse's main process,
// before anything is written.
//
// Every room is served, and every endpoint: what reaches it is nginx's
// decision. Missing capabilities degrade rather than refuse: without its own
// table, profile writes are main's; if it cannot start at all, every one of
// its paths is.
package app

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/build"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/creator"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/fedclient"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/pushcalc"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/writer"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/appservice"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_requests_total",
		Help: "Requests by endpoint, whether they write, which rooms they name, how they were served and the status class returned.",
	}, []string{"endpoint", "write", "room", "served", "status"})

	upstreamSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goevent_upstream_seconds",
		Help:    "Time waiting on the upstream (Synapse main), by endpoint.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"endpoint"})
)

// Settings is everything the event creator runs on.
type Settings struct {
	Synapse *synapsecfg.Config
	// Upstream is Synapse's main process.
	Upstream *proxy.Proxy
	// Auth is the worker's one client authenticator (platform/clientauth).
	Auth creator.Authenticator

	// DSN is homeserver.yaml's database, through pgcat. The creator reads
	// through one pool and writes what Synapse's event creator writes
	// through another.
	DSN                         string
	ReadMaxConns, WriteMaxConns int32
	ConnectTimeout              time.Duration

	Log zerolog.Logger
}

// App is the event creator.
type App struct {
	creator *creator.Creator
	up      *proxy.Proxy
	log     zerolog.Logger
	closers []func()
}

// New connects the event creator. An error means none of it can run; the
// caller proxies its paths to main.
func New(ctx context.Context, s Settings) (*App, error) {
	hs := s.Synapse
	a := &App{up: s.Upstream, log: s.Log}
	fail := func(err error) (*App, error) {
		a.Close()
		return nil, err
	}
	if s.Auth == nil {
		return nil, fmt.Errorf("creator: no client authenticator")
	}
	if hs.ReplicationSecret == "" {
		s.Log.Warn().Msg("no worker_replication_secret: replication requests are sent without one")
	}
	for _, w := range hs.Writers.Events {
		if _, ok := hs.Instance(w); !ok {
			return nil, fmt.Errorf("creator: events writer %q is not in instance_map", w)
		}
	}

	openCtx, cancel := context.WithTimeout(ctx, s.ConnectTimeout+20*time.Second)
	defer cancel()
	db, err := pdustore.Open(openCtx, s.DSN, s.ReadMaxConns)
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	a.closers = append(a.closers, db.Close)
	w, err := writer.Open(openCtx, s.DSN, s.WriteMaxConns)
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	a.closers = append(a.closers, w.Close)

	line, err := hs.SigningKeyLine()
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	key, err := build.ParseKey(hs.ServerName, line)
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	pushCfg, err := pushcalc.ConfigFor(hs)
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	services, err := appservice.Load(hs.AppServiceFiles, hs.ServerName)
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	var allow func(string) bool
	if hs.DomainWhitelist != nil {
		allow = func(server string) bool { return hs.DomainWhitelist[server] }
	}
	fed, err := fedclient.New(hs.ServerName, line, allow, hs.FederationIPRanges)
	if err != nil {
		return fail(fmt.Errorf("creator: %w", err))
	}
	repl := replication.NewClient(hs, replication.Creator)

	o := creator.Options{
		Store:          db,
		Writer:         w,
		Persister:      repl,
		Main:           repl,
		RemoteProfiles: fed,
		Policy:         fed,
		Delayed:        hs.Delayed,
		Key:            key,
		Push:           pushcalc.New(db, pushCfg),
		ServerName:     hs.ServerName,
		EventsWriter:   hs.Writers.EventsWriter,
		Sticky:         hs.Flag("msc4354_enabled", false),
		Log:            s.Log,

		Auth:                   s.Auth,
		AppServices:            appservice.NewRegistry(services),
		RedactionAllowedPeriod: hs.RedactionAllowedPeriod,
		Membership:             hs.Membership,
		MSC3848:                hs.Flag("msc3848_enabled", false),
		MSC4293:                hs.Flag("msc4293_enabled", false),
		MSC4155:                hs.Flag("msc4155_enabled", false),

		ProfileWriter:         repl,
		ProfileWriterInstance: hs.Writers.ProfileWriter(),
		MSC4069:               hs.Flag("msc4069_profile_inhibit_propagation", false),

		IncludeProfileUpdatesInSync: hs.IncludeProfileUpdatesInSync,
	}
	// Our own table, which profile propagations survive restarts in. Without
	// it, profile writes are main's.
	if err := w.Setup(openCtx); err != nil {
		s.Log.Warn().Err(err).Msg("cannot create goevent_profile_propagations; profile writes are proxied to main")
		o.ProfileWriter = nil
	}
	a.creator = creator.New(o)
	return a, nil
}

// Run resumes the profile propagations a previous run left unfinished, as
// Synapse resumes its scheduled tasks, then waits.
func (a *App) Run(ctx context.Context) error {
	if n, err := a.creator.ResumeProfiles(ctx); err != nil {
		a.log.Error().Err(err).Msg("could not read the unfinished profile propagations")
	} else if n > 0 {
		a.log.Info().Int("jobs", n).Msg("resumed profile propagations")
	}
	<-ctx.Done()
	return nil
}

// Close closes the pools.
func (a *App) Close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
	a.closers = nil
}

// Match reports whether the creator serves a request: the writes under
// Synapse's own servlet patterns. Reads under the same prefixes (a state
// event's GET, a profile's) are not its.
func Match(method, escapedPath string) bool {
	return route.Classify(method, escapedPath).Write
}

// Handler serves what Match accepts; anything it does not reproduce goes to
// main.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serve) }

func (a *App) serve(w http.ResponseWriter, r *http.Request) {
	req := route.Classify(r.Method, r.URL.EscapedPath())
	served := "proxy"
	if req.Write {
		var out creator.Outcome
		switch {
		case req.Endpoint == route.Profile:
			// A profile change names no room: it goes into every room the
			// user is in.
			out = a.creator.ServeProfile(w, r, req)
		case creator.Serves(req.Endpoint):
			out = a.creator.Serve(w, r, req)
		default:
			out = creator.Outcome{Fallback: "endpoint"}
		}
		if out.Fallback == "" {
			requests.WithLabelValues(string(req.Endpoint), "true", roomClass(req), "native", "native_"+out.Result).Inc()
			return
		}
		served = "fallback"
	}

	res := a.up.Forward(w, r, 0)
	upstreamSeconds.WithLabelValues(string(req.Endpoint)).Observe(res.Duration.Seconds())
	requests.WithLabelValues(string(req.Endpoint), strconv.FormatBool(req.Write),
		roomClass(req), served, statusClass(res)).Inc()
	if res.Err != nil {
		a.log.Warn().Err(res.Err).Str("endpoint", string(req.Endpoint)).
			Str("backend", res.Backend).Msg("proxy failed")
	}
}

// roomClass says which rooms a request names, for the counters. Every room
// ID is "allowlisted", as it was under goevent's rooms: all, so the series
// continue across the merge.
func roomClass(req route.Request) string {
	switch {
	case req.Room == "":
		return "none"
	case req.Room[0] == '#':
		// Join and knock by alias: unknown until resolved.
		return "alias"
	default:
		return "allowlisted"
	}
}

func statusClass(res proxy.Result) string {
	switch {
	case res.Canceled:
		return "canceled"
	case res.Err != nil:
		return "unreachable"
	case res.Status == 0:
		return "none"
	default:
		return strconv.Itoa(res.Status/100) + "xx"
	}
}
