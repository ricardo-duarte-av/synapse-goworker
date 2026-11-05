package typing

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// StreamName is the typing stream's NAME.
const StreamName = "typing"

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_typing_requests_total",
		Help: "PUT /rooms/{roomId}/typing/{userId}, by outcome.",
	}, []string{"outcome"})
	edus = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_typing_edus_total",
		Help: "Typing EDUs from other servers, by outcome.",
	}, []string{"outcome"})
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_typing_writer",
		Help: "1 while homeserver.yaml names this process in stream_writers.typing.",
	})
	held = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_typing_held",
		Help: "What the typing writer holds: rooms (with a serial), users (typing now), timers.",
	}, []string{"what"})
	serial = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_typing_serial",
		Help: "The typing stream's position: every change takes one.",
	})
	timeouts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goworker_typing_timeouts_total",
		Help: "Typing notices that ran out (a client's timeout, or a remote server's minute).",
	})
)

// Presence is the relay's bump (sync/presence.Client).
type Presence interface {
	Bump(ctx context.Context, userID, deviceID string) error
}

// Options builds an App.
type Options struct {
	Instance string
	// Synapse is the current homeserver.yaml (synapsecfg.Source.Current).
	Synapse func() *synapsecfg.Config
	Auth    verdictAuth
	Store   Store
	// Presence may be nil: presence is off.
	Presence Presence
	// Writer publishes the stream while this process writes it.
	Writer *streamwriter.Writer
	// OnRow, when set, is told every change in order (the sender's diff).
	OnRow func(Row)
	// Fallback is main, for a request while this process is not the writer.
	Fallback http.Handler
	Log      zerolog.Logger
}

// App is the typing writer.
type App struct {
	Options
	serverName string
	log        zerolog.Logger
	auth       verdictAuth
	store      Store
	presence   Presence
	fallback   http.Handler

	mu    sync.Mutex // role changes
	state atomic.Pointer[State]

	notWriter sync.Once
}

// New builds the writer, writing or not as homeserver.yaml says.
func New(o Options) *App {
	hs := o.Synapse()
	a := &App{
		Options: o, serverName: hs.ServerName, log: o.Log,
		auth: o.Auth, store: o.Store, presence: o.Presence, fallback: o.Fallback,
	}
	if a.presence == nil {
		a.presence = noPresence{}
	}
	a.SetSynapse(hs)
	return a
}

type noPresence struct{}

func (noPresence) Bump(context.Context, string, string) error { return nil }

// Writing reports whether this process writes typing now.
func (a *App) Writing() bool { return a.state.Load() != nil }

// SetSynapse applies a re-read homeserver.yaml: becoming the writer starts
// an empty stream at 0, and stopping drops it.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	a.mu.Lock()
	defer a.mu.Unlock()
	want := slices.Contains(hs.Writers.Typing, a.Instance)
	have := a.state.Load() != nil
	switch {
	case want && !have:
		st := NewState(a.onRow)
		a.state.Store(st)
		if a.Writer != nil {
			a.Writer.Add(stream{st})
		}
		writing.Set(1)
		a.log.Info().Msg("writing the typing stream (stream_writers.typing names this process); it starts at 0")
	case !want && have:
		a.state.Store(nil)
		if a.Writer != nil {
			a.Writer.Remove(StreamName)
		}
		writing.Set(0)
		a.log.Info().Strs("writers", hs.Writers.Typing).Msg("no longer writing the typing stream")
	}
}

func (a *App) onRow(r Row) {
	serial.Set(float64(r.Token))
	if a.OnRow != nil {
		a.OnRow(r)
	}
	if a.Writer != nil {
		a.Writer.Poke()
	}
}

func (a *App) trackPresence() bool { return a.Synapse().TrackPresence }

func (a *App) msc3848() bool { return a.Synapse().Flag("msc3848_enabled", false) }

// Handler serves what Match accepts.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serveClient) }

// Run checks timeouts every TimeoutInterval and prunes every
// ForgetTimeout, as Synapse's looping calls do, until ctx ends.
func (a *App) Run(ctx context.Context) error {
	tick := time.NewTicker(TimeoutInterval)
	defer tick.Stop()
	prune := time.NewTicker(ForgetTimeout)
	defer prune.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if st := a.state.Load(); st != nil {
				timeouts.Add(float64(st.HandleTimeouts()))
				rooms, users, timers := st.Size()
				held.WithLabelValues("rooms").Set(float64(rooms))
				held.WithLabelValues("users").Set(float64(users))
				held.WithLabelValues("timers").Set(float64(timers))
			}
		case <-prune.C:
			if st := a.state.Load(); st != nil {
				st.Prune()
			}
		}
	}
}

// stream is one State as the typing stream.
type stream struct{ st *State }

func (stream) Name() string          { return StreamName }
func (s stream) CurrentToken() int64 { return s.st.CurrentToken() }
func (s stream) UpdatesSince(_ context.Context, from, upto int64, limit int) ([]streamwriter.Update, int64, bool, error) {
	rows, upto, limited := s.st.UpdatesSince(from, upto, limit)
	out := make([]streamwriter.Update, len(rows))
	for i, r := range rows {
		out[i] = streamwriter.Update{Token: r.Token, Row: r.Row}
	}
	return out, upto, limited, nil
}
