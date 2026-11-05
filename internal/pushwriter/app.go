// Package pushwriter is Synapse's push-rules writer (rest/client/
// push_rule.py, handlers/push_rules.py, storage/databases/main/push_rule.py)
// for a homeserver whose stream_writers.push_rules names this process:
//
//   - PUT, DELETE and GET under /pushrules/ (the GETs any process could
//     serve; nginx sends the location here whole);
//   - copy_push_rules, which main calls on the writer when a user joins an
//     upgraded room (replication/http/push.py);
//   - the push_rules stream: rows in push_rules_stream under our
//     instance_name, IDs from push_rules_stream_sequence (platform/
//     streamid, as "push_rules_stream" in stream_positions), RDATA
//     "push_rules" for every follower, whose push rule caches drop on it.
//
// The role is taken at start only, as for the other stream-row writers;
// so are the in-memory ID generators of push_rules.id and
// push_rules_enable.id, which only the writer advances.
package pushwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
)

// StreamName is the replication stream's NAME.
const StreamName = "push_rules"

// PositionsName is its generator's stream_name in stream_positions.
const PositionsName = "push_rules_stream"

// Sequence is the stream's Postgres sequence.
const Sequence = "push_rules_stream_sequence"

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_push_rules_requests_total",
		Help: "Requests under /pushrules/, by method and outcome.",
	}, []string{"method", "outcome"})
	writes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_push_rules_writes_total",
		Help: "Push rule writes, by operation (add, delete, enable, disable, actions) and outcome (ok, refused, error). refused is an error Synapse gives the client.",
	}, []string{"op", "outcome"})
	copies = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_push_rules_copies_total",
		Help: "copy_push_rules calls (a user joining an upgraded room), by outcome; and the rules they copied (rule_copied).",
	}, []string{"outcome"})
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_push_rules_writer",
		Help: "1 while this process writes push rules (decided at start).",
	})
)

// Store is what the writer reads and writes (DB).
type Store interface {
	AddRule(ctx context.Context, lim Limits, w Write, userID, ruleID string, class int, conditions, actions, before, after string) error
	DeleteRule(ctx context.Context, w Write, userID, ruleID string) error
	SetEnabled(ctx context.Context, w Write, userID, ruleID string, enabled, isDefault bool) error
	SetActions(ctx context.Context, lim Limits, w Write, userID, ruleID, actions string, isDefault bool) error
	UserRules(ctx context.Context, userID string) ([]pushrules.UserRule, map[string]bool, error)
	Updates(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error)
}

// IDs allocates stream IDs (streamid.Generator).
type IDs interface {
	Next(ctx context.Context) (*streamid.ID, error)
	Position() int64
}

// Options builds an App.
type Options struct {
	Instance string
	Synapse  func() *synapsecfg.Config
	Auth     clientreq.Authenticator
	Store    Store
	// IDs, when nil, is opened on Pool as push_rules_stream_sequence.
	IDs  IDs
	Pool streamid.DB
	// Writer publishes the stream.
	Writer *streamwriter.Writer
	// Features are the flags the push rules are built under (sync's).
	Features pushrules.Features
	// Rules is the user's whole ruleset in the client format (sync's
	// PushRulesContent), for the GETs. Nil sends them to main.
	Rules func(ctx context.Context, userID string) (json.RawMessage, error)
	// EventsPosition is the events stream's current token as this process
	// knows it, which every push_rules_stream row records
	// (event_stream_ordering).
	EventsPosition func(ctx context.Context) (int64, error)
	// Limits are push_rules.limits; zero is DefaultLimits.
	Limits Limits
	// Fallback is main.
	Fallback http.Handler
	Log      zerolog.Logger
}

// App is the push-rules writer.
type App struct {
	Options
	ids     IDs
	writing bool
	served  map[string]bool
	// locks is _push_rule_linearizer: one PUT or DELETE per user at a time.
	locks sync.Map

	roleNoted sync.Once
	notWriter sync.Once
}

// New takes the role homeserver.yaml gives this process at start.
func New(ctx context.Context, o Options) (*App, error) {
	if o.Limits == (Limits{}) {
		o.Limits = DefaultLimits
	}
	hs := o.Synapse()
	a := &App{Options: o, served: pushrules.ServedBaseRuleIDs(o.Features)}
	if !slices.Contains(hs.Writers.PushRules, o.Instance) {
		writing.Set(0)
		return a, nil
	}
	if len(hs.Writers.PushRules) != 1 {
		return nil, fmt.Errorf("pushwriter: stream_writers.push_rules %v: there is only ever one writer", hs.Writers.PushRules)
	}
	a.ids = o.IDs
	if a.ids == nil {
		g, err := streamid.Open(ctx, o.Pool, streamid.Options{
			Stream: PositionsName, Instance: o.Instance, Sequence: Sequence,
			Writers: hs.Writers.PushRules, OnAdvance: a.poke,
		})
		if err != nil {
			return nil, err
		}
		a.ids = g
	}
	a.writing = true
	writing.Set(1)
	if a.Writer != nil {
		a.Writer.Add(stream{a})
	}
	a.Log.Info().Int64("position", a.ids.Position()).
		Msg("writing the push_rules stream (stream_writers.push_rules names this process)")
	return a, nil
}

func (a *App) poke() {
	if a.Writer != nil {
		a.Writer.Poke()
	}
}

// Writing reports whether this process writes push rules.
func (a *App) Writing() bool { return a.writing }

// SetSynapse notes a re-read: the role is not changed until restart.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	want := slices.Contains(hs.Writers.PushRules, a.Instance)
	if want != a.writing {
		a.roleNoted.Do(func() {
			a.Log.Warn().Strs("push_rules_writers", hs.Writers.PushRules).Bool("writing", a.writing).
				Msg("homeserver.yaml changed stream_writers.push_rules; restart goworker to take or drop the push-rules role")
		})
	}
}

func (a *App) lock(userID string) func() {
	l, _ := a.locks.LoadOrStore(userID, &sync.Mutex{})
	mu := l.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// stored runs one write under a stream ID, with the events position the
// row records, finished whatever happens.
func (a *App) stored(ctx context.Context, fn func(w Write) error) error {
	id, err := a.ids.Next(ctx)
	if err != nil {
		return err
	}
	w := Write{Instance: a.Instance, StreamID: id.Value}
	if a.EventsPosition != nil {
		w.EventStreamOrdering, err = a.EventsPosition(ctx)
	}
	if err == nil {
		err = fn(w)
	}
	if ferr := id.Finish(ctx, err == nil); err == nil && ferr != nil {
		err = ferr
	}
	return err
}

// stream is the push_rules stream.
type stream struct{ a *App }

func (stream) Name() string          { return StreamName }
func (s stream) CurrentToken() int64 { return s.a.ids.Position() }
func (s stream) UpdatesSince(ctx context.Context, from, upto int64, limit int) ([]streamwriter.Update, int64, bool, error) {
	rows, upto, limited, err := s.a.Store.Updates(ctx, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	out := make([]streamwriter.Update, len(rows))
	for i, r := range rows {
		out[i] = streamwriter.Update{Token: r.Token, Row: r.Row}
	}
	return out, upto, limited, nil
}
