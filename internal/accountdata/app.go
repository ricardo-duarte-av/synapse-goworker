// Package accountdata is Synapse's account-data writer
// (handlers/account_data.py AccountDataHandler, storage/databases/main/
// account_data.py and tags.py) for a homeserver whose
// stream_writers.account_data names this process:
//
//   - the client endpoints of rest/client/account_data.py and tags.py:
//     global and room account data (PUT, GET, and MSC3391's DELETE), and
//     room tags;
//   - the six replication endpoints every other process calls on the
//     writer (replication/http/account_data.py): main's room upgrade and
//     server notices, and wherever a read marker is stored;
//   - the account_data stream: rows in account_data, room_account_data and
//     room_tags_revisions, IDs from account_data_sequence
//     (platform/streamid), RDATA/POSITION for every follower
//     (platform/streamwriter); and, for m.ignored_user_list, rows on the
//     caches stream (platform/cachestream).
//
// The role is taken at start only, as the receipts writer's is: these are
// rows under a shared sequence, and a process that began writing on a
// re-read could write while the old writer, not yet restarted, still
// believes it is the writer.
//
// Module callbacks (on_account_data_updated) run on Synapse's writer; we
// have none to run, and say so at start if homeserver.yaml lists modules.
package accountdata

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

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// StreamName is the account_data stream's NAME, and its name in
// stream_positions.
const StreamName = "account_data"

// Sequence is the stream's Postgres sequence.
const Sequence = "account_data_sequence"

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_account_data_requests_total",
		Help: "Account data and tag requests from clients, by endpoint and outcome.",
	}, []string{"endpoint", "outcome"})
	writes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_account_data_writes_total",
		Help: "Account data writes, by operation (add_user, remove_user, add_room, remove_room, add_tag, remove_tag), caller (client, replication, local) and outcome (ok, unchanged, error).",
	}, []string{"op", "caller", "outcome"})
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_account_data_writer",
		Help: "1 while this process writes account data (decided at start).",
	})
	cachePublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goworker_account_data_cache_publish_failures_total",
		Help: "Ignored-user writes that committed but whose caches rows did not all reach the bus; the next caches write or REPLICATE announces our position.",
	})
)

// Store is what the writer reads and writes (DB).
type Store interface {
	AddUser(ctx context.Context, instance string, streamID int64, userID, dataType, content string) ([]cachestream.Row, error)
	RemoveUser(ctx context.Context, instance string, streamID int64, userID, dataType string) (bool, []cachestream.Row, error)
	AddRoom(ctx context.Context, streamID int64, userID, roomID, dataType, content string) error
	RemoveRoom(ctx context.Context, streamID int64, userID, roomID, dataType string) (bool, error)
	AddTag(ctx context.Context, streamID int64, userID, roomID, tag, content string) error
	RemoveTag(ctx context.Context, streamID int64, userID, roomID, tag string) error
	Global(ctx context.Context, userID, dataType string) (json.RawMessage, error)
	Room(ctx context.Context, userID, roomID, dataType string) (json.RawMessage, error)
	Tags(ctx context.Context, userID, roomID string) ([]Tag, error)
	HasMembership(ctx context.Context, userID, roomID string) (bool, error)
	Updates(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error)
}

// IDs allocates stream IDs (streamid.Generator).
type IDs interface {
	Next(ctx context.Context) (*streamid.ID, error)
	Position() int64
}

// Caches is the process's caches stream (cachestream.Writer).
type Caches interface {
	Do(write func() ([]cachestream.Row, error)) error
}

// Options builds an App.
type Options struct {
	Instance string
	Synapse  func() *synapsecfg.Config
	Auth     clientreq.Authenticator
	Store    Store
	// IDs, when nil, is opened on Pool as account_data_sequence.
	IDs  IDs
	Pool streamid.DB
	// Writer publishes the stream.
	Writer *streamwriter.Writer
	Caches Caches
	// PushRules is a user's m.push_rules, as GET .../account_data/m.push_rules
	// answers it (push_rules_for_user). Nil sends that GET to main.
	PushRules func(ctx context.Context, userID string) (json.RawMessage, error)
	// Fallback is main.
	Fallback http.Handler
	Log      zerolog.Logger
}

// App is the account-data writer.
type App struct {
	Options
	ids     IDs
	writing bool

	roleNoted sync.Once
	notWriter sync.Once
}

// New takes the role homeserver.yaml gives this process at start.
func New(ctx context.Context, o Options) (*App, error) {
	hs := o.Synapse()
	a := &App{Options: o}
	if !slices.Contains(hs.Writers.AccountData, o.Instance) {
		writing.Set(0)
		return a, nil
	}
	if len(hs.Writers.AccountData) != 1 {
		// Synapse refuses this too ("Must only specify one instance to
		// handle `account_data` messages").
		return nil, fmt.Errorf("accountdata: stream_writers.account_data %v: there is only ever one writer", hs.Writers.AccountData)
	}
	if o.Caches == nil {
		return nil, fmt.Errorf("accountdata: no caches stream to publish ignored-user invalidations on")
	}
	a.ids = o.IDs
	if a.ids == nil {
		g, err := streamid.Open(ctx, o.Pool, streamid.Options{
			Stream: StreamName, Instance: o.Instance, Sequence: Sequence,
			Writers: hs.Writers.AccountData, OnAdvance: a.poke,
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
	if hs.ClientAuth.Modules > 0 {
		a.Log.Warn().Msg("homeserver.yaml lists modules: their on_account_data_updated callbacks do not run while goworker writes account data")
	}
	a.Log.Info().Int64("position", a.ids.Position()).
		Msg("writing the account_data stream (stream_writers.account_data names this process)")
	return a, nil
}

func (a *App) poke() {
	if a.Writer != nil {
		a.Writer.Poke()
	}
}

// Writing reports whether this process writes account data.
func (a *App) Writing() bool { return a.writing }

// SetSynapse notes a re-read: the role is not changed until restart.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	want := slices.Contains(hs.Writers.AccountData, a.Instance)
	if want != a.writing {
		a.roleNoted.Do(func() {
			a.Log.Warn().Strs("account_data_writers", hs.Writers.AccountData).Bool("writing", a.writing).
				Msg("homeserver.yaml changed stream_writers.account_data; restart goworker to take or drop the account-data role")
		})
	}
}

func (a *App) msc3391() bool { return a.Synapse().Flag("msc3391_enabled", false) }

// stream is the account_data stream.
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
