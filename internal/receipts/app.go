// Package receipts is Synapse's receipts writer (handlers/receipts.py
// ReceiptsHandler, storage/databases/main/receipts.py) for a homeserver
// whose stream_writers.receipts names this process alone:
//
//   - POST /rooms/{roomId}/receipt/{receiptType}/{eventId} and
//     POST /rooms/{roomId}/read_markers, the two client endpoints that
//     write receipts (and m.fully_read, the read marker, which is room
//     account data: it goes to the account-data writer over replication);
//   - receipt EDUs from other servers;
//   - the receipts stream: rows in receipts_linearized under our
//     instance_name, IDs from receipts_sequence (platform/streamid), and
//     RDATA/POSITION for every follower (platform/streamwriter).
//
// The role is taken at start only. Receipts are rows under a shared
// sequence, and a process that began writing on a re-read could write
// while the old writer, not yet restarted, still believes it is the writer.
// A re-read that changes it is logged; the cutover restarts goworker.
package receipts

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

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// StreamName is the receipts stream's NAME, and its name in
// stream_positions.
const StreamName = "receipts"

// Sequence is the stream's Postgres sequence.
const Sequence = "receipts_sequence"

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_receipts_requests_total",
		Help: "Receipt and read-marker requests, by endpoint and outcome.",
	}, []string{"endpoint", "outcome"})
	stored = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_receipts_stored_total",
		Help: "Receipts handed to insert_receipt, by origin (local, remote) and result (new, old, error).",
	}, []string{"origin", "result"})
	edus = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_receipts_edus_total",
		Help: "Receipt EDUs from other servers, by outcome. A room or user dropped inside one is counted under receipts_dropped.",
	}, []string{"outcome"})
	dropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_receipts_dropped_total",
		Help: "Parts of remote receipt EDUs dropped as Synapse drops them, by reason.",
	}, []string{"reason"})
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_receipts_writer",
		Help: "1 while this process writes receipts (decided at start).",
	})
	markers = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_receipts_read_markers_total",
		Help: "m.fully_read updates, by outcome (moved, behind, error).",
	}, []string{"outcome"})
)

// Store is what the writer reads and writes (DB).
type Store interface {
	InsertLinearized(ctx context.Context, r Receipt, streamID int64, instance string) (stored, isNew bool, err error)
	InsertGraph(ctx context.Context, r Receipt) error
	Updates(ctx context.Context, instance string, from, upto int64, limit int) ([]Update, int64, bool, error)
	ThreadID(ctx context.Context, eventID string) (string, error)
	ThreadIDForReceipts(ctx context.Context, eventID string) (string, error)
	RoomAccountData(ctx context.Context, userID, roomID, dataType string) (json.RawMessage, error)
	EventOrdering(ctx context.Context, eventID, roomID string) (Ordering, error)
	HostJoined(ctx context.Context, roomID, host string) (bool, error)
	HostsApprox(ctx context.Context, roomID string) (map[string]struct{}, error)
}

// Access is what EventHandler.get_event says of the event a receipt names.
type Access int

const (
	Missing Access = iota
	Visible
	Forbidden
)

// Events answers get_event for one user and event (sync's EventAccessFor).
type Events interface {
	Access(ctx context.Context, userID, roomID, eventID string) (Access, error)
}

// AccountData stores room account data on its writer
// (add_account_data_to_room on a process that does not write it).
type AccountData interface {
	AddRoomAccountData(ctx context.Context, userID, roomID, dataType string, content json.RawMessage) error
}

// Presence is the relay's bump.
type Presence interface {
	Bump(ctx context.Context, userID, deviceID string) error
}

// IDs allocates stream IDs (streamid.Generator).
type IDs interface {
	Next(ctx context.Context) (*streamid.ID, error)
	Position() int64
}

type verdictAuth interface {
	Authenticate(ctx context.Context, r *http.Request) (clientauth.Verdict, error)
}

// Row is a stored receipt, as the stream carries it, for the sender
// (ThreadID "" for none, as the sender's own rows have it).
type Row struct {
	RoomID, ReceiptType, UserID, EventID, ThreadID string
	Data                                           json.RawMessage
}

// Options builds an App.
type Options struct {
	Instance string
	Synapse  func() *synapsecfg.Config
	Auth     verdictAuth
	Store    Store
	Events   Events
	// AccountData is the account-data writer; nil refuses m.fully_read.
	AccountData AccountData
	// Presence may be nil: presence is off.
	Presence Presence
	// IDs, when nil, is opened on Pool as receipts_sequence.
	IDs  IDs
	Pool streamid.DB
	// Writer publishes the stream.
	Writer *streamwriter.Writer
	// OnReceipt is told every stored receipt (the sender's own echo on the
	// bus is dropped).
	OnReceipt func(Row)
	// Fallback is main.
	Fallback http.Handler
	Log      zerolog.Logger
	// Worker carries the role lines (taken, dropped, a change that needs a
	// restart): component=worker, so a quiet Log does not hide them.
	Worker zerolog.Logger
}

// App is the receipts writer.
type App struct {
	Options
	serverName string
	ids        IDs
	writing    bool
	// markerLocks is read_marker_linearizer: one m.fully_read update per
	// (room, user) at a time.
	markerLocks sync.Map

	roleNoted sync.Once
	notWriter sync.Once
}

// New takes the role homeserver.yaml gives this process at start: writing
// receipts only while stream_writers.receipts names it alone.
func New(ctx context.Context, o Options) (*App, error) {
	hs := o.Synapse()
	a := &App{Options: o, serverName: hs.ServerName}
	if o.Presence == nil {
		a.Presence = noPresence{}
	}
	if !slices.Contains(hs.Writers.Receipts, o.Instance) {
		writing.Set(0)
		return a, nil
	}
	if len(hs.Writers.Receipts) != 1 {
		return nil, fmt.Errorf("receipts: stream_writers.receipts %v: this process writes receipts only alone", hs.Writers.Receipts)
	}
	a.ids = o.IDs
	if a.ids == nil {
		g, err := streamid.Open(ctx, o.Pool, streamid.Options{
			Stream: StreamName, Instance: o.Instance, Sequence: Sequence,
			Writers: hs.Writers.Receipts, OnAdvance: a.poke,
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
	a.Worker.Info().Int64("position", a.ids.Position()).
		Msg("writing the receipts stream (stream_writers.receipts names this process)")
	return a, nil
}

type noPresence struct{}

func (noPresence) Bump(context.Context, string, string) error { return nil }

func (a *App) poke() {
	if a.Writer != nil {
		a.Writer.Poke()
	}
}

// Writing reports whether this process writes receipts.
func (a *App) Writing() bool { return a.writing }

// SetSynapse notes a re-read: the role is not changed until restart.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	want := slices.Contains(hs.Writers.Receipts, a.Instance)
	if want != a.writing {
		a.roleNoted.Do(func() {
			a.Worker.Warn().Strs("receipts_writers", hs.Writers.Receipts).Bool("writing", a.writing).
				Msg("homeserver.yaml changed stream_writers.receipts; restart goworker to take or drop the receipts role")
		})
	}
}

func (a *App) trackPresence() bool { return a.Synapse().TrackPresence }

func (a *App) msc3848() bool { return a.Synapse().Flag("msc3848_enabled", false) }

func (a *App) msc4446() bool { return a.Synapse().Flag("msc4446_enabled", false) }

// stream is the receipts stream.
type stream struct{ a *App }

func (stream) Name() string          { return StreamName }
func (s stream) CurrentToken() int64 { return s.a.ids.Position() }
func (s stream) UpdatesSince(ctx context.Context, from, upto int64, limit int) ([]streamwriter.Update, int64, bool, error) {
	rows, upto, limited, err := s.a.Store.Updates(ctx, s.a.Instance, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	out := make([]streamwriter.Update, len(rows))
	for i, r := range rows {
		out[i] = streamwriter.Update{Token: r.Token, Row: r.Row}
	}
	return out, upto, limited, nil
}
