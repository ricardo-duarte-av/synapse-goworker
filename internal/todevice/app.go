// Package todevice is Synapse's to-device writer (handlers/devicemessage.py
// DeviceMessageHandler, storage/databases/main/deviceinbox.py) for a
// homeserver whose stream_writers.to_device names this process:
//
//   - PUT /sendToDevice/{eventType}/{txnId}, with HttpTransactionCache;
//   - m.direct_to_device EDUs from other servers (in-process from inbound,
//     fed_send_edu from other processes);
//   - the to_device stream: rows in device_inbox (local devices) and
//     device_federation_outbox (one EDU per destination), IDs from
//     device_inbox_sequence (platform/streamid), RDATA/POSITION for every
//     follower (platform/streamwriter); the destinations reach our own
//     federation sender in-process, since it drops our echo.
//
// The role is taken at start only, as for the other stream-row writers.
// Reading and deleting delivered messages stays where it is (sync); so do
// the old-row cleanups, which run on main (run_background_tasks).
package todevice

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// StreamName is the to_device stream's NAME, and its name in
// stream_positions.
const StreamName = "to_device"

// Sequence is the stream's Postgres sequence.
const Sequence = "device_inbox_sequence"

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_to_device_requests_total",
		Help: "PUT /sendToDevice requests, by outcome.",
	}, []string{"outcome"})
	messages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_to_device_messages_total",
		Help: "To-device messages handled, by origin (local: from our clients, remote: from other servers) and where they went (local: a recipient here, remote: a destination's EDU, dropped: an invalid recipient or a ratelimited key request).",
	}, []string{"origin", "to"})
	edus = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_to_device_edus_total",
		Help: "m.direct_to_device EDUs, by direction (in, out) and outcome: in handled, repeat (a message_id already stored), dropped (spoofed or non-compliant sender), error; out queued (one per EDU put in device_federation_outbox).",
	}, []string{"direction", "outcome"})
	resyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_to_device_device_resyncs_total",
		Help: "Remote users whose device cache a room key request showed stale, by outcome of the resync asked of the device-lists writer.",
	}, []string{"outcome"})
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_to_device_writer",
		Help: "1 while this process writes to-device messages (decided at start).",
	})
)

// Store is what the writer reads and writes (DB).
type Store interface {
	AddLocal(ctx context.Context, instance string, streamID int64, users []UserMessages) error
	AddRemote(ctx context.Context, instance string, streamID int64, destination, edu string, nowMS int64) error
	AddFromRemote(ctx context.Context, instance string, streamID int64, origin, messageID string, users []UserMessages, nowMS int64) (bool, error)
	SharesRoom(ctx context.Context, userID string) (bool, error)
	CachedDevices(ctx context.Context, userID string) (map[string]bool, error)
	MarkStale(ctx context.Context, userID string, nowMS int64) error
	RatelimitExempt(ctx context.Context, userID string) (bool, error)
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
	// IDs, when nil, is opened on Pool as device_inbox_sequence.
	IDs  IDs
	Pool streamid.DB
	// Writer publishes the stream.
	Writer *streamwriter.Writer
	// OnDestinations is told the destinations a client's messages were
	// queued for (federation_sender.send_device_messages, when this process
	// sends federation; our sender drops our own echo on the bus).
	OnDestinations func(destinations []string)
	// Resync is multi_user_device_resync on the device-lists writer, for a
	// remote user whose devices a room key request showed we lack. Nil
	// skips it.
	Resync func(ctx context.Context, userID string) error
	// Fallback is main.
	Fallback http.Handler
	Log      zerolog.Logger
	// Now and MessageID are replaced in tests.
	Now       func() time.Time
	MessageID func() string
}

// App is the to-device writer.
type App struct {
	Options
	serverName string
	ids        IDs
	writing    bool
	// keyRequests is the rc_key_requests ratelimiter, one per process as
	// Synapse's DeviceMessageHandler has one.
	keyRequests *ratelimit.Limiter
	txns        *txnCache

	roleNoted sync.Once
	notWriter sync.Once
}

// New takes the role homeserver.yaml gives this process at start.
func New(ctx context.Context, o Options) (*App, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MessageID == nil {
		o.MessageID = randomLetters
	}
	hs := o.Synapse()
	a := &App{Options: o, serverName: hs.ServerName, txns: newTxnCache(o.Now)}
	a.keyRequests = ratelimit.New(hs.RcKeyRequests, o.Now)
	if !slices.Contains(hs.Writers.ToDevice, o.Instance) {
		writing.Set(0)
		return a, nil
	}
	if len(hs.Writers.ToDevice) != 1 {
		// Synapse refuses this too ("Must only specify one instance to
		// handle `to_device` messages").
		return nil, fmt.Errorf("todevice: stream_writers.to_device %v: there is only ever one writer", hs.Writers.ToDevice)
	}
	a.ids = o.IDs
	if a.ids == nil {
		g, err := streamid.Open(ctx, o.Pool, streamid.Options{
			Stream: StreamName, Instance: o.Instance, Sequence: Sequence,
			Writers: hs.Writers.ToDevice, OnAdvance: a.poke,
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
		Msg("writing the to_device stream (stream_writers.to_device names this process)")
	return a, nil
}

func (a *App) poke() {
	if a.Writer != nil {
		a.Writer.Poke()
	}
}

// Writing reports whether this process writes to-device messages.
func (a *App) Writing() bool { return a.writing }

// SetSynapse notes a re-read: the role is not changed until restart.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	want := slices.Contains(hs.Writers.ToDevice, a.Instance)
	if want != a.writing {
		a.roleNoted.Do(func() {
			a.Log.Warn().Strs("to_device_writers", hs.Writers.ToDevice).Bool("writing", a.writing).
				Msg("homeserver.yaml changed stream_writers.to_device; restart goworker to take or drop the to-device role")
		})
	}
}

// stream is the to_device stream.
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
