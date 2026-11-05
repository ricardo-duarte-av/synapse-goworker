// Package devicelists is Synapse's device-lists writer
// (handlers/device.py DeviceWriterHandler, storage/databases/main/
// devices.py) for a homeserver whose stream_writers.device_lists names this
// process alone:
//
//   - the replication endpoints every other process calls on the writer
//     (replication/http/devices.py): notify_device_update,
//     notify_user_signature_update, handle_new_device_update;
//   - the device_lists stream: one ID generator ("device_lists_stream" in
//     stream_positions, device_lists_sequence) over device_lists_stream,
//     user_signature_stream, device_lists_outbound_pokes,
//     device_lists_changes_in_room and device_lists_remote_pending, and
//     RDATA [user_id, is_signature, hosts_calculated] for every follower;
//   - the conversion of device_lists_changes_in_room into
//     device_lists_outbound_pokes (_handle_new_device_update_async), which
//     decides which servers hear of a local device change: wrong, a remote
//     server keeps a stale device list and its users cannot decrypt.
//
// and the remote half (remote.go): m.device_list_update and
// m.signing_key_update EDUs, multi_user_device_resync over federation with
// its 30 s retry loop, partial-state rooms (device_handle_room_un_partial_stated),
// and remote cross-signing keys. The client endpoints are stage 3. The
// role is taken at start only.
package devicelists

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

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// StreamName is the replication stream's NAME.
const StreamName = "device_lists"

// PositionsName is its generator's stream_name in stream_positions.
const PositionsName = "device_lists_stream"

// Sequence is the stream's Postgres sequence.
const Sequence = "device_lists_sequence"

// lagInterval is DEVICE_LIST_CONVERSION_LAG_GAUGE_METRIC_UPDATE_INTERVAL.
const lagInterval = 30 * time.Second

var (
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_device_lists_writer",
		Help: "1 while this process writes device lists (decided at start).",
	})
	changes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_device_lists_changes_total",
		Help: "Device list changes written, by kind (device: notify_device_update, of a local or remote user; signature: notify_user_signature_update) and outcome.",
	}, []string{"kind", "outcome"})
	pokes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_device_lists_outbound_pokes_total",
		Help: "Device list changes in rooms converted to outbound federation pokes, by result: poked (a stream ID with destinations), no_hosts (a local change in a room of ours alone, or a remote user's), error.",
	}, []string{"result"})
	lagSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_device_lists_conversion_lag_seconds",
		Help: "Age of the oldest device list change not yet converted to outbound federation pokes (synapse_device_lists_changes_conversion_lag_seconds).",
	})
	lagStream = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_device_lists_conversion_stream_lag",
		Help: "Stream IDs between the device lists position and the position converted to outbound pokes (synapse_device_lists_changes_conversion_stream_lag).",
	})
)

// Store is what the writer reads and writes (DB).
type Store interface {
	RoomsForUser(ctx context.Context, userID string) ([]string, error)
	AddDeviceChange(ctx context.Context, instance string, ids []int64, userID string, deviceIDs, roomIDs []string, isMine bool, nowMS int64) error
	AddUserSignatureChange(ctx context.Context, instance string, id int64, fromUserID, userIDs string) error
	Unconverted(ctx context.Context, stream int64, room string, upto int64, limit int) ([]Change, error)
	AddOutboundPokes(ctx context.Context, instance string, id int64, userID, deviceID string, hosts []string, isMine bool, nowMS int64) error
	MarkRedundant(ctx context.Context, userID, deviceID, roomID string, upto int64) error
	ConvertedPos(ctx context.Context) (int64, string, error)
	SetConvertedPos(ctx context.Context, instance string, stream int64, room string) error
	ConversionLag(ctx context.Context) (*int64, int64, error)
	Hosts(ctx context.Context, roomID string) (map[string]struct{}, error)
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
	Store    Store
	// IDs, when nil, is opened on Pool as device_lists_sequence.
	IDs  IDs
	Pool streamid.DB
	// Writer publishes the stream.
	Writer *streamwriter.Writer
	// OnPokes is told the stream ID of outbound pokes just written
	// (federation_sender.send_device_messages): our sender drops our echo.
	OnPokes func(streamID int64)
	// RStore is the remote half's store (DB).
	RStore RemoteStore
	// Query fetches a remote user's devices over federation (nil: every
	// resync fails and is retried).
	Query Query
	// Caches is the process's caches stream, for cross-signing keys.
	Caches Caches
	// StateAtEvent is the state at a partial join's event.
	StateAtEvent StateAtEvent
	// Auth checks client access tokens, for the device endpoints.
	Auth clientreq.Authenticator
	// CStore is the client endpoints' store (DB).
	CStore ClientStore
	// CachesInsert writes caches-stream rows in a transaction of their own.
	CachesInsert func(ctx context.Context, cacheFunc string, keys [][]string) ([]cachestream.Row, error)
	// KStore is /keys/upload's store (DB).
	KStore KeysStore
	// Locks is the process's worker locks (inbound's); without them
	// /keys/upload goes to main.
	Locks Locker
	// SStore is /keys/signatures/upload's store (DB), and E2E the reader
	// of local users' keys it checks signatures against.
	SStore SignaturesStore
	E2E    e2ekeys.Querier
	// Fallback is main, for what is not served here.
	Fallback http.Handler
	Log      zerolog.Logger
	Now      func() time.Time
}

// App is the device-lists writer.
type App struct {
	Options
	serverName string
	ids        IDs
	writing    bool

	// The conversion loop: one at a time; a wake while it runs is noted
	// and it goes round again (_handle_new_device_update_is_processing,
	// _handle_new_device_update_new_data).
	convMu      sync.Mutex
	converting  bool
	convNewData bool

	r remote

	roleNoted sync.Once
}

// New takes the role homeserver.yaml gives this process at start.
func New(ctx context.Context, o Options) (*App, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	hs := o.Synapse()
	a := &App{Options: o, serverName: hs.ServerName}
	a.r.pending, a.r.signing, a.r.seen = map[string][]pendingUpdate{}, map[string][][2]gjson.Result{}, map[string]*seenEntry{}
	if !slices.Contains(hs.Writers.DeviceLists, o.Instance) {
		writing.Set(0)
		return a, nil
	}
	if len(hs.Writers.DeviceLists) != 1 {
		// Synapse allows several; this process writes alone (sync's token
		// is one aggregate position), and the EDUs and the conversion are
		// writers[0]'s anyway.
		return nil, fmt.Errorf("devicelists: stream_writers.device_lists %v: this process writes device lists only alone", hs.Writers.DeviceLists)
	}
	a.ids = o.IDs
	if a.ids == nil {
		g, err := streamid.Open(ctx, o.Pool, streamid.Options{
			Stream: PositionsName, Instance: o.Instance, Sequence: Sequence,
			Writers: hs.Writers.DeviceLists, OnAdvance: a.poke,
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
		Msg("writing the device_lists stream (stream_writers.device_lists names this process)")
	return a, nil
}

func (a *App) poke() {
	if a.Writer != nil {
		a.Writer.Poke()
	}
}

// Writing reports whether this process writes device lists.
func (a *App) Writing() bool { return a.writing }

// SetSynapse notes a re-read: the role is not changed until restart.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	want := slices.Contains(hs.Writers.DeviceLists, a.Instance)
	if want != a.writing {
		a.roleNoted.Do(func() {
			a.Log.Warn().Strs("device_lists_writers", hs.Writers.DeviceLists).Bool("writing", a.writing).
				Msg("homeserver.yaml changed stream_writers.device_lists; restart goworker to take or drop the device-lists role")
		})
	}
}

func (a *App) isMine(userID string) bool { return userid.Domain(userID) == a.serverName }

// Run converts what is pending at start (call_when_running) and reports
// the conversion lag every 30 s, until ctx ends.
func (a *App) Run(ctx context.Context) error {
	if !a.writing {
		<-ctx.Done()
		return nil
	}
	a.HandleNewDeviceUpdate()
	t := time.NewTicker(lagInterval)
	defer t.Stop()
	r := time.NewTicker(retryInterval)
	defer r.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			a.reportLag(ctx)
		case <-r.C:
			go a.retryResyncs(ctx)
		}
	}
}

// reportLag is _report_device_list_conversion_lag.
func (a *App) reportLag(ctx context.Context) {
	oldest, converted, err := a.Store.ConversionLag(ctx)
	if err != nil {
		a.Log.Warn().Err(err).Msg("device lists: conversion lag")
		return
	}
	if oldest == nil {
		lagSeconds.Set(0)
	} else {
		lagSeconds.Set(float64(max(0, a.Now().UnixMilli()-*oldest)) / 1000)
	}
	lagStream.Set(float64(max(0, a.ids.Position()-converted)))
}

// NotifyDeviceUpdate is DeviceWriterHandler.notify_device_update: the
// change in the streams, in batches of 1000 devices (a stream ID each),
// and for a local user the conversion woken.
func (a *App) NotifyDeviceUpdate(ctx context.Context, userID string, deviceIDs []string) error {
	if !a.writing {
		return ErrNotWriter
	}
	if len(deviceIDs) == 0 {
		return nil
	}
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		changes.WithLabelValues("device", "error").Inc()
		return err
	}
	mine := a.isMine(userID)
	for start := 0; start < len(deviceIDs); start += 1000 {
		batch := deviceIDs[start:min(start+1000, len(deviceIDs))]
		if err := a.withIDs(ctx, len(batch), func(ids []int64) error {
			return a.Store.AddDeviceChange(ctx, a.Instance, ids, userID, batch, rooms, mine, a.Now().UnixMilli())
		}); err != nil {
			changes.WithLabelValues("device", "error").Inc()
			return err
		}
	}
	changes.WithLabelValues("device", "ok").Inc()
	if mine {
		a.HandleNewDeviceUpdate()
	}
	return nil
}

// NotifyUserSignatureUpdate is notify_user_signature_update; userIDs is
// the list as json_encoder writes it.
func (a *App) NotifyUserSignatureUpdate(ctx context.Context, fromUserID, userIDs string) error {
	if !a.writing {
		return ErrNotWriter
	}
	err := a.withIDs(ctx, 1, func(ids []int64) error {
		return a.Store.AddUserSignatureChange(ctx, a.Instance, ids[0], fromUserID, userIDs)
	})
	if err != nil {
		changes.WithLabelValues("signature", "error").Inc()
		return err
	}
	changes.WithLabelValues("signature", "ok").Inc()
	return nil
}

// withIDs takes n stream IDs (get_next_mult_txn), runs fn, and finishes
// them all, failed or not.
func (a *App) withIDs(ctx context.Context, n int, fn func(ids []int64) error) error {
	held := make([]*streamid.ID, 0, n)
	ids := make([]int64, 0, n)
	var err error
	for range n {
		var id *streamid.ID
		if id, err = a.ids.Next(ctx); err != nil {
			break
		}
		held = append(held, id)
		ids = append(ids, id.Value)
	}
	if err == nil {
		err = fn(ids)
	}
	for _, id := range held {
		if ferr := id.Finish(ctx, err == nil); err == nil && ferr != nil {
			err = ferr
		}
	}
	return err
}

// ErrNotWriter is a write for a process that does not write device lists.
var ErrNotWriter = fmt.Errorf("devicelists: this process is not the device-lists writer")

// stream is the device_lists stream.
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
