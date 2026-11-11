// Package presencewriter is Synapse's presence writer (handlers/
// presence.py PresenceHandler, storage/databases/main/presence.py,
// replication/http/presence.py) for a homeserver whose presence is
// disabled (presence.enabled: false), and the presence client endpoints
// (rest/client/presence.py) any process serves:
//
//   - the presence stream: presence_stream rows under our instance_name,
//     IDs from presence_stream_sequence (platform/streamid, as
//     "presence_stream" in stream_positions), RDATA "presence";
//   - the presence_federation stream, in memory, which with presence
//     disabled never leaves 0, as a writer's restart leaves it;
//   - at start, _mark_stale_presence_as_offline: states left from when
//     presence was enabled marked offline, 500 to a write, as Synapse
//     does to stop clients showing them for ever. With presence disabled
//     nothing goes over federation for them: the federation sender's
//     send_presence_to_destinations returns at once while presence is
//     not tracked, and the presence_federation queue is never reached;
//   - presence_set_state and bump_presence_active_time, which other
//     processes call on the writer: their errors (Synapse's checks run
//     before the "not tracked" return), then nothing;
//   - fed_send_edu for m.presence: on_edu drops it while presence is not
//     tracked;
//   - GET and PUT /presence/{userId}/status, while presence is disabled
//     (read on each request), whether or not this process writes: GET is
//     always offline, PUT is rc_presence.per_user, its body's checks, and
//     nothing else.
//
// What a writer does only while presence is enabled is not here: the
// state machine and its timers, USER_SYNC and CLEAR_USER_SYNC from other
// processes (sent only while presence is tracked), incoming EDUs, the
// federation fan-out, the presence router. goworker refuses the role while
// presence is enabled or "untracked", and sends the client endpoints to
// main.
//
// The role is taken at start only, as for the other stream-row writers.
package presencewriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// StreamName is the presence stream's NAME.
const StreamName = "presence"

// FederationStreamName is PresenceFederationStream's NAME.
const FederationStreamName = "presence_federation"

// PositionsName is the generator's stream_name in stream_positions.
const PositionsName = "presence_stream"

// Sequence is the stream's Postgres sequence.
const Sequence = "presence_stream_sequence"

// staleBatch is _mark_stale_presence_as_offline's batch_iter size.
const staleBatch = 500

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_requests_total",
		Help: "GET and PUT /presence/{userId}/status, by method and outcome (ok, refused: an error Synapse gives the client, ratelimited, to_main: presence enabled, auth_rejected, auth_unavailable, error).",
	}, []string{"method", "outcome"})
	replicationCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_replication_total",
		Help: "Calls other processes make on the presence writer (presence_set_state, bump_presence_active_time, fed_send_edu for m.presence), by endpoint and outcome (ignored: presence is disabled, refused, error).",
	}, []string{"endpoint", "outcome"})
	staleOffline = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goworker_presence_stale_offline_total",
		Help: "Presence states left from when presence was enabled, marked offline at start as Synapse does.",
	})
	writing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_presence_writer",
		Help: "1 while this process writes the presence stream (decided at start).",
	})
)

// Store is what the app reads and writes (DB).
type Store interface {
	Active(ctx context.Context) ([]State, error)
	Persist(ctx context.Context, instance string, ids []int64, states []State) error
	Updates(ctx context.Context, from, upto int64, limit int) ([]streamwriter.Update, int64, bool, error)
	RatelimitExempt(ctx context.Context, userID string) (bool, error)
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
	// IDs, when nil, is opened on Pool as presence_stream_sequence.
	IDs  IDs
	Pool streamid.DB
	// Writer publishes the streams; the role cannot be taken without it.
	Writer *streamwriter.Writer
	// Fallback is main.
	Fallback http.Handler
	// Now is time.Now when nil.
	Now func() time.Time
	// What the writer needs while presence is enabled; without any of
	// them it refuses the role then.
	Rooms RoomStore
	// SendPresence hands states to this process's federation sender
	// (send_presence_to_destinations), which keeps to its shard.
	SendPresence func(states []Presence, destinations []string)
	// EventsPosition is the events stream's current token, for joins.
	EventsPosition func(ctx context.Context) (int64, error)
	// Lines carries USER_SYNC and CLEAR_USER_SYNC from other processes.
	Lines *bus.Consumer
	Log   zerolog.Logger
	// Worker carries the role lines: component=worker, so a quiet Log does
	// not hide them.
	Worker zerolog.Logger
}

// App is the presence endpoints, and the writer when the role is ours.
type App struct {
	Options
	ids     IDs
	writing bool
	// stale is what _get_active_presence found at start, for Run.
	stale []State
	// perUser is rc_presence.per_user, as homeserver.yaml gave it at start:
	// the PUT servlet's limiter; syncLimiter is the sync servlet's, a
	// Ratelimiter of its own over the same settings.
	perUser     *ratelimit.Limiter
	syncLimiter *ratelimit.Limiter
	// tr is the writer's state while presence is enabled.
	tr *tracker

	roleNoted     sync.Once
	enabledNoted  sync.Once
	enabledToMain sync.Once
}

// New takes the role homeserver.yaml gives this process at start.
func New(ctx context.Context, o Options) (*App, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	hs := o.Synapse()
	a := &App{Options: o, perUser: ratelimit.New(hs.RcPresence, o.Now), syncLimiter: ratelimit.New(hs.RcPresence, o.Now)}
	if !slices.Contains(hs.Writers.Presence, o.Instance) {
		writing.Set(0)
		return a, nil
	}
	switch {
	case len(hs.Writers.Presence) != 1:
		return nil, fmt.Errorf("presencewriter: stream_writers.presence %v: there is only ever one writer", hs.Writers.Presence)
	case hs.PresenceEnabled && (hs.ClientAuth.Modules > 0 || hs.PresenceRouter):
		return nil, errors.New("presencewriter: presence is enabled and homeserver.yaml loads modules, one of which may " +
			"route presence; goworker does not run them: leave the role to a Synapse process")
	case hs.PresenceEnabled && !slices.Equal(hs.SenderInstances, []string{o.Instance}):
		return nil, fmt.Errorf("presencewriter: presence is enabled and federation_sender_instances is %v: goworker "+
			"writes presence only as the one federation sender (it keeps no presence_federation queue for others)", hs.SenderInstances)
	case hs.PresenceEnabled && (o.Rooms == nil || o.EventsPosition == nil):
		return nil, errors.New("presencewriter: presence is enabled and the writer has no room store")
	case o.Writer == nil:
		return nil, errors.New("presencewriter: the presence writer needs homeserver.yaml's redis")
	}
	a.ids = o.IDs
	if a.ids == nil {
		g, err := streamid.Open(ctx, o.Pool, streamid.Options{
			Stream: PositionsName, Instance: o.Instance, Sequence: Sequence,
			Writers: hs.Writers.Presence, OnAdvance: o.Writer.Poke,
		})
		if err != nil {
			return nil, err
		}
		a.ids = g
	}
	stale, err := o.Store.Active(ctx)
	if err != nil {
		return nil, fmt.Errorf("presencewriter: active presence: %w", err)
	}
	a.writing = true
	writing.Set(1)
	if hs.PresenceEnabled {
		excluded := map[string]struct{}{}
		for _, r := range hs.ExcludeRoomsFromPresence {
			excluded[r] = struct{}{}
		}
		a.tr = &tracker{
			a: a, track: hs.TrackPresence, server: hs.ServerName, excluded: excluded,
			t: timers{idle: hs.IdleTimeout.Milliseconds(), syncOnline: hs.SyncOnlineTimeout.Milliseconds(),
				lastActiveGranularity: hs.LastActiveGranularity.Milliseconds()},
			current: map[string]Presence{}, devices: map[string]map[devKey]*device{},
			unpersisted: map[string]struct{}{}, syncs: map[userDevice]int{},
			external: map[string]map[userDevice]struct{}{}, externalUpdated: map[string]int64{},
			hosts: newHostsCache(),
		}
		if a.tr.eventPos, err = o.EventsPosition(ctx); err != nil {
			return nil, fmt.Errorf("presencewriter: events position: %w", err)
		}
		a.tr.start(stale)
	} else {
		a.stale = stale
	}
	o.Writer.Add(stream{a})
	o.Writer.Add(federationStream{})
	mode := "disabled"
	switch {
	case hs.TrackPresence:
		mode = "enabled"
	case hs.PresenceEnabled:
		mode = "untracked"
	}
	a.Worker.Info().Int64("position", a.ids.Position()).Int("active", len(stale)).Str("presence", mode).
		Msg("writing the presence stream (stream_writers.presence names this process)")
	return a, nil
}

// Writing reports whether this process writes presence.
func (a *App) Writing() bool { return a.writing }

// SetSynapse notes a re-read: the role is not changed until restart.
func (a *App) SetSynapse(hs *synapsecfg.Config) {
	want := slices.Contains(hs.Writers.Presence, a.Instance)
	if want != a.writing {
		a.roleNoted.Do(func() {
			a.Worker.Warn().Strs("presence_writers", hs.Writers.Presence).Bool("writing", a.writing).
				Msg("homeserver.yaml changed stream_writers.presence; restart goworker to take or drop the presence role")
		})
	}
	if a.writing && hs.PresenceEnabled != (a.tr != nil) {
		a.enabledNoted.Do(func() {
			a.Worker.Warn().Bool("presence_enabled", hs.PresenceEnabled).
				Msg("homeserver.yaml changed presence.enabled; restart goworker (and every Synapse process) to apply it")
		})
	}
}

// Run is what the writer does after start: _mark_stale_presence_as_offline,
// once, when there is anything to mark.
func (a *App) Run(ctx context.Context) error {
	if a.tr != nil {
		a.tr.run(ctx, a.Lines)
		return nil
	}
	if !a.writing || len(a.stale) == 0 {
		return nil
	}
	states := make([]State, len(a.stale))
	for i, s := range a.stale {
		s.State = "offline"
		s.StatusMsg = nil
		s.CurrentlyActive = false
		states[i] = s
	}
	a.stale = nil
	a.Worker.Info().Int("states", len(states)).
		Msg("presence is disabled: marking stale presence states as offline")
	for start := 0; start < len(states); start += staleBatch {
		batch := states[start:min(start+staleBatch, len(states))]
		if err := a.persist(ctx, batch); err != nil {
			// A background process's exception: logged, and the rest left
			// for the next start.
			a.Worker.Error().Err(err).Msg("marking stale presence states as offline")
			return nil
		}
		staleOffline.Add(float64(len(batch)))
	}
	return nil
}

// persist is update_presence: an ID per state (get_next_mult), one
// transaction, every ID finished together.
func (a *App) persist(ctx context.Context, states []State) error {
	ids := make([]*streamid.ID, 0, len(states))
	finish := func(ok bool) error {
		var first error
		for _, id := range ids {
			if err := id.Finish(ctx, ok); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	for range states {
		id, err := a.ids.Next(ctx)
		if err != nil {
			_ = finish(false)
			return err
		}
		ids = append(ids, id)
	}
	values := make([]int64, len(ids))
	for i, id := range ids {
		values[i] = id.Value
	}
	err := a.Store.Persist(ctx, a.Instance, values, states)
	if ferr := finish(err == nil); err == nil {
		err = ferr
	}
	return err
}

// validPresence is VALID_PRESENCE: busy too under MSC3026.
func (a *App) validPresence(p string) bool {
	switch p {
	case "online", "unavailable", "offline":
		return true
	case "busy":
		return a.Synapse().Flag("msc3026_enabled", false)
	}
	return false
}

// Endpoints are the replication endpoints the writer serves.
func (a *App) Endpoints() []replserver.Endpoint {
	return []replserver.Endpoint{
		{
			Name: "presence_set_state", Method: http.MethodPost, PathArgs: []string{"user_id"},
			WaitForStreams: true, Handle: a.setState,
		},
		{
			Name: "bump_presence_active_time", Method: http.MethodPost, PathArgs: []string{"user_id"},
			WaitForStreams: true, Handle: a.bump,
		},
	}
}

// errInternal is an exception Synapse does not catch: a 500.
var errInternal = errors.New("presencewriter: internal error")

// setState is ReplicationPresenceSetState._handle_request with
// PresenceHandler.set_state while presence is not tracked: its arguments
// read in order (a missing one is a KeyError), the state's presence
// checked, then nothing.
func (a *App) setState(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
	const ep = "presence_set_state"
	if perr := userid.Check(req.Args["user_id"]); perr != nil {
		replicationCalls.WithLabelValues(ep, "refused").Inc()
		return 0, nil, &replserver.Error{Code: http.StatusBadRequest, Errcode: "M_INVALID_PARAM", Msg: perr.Msg}
	}
	stateRaw, ok := req.Content["state"]
	if _, fok := req.Content["force_notify"]; !ok || !fok {
		replicationCalls.WithLabelValues(ep, "error").Inc()
		return 0, nil, fmt.Errorf("%w: %s without state or force_notify", errInternal, ep)
	}
	// state.get(...), then state["presence"]: anything but a dict, or no
	// presence, is an exception. A repeated key is the last, as a dict's.
	var state map[string]json.RawMessage
	if len(stateRaw) == 0 || stateRaw[0] != '{' || json.Unmarshal(stateRaw, &state) != nil {
		replicationCalls.WithLabelValues(ep, "error").Inc()
		return 0, nil, fmt.Errorf("%w: %s: state is not an object", errInternal, ep)
	}
	presenceRaw, ok := state["presence"]
	if !ok {
		replicationCalls.WithLabelValues(ep, "error").Inc()
		return 0, nil, fmt.Errorf("%w: %s: no presence in state", errInternal, ep)
	}
	var presence string
	if json.Unmarshal(presenceRaw, &presence) != nil || !a.validPresence(presence) {
		replicationCalls.WithLabelValues(ep, "refused").Inc()
		return 0, nil, &replserver.Error{Code: http.StatusBadRequest, Errcode: "M_UNKNOWN", Msg: "Invalid presence state"}
	}
	if !a.Tracking() {
		replicationCalls.WithLabelValues(ep, "ignored").Inc()
		return http.StatusOK, map[string]any{}, nil
	}
	msg, err := statusMsg(state["status_msg"])
	if err != nil {
		replicationCalls.WithLabelValues(ep, "error").Inc()
		return 0, nil, err
	}
	var dev devKey
	var deviceID *string
	if raw, ok := req.Content["device_id"]; ok && json.Unmarshal(raw, &deviceID) == nil {
		dev = deviceKey(deviceID)
	}
	sources.WithLabelValues("set_state").Inc()
	a.tr.op.Lock()
	err = a.tr.setState(ctx, req.Args["user_id"], dev, presence, msg,
		truthyJSON(gjson.ParseBytes(req.Content["force_notify"])), truthyJSON(gjson.ParseBytes(req.Content["is_sync"])))
	a.tr.op.Unlock()
	if err != nil {
		replicationCalls.WithLabelValues(ep, "error").Inc()
		return 0, nil, err
	}
	replicationCalls.WithLabelValues(ep, "handled").Inc()
	return http.StatusOK, map[string]any{}, nil
}

// statusMsg is state.get("status_msg"): a string or None. Anything else
// Synapse would try to store; refused here.
func statusMsg(raw json.RawMessage) (*string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("%w: status_msg %s", errInternal, raw)
	}
	return &s, nil
}

// bump is ReplicationBumpPresenceActiveTime._handle_request with
// bump_presence_active_time while presence is not tracked.
func (a *App) bump(ctx context.Context, req replserver.Request) (int, map[string]any, error) {
	const ep = "bump_presence_active_time"
	if perr := userid.Check(req.Args["user_id"]); perr != nil {
		replicationCalls.WithLabelValues(ep, "refused").Inc()
		return 0, nil, &replserver.Error{Code: http.StatusBadRequest, Errcode: "M_INVALID_PARAM", Msg: perr.Msg}
	}
	if !a.Tracking() {
		replicationCalls.WithLabelValues(ep, "ignored").Inc()
		return http.StatusOK, map[string]any{}, nil
	}
	var deviceID *string
	if raw, ok := req.Content["device_id"]; ok {
		_ = json.Unmarshal(raw, &deviceID)
	}
	sources.WithLabelValues("bump").Inc()
	a.tr.op.Lock()
	err := a.tr.bump(ctx, req.Args["user_id"], deviceKey(deviceID))
	a.tr.op.Unlock()
	if err != nil {
		replicationCalls.WithLabelValues(ep, "error").Inc()
		return 0, nil, err
	}
	replicationCalls.WithLabelValues(ep, "handled").Inc()
	return http.StatusOK, map[string]any{}, nil
}

// ReceiveEDU is on_edu for m.presence while presence is not tracked: it
// returns before any handler.
func (a *App) ReceiveEDU(ctx context.Context, origin string, content json.RawMessage) error {
	if a.Tracking() {
		replicationCalls.WithLabelValues("fed_send_edu", "handled").Inc()
		return a.presenceEDU(ctx, origin, content)
	}
	replicationCalls.WithLabelValues("fed_send_edu", "ignored").Inc()
	return nil
}

// stream is the presence stream.
type stream struct{ a *App }

func (stream) Name() string          { return StreamName }
func (s stream) CurrentToken() int64 { return s.a.ids.Position() }
func (s stream) UpdatesSince(ctx context.Context, from, upto int64, limit int) ([]streamwriter.Update, int64, bool, error) {
	return s.a.Store.Updates(ctx, from, upto, limit)
}

// federationStream is PresenceFederationStream on the writer: its queue's
// _next_id - 1, which nothing advances while presence is disabled.
type federationStream struct{}

func (federationStream) Name() string        { return FederationStreamName }
func (federationStream) CurrentToken() int64 { return 0 }
func (federationStream) UpdatesSince(_ context.Context, _, upto int64, _ int) ([]streamwriter.Update, int64, bool, error) {
	return nil, upto, false, nil
}
