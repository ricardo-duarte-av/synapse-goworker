package devicelists

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

var (
	remoteEDUs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_device_lists_remote_edus_total",
		Help: "Device list and signing key EDUs from other servers, by type (device_list_update, signing_key_update) and outcome: applied (a delta), resync (a gap: the list is fetched), dropped (spoofed, non-compliant, or no shared room), error.",
	}, []string{"type", "outcome"})
	resyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_device_lists_resyncs_total",
		Help: "Remote device list resyncs, by outcome: cached (no federation needed), fetched, unchanged (the cache matched), failed (marked stale), denied (federation not allowed to the server).",
	}, []string{"outcome"})
	resyncPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_device_lists_resync_pending",
		Help: "Remote users whose device lists are marked stale (device_lists_remote_resync), as of the last retry pass.",
	})
)

// retryInterval is _maybe_retry_device_resync's period.
const retryInterval = 30 * time.Second

// ErrFederationDenied is FederationDeniedError: the destination is not in
// federation_domain_whitelist. A resync it stops is not marked stale.
var ErrFederationDenied = errors.New("devicelists: federation with this server is not allowed")

// Query is GET /_matrix/federation/v1/user/devices/{userId} on the user's
// server, under its retry limiter: the answer, or an error
// (ErrFederationDenied, or anything else: a failure).
type Query func(ctx context.Context, destination, userID string) (json.RawMessage, error)

// StateAtEvent is get_state_ids_for_event(await_full_state=False):
// member state_key to event ID.
type StateAtEvent func(ctx context.Context, eventID string) (map[string]string, error)

// RemoteStore is what the remote half reads and writes (DB).
type RemoteStore interface {
	AnyPartialRooms(ctx context.Context) (bool, error)
	AddRemotePending(ctx context.Context, instance string, id int64, userID, deviceID string) error
	Extremity(ctx context.Context, userID string) (*string, error)
	NeedsResync(ctx context.Context, userID string) (bool, error)
	AllNeedingResync(ctx context.Context) ([]string, error)
	MarkStale(ctx context.Context, userIDs []string, nowMS int64) error
	MarkValid(ctx context.Context, userID string) error
	CachedDevices(ctx context.Context, userID string) (map[string]json.RawMessage, error)
	UpdateRemoteEntry(ctx context.Context, userID, deviceID, content string, deleted bool, streamID string) error
	ReplaceRemoteCache(ctx context.Context, userID string, devices []RemoteDevice, streamID string) error
	CrossSigningKeys(ctx context.Context, userID string) (map[string]json.RawMessage, error)
	SetCrossSigningKey(ctx context.Context, instance string, userID, keyType, key string, nowMS int64) ([]cachestream.Row, error)
	RetryDue(ctx context.Context, hosts []string, nowMS int64) (map[string]bool, error)
	PendingForRoom(ctx context.Context, roomID string) ([][2]string, error)
	PartialJoin(ctx context.Context, roomID string) (string, int64, error)
	ChangesInRoom(ctx context.Context, roomID string, min int64) ([][2]string, bool, error)
	DeviceChangesForUsers(ctx context.Context, from, upto int64, userIDs []string) ([][2]string, error)
	LocalUsersInRoom(ctx context.Context, roomID string) ([]string, error)
	PartialServersAtJoin(ctx context.Context, roomID string) ([]string, error)
	CurrentMembers(ctx context.Context, roomID string) (map[string]string, error)
	CurrentHosts(ctx context.Context, roomID string) ([]string, error)
}

// Caches is the process's caches stream (cachestream.Writer).
type Caches interface {
	Do(write func() ([]cachestream.Row, error)) error
}

// remote is DeviceListUpdater and SigningKeyEduUpdater's state.
type remote struct {
	mu      sync.Mutex
	pending map[string][]pendingUpdate   // user -> device list updates
	signing map[string][][2]gjson.Result // user -> (master, self_signing)
	seen    map[string]*seenEntry        // _seen_updates (ExpiringCache)
	edu     sync.Map                     // _remote_edu_linearizer
	sk      sync.Map                     // the signing keys' linearizer
	resync  sync.Map                     // _resync_linearizer
	retry   sync.Mutex                   // _resync_retry_lock
}

type pendingUpdate struct {
	deviceID, streamID string
	prevIDs            []string
	content            gjson.Result // the EDU minus user_id, device_id, stream_id, prev_id
}

// seenEntry is one user's seen stream IDs: Python values, so a string
// "5" and an int 5 differ (a resync stores the answer's stream_id as it
// came, an EDU's as str()).
type seenEntry struct {
	ids map[string]bool
	at  time.Time
}

const (
	seenExpiry = 30 * time.Minute
	seenMax    = 10000
)

func pyKey(v gjson.Result) string {
	if v.Type == gjson.String {
		return "s:" + v.Str
	}
	return "x:" + v.Raw
}

func lock(m *sync.Map, key string) func() {
	l, _ := m.LoadOrStore(key, &sync.Mutex{})
	mu := l.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (a *App) seenGet(user string) map[string]bool {
	a.r.mu.Lock()
	defer a.r.mu.Unlock()
	e := a.r.seen[user]
	if e == nil || a.Now().Sub(e.at) > seenExpiry {
		return nil
	}
	return e.ids
}

// seenAdd is setdefault(user, set()).update(...): an existing entry keeps
// its time.
func (a *App) seenAdd(user string, keys []string, replace bool) {
	a.r.mu.Lock()
	defer a.r.mu.Unlock()
	e := a.r.seen[user]
	if replace || e == nil || a.Now().Sub(e.at) > seenExpiry {
		if len(a.r.seen) >= seenMax {
			var oldest string
			var ot time.Time
			for k, v := range a.r.seen {
				if oldest == "" || v.at.Before(ot) {
					oldest, ot = k, v.at
				}
			}
			delete(a.r.seen, oldest)
		}
		e = &seenEntry{ids: map[string]bool{}, at: a.Now()}
		a.r.seen[user] = e
	}
	for _, k := range keys {
		e.ids[k] = true
	}
}

func invalidID(s string) error {
	return &replserver.Error{Code: 400, Errcode: "M_UNKNOWN", Msg: "Invalid ID: " + pyjson.ReprString(s)}
}

// popped is the content without the keys Synapse pops, as json_encoder
// writes it.
func popped(content gjson.Result, keys ...string) (string, gjson.Result, error) {
	var b strings.Builder
	b.WriteByte('{')
	first := true
	content.ForEach(func(k, v gjson.Result) bool {
		if slices.Contains(keys, k.Str) {
			return true
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(k.Raw)
		b.WriteByte(':')
		b.WriteString(v.Raw)
		return true
	})
	b.WriteByte('}')
	enc, err := pyjson.Encode([]byte(b.String()))
	return enc, gjson.Parse(enc), err
}

// ReceiveDeviceListUpdate is incoming_device_list_update.
func (a *App) ReceiveDeviceListUpdate(ctx context.Context, origin string, raw json.RawMessage) error {
	err := a.receiveDeviceList(ctx, origin, raw)
	if err != nil {
		remoteEDUs.WithLabelValues("device_list_update", "error").Inc()
	}
	return err
}

func (a *App) receiveDeviceList(ctx context.Context, origin string, raw json.RawMessage) error {
	if !a.writing {
		return ErrNotWriter
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return err
	}
	c := gjson.ParseBytes(raw)
	if !c.IsObject() {
		return errors.New("devicelists: EDU content is not an object")
	}
	userV, devV, streamV := c.Get("user_id"), c.Get("device_id"), c.Get("stream_id")
	for name, v := range map[string]gjson.Result{"user_id": userV, "device_id": devV, "stream_id": streamV} {
		if !v.Exists() {
			return fmt.Errorf("devicelists: EDU has no %q", name) // KeyError
		}
	}
	if userV.Type != gjson.String || devV.Type != gjson.String {
		return errors.New("devicelists: EDU user_id or device_id is not a string")
	}
	user, device := userV.Str, devV.Str
	streamID, err := pyjson.Str([]byte(streamV.Raw))
	if err != nil {
		return err
	}
	var prevIDs []string
	if p := c.Get("prev_id"); p.Exists() {
		if !p.IsArray() {
			return &replserver.Error{Code: 400, Errcode: "M_UNKNOWN", Msg: "Device list update had an invalid 'prev_ids' field"}
		}
		for _, x := range p.Array() {
			s, err := pyjson.Str([]byte(x.Raw))
			if err != nil {
				return err
			}
			prevIDs = append(prevIDs, s)
		}
	}
	if !strings.Contains(user, ":") {
		return invalidID(user)
	}
	if userid.Domain(user) != origin {
		remoteEDUs.WithLabelValues("device_list_update", "dropped").Inc()
		a.Log.Warn().Str("user", user).Str("device", device).Str("origin", origin).Msg("Got device list update edu from the wrong origin")
		return nil
	}
	if !userid.Compliant(user) {
		remoteEDUs.WithLabelValues("device_list_update", "dropped").Inc()
		a.Log.Warn().Str("user", user).Str("origin", origin).Msg("Dropping device list update edu for non-compliant user ID")
		return nil
	}
	_, content, err := popped(c, "user_id", "device_id", "stream_id", "prev_id")
	if err != nil {
		return err
	}
	partial, err := a.RStore.AnyPartialRooms(ctx)
	if err != nil {
		return err
	}
	if partial {
		if err := a.withIDs(ctx, 1, func(ids []int64) error {
			return a.RStore.AddRemotePending(ctx, a.Instance, ids[0], user, device)
		}); err != nil {
			return err
		}
	}
	rooms, err := a.Store.RoomsForUser(ctx, user)
	if err != nil {
		return err
	}
	if len(rooms) == 0 {
		remoteEDUs.WithLabelValues("device_list_update", "dropped").Inc()
		a.Log.Warn().Str("user", user).Str("device", device).Msg("Got device list update edu, but don't share a room")
		return nil
	}
	a.r.mu.Lock()
	a.r.pending[user] = append(a.r.pending[user], pendingUpdate{device, streamID, prevIDs, content})
	a.r.mu.Unlock()
	return a.handleDeviceUpdates(ctx, user)
}

// handleDeviceUpdates is _handle_device_updates: every update pending for
// the user, applied as deltas when their prev_ids chain on to what we
// hold, else a resync in the background.
func (a *App) handleDeviceUpdates(ctx context.Context, user string) error {
	defer lock(&a.r.edu, user)()
	a.r.mu.Lock()
	updates := a.r.pending[user]
	delete(a.r.pending, user)
	a.r.mu.Unlock()
	if len(updates) == 0 {
		return nil
	}
	resync, err := a.needResync(ctx, user, updates)
	if err != nil {
		return err
	}
	var devs []string
	for _, u := range updates {
		devs = append(devs, u.deviceID)
	}
	a.Log.Info().Str("user", user).Bool("resync", resync).Strs("devices", devs).Msg("Received device list update")
	if resync {
		remoteEDUs.WithLabelValues("device_list_update", "resync").Add(float64(len(updates)))
		if err := a.RStore.MarkStale(ctx, []string{user}, a.Now().UnixMilli()); err != nil {
			return err
		}
		go func() {
			rctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if _, err := a.MultiUserDeviceResync(rctx, []string{user}, false); err != nil {
				a.Log.Warn().Err(err).Str("user", user).Msg("device list resync failed")
			}
		}()
		return nil
	}
	for _, u := range updates {
		deleted := clientTruthy(u.content.Get("deleted"))
		if err := a.RStore.UpdateRemoteEntry(ctx, user, u.deviceID, u.content.Raw, deleted, u.streamID); err != nil {
			return err
		}
	}
	if err := a.NotifyDeviceUpdate(ctx, user, devs); err != nil {
		return err
	}
	var keys []string
	for _, u := range updates {
		keys = append(keys, "s:"+u.streamID)
	}
	a.seenAdd(user, keys, false)
	remoteEDUs.WithLabelValues("device_list_update", "applied").Add(float64(len(updates)))
	return nil
}

// clientTruthy is bool() of a JSON value, absent being falsy.
func clientTruthy(v gjson.Result) bool {
	if !v.Exists() {
		return false
	}
	switch v.Type {
	case gjson.Null, gjson.False:
		return false
	case gjson.True:
		return true
	case gjson.String:
		return v.Str != ""
	case gjson.Number:
		return v.Float() != 0
	}
	if v.IsArray() {
		return len(v.Array()) > 0
	}
	n := 0
	v.ForEach(func(_, _ gjson.Result) bool { n++; return false })
	return n > 0
}

// needResync is _need_to_do_resync.
func (a *App) needResync(ctx context.Context, user string, updates []pendingUpdate) (bool, error) {
	seen := a.seenGet(user)
	ext, err := a.RStore.Extremity(ctx, user)
	if err != nil {
		return false, err
	}
	inUpdates := map[string]bool{}
	for _, u := range updates {
		if len(u.prevIDs) == 0 {
			return true, nil
		}
		for _, p := range u.prevIDs {
			switch {
			case ext != nil && p == *ext:
			case seen["s:"+p]:
			case inUpdates[p]:
			default:
				return true, nil
			}
		}
		inUpdates[u.streamID] = true
	}
	return false, nil
}

// MultiUserDeviceResync is DeviceListUpdater.multi_user_device_resync:
// users of one server, each fetched (unless our cache is current) under
// its own linearizer; with markFailed, the failures marked stale. The
// answer is each user's federation answer, or null.
func (a *App) MultiUserDeviceResync(ctx context.Context, users []string, markFailed bool) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	if len(users) == 0 {
		return out, nil
	}
	origins := map[string]bool{}
	for _, u := range users {
		if userid.Check(u) != nil {
			return nil, &replserver.Error{Code: 400, Errcode: "M_INVALID_PARAM", Msg: userid.Check(u).Msg}
		}
		origins[userid.Domain(u)] = true
	}
	if len(origins) != 1 {
		var list []string
		for o := range origins {
			list = append(list, pyjson.ReprString(o))
		}
		return nil, &replserver.Error{Code: 400, Errcode: "M_INVALID_PARAM", Msg: "Only one origin permitted, got {" + strings.Join(list, ", ") + "}"}
	}
	var failed []string
	for _, u := range users {
		unlock := lock(&a.r.resync, u)
		res, fail, err := a.resyncOne(ctx, u)
		unlock()
		if err != nil {
			return nil, err
		}
		out[u] = res
		if fail {
			failed = append(failed, u)
		}
	}
	if markFailed {
		if err := a.RStore.MarkStale(ctx, failed, a.Now().UnixMilli()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// resyncOne is _user_device_resync_returning_failed.
func (a *App) resyncOne(ctx context.Context, user string) (json.RawMessage, bool, error) {
	ext, err := a.RStore.Extremity(ctx, user)
	if err != nil {
		return nil, false, err
	}
	stale, err := a.RStore.NeedsResync(ctx, user)
	if err != nil {
		return nil, false, err
	}
	if ext != nil && *ext != "" && !stale {
		// get_users_whose_devices_are_cached: the cached devices answer.
		cached, err := a.RStore.CachedDevices(ctx, user)
		if err != nil {
			return nil, false, err
		}
		b, _ := json.Marshal(cached)
		resyncs.WithLabelValues("cached").Inc()
		return b, false, nil
	}
	if a.Query == nil {
		resyncs.WithLabelValues("failed").Inc()
		return nil, true, nil
	}
	ans, err := a.Query(ctx, userid.Domain(user), user)
	if errors.Is(err, ErrFederationDenied) {
		resyncs.WithLabelValues("denied").Inc()
		a.Log.Info().Str("user", user).Msg("device list resync: federation with the server is not allowed")
		return nil, false, nil
	}
	if err != nil {
		resyncs.WithLabelValues("failed").Inc()
		a.Log.Warn().Err(err).Str("user", user).Msg("Failed to handle device list update")
		return nil, true, nil
	}
	ans, err = pyjson.Dedupe(ans)
	if err != nil {
		return nil, false, err
	}
	res := gjson.ParseBytes(ans)
	streamV, devsV := res.Get("stream_id"), res.Get("devices")
	if !streamV.Exists() || !devsV.Exists() {
		return nil, false, errors.New("devicelists: resync answer lacks stream_id or devices") // KeyError
	}
	if !devsV.IsArray() {
		return nil, false, errors.New("devicelists: resync answer devices is not a list")
	}
	devices := devsV.Array()
	ignore := false
	if len(devices) > 1000 {
		a.Log.Warn().Str("user", user).Int("devices", len(devices)).Msg("Ignoring device list snapshot as it has >1K devs")
		devices, ignore = nil, true
	} else if ext != nil {
		cached, err := a.RStore.CachedDevices(ctx, user)
		if err != nil {
			return nil, false, err
		}
		if sameDevices(cached, devices) {
			a.Log.Info().Str("user", user).Msg("Skipping device list resync, as our cache matches already")
			devices, ignore = nil, true
			resyncs.WithLabelValues("unchanged").Inc()
		}
	}
	streamStr, err := pyjson.Str([]byte(streamV.Raw))
	if err != nil {
		return nil, false, err
	}
	var ids []string
	if !ignore {
		var rows []RemoteDevice
		for _, d := range devices {
			id := d.Get("device_id")
			if !d.IsObject() || !id.Exists() {
				return nil, false, errors.New("devicelists: resync device without device_id") // KeyError
			}
			enc, err := pyjson.Encode([]byte(d.Raw))
			if err != nil {
				return nil, false, err
			}
			idStr, err := pyjson.Str([]byte(id.Raw))
			if err != nil {
				return nil, false, err
			}
			rows = append(rows, RemoteDevice{idStr, enc})
			ids = append(ids, idStr)
		}
		if err := a.RStore.ReplaceRemoteCache(ctx, user, rows, streamStr); err != nil {
			return nil, false, err
		}
		resyncs.WithLabelValues("fetched").Inc()
	}
	if err := a.RStore.MarkValid(ctx, user); err != nil {
		return nil, false, err
	}
	csIDs, err := a.crossSigningUpdate(ctx, user, res.Get("master_key"), res.Get("self_signing_key"))
	if err != nil {
		return nil, false, err
	}
	ids = append(ids, csIDs...)
	if len(ids) > 0 {
		if err := a.NotifyDeviceUpdate(ctx, user, ids); err != nil {
			return nil, false, err
		}
	}
	a.seenAdd(user, []string{pyKey(streamV)}, true)
	return json.RawMessage(ans), false, nil
}

// sameDevices is cached_devices == {d["device_id"]: d for d in devices}.
func sameDevices(cached map[string]json.RawMessage, devices []gjson.Result) bool {
	got := map[string]any{}
	for _, d := range devices {
		var v any
		_ = json.Unmarshal([]byte(d.Raw), &v)
		got[d.Get("device_id").String()] = v
	}
	want := map[string]any{}
	for k, raw := range cached {
		var v any
		_ = json.Unmarshal(raw, &v)
		want[k] = v
	}
	return reflect.DeepEqual(got, want)
}

func jsonEqual(a gjson.Result, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal([]byte(a.Raw), &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// crossSigningUpdate is process_cross_signing_key_update: a master or
// self-signing key that differs from ours is stored, and its key ID is a
// "device" that changed.
func (a *App) crossSigningUpdate(ctx context.Context, user string, master, self gjson.Result) ([]string, error) {
	current, err := a.RStore.CrossSigningKeys(ctx, user)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, k := range []struct {
		typ string
		v   gjson.Result
	}{{"master", master}, {"self_signing", self}} {
		if !clientTruthy(k.v) {
			continue
		}
		if cur, ok := current[k.typ]; ok && jsonEqual(k.v, cur) {
			continue
		}
		id, err := verifyKeyVersion(k.v)
		if err != nil {
			return nil, err
		}
		enc, err := pyjson.Encode([]byte(k.v.Raw))
		if err != nil {
			return nil, err
		}
		if err := a.Caches.Do(func() ([]cachestream.Row, error) {
			return a.RStore.SetCrossSigningKey(ctx, a.Instance, user, k.typ, enc, a.Now().UnixMilli())
		}); err != nil {
			var perr *cachestream.PublishError
			if !errors.As(err, &perr) {
				return nil, err
			}
			a.Log.Warn().Err(err).Msg("device lists: cross-signing key stored, caches row not published")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// verifyKeyVersion is get_verify_key_from_cross_signing_key's version: the
// part after the colon of the one key ID in "keys" (more or none is an
// error, as is a key of another algorithm).
func verifyKeyVersion(key gjson.Result) (string, error) {
	keys := key.Get("keys")
	if !keys.IsObject() {
		return "", errors.New("devicelists: cross-signing key has no keys")
	}
	var ids []string
	keys.ForEach(func(k, _ gjson.Result) bool { ids = append(ids, k.Str); return true })
	if len(ids) != 1 {
		return "", errors.New("Key does not contain exactly one key")
	}
	alg, version, ok := strings.Cut(ids[0], ":")
	if !ok || alg != "ed25519" {
		return "", errors.New("Unsupported signing algorithm")
	}
	return version, nil
}

// ReceiveSigningKeyUpdate is incoming_signing_key_update.
func (a *App) ReceiveSigningKeyUpdate(ctx context.Context, origin string, raw json.RawMessage) error {
	err := a.receiveSigning(ctx, origin, raw)
	if err != nil {
		remoteEDUs.WithLabelValues("signing_key_update", "error").Inc()
	}
	return err
}

func (a *App) receiveSigning(ctx context.Context, origin string, raw json.RawMessage) error {
	if !a.writing {
		return ErrNotWriter
	}
	raw, err := pyjson.Dedupe(raw)
	if err != nil {
		return err
	}
	c := gjson.ParseBytes(raw)
	userV := c.Get("user_id")
	if !c.IsObject() || !userV.Exists() {
		return errors.New(`devicelists: EDU has no "user_id"`)
	}
	if userV.Type != gjson.String {
		return errors.New("devicelists: EDU user_id is not a string")
	}
	user := userV.Str
	if !strings.Contains(user, ":") {
		return invalidID(user)
	}
	if userid.Domain(user) != origin {
		remoteEDUs.WithLabelValues("signing_key_update", "dropped").Inc()
		a.Log.Warn().Str("user", user).Str("origin", origin).Msg("Got signing key update edu from the wrong origin")
		return nil
	}
	if !userid.Compliant(user) {
		remoteEDUs.WithLabelValues("signing_key_update", "dropped").Inc()
		a.Log.Warn().Str("user", user).Str("origin", origin).Msg("Dropping signing key update edu for non-compliant user ID")
		return nil
	}
	rooms, err := a.Store.RoomsForUser(ctx, user)
	if err != nil {
		return err
	}
	if len(rooms) == 0 {
		remoteEDUs.WithLabelValues("signing_key_update", "dropped").Inc()
		return nil
	}
	a.r.mu.Lock()
	a.r.signing[user] = append(a.r.signing[user], [2]gjson.Result{c.Get("master_key"), c.Get("self_signing_key")})
	a.r.mu.Unlock()

	defer lock(&a.r.sk, user)()
	a.r.mu.Lock()
	updates := a.r.signing[user]
	delete(a.r.signing, user)
	a.r.mu.Unlock()
	if len(updates) == 0 {
		return nil
	}
	var ids []string
	for _, u := range updates {
		got, err := a.crossSigningUpdate(ctx, user, u[0], u[1])
		if err != nil {
			return err
		}
		ids = append(ids, got...)
	}
	if err := a.NotifyDeviceUpdate(ctx, user, ids); err != nil {
		return err
	}
	remoteEDUs.WithLabelValues("signing_key_update", "applied").Inc()
	return nil
}

// retryResyncs is _maybe_retry_device_resync: every stale user whose
// server is not backed off, resynced in turn; a pass still running skips
// the next.
func (a *App) retryResyncs(ctx context.Context) {
	if !a.r.retry.TryLock() {
		return
	}
	defer a.r.retry.Unlock()
	users, err := a.RStore.AllNeedingResync(ctx)
	if err != nil {
		a.Log.Warn().Err(err).Msg("device lists: resync retry")
		return
	}
	resyncPending.Set(float64(len(users)))
	hostSet := map[string]bool{}
	for _, u := range users {
		hostSet[userid.Domain(u)] = true
	}
	hosts := make([]string, 0, len(hostSet))
	for h := range hostSet {
		hosts = append(hosts, h)
	}
	due, err := a.RStore.RetryDue(ctx, hosts, a.Now().UnixMilli())
	if err != nil {
		a.Log.Warn().Err(err).Msg("device lists: resync retry")
		return
	}
	for _, u := range users {
		if ctx.Err() != nil {
			return
		}
		if !due[userid.Domain(u)] {
			continue
		}
		if _, err := a.MultiUserDeviceResync(ctx, []string{u}, false); err != nil {
			a.Log.Debug().Err(err).Str("user", u).Msg("Could not resync the device list")
		}
	}
}

// HandleRoomUnPartialStated is DeviceWriterHandler.handle_room_un_partial_stated:
// the remote updates held for the room into the streams, then pokes for
// our devices' changes to the servers that joined while the room was
// partial.
func (a *App) HandleRoomUnPartialStated(ctx context.Context, roomID string) error {
	if !a.writing {
		return ErrNotWriter
	}
	pending, err := a.RStore.PendingForRoom(ctx, roomID)
	if err != nil {
		return err
	}
	for _, p := range pending {
		a.Log.Info().Str("room", roomID).Str("user", p[0]).Str("device", p[1]).Msg("Got pending device list update in room")
		if err := a.withIDs(ctx, 1, func(ids []int64) error {
			return a.Store.AddDeviceChange(ctx, a.Instance, ids, p[0], []string{p[1]}, []string{roomID}, a.isMine(p[0]), a.Now().UnixMilli())
		}); err != nil {
			return err
		}
	}
	joinEvent, dlStream, err := a.RStore.PartialJoin(ctx, roomID)
	if err != nil {
		return err
	}
	changes, ok, err := a.RStore.ChangesInRoom(ctx, roomID, dlStream)
	if err != nil {
		return err
	}
	var local [][2]string
	if ok {
		for _, c := range changes {
			if a.isMine(c[0]) {
				local = append(local, c)
			}
		}
	} else {
		users, err := a.RStore.LocalUsersInRoom(ctx, roomID)
		if err != nil {
			return err
		}
		if local, err = a.RStore.DeviceChangesForUsers(ctx, dlStream, a.ids.Position(), users); err != nil {
			return err
		}
	}
	if len(local) == 0 {
		return nil
	}
	if a.StateAtEvent == nil {
		return errors.New("devicelists: no state reader for partial-state rooms")
	}
	atJoin, err := a.StateAtEvent(ctx, joinEvent)
	if err != nil {
		return err
	}
	current, err := a.RStore.CurrentMembers(ctx, roomID)
	if err != nil {
		return err
	}
	changed := map[string]bool{}
	for k := range atJoin {
		if atJoin[k] != current[k] {
			changed[userid.Domain(k)] = true
		}
	}
	for k := range current {
		if atJoin[k] != current[k] {
			changed[userid.Domain(k)] = true
		}
	}
	hosts, err := a.RStore.CurrentHosts(ctx, roomID)
	if err != nil {
		return err
	}
	for _, h := range hosts {
		changed[h] = true
	}
	known, err := a.RStore.PartialServersAtJoin(ctx, roomID)
	if err != nil {
		return err
	}
	for _, h := range known {
		delete(changed, h)
	}
	delete(changed, a.serverName)
	if len(changed) == 0 {
		return nil
	}
	list := make([]string, 0, len(changed))
	for h := range changed {
		list = append(list, h)
	}
	slices.Sort(list)
	a.Log.Info().Int("hosts", len(list)).Msg("Found changed hosts to send device list updates to")
	for _, c := range local {
		var pokeID int64
		if err := a.withIDs(ctx, 1, func(ids []int64) error {
			pokeID = ids[0]
			return a.Store.AddOutboundPokes(ctx, a.Instance, ids[0], c[0], c[1], list, a.isMine(c[0]), a.Now().UnixMilli())
		}); err != nil {
			return err
		}
		if a.OnPokes != nil {
			a.OnPokes(pokeID)
		}
	}
	return nil
}
