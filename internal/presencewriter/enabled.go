package presencewriter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// PresenceHandler while presence is enabled (handlers/presence.py, v1.165.0).
//
// Synapse runs it on one event loop: nothing else runs between two awaits.
// Here every operation that changes the state (set_state, bump, a sync's
// start and end, USER_SYNC, an EDU, the timeouts, a flush, a join's
// fan-out) runs whole under op, its database reads and writes included, so
// no two interleave at all; readers (GET, the sender's lookups) take only
// mu, which guards the maps.

var (
	notifyReasons = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_notify_reasons_total",
		Help: "State changes worth telling anyone of (should_notify), by locality (local, remote) and reason, as Synapse's synapse_handler_presence_notify_reason.",
	}, []string{"locality", "reason"})
	transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_state_transitions_total",
		Help: "Presence state changes by locality and from/to state, as Synapse's synapse_handler_presence_state_transition.",
	}, []string{"locality", "from", "to"})
	updates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_updates_total",
		Help: "Presence updates handled (_update_states), by what came of them: notified (persisted, streamed, sent to interested servers), pinged (resent to other servers only), quiet (kept in memory, flushed later).",
	}, []string{"outcome"})
	sources = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_sources_total",
		Help: "What the presence writer was told, by source: set_state, sync_start, sync_end, bump, user_sync, clear_user_sync, edu (one per user in an m.presence EDU), timeout (a timer that fired), join (a membership that sent presence to new servers).",
	}, []string{"source"})
	held = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_presence_held",
		Help: "What the presence writer holds in memory, by what: users (user_to_current_state), devices, timers (wheel timer entries), unpersisted, syncs (devices syncing here), external_processes.",
	}, []string{"what"})
	fanout = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_fanout_total",
		Help: "Presence states handed to the federation sender, by why: notify (a change), ping (a resend before remote servers time us out), join (servers new to a room).",
	}, []string{"why"})
	deltaPosition = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_presence_event_position",
		Help: "How far the presence writer has read current state deltas for joins (Synapse's event_processing_positions{name=\"presence\"}).",
	})
	failures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_presence_failures_total",
		Help: "Presence work that failed, by what: persist, flush, deltas, edu, user_sync, fanout.",
	}, []string{"what"})
)

// RoomStore is what the enabled writer reads of rooms and old states.
type RoomStore interface {
	PresenceForUsers(ctx context.Context, userIDs []string) (map[string]Presence, error)
	RoomsForUser(ctx context.Context, userID string) ([]string, error)
	UsersInRoom(ctx context.Context, roomID string) ([]string, error)
	HostsInRoom(ctx context.Context, roomID string) ([]string, error)
	StateDeltas(ctx context.Context, prev, maxID int64, limit int) (int64, []StateDelta, error)
	Membership(ctx context.Context, eventID string) (string, error)
}

// tracker is the writer's state while presence is enabled.
type tracker struct {
	a        *App
	track    bool
	t        timers
	excluded map[string]struct{}
	server   string

	op sync.Mutex
	mu sync.Mutex
	// current is user_to_current_state.
	current map[string]Presence
	// devices is _user_to_device_to_current_state.
	devices map[string]map[devKey]*device
	wheel   wheelTimer
	// unpersisted is unpersisted_users_changes.
	unpersisted map[string]struct{}
	// syncs is _user_device_to_num_current_syncs.
	syncs map[userDevice]int
	// external and externalUpdated are external_process_to_current_syncs
	// and external_process_last_updated_ms.
	external        map[string]map[userDevice]struct{}
	externalUpdated map[string]int64
	// eventPos is _event_pos.
	eventPos int64
}

func (t *tracker) now() int64 { return t.a.Now().UnixMilli() }

func (t *tracker) isMine(userID string) bool { return userid.Domain(userID) == t.server }

// start is PresenceHandler.__init__ with presence enabled: the states not
// offline, each a pseudo-device and its timers when tracked.
func (t *tracker) start(active []State) {
	now := t.now()
	for _, s := range active {
		p := fromRow(s)
		t.current[p.UserID] = p
		if !t.track {
			continue
		}
		t.devices[p.UserID] = map[devKey]*device{{}: {state: p.State, lastActiveTS: p.LastActiveTS, lastSyncTS: p.LastUserSyncTS}}
		t.wheel.insert(now, p.UserID, p.LastActiveTS+t.t.idle)
		t.wheel.insert(now, p.UserID, p.LastUserSyncTS+t.t.syncOnline)
		if t.isMine(p.UserID) {
			t.wheel.insert(now, p.UserID, p.LastFederationUpdateTS+federationPingInterval)
		} else {
			t.wheel.insert(now, p.UserID, p.LastFederationUpdateTS+federationTimeout)
		}
	}
	t.gauges()
}

func fromRow(s State) Presence {
	return Presence{UserID: s.UserID, State: s.State, LastActiveTS: deref(s.LastActiveTS),
		LastFederationUpdateTS: deref(s.LastFederationUpdateTS), LastUserSyncTS: deref(s.LastUserSyncTS),
		StatusMsg: s.StatusMsg, CurrentlyActive: s.CurrentlyActive}
}

func toRow(p Presence) State {
	return State{UserID: p.UserID, State: p.State, LastActiveTS: &p.LastActiveTS,
		LastFederationUpdateTS: &p.LastFederationUpdateTS, LastUserSyncTS: &p.LastUserSyncTS,
		StatusMsg: p.StatusMsg, CurrentlyActive: p.CurrentlyActive}
}

func (t *tracker) gauges() {
	devices := 0
	for _, d := range t.devices {
		devices += len(d)
	}
	syncs := 0
	for _, n := range t.syncs {
		if n > 0 {
			syncs++
		}
	}
	held.WithLabelValues("users").Set(float64(len(t.current)))
	held.WithLabelValues("devices").Set(float64(devices))
	held.WithLabelValues("timers").Set(float64(t.wheel.len()))
	held.WithLabelValues("unpersisted").Set(float64(len(t.unpersisted)))
	held.WithLabelValues("syncs").Set(float64(syncs))
	held.WithLabelValues("external_processes").Set(float64(len(t.external)))
}

// currentStates is current_state_for_users: memory, then the database; a
// user with neither gets the default state, which is kept. Called under op.
func (t *tracker) currentStates(ctx context.Context, userIDs []string) (map[string]Presence, error) {
	out := make(map[string]Presence, len(userIDs))
	var missing []string
	t.mu.Lock()
	for _, u := range userIDs {
		if p, ok := t.current[u]; ok {
			out[u] = p
		} else {
			missing = append(missing, u)
		}
	}
	t.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	res, err := t.a.Rooms.PresenceForUsers(ctx, missing)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, u := range missing {
		if p, ok := res[u]; ok {
			out[u] = p
			continue
		}
		p := defaultPresence(u)
		out[u] = p
		t.current[u] = p
	}
	return out, nil
}

func (t *tracker) currentState(ctx context.Context, userID string) (Presence, error) {
	m, err := t.currentStates(ctx, []string{userID})
	return m[userID], err
}

// devicesOf is _user_to_device_to_current_state.setdefault(user, {}).
// Called with mu held.
func (t *tracker) devicesOf(userID string) map[devKey]*device {
	d, ok := t.devices[userID]
	if !ok {
		d = map[devKey]*device{}
		t.devices[userID] = d
	}
	return d
}

// updateStates is _update_states. Called under op.
func (t *tracker) updateStates(ctx context.Context, states []Presence, forceNotify bool) error {
	now := t.now()
	// Only the last change of each user, in the order users first appear.
	var order []string
	last := map[string]Presence{}
	for _, s := range states {
		if _, ok := last[s.UserID]; !ok {
			order = append(order, s.UserID)
		}
		last[s.UserID] = s
	}
	var toNotify, toPing []Presence
	notified := map[string]struct{}{}
	t.mu.Lock()
	for _, u := range order {
		prev, ok := t.current[u]
		if !ok {
			prev = defaultPresence(u)
		}
		ns, notify, ping := handleUpdate(prev, last[u], t.isMine(u), &t.wheel, now, !t.track, t.t)
		if forceNotify {
			notify = true
		}
		t.current[u] = ns
		switch {
		case notify:
			toNotify = append(toNotify, ns)
			notified[u] = struct{}{}
		case ping:
			toPing = append(toPing, ns)
		}
	}
	for _, u := range order {
		if _, ok := notified[u]; !ok {
			t.unpersisted[u] = struct{}{}
		} else {
			delete(t.unpersisted, u)
		}
	}
	t.gauges()
	t.mu.Unlock()
	updates.WithLabelValues("notified").Add(float64(len(toNotify)))
	updates.WithLabelValues("pinged").Add(float64(len(toPing)))
	updates.WithLabelValues("quiet").Add(float64(len(order) - len(toNotify) - len(toPing)))

	var err error
	if len(toNotify) > 0 {
		if err = t.persistAndNotify(ctx, toNotify); err != nil {
			failures.WithLabelValues("persist").Inc()
		}
	}
	if len(toPing) > 0 {
		t.sendToInterested(ctx, toPing, "ping")
	}
	return err
}

// persistAndNotify is _persist_and_notify: the rows (whose RDATA wakes
// every follower, our sync included), then the interested servers.
func (t *tracker) persistAndNotify(ctx context.Context, states []Presence) error {
	rows := make([]State, len(states))
	for i, s := range states {
		rows[i] = toRow(s)
	}
	if err := t.a.persist(ctx, rows); err != nil {
		return err
	}
	// maybe_send_presence_to_interested_destinations: our users only.
	var mine []Presence
	for _, s := range states {
		if t.isMine(s.UserID) {
			mine = append(mine, s)
		}
	}
	if len(mine) > 0 {
		t.sendToInterested(ctx, mine, "notify")
	}
	return nil
}

// sendToInterested is get_interested_remotes, each state to the hosts of
// the rooms its user is in (excluded rooms aside), handed to the sender.
func (t *tracker) sendToInterested(ctx context.Context, states []Presence, why string) {
	if t.a.SendPresence == nil {
		return
	}
	for _, s := range states {
		rooms, err := t.a.Rooms.RoomsForUser(ctx, s.UserID)
		if err != nil {
			failures.WithLabelValues("fanout").Inc()
			t.a.Log.Error().Err(err).Str("user_id", s.UserID).Msg("presence: rooms of a user")
			continue
		}
		hosts := map[string]struct{}{}
		for _, r := range rooms {
			if _, ok := t.excluded[r]; ok {
				continue
			}
			hs, err := t.a.Rooms.HostsInRoom(ctx, r)
			if err != nil {
				failures.WithLabelValues("fanout").Inc()
				t.a.Log.Error().Err(err).Str("room_id", r).Msg("presence: hosts in a room")
				continue
			}
			for _, h := range hs {
				hosts[h] = struct{}{}
			}
		}
		if len(hosts) == 0 {
			continue
		}
		dests := make([]string, 0, len(hosts))
		for h := range hosts {
			dests = append(dests, h)
		}
		fanout.WithLabelValues(why).Inc()
		t.a.SendPresence([]Presence{s}, dests)
	}
}

// setState is PresenceHandler.set_state, after the presence is validated.
func (t *tracker) setState(ctx context.Context, userID string, dev devKey, presence string, statusMsg *string, forceNotify, isSync bool) error {
	if !t.track {
		return nil
	}
	now := t.now()
	prev, err := t.currentState(ctx, userID)
	if err != nil {
		return err
	}
	if prev.State == stateBusy && isSync {
		presence = stateBusy
	}
	t.mu.Lock()
	devices := t.devicesOf(userID)
	d, ok := devices[dev]
	if !ok {
		d = &device{state: stateOffline}
		devices[dev] = d
	}
	d.state = presence
	d.lastActiveTS = now
	if isSync {
		d.lastSyncTS = now
	}
	combined := combineDeviceStates(devices)
	t.mu.Unlock()

	ns := prev
	ns.State = combined
	if combined == stateOnline || combined == stateBusy {
		ns.LastActiveTS = now
	}
	if isSync {
		ns.LastUserSyncTS = now
	} else {
		ns.StatusMsg = statusMsg
	}
	return t.updateStates(ctx, []Presence{ns}, forceNotify)
}

// bump is PresenceHandler.bump_presence_active_time.
func (t *tracker) bump(ctx context.Context, userID string, dev devKey) error {
	if !t.track {
		return nil
	}
	now := t.now()
	t.mu.Lock()
	devices := t.devicesOf(userID)
	d, ok := devices[dev]
	if !ok {
		d = &device{state: stateOffline}
		devices[dev] = d
	}
	d.lastActiveTS = now
	if d.state == stateUnavailable {
		d.state = stateOnline
	}
	combined := combineDeviceStates(devices)
	t.mu.Unlock()
	prev, err := t.currentState(ctx, userID)
	if err != nil {
		return err
	}
	prev.LastActiveTS = now
	prev.State = combined
	return t.updateStates(ctx, []Presence{prev}, false)
}

// syncEnd is user_syncing's _end: the sync is over, and last_user_sync_ts
// is now.
func (t *tracker) syncEnd(ctx context.Context, k userDevice) error {
	t.mu.Lock()
	t.syncs[k]--
	t.mu.Unlock()
	prev, err := t.currentState(ctx, k.user)
	if err != nil {
		return err
	}
	prev.LastUserSyncTS = t.now()
	return t.updateStates(ctx, []Presence{prev}, false)
}

// externalRow is update_external_syncs_row (USER_SYNC).
func (t *tracker) externalRow(ctx context.Context, process, userID string, dev devKey, syncing bool, syncTime int64) error {
	prev, err := t.currentState(ctx, userID)
	if err != nil {
		return err
	}
	k := userDevice{userID, dev}
	t.mu.Lock()
	set, ok := t.external[process]
	if !ok {
		set = map[userDevice]struct{}{}
		t.external[process] = set
	}
	_, present := set[k]
	var update bool
	switch {
	case syncing && !present:
		set[k] = struct{}{}
	case !syncing && present:
		devices := t.devicesOf(userID)
		d, ok := devices[dev]
		if !ok {
			d = &device{state: stateOffline}
			devices[dev] = d
		}
		d.lastSyncTS = syncTime
		update = true
	}
	t.mu.Unlock()
	if update {
		prev.LastUserSyncTS = syncTime
		err = t.updateStates(ctx, []Presence{prev}, false)
		t.mu.Lock()
		delete(set, k)
		t.mu.Unlock()
	}
	t.mu.Lock()
	t.externalUpdated[process] = t.now()
	t.gauges()
	t.mu.Unlock()
	return err
}

// externalClear is update_external_syncs_clear (CLEAR_USER_SYNC).
func (t *tracker) externalClear(ctx context.Context, process string) error {
	now := t.now()
	t.mu.Lock()
	set := t.external[process]
	delete(t.external, process)
	users := map[string]struct{}{}
	for k := range set {
		devices := t.devicesOf(k.user)
		d, ok := devices[k.dev]
		if !ok {
			d = &device{state: stateOffline}
			devices[k.dev] = d
		}
		d.lastSyncTS = now
		users[k.user] = struct{}{}
	}
	t.mu.Unlock()
	ids := make([]string, 0, len(users))
	for u := range users {
		ids = append(ids, u)
	}
	var err error
	if len(ids) > 0 {
		var prev map[string]Presence
		if prev, err = t.currentStates(ctx, ids); err == nil {
			states := make([]Presence, 0, len(prev))
			for _, p := range prev {
				p.LastUserSyncTS = now
				states = append(states, p)
			}
			err = t.updateStates(ctx, states, false)
		}
	}
	t.mu.Lock()
	delete(t.externalUpdated, process)
	t.gauges()
	t.mu.Unlock()
	return err
}

// timeouts is _handle_timeouts.
func (t *tracker) timeouts(ctx context.Context) error {
	now := t.now()
	t.mu.Lock()
	check := map[string]struct{}{}
	for _, u := range t.wheel.fetch(now) {
		check[u] = struct{}{}
	}
	for process, last := range t.externalUpdated {
		if now-last > externalProcessExpiry {
			for k := range t.external[process] {
				check[k.user] = struct{}{}
			}
			delete(t.external, process)
			delete(t.externalUpdated, process)
		}
	}
	syncing := map[userDevice]struct{}{}
	for k, n := range t.syncs {
		if n != 0 {
			syncing[k] = struct{}{}
		}
	}
	for _, set := range t.external {
		for k := range set {
			syncing[k] = struct{}{}
		}
	}
	var changes []Presence
	for u := range check {
		s, ok := t.current[u]
		if !ok {
			s = defaultPresence(u)
		}
		devices := t.devices[u]
		if devices == nil {
			devices = map[devKey]*device{}
		}
		if ns, changed := handleTimeout(s, t.isMine(u), syncing, devices, now, t.t); changed {
			changes = append(changes, ns)
		}
	}
	t.mu.Unlock()
	sources.WithLabelValues("timeout").Add(float64(len(check)))
	if len(changes) == 0 {
		return nil
	}
	return t.updateStates(ctx, changes, false)
}

// flush is _persist_unpersisted_changes: states changed without a notify,
// written so a restart finds them.
func (t *tracker) flush(ctx context.Context) error {
	t.mu.Lock()
	rows := make([]State, 0, len(t.unpersisted))
	for u := range t.unpersisted {
		rows = append(rows, toRow(t.current[u]))
	}
	t.unpersisted = map[string]struct{}{}
	t.gauges()
	t.mu.Unlock()
	if len(rows) == 0 {
		return nil
	}
	t.a.Log.Info().Int("states", len(rows)).Msg("Persisting unpersisted presence updates")
	return t.a.persist(ctx, rows)
}

// incoming is incoming_presence: an m.presence EDU from origin. A value
// Synapse would raise on (a push that is not an object, a non-numeric
// last_active_ago) fails the whole EDU, as the exception does; a status
// message or state that is not a string is refused here where Synapse
// would try to store it.
func (t *tracker) incoming(ctx context.Context, origin string, content json.RawMessage) error {
	if !t.track {
		return nil
	}
	now := t.now()
	c := gjson.ParseBytes(content)
	if !c.IsObject() {
		return errors.New("presence EDU content is not an object")
	}
	push := c.Get("push")
	if !push.Exists() {
		return nil
	}
	if !push.IsArray() {
		return errors.New("presence EDU push is not a list")
	}
	var out []Presence
	for _, p := range push.Array() {
		if !p.IsObject() {
			return errors.New("a presence push is not an object")
		}
		p = gjson.Parse(dedupe(p.Raw))
		u := p.Get("user_id")
		if !truthyJSON(u) {
			t.a.Log.Info().Str("origin", origin).Msg("Got presence update with no 'user_id'")
			continue
		}
		userID := u.String()
		if u.Type != gjson.String || userid.Domain(userID) != origin {
			t.a.Log.Info().Str("origin", origin).Str("user_id", u.Raw).Msg("Got presence update with bad 'user_id'")
			continue
		}
		if !userid.Compliant(userID) {
			t.a.Log.Info().Str("origin", origin).Str("user_id", userID).Msg("Dropping presence update for non-compliant user ID")
			continue
		}
		st := p.Get("presence")
		if !truthyJSON(st) {
			t.a.Log.Info().Str("origin", origin).Msg("Got presence update with no 'presence_state'")
			continue
		}
		if st.Type != gjson.String {
			return fmt.Errorf("presence from %s: presence %s is not a string", origin, st.Raw)
		}
		prev, err := t.currentState(ctx, userID)
		if err != nil {
			return err
		}
		prev.State = st.String()
		prev.LastFederationUpdateTS = now
		if la := p.Get("last_active_ago"); la.Exists() && la.Type != gjson.Null {
			if la.Type != gjson.Number {
				return fmt.Errorf("presence from %s: last_active_ago %s", origin, la.Raw)
			}
			prev.LastActiveTS = now - int64(la.Num)
		}
		switch m := p.Get("status_msg"); m.Type {
		case gjson.Null:
			prev.StatusMsg = nil
		case gjson.String:
			s := m.String()
			prev.StatusMsg = &s
		default:
			if m.Exists() {
				return fmt.Errorf("presence from %s: status_msg %s", origin, m.Raw)
			}
			prev.StatusMsg = nil
		}
		prev.CurrentlyActive = truthyJSON(p.Get("currently_active"))
		out = append(out, prev)
	}
	sources.WithLabelValues("edu").Add(float64(len(out)))
	if len(out) == 0 {
		return nil
	}
	return t.updateStates(ctx, out, false)
}

// dedupe is a dict's view of a JSON object: a repeated key is its last.
func dedupe(raw string) string {
	if b, err := pyjson.Dedupe([]byte(raw)); err == nil {
		return string(b)
	}
	return raw
}

// truthyJSON is Python's bool() of a JSON value.
func truthyJSON(v gjson.Result) bool {
	switch v.Type {
	case gjson.String:
		return v.Str != ""
	case gjson.Number:
		return v.Num != 0
	case gjson.True:
		return true
	case gjson.JSON:
		return len(bytes.TrimSpace([]byte(v.Raw))) > 2
	}
	return false
}

// deltas is notify_new_event's _unsafe_process: current state deltas up to
// the events stream's position, for joins.
func (t *tracker) deltas(ctx context.Context) error {
	for {
		maxPos, err := t.a.EventsPosition(ctx)
		if err != nil {
			return err
		}
		t.mu.Lock()
		pos := t.eventPos
		t.mu.Unlock()
		if pos >= maxPos {
			return nil
		}
		upto, ds, err := t.a.Rooms.StateDeltas(ctx, pos, maxPos, 100)
		if err != nil {
			return err
		}
		var order []string
		byRoom := map[string][]StateDelta{}
		for _, d := range ds {
			if _, ok := byRoom[d.RoomID]; !ok {
				order = append(order, d.RoomID)
			}
			byRoom[d.RoomID] = append(byRoom[d.RoomID], d)
		}
		for _, r := range order {
			if err := t.roomDeltas(ctx, r, byRoom[r]); err != nil {
				return err
			}
		}
		t.mu.Lock()
		t.eventPos = upto
		t.mu.Unlock()
		deltaPosition.Set(float64(upto))
	}
}

// roomDeltas is _handle_state_delta: presence for servers new to a room,
// and of users new to it.
func (t *tracker) roomDeltas(ctx context.Context, roomID string, ds []StateDelta) error {
	if _, ok := t.excluded[roomID]; ok {
		return nil
	}
	joined := map[string]struct{}{}
	for _, d := range ds {
		if d.Type != "m.room.member" || d.EventID == nil {
			continue
		}
		m, err := t.a.Rooms.Membership(ctx, *d.EventID)
		if err != nil {
			return err
		}
		if m != "join" {
			continue
		}
		if d.PrevEventID != nil {
			pm, err := t.a.Rooms.Membership(ctx, *d.PrevEventID)
			if err != nil {
				return err
			}
			if pm == "join" {
				continue
			}
		}
		joined[d.StateKey] = struct{}{}
	}
	if len(joined) == 0 {
		return nil
	}
	sources.WithLabelValues("join").Add(float64(len(joined)))
	users, err := t.a.Rooms.UsersInRoom(ctx, roomID)
	if err != nil {
		return err
	}
	var prevLocal []string
	prevRemote := map[string]struct{}{}
	for _, u := range users {
		if _, ok := joined[u]; ok {
			continue
		}
		if t.isMine(u) {
			prevLocal = append(prevLocal, u)
		} else {
			prevRemote[userid.Domain(u)] = struct{}{}
		}
	}
	var newLocal []string
	newRemote := map[string]struct{}{}
	for u := range joined {
		if t.isMine(u) {
			newLocal = append(newLocal, u)
		} else if h := userid.Domain(u); !has(prevRemote, h) {
			newRemote[h] = struct{}{}
		}
	}
	if len(prevLocal) > 0 && len(newRemote) > 0 {
		local, err := t.currentStates(ctx, prevLocal)
		if err != nil {
			return err
		}
		now := t.now()
		var states []Presence
		for _, s := range local {
			if s.State != stateOffline || now-s.LastActiveTS < 7*24*60*60*1000 || s.StatusMsg != nil {
				states = append(states, s)
			}
		}
		t.toDestinations(states, keys(newRemote))
	}
	if len(newLocal) > 0 && (len(prevRemote) > 0 || len(newRemote) > 0) {
		local, err := t.currentStates(ctx, newLocal)
		if err != nil {
			return err
		}
		states := make([]Presence, 0, len(local))
		for _, s := range local {
			states = append(states, s)
		}
		all := keys(prevRemote)
		for h := range newRemote {
			all = append(all, h)
		}
		t.toDestinations(states, all)
	}
	return nil
}

// toDestinations is PresenceFederationQueue.send_presence_to_destinations
// with this process the only federation sender: straight to it.
func (t *tracker) toDestinations(states []Presence, dests []string) {
	if len(states) == 0 || len(dests) == 0 || t.a.SendPresence == nil {
		return
	}
	fanout.WithLabelValues("join").Add(float64(len(states)))
	t.a.SendPresence(states, dests)
}

func has(m map[string]struct{}, k string) bool { _, ok := m[k]; return ok }

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// userSync is USER_SYNC or CLEAR_USER_SYNC from another process, or false
// for any other line.
func (t *tracker) userSync(ctx context.Context, line string) bool {
	name, rest, _ := strings.Cut(line, " ")
	switch name {
	case "USER_SYNC":
		parts := strings.SplitN(rest, " ", 5)
		if len(parts) != 5 || (parts[3] != "start" && parts[3] != "end") {
			t.a.Log.Warn().Str("line", line).Msg("presence: a USER_SYNC that does not parse")
			return true
		}
		ms, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			t.a.Log.Warn().Str("line", line).Msg("presence: a USER_SYNC that does not parse")
			return true
		}
		dev := devKey{id: parts[2], isSet: true}
		if parts[2] == "None" {
			dev = devKey{}
		}
		sources.WithLabelValues("user_sync").Inc()
		t.op.Lock()
		err = t.externalRow(ctx, parts[0], parts[1], dev, parts[3] == "start", ms)
		t.op.Unlock()
		if err != nil {
			failures.WithLabelValues("user_sync").Inc()
			t.a.Log.Error().Err(err).Msg("presence: USER_SYNC")
		}
		return true
	case "CLEAR_USER_SYNC":
		sources.WithLabelValues("clear_user_sync").Inc()
		t.op.Lock()
		err := t.externalClear(ctx, strings.TrimSpace(rest))
		t.op.Unlock()
		if err != nil {
			failures.WithLabelValues("user_sync").Inc()
			t.a.Log.Error().Err(err).Msg("presence: CLEAR_USER_SYNC")
		}
		return true
	}
	return false
}

// run is the enabled writer's background work: the timeouts every 5 s
// after 30 s, the flush every minute, the joins as the events stream moves,
// USER_SYNC from the bus; and the flush at shutdown (_on_shutdown).
func (t *tracker) run(ctx context.Context, lines *bus.Consumer) {
	var wg sync.WaitGroup
	if lines != nil && t.track {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ev, ok := lines.Next(ctx)
				if !ok {
					return
				}
				if ev.Kind == bus.Message {
					t.userSync(ctx, ev.Line)
				}
			}
		}()
	}
	every := func(first, period time.Duration, fn func(context.Context) error, what string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTimer(first)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				t.op.Lock()
				err := fn(ctx)
				t.op.Unlock()
				if err != nil && ctx.Err() == nil {
					failures.WithLabelValues(what).Inc()
					t.a.Log.Error().Err(err).Str("what", what).Msg("presence")
				}
				timer.Reset(period)
			}
		}()
	}
	if t.track {
		every(30*time.Second, 5*time.Second, t.timeouts, "timeouts")
		every(time.Second, time.Second, t.deltas, "deltas")
	}
	every(time.Minute, time.Minute, t.flush, "flush")
	<-ctx.Done()
	wg.Wait()
	// _on_shutdown: what is held and not yet written.
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t.op.Lock()
	t.mu.Lock()
	n := len(t.unpersisted)
	t.mu.Unlock()
	t.a.Worker.Info().Int("states", n).Msg("presence: persisting unpersisted changes before shutting down")
	if err := t.flush(sctx); err != nil {
		failures.WithLabelValues("flush").Inc()
		t.a.Worker.Error().Err(err).Msg("presence: persisting at shutdown")
	}
	t.op.Unlock()
}
