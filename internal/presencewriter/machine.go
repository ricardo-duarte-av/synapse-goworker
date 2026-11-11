package presencewriter

import "math"

// The presence state machine of handlers/presence.py (v1.165.0), as pure
// functions: handle_update, handle_timeout(s), should_notify,
// _combine_device_states and format_user_presence_state, with the wheel
// timer of util/wheel_timer.py. Times are milliseconds, as Synapse's.

// Presence states (api/constants.py PresenceState).
const (
	stateOnline      = "online"
	stateUnavailable = "unavailable"
	stateOffline     = "offline"
	stateBusy        = "busy"
)

// Synapse's constants (handlers/presence.py).
const (
	// busyOnlineTimeout is how long a busy device that stopped syncing
	// stays busy before it goes offline.
	busyOnlineTimeout int64 = 60 * 60 * 1000
	// federationTimeout is how long a remote user stays as their server
	// last said without hearing from it again.
	federationTimeout int64 = 30 * 60 * 1000
	// federationPingInterval is how often our users' presence is resent to
	// other servers.
	federationPingInterval int64 = 25 * 60 * 1000
	// externalProcessExpiry is how long another process's syncs count
	// without a USER_SYNC from it.
	externalProcessExpiry int64 = 5 * 60 * 1000
)

// Presence is UserPresenceState. StatusMsg nil is None.
type Presence struct {
	UserID                 string
	State                  string
	LastActiveTS           int64
	LastFederationUpdateTS int64
	LastUserSyncTS         int64
	StatusMsg              *string
	CurrentlyActive        bool
}

// defaultPresence is UserPresenceState.default.
func defaultPresence(userID string) Presence {
	return Presence{UserID: userID, State: stateOffline}
}

// equal is attrs' __eq__: every field.
func (p Presence) equal(o Presence) bool {
	return p.UserID == o.UserID && p.State == o.State && p.LastActiveTS == o.LastActiveTS &&
		p.LastFederationUpdateTS == o.LastFederationUpdateTS && p.LastUserSyncTS == o.LastUserSyncTS &&
		sameMsg(p.StatusMsg, o.StatusMsg) && p.CurrentlyActive == o.CurrentlyActive
}

func sameMsg(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// devKey is a device ID or None: the pseudo-device of a state restored at
// start, and the requests of an appservice without a device.
type devKey struct {
	id    string
	isSet bool
}

func deviceKey(id *string) devKey {
	if id == nil {
		return devKey{}
	}
	return devKey{id: *id, isSet: true}
}

// device is UserDevicePresenceState.
type device struct {
	state        string
	lastActiveTS int64
	lastSyncTS   int64
}

// userDevice is a (user ID, device ID) pair, as the syncing sets hold it.
type userDevice struct {
	user string
	dev  devKey
}

// timers are the configurable timers, in ms.
type timers struct {
	idle, syncOnline, lastActiveGranularity int64
}

// presenceByPriority is PRESENCE_BY_PRIORITY. An unknown state (only a
// remote one can be) is never a device's, so it never meets this table.
var presenceByPriority = map[string]int{stateBusy: 4, stateOnline: 3, stateUnavailable: 2, stateOffline: 1}

// combineDeviceStates is _combine_device_states: the state of the device of
// highest priority, ties broken by the most recently active.
func combineDeviceStates(devices map[devKey]*device) string {
	presence := stateOffline
	var lastActive int64 = -1
	for _, d := range devices {
		p, cur := presenceByPriority[d.state], presenceByPriority[presence]
		if p > cur || (p == cur && d.lastActiveTS > lastActive) {
			presence, lastActive = d.state, d.lastActiveTS
		}
	}
	return presence
}

// notifyReason is should_notify: whether a change is worth telling anyone
// of, and its notify_reason label ("" for no).
func notifyReason(old, new Presence, lastActiveGranularity int64) string {
	switch {
	case old.equal(new):
		return ""
	case !sameMsg(old.StatusMsg, new.StatusMsg):
		return "status_msg_change"
	case old.State != new.State:
		return "state_change"
	case old.State == stateOnline:
		if new.CurrentlyActive != old.CurrentlyActive {
			return "current_active_change"
		}
		if new.LastActiveTS-old.LastActiveTS > lastActiveGranularity && !new.CurrentlyActive {
			return "last_active_change_online"
		}
	case new.LastActiveTS-old.LastActiveTS > lastActiveGranularity:
		return "last_active_change_not_online"
	}
	return ""
}

// wheel is the wheel timer's interface, as handle_update uses it.
type wheelInserter interface {
	insert(now int64, obj string, then int64)
}

// handleUpdate is handle_update: the timers a new state sets, and whether
// it is persisted and notified, or only pinged to other servers.
func handleUpdate(prev, new Presence, isMine bool, w wheelInserter, now int64, persist bool, t timers) (Presence, bool, bool) {
	userID := new.UserID
	notify, ping := false, false
	if isMine {
		if new.State == stateOnline {
			if !persist {
				w.insert(now, userID, new.LastActiveTS+t.idle)
			}
			active := now-new.LastActiveTS < t.lastActiveGranularity
			new.CurrentlyActive = active
			if active && !persist {
				w.insert(now, userID, new.LastActiveTS+t.lastActiveGranularity)
			}
		}
		if new.State != stateOffline {
			if !persist {
				w.insert(now, userID, new.LastUserSyncTS+t.syncOnline)
			}
			if now-new.LastFederationUpdateTS > federationPingInterval {
				new.LastFederationUpdateTS = now
				ping = true
			}
		}
		if new.State == stateBusy && !persist {
			w.insert(now, userID, new.LastUserSyncTS+busyOnlineTimeout)
		}
	} else if !persist {
		w.insert(now, userID, new.LastFederationUpdateTS+federationTimeout)
	}
	if reason := notifyReason(prev, new, t.lastActiveGranularity); reason != "" {
		notifyReasons.WithLabelValues(locality(isMine), reason).Inc()
		if reason == "state_change" {
			transitions.WithLabelValues(locality(isMine), prev.State, new.State).Inc()
		}
		new.LastFederationUpdateTS = now
		notify = true
	}
	return new, notify, ping
}

func locality(isMine bool) string {
	if isMine {
		return "local"
	}
	return "remote"
}

// handleTimeout is handle_timeout: a user's state once their timers may
// have fired, or false for no change. devices is changed in place, as
// Synapse changes the user's device states.
func handleTimeout(state Presence, isMine bool, syncing map[userDevice]struct{}, devices map[devKey]*device, now int64, t timers) (Presence, bool) {
	if state.State == stateOffline {
		return state, false
	}
	changed := false
	if isMine {
		deviceChanged := false
		var offline []devKey
		for id, d := range devices {
			if d.state == stateOnline && now-d.lastActiveTS > t.idle {
				d.state = stateUnavailable
				deviceChanged = true
			}
			if _, ok := syncing[userDevice{state.UserID, id}]; !ok {
				syncOrActive := max(d.lastSyncTS, d.lastActiveTS)
				timeout := t.syncOnline
				if d.state == stateBusy {
					timeout = busyOnlineTimeout
				}
				if now-syncOrActive > timeout {
					offline = append(offline, id)
					deviceChanged = true
				}
			}
		}
		for _, id := range offline {
			delete(devices, id)
		}
		if deviceChanged {
			if p := combineDeviceStates(devices); p != state.State {
				state.State = p
				changed = true
			}
		}
		if now-state.LastActiveTS > t.lastActiveGranularity {
			changed = true
		}
		if now-state.LastFederationUpdateTS > federationPingInterval {
			changed = true
		}
	} else if now-state.LastFederationUpdateTS > federationTimeout {
		state.State = stateOffline
		changed = true
	}
	return state, changed
}

// formatPresence is format_user_presence_state, for the client API and
// federation (includeUserID). Its keys are in Synapse's order.
func formatPresence(s Presence, now int64, includeUserID bool) orderedJSON {
	out := orderedJSON{{"presence", s.State}}
	if includeUserID {
		out = append(out, kv{"user_id", s.UserID})
	}
	if s.LastActiveTS != 0 {
		out = append(out, kv{"last_active_ago", now - s.LastActiveTS})
	}
	if s.StatusMsg != nil && *s.StatusMsg != "" {
		out = append(out, kv{"status_msg", *s.StatusMsg})
	}
	if s.State == stateOnline {
		out = append(out, kv{"currently_active", s.CurrentlyActive})
	}
	return out
}

// wheelTimer is util/wheel_timer.py's WheelTimer with 5-second buckets:
// objects come back from fetch once the bucket after their time has passed.
type wheelTimer struct {
	entries []wheelEntry
}

type wheelEntry struct {
	endKey   int64
	elements map[string]struct{}
}

const wheelBucket = 5000

// wheelKey is int(t / bucket): Python's int() truncates towards zero.
func wheelKey(t int64) int64 { return int64(math.Trunc(float64(t) / wheelBucket)) }

func (w *wheelTimer) insert(now int64, obj string, then int64) {
	thenKey, nowKey := wheelKey(then)+1, wheelKey(now)
	if len(w.entries) > 0 {
		minKey, maxKey := w.entries[0].endKey, w.entries[len(w.entries)-1].endKey
		if thenKey <= maxKey {
			// The max protects against inserts for times in the past.
			w.entries[max(minKey, thenKey)-minKey].elements[obj] = struct{}{}
			return
		}
	}
	lastKey := nowKey + 1
	if len(w.entries) > 0 {
		lastKey = w.entries[len(w.entries)-1].endKey + 1
	}
	thenKey = max(lastKey, thenKey)
	for k := lastKey; k <= thenKey; k++ {
		w.entries = append(w.entries, wheelEntry{endKey: k, elements: map[string]struct{}{}})
	}
	w.entries[len(w.entries)-1].elements[obj] = struct{}{}
}

func (w *wheelTimer) fetch(now int64) []string {
	nowKey := wheelKey(now)
	var out []string
	for len(w.entries) > 0 && w.entries[0].endKey <= nowKey {
		for o := range w.entries[0].elements {
			out = append(out, o)
		}
		w.entries[0] = wheelEntry{}
		w.entries = w.entries[1:]
	}
	return out
}

func (w *wheelTimer) len() int {
	n := 0
	for _, e := range w.entries {
		n += len(e.elements)
	}
	return n
}
