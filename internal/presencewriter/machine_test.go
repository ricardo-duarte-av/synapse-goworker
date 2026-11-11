package presencewriter

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// Synapse's tests/handlers/test_presence.py (v1.165.0), PresenceUpdateTestCase
// and PresenceTimeoutTestCase, with the default timers.

var defaults = timers{idle: 5 * 60 * 1000, syncOnline: 30 * 1000, lastActiveGranularity: 60 * 1000}

type recWheel struct{ calls []string }

func (r *recWheel) insert(now int64, obj string, then int64) {
	r.calls = append(r.calls, fmt.Sprintf("%d %s %d", now, obj, then))
}

func (r *recWheel) sorted() []string { c := slices.Clone(r.calls); slices.Sort(c); return c }

func want(now int64, user string, thens ...int64) []string {
	var out []string
	for _, t := range thens {
		out = append(out, fmt.Sprintf("%d %s %d", now, user, t))
	}
	slices.Sort(out)
	return out
}

func TestHandleUpdate(t *testing.T) {
	const u, now = "@foo:bar", int64(5000000)
	online := func(lastActive int64, active bool) Presence {
		p := defaultPresence(u)
		p.State, p.LastActiveTS, p.CurrentlyActive = stateOnline, lastActive, active
		return p
	}
	g := defaults.lastActiveGranularity
	cases := []struct {
		name               string
		prev, new          Presence
		mine               bool
		notify, ping, curr bool
		fedTS              int64
		timers             []string
	}{
		{"offline_to_online", defaultPresence(u), func() Presence { p := defaultPresence(u); p.State, p.LastActiveTS = stateOnline, now; return p }(),
			true, true, true, true, now, want(now, u, now+defaults.idle, defaults.syncOnline, now+g)},
		{"online_to_online", online(now, true), online(now, true), true, false, true, true, now,
			want(now, u, now+defaults.idle, defaults.syncOnline, now+g)},
		{"online_to_online_last_active_noop", online(now-g-10, true), online(now, true), true, false, true, true, now,
			want(now, u, now+defaults.idle, defaults.syncOnline, now+g)},
		{"online_to_online_last_active", online(now-g-1, true), online(now-g-1, true), true, true, true, false, now,
			want(now, u, now-g-1+defaults.idle, defaults.syncOnline)},
		{"remote_ping_timer", online(now, false), online(now, false), false, false, false, false, 0,
			want(now, u, federationTimeout)},
		{"online_to_offline", online(now, true), func() Presence { p := online(now, true); p.State = stateOffline; return p }(),
			true, true, false, true, now, nil},
		{"online_to_idle", online(now, true), func() Presence { p := online(now, true); p.State = stateUnavailable; return p }(),
			true, true, true, true, now, want(now, u, defaults.syncOnline)},
	}
	for _, c := range cases {
		w := &recWheel{}
		got, notify, ping := handleUpdate(c.prev, c.new, c.mine, w, now, false, defaults)
		if notify != c.notify || ping != c.ping {
			t.Errorf("%s: notify %v ping %v", c.name, notify, ping)
		}
		if c.name != "online_to_offline" && got.CurrentlyActive != c.curr {
			t.Errorf("%s: currently_active %v", c.name, got.CurrentlyActive)
		}
		if got.State != c.new.State || !sameMsg(got.StatusMsg, c.new.StatusMsg) {
			t.Errorf("%s: state %+v", c.name, got)
		}
		if c.mine && got.LastFederationUpdateTS != c.fedTS {
			t.Errorf("%s: last_federation_update_ts %d", c.name, got.LastFederationUpdateTS)
		}
		if !slices.Equal(w.sorted(), c.timers) {
			t.Errorf("%s: timers\n%s\nwant\n%s", c.name, strings.Join(w.sorted(), "\n"), strings.Join(c.timers, "\n"))
		}
	}
	// test_override: persist (untracked) sets no timers, whatever the change.
	states := []string{stateBusy, stateOnline, stateUnavailable, stateOffline}
	for _, a := range states {
		for _, b := range states {
			if a == b {
				continue
			}
			w := &recWheel{}
			prev := online(now, true)
			prev.State = a
			nw := prev
			nw.State = b
			handleUpdate(prev, nw, true, w, now, true, defaults)
			if len(w.calls) != 0 {
				t.Errorf("override %s->%s: timers %v", a, b, w.calls)
			}
		}
	}
}

func TestHandleTimeout(t *testing.T) {
	const u, dev, now = "@foo:bar", "dev-1", int64(5000000)
	msg := "I'm here!"
	mk := func(state string, lastActive, lastSync, lastFed int64) Presence {
		p := defaultPresence(u)
		p.State, p.LastActiveTS, p.LastUserSyncTS, p.LastFederationUpdateTS, p.StatusMsg = state, lastActive, lastSync, lastFed, &msg
		return p
	}
	devs := func(p Presence) map[devKey]*device {
		return map[devKey]*device{{id: dev, isSet: true}: {state: p.State, lastActiveTS: p.LastActiveTS, lastSyncTS: p.LastUserSyncTS}}
	}
	syncingDev := map[userDevice]struct{}{{u, devKey{id: dev, isSet: true}}: {}}
	cases := []struct {
		name    string
		state   Presence
		mine    bool
		syncing map[userDevice]struct{}
		changed bool
		want    string // the new state, or "same" for the old one unchanged
	}{
		{"idle_timer", mk(stateOnline, now-defaults.idle-1, now, 0), true, nil, true, stateUnavailable},
		{"busy_no_idle", mk(stateBusy, now-defaults.idle-1, now, 0), true, nil, true, stateBusy},
		{"sync_timeout", mk(stateOnline, 0, now-defaults.syncOnline-1, 0), true, nil, true, stateOffline},
		{"sync_online", mk(stateOnline, now-defaults.syncOnline-1, now-defaults.syncOnline-1, 0), true, syncingDev, true, stateOnline},
		{"federation_ping", mk(stateOnline, now, now, now-federationPingInterval-1), true, nil, true, "same"},
		{"no_timeout", func() Presence { p := mk(stateOnline, now, now, now); p.StatusMsg = nil; return p }(), true, nil, false, ""},
		{"federation_timeout", mk(stateOnline, now, now, now-federationTimeout-1), false, nil, true, stateOffline},
		{"last_active", mk(stateOnline, now-defaults.lastActiveGranularity-1, now, now), true, nil, true, "same"},
	}
	for _, c := range cases {
		d := devs(c.state)
		if !c.mine {
			d = map[devKey]*device{}
		}
		got, changed := handleTimeout(c.state, c.mine, c.syncing, d, now, defaults)
		if changed != c.changed {
			t.Errorf("%s: changed %v", c.name, changed)
			continue
		}
		if !changed {
			continue
		}
		if c.want == "same" {
			if !got.equal(c.state) {
				t.Errorf("%s: %+v", c.name, got)
			}
			continue
		}
		if got.State != c.want || !sameMsg(got.StatusMsg, c.state.StatusMsg) {
			t.Errorf("%s: %s %v", c.name, got.State, got.StatusMsg)
		}
	}
}

func TestCombineAndFormat(t *testing.T) {
	d := map[devKey]*device{
		{id: "a", isSet: true}: {state: stateUnavailable, lastActiveTS: 50},
		{id: "b", isSet: true}: {state: stateOnline, lastActiveTS: 10},
		{}:                     {state: stateOffline, lastActiveTS: 99},
	}
	if got := combineDeviceStates(d); got != stateOnline {
		t.Errorf("combine %s", got)
	}
	if got := combineDeviceStates(map[devKey]*device{}); got != stateOffline {
		t.Errorf("combine empty %s", got)
	}
	msg := "hé"
	p := Presence{UserID: "@a:b", State: stateOnline, LastActiveTS: 1000, StatusMsg: &msg, CurrentlyActive: true}
	if got := string(formatPresence(p, 3000, true).encode()); got != `{"presence":"online","user_id":"@a:b","last_active_ago":2000,"status_msg":"h`+"\\u00e9"+`","currently_active":true}` {
		t.Errorf("format %s", got)
	}
	empty := ""
	p = Presence{UserID: "@a:b", State: stateOffline, StatusMsg: &empty}
	if got := string(formatPresence(p, 3000, false).encode()); got != `{"presence":"offline"}` {
		t.Errorf("format %s", got)
	}
}

func TestWheelTimer(t *testing.T) {
	// tests/util/test_wheel_timer.py
	w := &wheelTimer{}
	w.insert(100, "1", 150)
	if got := w.fetch(101); len(got) != 0 {
		t.Fatal(got)
	}
	if got := w.fetch(110); len(got) != 0 {
		t.Fatal(got)
	}
	if got := w.fetch(120); len(got) != 0 {
		t.Fatal(got)
	}
	if got := w.fetch(5000); !slices.Equal(got, []string{"1"}) {
		t.Fatal(got)
	}
	// Times in the past come back on the next fetch past now's bucket.
	w.insert(10000, "a", 1)
	w.insert(10000, "b", 20000)
	if got := w.fetch(14999); len(got) != 0 {
		t.Fatal(got)
	}
	if got := w.fetch(15000); !slices.Equal(got, []string{"a"}) {
		t.Fatal(got)
	}
	if w.len() != 1 {
		t.Fatal(w.len())
	}
}
