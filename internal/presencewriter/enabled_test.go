package presencewriter

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// The handler-level tests of tests/handlers/test_presence.py
// (PresenceHandlerTestCase, PresenceJoinTestCase), with a fake clock and
// the 5-second timeout loop stepped by hand.

type fakeRooms struct {
	rooms   map[string][]string // user -> rooms
	members map[string][]string // room -> users
	db      map[string]Presence
	deltas  []StateDelta
	member  map[string]string // event -> membership
}

func (f *fakeRooms) PresenceForUsers(_ context.Context, users []string) (map[string]Presence, error) {
	out := map[string]Presence{}
	for _, u := range users {
		if p, ok := f.db[u]; ok {
			out[u] = p
		}
	}
	return out, nil
}
func (f *fakeRooms) RoomsForUser(_ context.Context, u string) ([]string, error) {
	return f.rooms[u], nil
}
func (f *fakeRooms) UsersInRoom(_ context.Context, r string) ([]string, error) {
	return f.members[r], nil
}
func (f *fakeRooms) HostsInRoom(_ context.Context, r string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	for _, u := range f.members[r] {
		if h := u[indexColon(u)+1:]; !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	return out, nil
}
func (f *fakeRooms) StateDeltas(_ context.Context, prev, maxID int64, _ int) (int64, []StateDelta, error) {
	var out []StateDelta
	for _, d := range f.deltas {
		if d.StreamID > prev && d.StreamID <= maxID {
			out = append(out, d)
		}
	}
	return maxID, out, nil
}
func (f *fakeRooms) Membership(_ context.Context, e string) (string, error) { return f.member[e], nil }

func indexColon(s string) int {
	for i := range s {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

type sent struct {
	users []string
	dests []string
}

type harness struct {
	t      *testing.T
	a      *App
	now    time.Time
	st     *fakeStore
	rooms  *fakeRooms
	sent   []sent
	events int64
}

func newHarness(t *testing.T, mutate func(hs *synapsecfg.Config)) *harness {
	t.Helper()
	h := &harness{t: t, now: time.UnixMilli(5_000_000), st: &fakeStore{},
		rooms: &fakeRooms{rooms: map[string][]string{}, members: map[string][]string{}, db: map[string]Presence{}, member: map[string]string{}}}
	hs := disabled("gw")
	hs.ServerName = "server"
	hs.PresenceEnabled, hs.TrackPresence = true, true
	hs.SenderInstances = []string{"gw"}
	hs.IdleTimeout, hs.SyncOnlineTimeout, hs.LastActiveGranularity = 5*time.Minute, 30*time.Second, time.Minute
	if mutate != nil {
		mutate(hs)
	}
	a, err := New(context.Background(), Options{
		Instance: "gw", Synapse: func() *synapsecfg.Config { return hs },
		Auth:  fakeAuth{clientauth.Verdict{Valid: true, UserID: "@test:server", DeviceID: "dev-1"}},
		Store: h.st, Pool: &memDB{seq: 10}, Writer: streamwriter.New(streamwriter.Options{Instance: "gw"}),
		Rooms: h.rooms,
		SendPresence: func(states []Presence, dests []string) {
			s := sent{dests: slices.Sorted(slices.Values(dests))}
			for _, p := range states {
				s.users = append(s.users, p.UserID)
			}
			sort.Strings(s.users)
			h.sent = append(h.sent, s)
		},
		EventsPosition: func(context.Context) (int64, error) { return h.events, nil },
		Now:            func() time.Time { return h.now },
		Log:            zerolog.Nop(), Worker: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.a = a
	return h
}

// advance moves the clock, running the timeouts every 5 s as the looping
// call does.
func (h *harness) advance(d time.Duration) {
	for step := 5 * time.Second; d > 0; d -= step {
		h.now = h.now.Add(min(step, d))
		h.a.tr.op.Lock()
		if err := h.a.tr.timeouts(context.Background()); err != nil {
			h.t.Fatal(err)
		}
		h.a.tr.op.Unlock()
	}
}

func (h *harness) state(u string) Presence {
	h.t.Helper()
	m, err := h.a.tr.currentStates(context.Background(), []string{u})
	if err != nil {
		h.t.Fatal(err)
	}
	return m[u]
}

func (h *harness) set(u, dev, presence string, msg *string) {
	h.t.Helper()
	h.a.tr.op.Lock()
	defer h.a.tr.op.Unlock()
	if err := h.a.tr.setState(context.Background(), u, devOf(dev), presence, msg, false, false); err != nil {
		h.t.Fatal(err)
	}
}

func ptrStr(s string) *string { return &s }

const me = "@test:server"

func TestUserGoesOfflineByTimeoutStatusMsgRemains(t *testing.T) {
	h := newHarness(t, nil)
	h.set(me, "dev-1", stateOnline, ptrStr("I'm here!"))
	h.advance(15 * time.Second)
	if s := h.state(me); s.State != stateOnline || *s.StatusMsg != "I'm here!" {
		t.Fatalf("%+v", s)
	}
	// Synapse's test sees offline at 45 s because its reactor fires the
	// 5-second loop once per jump, after its 30-second delay. Stepped every
	// 5 s, the sync timer (last_user_sync_ts 0 + 30 s) fires on the first
	// pass with the device still active and is not set again; the next
	// look is the last-active timer at 60 s. Synapse does the same live.
	h.advance(50 * time.Second)
	if s := h.state(me); s.State != stateOffline || s.StatusMsg == nil || *s.StatusMsg != "I'm here!" {
		t.Fatalf("%+v", s)
	}
	// Persisted twice: online, then offline.
	if len(h.st.written) != 2 || h.st.written[1].State != stateOffline {
		t.Fatalf("rows %+v", h.st.written)
	}
}

func TestSetStateStatusMsg(t *testing.T) {
	h := newHarness(t, nil)
	h.set(me, "dev-1", stateOnline, ptrStr("I'm here!"))
	h.set(me, "dev-1", stateOffline, nil)
	if s := h.state(me); s.State != stateOffline || s.StatusMsg != nil {
		t.Fatalf("offline with no status: %+v", s)
	}
	h.set(me, "dev-1", stateOnline, ptrStr("x"))
	h.set(me, "dev-1", stateOnline, nil)
	if s := h.state(me); s.State != stateOnline || s.StatusMsg != nil {
		t.Fatalf("reset online: %+v", s)
	}
}

func TestSyncingKeepsStatusAndBusy(t *testing.T) {
	h := newHarness(t, nil)
	v := clientauth.Verdict{UserID: me, DeviceID: "dev-1"}
	h.set(me, "dev-1", stateUnavailable, ptrStr("I'm here!"))
	end := h.a.UserSyncing(context.Background(), v, stateOffline, true) // affect_presence false
	end()
	if s := h.state(me); s.State != stateUnavailable {
		t.Fatalf("not set: %+v", s)
	}
	h.a.UserSyncing(context.Background(), v, stateOnline, true)
	if s := h.state(me); s.State != stateOnline || *s.StatusMsg != "I'm here!" {
		t.Fatalf("keeps status: %+v", s)
	}

	h = newHarness(t, nil)
	h.set(me, "dev-1", stateBusy, ptrStr("I'm busy!"))
	h.a.UserSyncing(context.Background(), v, stateOnline, true)
	if s := h.state(me); s.State != stateBusy {
		t.Fatalf("keeps busy: %+v", s)
	}
	h.advance(5 * time.Minute / 1000)
	h.advance(5 * time.Second)
	if s := h.state(me); s.State != stateBusy {
		t.Fatalf("busy after timeouts: %+v", s)
	}
	h.set(me, "dev-1", stateOnline, ptrStr("I'm busy!"))
	if s := h.state(me); s.State != stateOnline {
		t.Fatalf("off busy: %+v", s)
	}
}

func TestOngoingSyncStaysOnline(t *testing.T) {
	h := newHarness(t, nil)
	v := clientauth.Verdict{UserID: me, DeviceID: "dev-1"}
	end := h.a.UserSyncing(context.Background(), v, stateOnline, false)
	h.advance(2 * time.Minute)
	if s := h.state(me); s.State != stateOnline && s.State != stateUnavailable {
		t.Fatalf("a device in a sync went %s", s.State)
	}
	// The end runs in the background, as run_in_background.
	h.a.tr.op.Lock()
	if err := h.a.tr.syncEnd(context.Background(), userDevice{me, devOf("dev-1")}); err != nil {
		t.Fatal(err)
	}
	h.a.tr.op.Unlock()
	_ = end
	h.advance(40 * time.Second)
	if s := h.state(me); s.State != stateOffline {
		t.Fatalf("after the sync ended: %s", s.State)
	}
}

func TestExternalProcessTimeout(t *testing.T) {
	h := newHarness(t, nil)
	// A worker's sync: set_state over replication, then USER_SYNC start.
	h.a.tr.op.Lock()
	if err := h.a.tr.setState(context.Background(), me, devOf("dev-1"), stateOnline, nil, false, true); err != nil {
		t.Fatal(err)
	}
	h.a.tr.op.Unlock()
	if !h.a.tr.userSync(context.Background(), "USER_SYNC worker1 "+me+" dev-1 start 5000000") {
		t.Fatal("not a USER_SYNC")
	}
	h.advance(time.Duration(externalProcessExpiry/2) * time.Millisecond)
	if s := h.state(me); s.State == stateOffline {
		t.Fatalf("timed out while the process syncs: %+v", s)
	}
	h.advance(time.Duration(externalProcessExpiry) * time.Millisecond)
	if s := h.state(me); s.State != stateOffline {
		t.Fatalf("not timed out after the process expired: %+v", s)
	}
}

func TestUserSyncEndAndClear(t *testing.T) {
	h := newHarness(t, nil)
	h.a.tr.op.Lock()
	_ = h.a.tr.setState(context.Background(), me, devOf("dev-1"), stateOnline, nil, false, true)
	h.a.tr.op.Unlock()
	h.a.tr.userSync(context.Background(), "USER_SYNC w "+me+" None start 5000000")
	h.a.tr.userSync(context.Background(), "USER_SYNC w2 "+me+" dev-1 start 5000000")
	h.advance(time.Minute)
	if s := h.state(me); s.State == stateOffline {
		t.Fatal("offline while syncing elsewhere")
	}
	h.a.tr.userSync(context.Background(), "CLEAR_USER_SYNC w2")
	h.a.tr.userSync(context.Background(), "USER_SYNC w "+me+" None end 5060000")
	h.advance(time.Minute)
	if s := h.state(me); s.State != stateOffline {
		t.Fatalf("still %s", s.State)
	}
	if h.a.tr.userSync(context.Background(), "POSITION events x 1 2") {
		t.Fatal("POSITION taken for a USER_SYNC")
	}
}

func TestUntrackedDoesNotIdle(t *testing.T) {
	h := newHarness(t, func(hs *synapsecfg.Config) { hs.TrackPresence = false })
	p := h.state(me)
	p.State = stateOnline
	h.a.tr.op.Lock()
	_ = h.a.tr.updateStates(context.Background(), []Presence{p}, false)
	h.a.tr.op.Unlock()
	h.advance(30 * time.Second)
	if s := h.state(me); s.State != stateOnline {
		t.Fatalf("%+v", s)
	}
}

func TestIncomingEDU(t *testing.T) {
	h := newHarness(t, nil)
	edu := `{"push":[
		{"user_id":"@a:remote","presence":"online","last_active_ago":1000,"status_msg":"hi","currently_active":true},
		{"user_id":"@b:elsewhere","presence":"online"},
		{"user_id":"@d:remote"},
		{"presence":"online"}]}`
	if err := h.a.ReceiveEDU(context.Background(), "remote", json.RawMessage(edu)); err != nil {
		t.Fatal(err)
	}
	s := h.state("@a:remote")
	if s.State != stateOnline || s.LastActiveTS != h.now.UnixMilli()-1000 || *s.StatusMsg != "hi" || !s.CurrentlyActive {
		t.Fatalf("%+v", s)
	}
	for _, u := range []string{"@b:elsewhere", "@d:remote"} {
		if h.state(u).State != stateOffline {
			t.Errorf("%s taken", u)
		}
	}
	if len(h.st.written) != 1 || len(h.sent) != 0 {
		t.Fatalf("rows %d, sent %d (a remote user's presence is not sent on)", len(h.st.written), len(h.sent))
	}
	h.advance(31 * time.Minute)
	if h.state("@a:remote").State != stateOffline {
		t.Fatal("remote user not timed out")
	}
	if err := h.a.ReceiveEDU(context.Background(), "remote", json.RawMessage(`{"push":[{"user_id":"@a:remote","presence":"online","last_active_ago":"x"}]}`)); err == nil {
		t.Fatal("a non-numeric last_active_ago taken")
	}
}

func TestFanoutAndExcludedRooms(t *testing.T) {
	h := newHarness(t, func(hs *synapsecfg.Config) { hs.ExcludeRoomsFromPresence = []string{"!big:server"} })
	h.rooms.rooms[me] = []string{"!r:server", "!big:server"}
	h.rooms.members["!r:server"] = []string{me, "@x:one", "@y:two"}
	h.rooms.members["!big:server"] = []string{me, "@z:three"}
	h.set(me, "dev-1", stateOnline, nil)
	if len(h.sent) != 1 || !slices.Equal(h.sent[0].dests, []string{"one", "server", "two"}) {
		t.Fatalf("%+v", h.sent)
	}
	h.rooms.rooms["@x:one"] = []string{"!r:server"}
	h.rooms.rooms["@z:three"] = []string{"!big:server"}
	if ok, _ := h.a.isVisible(context.Background(), me, "@x:one"); !ok {
		t.Error("shared room not visible")
	}
	if ok, _ := h.a.isVisible(context.Background(), me, "@z:three"); ok {
		t.Error("excluded room visible")
	}
}

func TestJoins(t *testing.T) {
	h := newHarness(t, nil)
	h.set(me, "dev-1", stateOnline, nil)
	h.sent = nil
	ev := func(s string) *string { return &s }
	// test_remote_joins: a remote user joins a room we are in.
	h.rooms.members["!r:server"] = []string{me, "@remote:far"}
	h.rooms.deltas = []StateDelta{{StreamID: 5, RoomID: "!r:server", Type: "m.room.member", StateKey: "@remote:far", EventID: ev("$j")}}
	h.rooms.member["$j"] = "join"
	h.events = 5
	h.a.tr.op.Lock()
	if err := h.a.tr.deltas(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.a.tr.op.Unlock()
	if len(h.sent) != 1 || !slices.Equal(h.sent[0].users, []string{me}) || !slices.Equal(h.sent[0].dests, []string{"far"}) {
		t.Fatalf("%+v", h.sent)
	}
	// A second user from that server: nothing new to tell it.
	h.sent = nil
	h.rooms.members["!r:server"] = []string{me, "@remote:far", "@other:far"}
	h.rooms.deltas = append(h.rooms.deltas, StateDelta{StreamID: 6, RoomID: "!r:server", Type: "m.room.member", StateKey: "@other:far", EventID: ev("$k")})
	h.rooms.member["$k"] = "join"
	h.events = 6
	h.a.tr.op.Lock()
	_ = h.a.tr.deltas(context.Background())
	h.a.tr.op.Unlock()
	if len(h.sent) != 0 {
		t.Fatalf("%+v", h.sent)
	}
	// A profile change (join to join) is not a join.
	h.rooms.deltas = append(h.rooms.deltas, StateDelta{StreamID: 7, RoomID: "!r:server", Type: "m.room.member", StateKey: "@new:server", EventID: ev("$p"), PrevEventID: ev("$q")})
	h.rooms.member["$p"], h.rooms.member["$q"] = "join", "join"
	h.events = 7
	h.a.tr.op.Lock()
	_ = h.a.tr.deltas(context.Background())
	h.a.tr.op.Unlock()
	if len(h.sent) != 0 {
		t.Fatalf("%+v", h.sent)
	}
	// test_remote_gets_presence_when_local_user_joins.
	h.rooms.members["!r:server"] = []string{me, "@remote:far", "@other:far", "@new:server"}
	h.rooms.deltas = append(h.rooms.deltas, StateDelta{StreamID: 8, RoomID: "!r:server", Type: "m.room.member", StateKey: "@new:server", EventID: ev("$n"), PrevEventID: ev("$leave")})
	h.rooms.member["$n"], h.rooms.member["$leave"] = "join", "leave"
	h.events = 8
	h.a.tr.op.Lock()
	_ = h.a.tr.deltas(context.Background())
	h.a.tr.op.Unlock()
	if len(h.sent) != 1 || !slices.Equal(h.sent[0].users, []string{"@new:server"}) || !slices.Equal(h.sent[0].dests, []string{"far"}) {
		t.Fatalf("%+v", h.sent)
	}
}

func TestFlushAndRestore(t *testing.T) {
	h := newHarness(t, nil)
	v := clientauth.Verdict{UserID: me, DeviceID: "dev-1"}
	h.a.UserSyncing(context.Background(), v, stateOnline, false)
	n := len(h.st.written)
	h.now = h.now.Add(10 * time.Second)
	h.a.tr.op.Lock()
	_ = h.a.tr.syncEnd(context.Background(), userDevice{me, devOf("dev-1")})
	if len(h.st.written) != n {
		h.a.tr.op.Unlock()
		t.Fatal("a quiet change was persisted at once")
	}
	if err := h.a.tr.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.a.tr.op.Unlock()
	if len(h.st.written) != n+1 || *h.st.written[n].LastUserSyncTS != h.now.UnixMilli() {
		t.Fatalf("flush %+v", h.st.written)
	}
	// A restart restores the states not offline as pseudo-devices, which
	// time out like any device.
	h2 := newHarness(t, nil)
	h2.a.tr.start([]State{toRow(h.state(me))})
	if s := h2.state(me); s.State != stateOnline {
		t.Fatalf("restored %+v", s)
	}
	h2.now = h.now
	h2.advance(time.Minute)
	if s := h2.state(me); s.State != stateOffline {
		t.Fatalf("restored state did not time out: %+v", s)
	}
}
