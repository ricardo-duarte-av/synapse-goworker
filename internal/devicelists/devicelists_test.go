package devicelists

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

type poke struct {
	id           int64
	user, device string
	hosts        []string
}

type fakeStore struct {
	mu        sync.Mutex
	rooms     map[string][]string            // user -> rooms
	hosts     map[string]map[string]struct{} // room -> hosts
	changes   []Change                       // device_lists_changes_in_room, unconverted only
	converted map[string]bool
	pokes     []poke
	pos       struct {
		stream int64
		room   string
	}
	stream []string
}

func (f *fakeStore) RoomsForUser(_ context.Context, u string) ([]string, error) {
	return f.rooms[u], nil
}
func (f *fakeStore) AddDeviceChange(_ context.Context, _ string, ids []int64, u string, devs, rooms []string, mine bool, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, d := range devs {
		f.stream = append(f.stream, fmt.Sprintf("%d %s %s", ids[i], u, d))
		for _, r := range rooms {
			if mine {
				f.changes = append(f.changes, Change{UserID: u, DeviceID: d, RoomID: r, StreamID: ids[i]})
			}
		}
	}
	return nil
}
func (f *fakeStore) AddUserSignatureChange(context.Context, string, int64, string, string) error {
	return nil
}
func (f *fakeStore) Unconverted(_ context.Context, s int64, r string, upto int64, limit int) ([]Change, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Change
	sort.Slice(f.changes, func(i, j int) bool {
		if f.changes[i].StreamID != f.changes[j].StreamID {
			return f.changes[i].StreamID < f.changes[j].StreamID
		}
		return f.changes[i].RoomID < f.changes[j].RoomID
	})
	for _, c := range f.changes {
		key := fmt.Sprintf("%d|%s|%s|%s", c.StreamID, c.RoomID, c.UserID, c.DeviceID)
		if f.converted[key] || c.StreamID > upto {
			continue
		}
		if c.StreamID > s || (c.StreamID == s && c.RoomID > r) {
			out = append(out, c)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func (f *fakeStore) AddOutboundPokes(_ context.Context, _ string, id int64, u, d string, hosts []string, _ bool, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pokes = append(f.pokes, poke{id, u, d, append([]string(nil), hosts...)})
	return nil
}
func (f *fakeStore) MarkRedundant(_ context.Context, u, d, r string, upto int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.changes {
		if c.StreamID > upto && c.UserID == u && c.DeviceID == d && c.RoomID == r {
			f.converted[fmt.Sprintf("%d|%s|%s|%s", c.StreamID, c.RoomID, c.UserID, c.DeviceID)] = true
		}
	}
	return nil
}
func (f *fakeStore) ConvertedPos(context.Context) (int64, string, error) {
	return f.pos.stream, f.pos.room, nil
}
func (f *fakeStore) SetConvertedPos(_ context.Context, _ string, s int64, r string) error {
	f.pos.stream, f.pos.room = s, r
	return nil
}
func (f *fakeStore) ConversionLag(context.Context) (*int64, int64, error) {
	return nil, f.pos.stream, nil
}
func (f *fakeStore) Hosts(_ context.Context, r string) (map[string]struct{}, error) {
	return f.hosts[r], nil
}
func (f *fakeStore) Updates(context.Context, int64, int64, int) ([]Update, int64, bool, error) {
	return nil, 0, false, nil
}

type memDB struct {
	mu  sync.Mutex
	seq int64
}

func (m *memDB) DeleteStale(context.Context, string, []string) error         { return nil }
func (m *memDB) LastValue(context.Context, string) (int64, error)            { return m.seq, nil }
func (m *memDB) UpsertPosition(context.Context, string, string, int64) error { return nil }
func (m *memDB) NextVal(context.Context, string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	return m.seq, nil
}

func set(hosts ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, h := range hosts {
		m[h] = struct{}{}
	}
	return m
}

func newApp(t *testing.T, st *fakeStore, onPokes func(int64)) *App {
	t.Helper()
	hs := &synapsecfg.Config{ServerName: "hs.test"}
	hs.Writers.DeviceLists = []string{"gw"}
	st.converted = map[string]bool{}
	st.pos.stream = 100
	a, err := New(context.Background(), Options{Instance: "gw", Synapse: func() *synapsecfg.Config { return hs },
		Store: st, Pool: &memDB{seq: 100}, OnPokes: onPokes, Log: zerolog.Nop(), Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestConversion(t *testing.T) {
	st := &fakeStore{
		rooms: map[string][]string{"@me:hs.test": {"!a", "!b", "!solo"}, "@x:remote.test": {"!a"}},
		hosts: map[string]map[string]struct{}{
			"!a":    set("hs.test", "one.test", "two.test"),
			"!b":    set("hs.test", "two.test", "three.test"),
			"!solo": set("hs.test"),
		},
	}
	var poked []int64
	a := newApp(t, st, func(id int64) { poked = append(poked, id) })
	ctx := context.Background()
	if err := a.NotifyDeviceUpdate(ctx, "@me:hs.test", []string{"D1", "D2"}); err != nil {
		t.Fatal(err)
	}
	// A remote user's change is in the streams, never converted.
	if err := a.NotifyDeviceUpdate(ctx, "@x:remote.test", []string{"R1"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		a.convMu.Lock()
		busy := a.converting
		a.convMu.Unlock()
		if !busy || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := a.convert(ctx); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range st.pokes {
		got = append(got, fmt.Sprintf("%s %s", p.device, strings.Join(p.hosts, ",")))
	}
	// D1 (stream 101) in !a then !b: !b leaves out two.test, already poked
	// for the same stream ID; !solo has no other server. Likewise D2.
	want := []string{"D1 one.test,two.test", "D1 three.test", "D2 one.test,two.test", "D2 three.test"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("pokes\n got  %v\n want %v", got, want)
	}
	if len(poked) != 4 {
		t.Errorf("sender poked %v", poked)
	}
	// The position ends at the current token: nothing left.
	if st.pos.stream != a.ids.Position() || st.pos.room != "" {
		t.Errorf("position %+v, current %d", st.pos, a.ids.Position())
	}
	if len(st.stream) != 3 {
		t.Errorf("stream rows %v", st.stream)
	}
}

func TestBatches(t *testing.T) {
	st := &fakeStore{rooms: map[string][]string{}, hosts: map[string]map[string]struct{}{}}
	a := newApp(t, st, nil)
	devs := make([]string, 2500)
	for i := range devs {
		devs[i] = fmt.Sprintf("D%d", i)
	}
	if err := a.NotifyDeviceUpdate(context.Background(), "@x:remote.test", devs); err != nil {
		t.Fatal(err)
	}
	if len(st.stream) != 2500 || a.ids.Position() != 2600 {
		t.Errorf("%d rows, position %d", len(st.stream), a.ids.Position())
	}
	if err := a.NotifyDeviceUpdate(context.Background(), "@x:remote.test", nil); err != nil || a.ids.Position() != 2600 {
		t.Error("an empty update took IDs")
	}
}
