package creator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/build"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/writer"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

const (
	room = "!r:example.org"
	user = "@u:example.org"
)

// fakeRoom is a v11 room with a create event, power levels and @u joined,
// whose one extremity is the member event, in state group 7 (a delta of 6).
type fakeRoom struct {
	create, topic string
	events        map[string]*pdustore.StoredEvent
	state         pdu.StateMap
	prevs         []string
	txn           string
	token         *testToken
	partial       bool

	key *build.Key
	// groupOf and groups are prev events' state groups other than 7, the
	// one f.state is.
	groupOf     map[string]int64
	groups      map[int64]pdu.StateMap
	profiles    map[string][2]*string
	blocked     bool
	accountData map[string]bool
	exempt      bool
	annotated   map[string]bool // aggregation keys the user has sent
	version     string          // the room version, "11" unless made otherwise
	v1IDs       int
}

// proto fills in what the room version needs: v1/v2 events carry a chosen
// ID and their prev and auth events' reference hashes.
func (f *fakeRoom) proto(t *testing.T, p build.Proto) build.Proto {
	t.Helper()
	p.RoomVersion = id.RoomVersion(f.version)
	if f.version == "1" || f.version == "2" {
		f.v1IDs++
		p.EventID = fmt.Sprintf("$%d:example.org", f.v1IDs)
		p.RefHashes = map[string][]byte{}
		for _, e := range append(slices.Clone(p.PrevEvents), p.AuthEvents...) {
			h, err := build.RefHash(f.events[e].Event.JSON, p.RoomVersion)
			if err != nil {
				t.Fatal(err)
			}
			p.RefHashes[e] = h
		}
	}
	return p
}

func newFakeRoom(t *testing.T, key *build.Key) *fakeRoom {
	return newFakeRoomVersion(t, key, "11")
}

func newFakeRoomVersion(t *testing.T, key *build.Key, version string) *fakeRoom {
	t.Helper()
	f := &fakeRoom{version: version, events: map[string]*pdustore.StoredEvent{}, state: pdu.StateMap{},
		token: &testToken{ID: 42, UserID: user, DeviceID: "DEV", Used: true}}
	rv, _ := pdu.LookupRoomVersion(version)
	var prev []string
	depth := int64(1)
	add := func(typ, sk, content string, auth []string) string {
		s := sk
		b, err := build.Build(f.proto(t, build.Proto{RoomID: room, Sender: user, Type: typ, StateKey: &s,
			Content: jsontext.Value(content), PrevEvents: prev, AuthEvents: auth, Depth: depth, OriginServerTS: 1}), key)
		if err != nil {
			t.Fatal(err)
		}
		ev, err := pdu.ParseEvent(b.JSON, b.ID, rv, false)
		if err != nil {
			t.Fatal(err)
		}
		f.events[b.ID] = &pdustore.StoredEvent{Event: ev, StreamOrdering: depth}
		f.state[pdu.StateKey{Type: typ, StateKey: sk}] = b.ID
		prev = []string{b.ID}
		depth++
		return b.ID
	}
	create := add("m.room.create", "", `{"room_version":"`+version+`","creator":"`+user+`"}`, nil)
	member := add("m.room.member", user, `{"membership":"join"}`, []string{create})
	pl := add("m.room.power_levels", "", `{"users":{"`+user+`":100},"events_default":0}`, []string{create, member})
	f.create = create
	f.topic = add("m.room.topic", "", `{"topic":"old"}`, []string{create, member, pl})
	f.prevs = prev
	f.key = key
	return f
}

// addState puts one more state event in the room, sent by @u, as the new
// extremity.
func (f *fakeRoom) addState(t *testing.T, typ, sk, content string) string {
	t.Helper()
	rv, _ := pdu.LookupRoomVersion(f.version)
	s := sk
	depth := f.events[f.prevs[0]].Depth + 1
	auth := []string{f.create, f.state[pdu.StateKey{Type: "m.room.power_levels"}], f.state[pdu.StateKey{Type: "m.room.member", StateKey: user}]}
	b, err := build.Build(f.proto(t, build.Proto{RoomID: room, Sender: user, Type: typ, StateKey: &s,
		Content: jsontext.Value(content), PrevEvents: f.prevs, AuthEvents: auth, Depth: depth, OriginServerTS: 1}), f.key)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := pdu.ParseEvent(b.JSON, b.ID, rv, false)
	if err != nil {
		t.Fatal(err)
	}
	f.events[b.ID] = &pdustore.StoredEvent{Event: ev, StreamOrdering: depth}
	f.state[pdu.StateKey{Type: typ, StateKey: sk}] = b.ID
	f.prevs = []string{b.ID}
	return b.ID
}

func (f *fakeRoom) Profile(_ context.Context, u string) (*string, *string, bool, error) {
	p, ok := f.profiles[u]
	return p[0], p[1], ok, nil
}
func (f *fakeRoom) RoomBlocked(context.Context, string) (bool, error) { return f.blocked, nil }
func (f *fakeRoom) LocalMembership(_ context.Context, u, _ string) (string, string, error) {
	id, ok := f.state[pdu.StateKey{Type: "m.room.member", StateKey: u}]
	if !ok {
		return "", "", nil
	}
	return gjson.Get(f.events[id].Content.Raw, "membership").Str, id, nil
}
func (f *fakeRoom) RatelimitExempt(context.Context, string) (bool, error) { return f.exempt, nil }
func (f *fakeRoom) RatelimitOverride(context.Context, string) (bool, *int64, *int64, error) {
	return f.exempt, nil, nil, nil
}
func (f *fakeRoom) UserFlags(_ context.Context, u string) (bool, bool, bool, error) {
	if f.token != nil && u == f.token.UserID {
		return true, f.token.Admin, f.token.Suspended, nil
	}
	return u == user || u == other || u == "@bot:example.org", false, false, nil
}
func (f *fakeRoom) ShadowBanned(_ context.Context, u string) (bool, error) {
	return f.token != nil && u == f.token.UserID && f.token.ShadowBan, nil
}
func (f *fakeRoom) DeviceExists(_ context.Context, u, d string) (bool, error) {
	return d == "BRIDGEDEV", nil
}
func (f *fakeRoom) RecentJoins(context.Context, string, int64) ([]int64, error) { return nil, nil }
func (f *fakeRoom) AccountData(_ context.Context, u, typ string) ([]byte, bool, error) {
	if f.accountData[u+" "+typ] {
		return []byte("{}"), true, nil
	}
	return nil, false, nil
}

func (f *fakeRoom) TxnEvent(_ context.Context, r, u, d, txn string) (string, error) {
	if f.txn != "" && txn == "seen" {
		return f.txn, nil
	}
	return "", pdustore.ErrNotFound
}
func (f *fakeRoom) RoomVersion(context.Context, string) (string, error)         { return f.version, nil }
func (f *fakeRoom) PartialState(context.Context, string) (bool, error)          { return f.partial, nil }
func (f *fakeRoom) PrevEventsForRoom(context.Context, string) ([]string, error) { return f.prevs, nil }
func (f *fakeRoom) RoomMaxStream(context.Context, string) (int64, error)        { return 3, nil }
func (f *fakeRoom) Depths(_ context.Context, ids []string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, i := range ids {
		if e := f.events[i]; e != nil {
			out[i] = e.Depth
		}
	}
	return out, nil
}
func (f *fakeRoom) StateGroupForEvent(_ context.Context, id string) (int64, error) {
	if g, ok := f.groupOf[id]; ok {
		return g, nil
	}
	return 7, nil
}
func (f *fakeRoom) StateForGroup(_ context.Context, g int64) (pdu.StateMap, error) {
	if s, ok := f.groups[g]; ok {
		return s, nil
	}
	return f.state, nil
}
func (f *fakeRoom) StateDelta(_ context.Context, g int64) (int64, bool, pdu.StateMap, error) {
	if _, ok := f.groups[g]; ok {
		return 0, false, nil, nil
	}
	return 6, true, pdu.StateMap{{Type: "m.room.power_levels"}: f.prevs[0]}, nil
}
func (f *fakeRoom) GetEvents(_ context.Context, ids []string) (map[string]*pdu.Event, error) {
	out := map[string]*pdu.Event{}
	for _, i := range ids {
		if e := f.events[i]; e != nil {
			out[i] = e.Event
		}
	}
	return out, nil
}
func (f *fakeRoom) AuthEdges(_ context.Context, ids []string) (map[string][]string, error) {
	out := map[string][]string{}
	for _, i := range ids {
		if e := f.events[i]; e != nil {
			out[i] = e.AuthEvents
		}
	}
	return out, nil
}
func (f *fakeRoom) GetStoredEvents(_ context.Context, ids []string) (map[string]*pdustore.StoredEvent, map[string]error, error) {
	out := map[string]*pdustore.StoredEvent{}
	for _, i := range ids {
		if e := f.events[i]; e != nil {
			out[i] = e
		}
	}
	return out, map[string]error{}, nil
}
func (f *fakeRoom) RejectionColumn(context.Context, string) (*string, error) { return nil, nil }
func (f *fakeRoom) EventRoom(_ context.Context, ev string) (string, error) {
	if f.events[ev] != nil {
		return room, nil
	}
	return "", pdustore.ErrNotFound
}
func (f *fakeRoom) IsRelationTarget(context.Context, string) (bool, error) { return false, nil }
func (f *fakeRoom) HasRelation(context.Context, string) (bool, error)      { return false, nil }
func (f *fakeRoom) HasAnnotated(_ context.Context, _, _, _, key string) (bool, error) {
	return f.annotated[key], nil
}

type fakeWriter struct {
	jobs        map[string]writer.ProfileJob
	checkpoints []string
	calls       []string
	groupDelta  []writer.StateRow
	groupPrev   int64
	bumped      []int64
	nextGroup   int64
	delayed     []writer.DelayedEvent
}

func (w *fakeWriter) AddDelayedEvent(_ context.Context, ev writer.DelayedEvent, limit int) (int64, error) {
	w.calls = append(w.calls, "delayed")
	w.delayed = append(w.delayed, ev)
	if len(w.delayed) > limit {
		return 0, &writer.DelayedLimitError{}
	}
	return ev.SendTS, nil
}
func (w *fakeWriter) BumpDeletion(_ context.Context, g []int64) ([]int64, error) {
	w.calls = append(w.calls, "bump")
	w.bumped = append(w.bumped, g...)
	return nil, nil
}
func (w *fakeWriter) SetRoomParticipation(context.Context, string, string) error {
	w.calls = append(w.calls, "participation")
	return nil
}
func (w *fakeWriter) StagePush(context.Context, string, []writer.PushRow) error {
	w.calls = append(w.calls, "stage")
	return nil
}
func (w *fakeWriter) StoreStateGroup(_ context.Context, ev, room string, prev int64, delta []writer.StateRow, _ func() ([]writer.StateRow, error)) (int64, error) {
	w.calls = append(w.calls, "state_group")
	w.groupDelta, w.groupPrev = delta, prev
	if w.nextGroup == 0 {
		w.nextGroup = 8
	}
	w.nextGroup++
	return w.nextGroup - 1, nil
}
func (w *fakeWriter) StoreFullStateGroup(_ context.Context, ev, room string, rows []writer.StateRow) (int64, error) {
	w.calls = append(w.calls, "full_state_group")
	w.groupDelta, w.groupPrev = rows, 0
	return 50, nil
}
func (w *fakeWriter) UnstagePush(context.Context, string) error {
	w.calls = append(w.calls, "unstage")
	return nil
}

type fakePersister struct {
	entries []json.RawMessage
	err     error
	answer  string
}

func (p *fakePersister) SendClientEvents(_ context.Context, instance string, entries []json.RawMessage) (int64, string, error) {
	p.entries = append(p.entries, entries...)
	if p.err != nil {
		return 0, "", p.err
	}
	return 99, p.answer, nil
}

type harness struct {
	c    *Creator
	f    *fakeRoom
	w    *fakeWriter
	p    *fakePersister
	main *fakeMain
}

type fakeMain struct {
	left    []string
	delayed []int64
}

func (m *fakeMain) DelayedEventAdded(_ context.Context, next int64) error {
	m.delayed = append(m.delayed, next)
	return nil
}
func (m *fakeMain) UserLeftRoom(_ context.Context, roomID, userID string) error {
	m.left = append(m.left, roomID+" "+userID)
	return nil
}

// testToken is an access token and its user's flags, as fakeAuth and the
// fake store answer for "tok".
type testToken struct {
	ID       int64
	UserID   string
	Owner    string
	DeviceID string
	Puppet   bool
	Used     bool
	Guest    bool
	Admin    bool
	// The users row's.
	ShadowBan, Suspended bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessVersion(t, "11")
}

func newHarnessVersion(t *testing.T, version string) *harness {
	t.Helper()
	key, err := build.ParseKey("example.org", federation.GenerateSigningKey().SynapseString())
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{f: newFakeRoomVersion(t, key, version), w: &fakeWriter{}, p: &fakePersister{}, main: &fakeMain{}}
	lim := synapsecfg.Ratelimit{PerSecond: 1, Burst: 10}
	h.c = New(Options{Store: h.f, Writer: h.w, Persister: h.p, Key: key,
		ServerName: "example.org", EventsWriter: func(string) string { return "persister" }, Sticky: true,
		Membership: synapsecfg.Membership{AllowPerRoomProfiles: true, IncludeProfileDataOnInvite: true,
			JoinsLocal: lim, JoinsPerRoom: lim, InvitesPerRoom: lim, InvitesPerUser: lim, InvitesPerIssuer: lim},
		MSC4155: true, MSC4293: true, Main: h.main,
		Delayed:     synapsecfg.Delayed{MaxDelayMS: 86400000, PerUser: 2, RcMessage: synapsecfg.Ratelimit{Key: "rc_message", PerSecond: 1, Burst: 10}},
		ShadowPause: func(time.Duration) time.Duration { return 0 },
		Log:         zerolog.Nop(), Now: func() time.Time { return time.UnixMilli(1700000000000) }})
	h.c.o.Auth = &fakeAuth{h: h}
	h.c.verify = func(pending) {}
	return h
}

func (h *harness) send(t *testing.T, method, txn, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	path := "/_matrix/client/v3/rooms/" + room + "/send/m.room.message"
	req := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: txn}
	if txn != "" {
		path += "/" + txn
	}
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	out := h.c.Serve(rec, r, req)
	return rec, out
}

func TestNativeSend(t *testing.T) {
	h := newHarness(t)
	rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Fallback != "" || out.Result != "created" {
		t.Fatalf("outcome %+v", out)
	}
	if rec.Code != 200 {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	eventID := gjson.Get(rec.Body.String(), "event_id").Str
	if !strings.HasPrefix(eventID, "$") {
		t.Fatalf("response %s", rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,participation,stage" {
		t.Errorf("writes %s", got)
	}
	if len(h.p.entries) != 1 {
		t.Fatalf("%d entries sent", len(h.p.entries))
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"room_version":                     `"11"`,
		"event_format_version":             "3",
		"outlier":                          "false",
		"rejected_reason":                  "null",
		"ratelimit":                        "true",
		"extra_users":                      "[]",
		"internal_metadata.device_id":      `"DEV"`,
		"internal_metadata.txn_id":         `"t1"`,
		"context.state_group":              "7",
		"context.state_group_before_event": "7",
		"context.rejected":                 "null",
		"context.state_delta_due_to_event": "[]",
		"context.app_service_id":           "null",
		"context.partial_state":            "false",
		"context.state_group_deltas.#":     "2",
		"context.state_group_deltas.0":     "[7,7,[]]",
		"context.state_group_deltas.1.0":   "6",
		"context.state_group_deltas.1.1":   "7",
		"requester.user_id":                `"` + user + `"`,
		"requester.access_token_id":        "42",
		"requester.device_id":              `"DEV"`,
		"requester.is_guest":               "false",
		"requester.shadow_banned":          "false",
		"requester.scope":                  "[]",
		"requester.app_server_id":          "null",
		"requester.authenticated_entity":   `"` + user + `"`,
		"event.unsigned.age_ts":            "1700000000000",
		"event.origin_server_ts":           "1700000000000",
		"event.depth":                      "5",
		"event.prev_events.#":              "1",
		"event.auth_events.#":              "3",
		"event.content.body":               `"hi"`,
		"event.type":                       `"m.room.message"`,
		"event.sender":                     `"` + user + `"`,
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("internal_metadata.token_id").Exists() {
		t.Error("token_id in internal_metadata: create_event no longer sets it")
	}
	// The event the writer gets is the one we answered with.
	rv, _ := pdu.LookupRoomVersion("11")
	gotID, err := pdu.CalculateEventID([]byte(e.Get("event").Raw), rv)
	if err != nil || gotID != eventID {
		t.Errorf("event id of the sent event %s (%v), answered %s", gotID, err, eventID)
	}
}

func TestPOSTHasNoTxnID(t *testing.T) {
	h := newHarness(t)
	if _, out := h.send(t, http.MethodPost, "", `{"msgtype":"m.text","body":"hi"}`); out.Result != "created" {
		t.Fatalf("outcome %+v", out)
	}
	if gjson.GetBytes(h.p.entries[0], "internal_metadata.txn_id").Exists() {
		t.Error("txn_id sent without a transaction")
	}
}

func TestStickyIsSent(t *testing.T) {
	h := newHarness(t)
	path := "/_matrix/client/v3/rooms/" + room + "/send/m.room.message/t?" + StickyParam + "=60000"
	r := httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"msgtype":"m.text","body":"hi"}`))
	r.Header.Set("Authorization", "Bearer tok")
	out := h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
	if out.Result != "created" {
		t.Fatalf("outcome %+v", out)
	}
	if gjson.GetBytes(h.p.entries[0], "event.msc4354_sticky.duration_ms").Int() != 60000 {
		t.Errorf("sticky missing: %s", h.p.entries[0])
	}
}

// stopped is why a request was not served: its fallback, or "refused:" and
// the refusal.
func stopped(out Outcome) string {
	if out.Refused != "" {
		return "refused:" + out.Refused
	}
	return out.Fallback
}

// Every fallback and refusal is decided before the first write.
func TestFallbacksWriteNothing(t *testing.T) {
	cases := map[string]func(h *harness) (string, string){
		"no extremities": func(h *harness) (string, string) {
			h.f.prevs = nil
			return `{"msgtype":"m.text","body":"hi"}`, "prev_events_0"
		},
		"not joined": func(h *harness) (string, string) {
			h.f.token = &testToken{ID: 1, UserID: user, DeviceID: "DEV", Used: true}
			delete(h.f.state, pdu.StateKey{Type: "m.room.member", StateKey: user})
			return `{"msgtype":"m.text","body":"hi"}`, "refused:not_in_room"
		},
		"partial state": func(h *harness) (string, string) {
			h.f.partial = true
			return `{"msgtype":"m.text","body":"hi"}`, "partial_state"
		},
		"bad message":   func(*harness) (string, string) { return `{"msgtype":"m.text"}`, "refused:invalid_message" },
		"not an object": func(*harness) (string, string) { return `[1]`, "refused:not_object" },
		"bad mentions": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","m.mentions":{"room":1}}`, "refused:invalid_mentions"
		},
		"unknown relation": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","m.relates_to":{"rel_type":"m.replace","event_id":"$nope"}}`, "refused:relation_unknown"
		},
		"float": func(*harness) (string, string) {
			return `{"msgtype":"m.text","body":"x","n":1.5}`, "refused:non_canonical_json"
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			body, want := setup(h)
			rec, out := h.send(t, http.MethodPut, "t1", body)
			if stopped(out) != want {
				t.Fatalf("stopped %q, want %q", stopped(out), want)
			}
			if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
				t.Errorf("a fallback wrote: %v, %d entries", h.w.calls, len(h.p.entries))
			}
			if out.Fallback != "" && (rec.Code != 200 || rec.Body.Len() != 0) {
				t.Errorf("a fallback answered the client: %d %s", rec.Code, rec.Body)
			}
		})
	}
}

// An escaped surrogate pair is one character, to Python as to gjson: a
// reaction whose key is an escaped emoji is served (Synapse's
// M_DUPLICATE_ANNOTATION here), not left to Synapse.
func TestEscapedEmojiReaction(t *testing.T) {
	h := newHarness(t)
	h.f.annotated = map[string]bool{"\U0001F600": true}
	body := `{"m.relates_to":{"rel_type":"m.annotation","event_id":"` + h.f.prevs[0] + `","key":"\ud83d\ude00"}}`
	req := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.reaction", TxnID: "t1"}
	rec, out := h.put(t, req, "/send/m.reaction/t1", body)
	if stopped(out) != "refused:duplicate_annotation" || rec.Code != 400 {
		t.Fatalf("stopped %q: %d %s", stopped(out), rec.Code, rec.Body)
	}
}

func TestTxnDedupAnswersWithoutWriting(t *testing.T) {
	h := newHarness(t)
	h.f.txn = "$earlier"
	rec, out := h.send(t, http.MethodPut, "seen", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "txn_dedup" || gjson.Get(rec.Body.String(), "event_id").Str != "$earlier" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Error("a deduplicated transaction wrote")
	}
}

// The writer's refusal reaches the client as Synapse relays it, and the
// staged push actions are removed.
func TestWriterRefusalIsRelayed(t *testing.T) {
	h := newHarness(t)
	h.p.err = &replication.HTTPError{Status: 429, Body: `{"errcode":"M_LIMIT_EXCEEDED","retry_after_ms":100}`}
	rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "writer_429" || rec.Code != 429 || !strings.Contains(rec.Body.String(), "M_LIMIT_EXCEEDED") {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,participation,stage,unstage" {
		t.Errorf("writes %s", got)
	}
}

func TestWriterDedupAnswersWithItsEvent(t *testing.T) {
	h := newHarness(t)
	h.p.answer = "$theirs"
	rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "writer_dedup" || gjson.Get(rec.Body.String(), "event_id").Str != "$theirs" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
}

// A fallback leaves the body readable for the proxy.
func TestFallbackLeavesBodyReadable(t *testing.T) {
	h := newHarness(t)
	h.f.partial = true
	const body = `{"msgtype":"m.text","body":"hi"}`
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.room.message/t", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
	b := make([]byte, 100)
	n, _ := r.Body.Read(b)
	if string(b[:n]) != body {
		t.Errorf("body after fallback: %q", b[:n])
	}
}

// TestValidatorAgainstSynapse runs validate_builder and validate_new's port
// over vectors Synapse answered itself (testdata/oracle_validator.py).
func TestValidatorAgainstSynapse(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_validator.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases [][]json.RawMessage
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	c := &Creator{o: Options{ServerName: "example.org"}}
	for _, cs := range cases {
		var typ, content string
		var sk *string
		var want []any
		_ = json.Unmarshal(cs[0], &typ)
		_ = json.Unmarshal(cs[1], &sk)
		_ = json.Unmarshal(cs[2], &content)
		_ = json.Unmarshal(cs[3], &want)
		in := &input{typ: typ, stateKey: sk, content: []byte(content)}
		v := gjson.Parse(content)
		st := validateBuilder(in, v, room)
		if st == nil {
			st = c.validateNew(in, in.content, v)
		}
		name := fmt.Sprintf("%s %v %s", typ, sk != nil, content)
		switch {
		case want == nil && st != nil:
			t.Errorf("%s: Synapse accepts it, we %+v %+v", name, st.fallback, st.ref)
		case want != nil && st == nil:
			t.Errorf("%s: Synapse refuses it (%v), we accept it", name, want)
		case want != nil && st.ref == nil:
			t.Errorf("%s: Synapse refuses it (%v), we fall back (%s)", name, want, st.fallback)
		case want != nil:
			got := []any{float64(st.ref.status), st.ref.errcode, st.ref.msg}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s:\n got  %v\n want %v", name, got, want)
			}
		}
	}
}

func TestRoomLocksSerialise(t *testing.T) {
	var l roomLocks
	unlock := l.lock("!a")
	done := make(chan struct{})
	go func() {
		u := l.lock("!a")
		u()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("second lock of the same room did not wait")
	case <-time.After(20 * time.Millisecond):
	}
	other := l.lock("!b")
	other()
	unlock()
	<-done
	if len(l.rooms) != 0 {
		t.Errorf("%d rooms left in the lock table", len(l.rooms))
	}
}

func (h *harness) put(t *testing.T, req route.Request, path, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, req)
}

func stateReq(typ, key string) route.Request {
	return route.Request{Endpoint: route.State, Write: true, Room: room, Action: typ, StateKey: key}
}

func TestNativeState(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, stateReq("m.room.topic", ""), "/state/m.room.topic/", `{"topic":"new"}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	id := gjson.Get(rec.Body.String(), "event_id").Str
	if got := strings.Join(h.w.calls, ","); got != "bump,state_group,stage" {
		t.Errorf("writes %s (no room participation for state)", got)
	}
	if len(h.w.groupDelta) != 1 || h.w.groupDelta[0] != (writer.StateRow{Type: "m.room.topic", StateKey: "", EventID: id}) {
		t.Errorf("group delta %v", h.w.groupDelta)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"context.state_group":              "8",
		"context.state_group_before_event": "7",
		"context.state_group_deltas.#":     "2",
		"context.state_group_deltas.0":     `[7,8,[["m.room.topic","","` + id + `"]]]`,
		"context.state_group_deltas.1.0":   "6",
		"context.state_group_deltas.1.1":   "7",
		"context.state_delta_due_to_event": `[["m.room.topic","","` + id + `"]]`,
		"event.state_key":                  `""`,
		"event.unsigned.replaces_state":    `"` + h.f.topic + `"`,
		"event.content":                    `{"topic":"new"}`,
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("internal_metadata.txn_id").Exists() {
		t.Error("/state has no transaction")
	}
}

func TestStateDedupWritesNothing(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, stateReq("m.room.topic", ""), "/state/m.room.topic/", `{ "topic" : "old" }`)
	if out.Result != "state_dedup" || gjson.Get(rec.Body.String(), "event_id").Str != h.f.topic {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Errorf("a duplicate wrote: %v", h.w.calls)
	}
}

func TestStateFallbacks(t *testing.T) {
	for _, tc := range []struct {
		typ, key, body, want string
	}{
		{"m.room.member", "@x:example.org", `{"membership":"join"}`, "refused:auth_rules"},
		{"m.room.power_levels", "", `{"users":{"@u:example.org":"100"}}`, "refused:invalid_power_levels"},
		{"m.room.server_acl", "", `{"allow":["*"],"deny":["example.org"]}`, "refused:acl_denies_us"},
		{"m.room.create", "", `{}`, "refused:type_create"},
		{"m.room.topic", "", `{"topic":1}`, "refused:invalid_topic"},
		{"m.room.name", "", `{}`, "refused:invalid_name"},
		{"m.room.tombstone", "", `{"replacement_room":"` + room + `"}`, "refused:invalid_tombstone"},
		{"m.room.tombstone", "", `{}`, "refused:invalid_tombstone"},
		{"m.room.retention", "", `{"min_lifetime":10,"max_lifetime":5}`, "refused:invalid_retention"},
		{"m.room.aliases", "example.org", `{"aliases":["#` + strings.Repeat("a", 300) + `:example.org"]}`, "refused:alias_too_long"},
	} {
		h := newHarness(t)
		_, out := h.put(t, stateReq(tc.typ, tc.key), "/state/"+tc.typ+"/"+tc.key, tc.body)
		if stopped(out) != tc.want {
			t.Errorf("%s: stopped %q, want %q", tc.typ, stopped(out), tc.want)
		}
		if len(h.w.calls) != 0 {
			t.Errorf("%s: wrote %v", tc.typ, h.w.calls)
		}
	}
}

func redactReq(target, txn string) route.Request {
	return route.Request{Endpoint: route.Redact, Write: true, Room: room, Action: target, TxnID: txn}
}

func TestNativeRedaction(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, redactReq(h.f.topic, "r1"), "/redact/x/r1", `{"reason":"test"}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.type":                   `"m.room.redaction"`,
		"event.content.redacts":        `"` + h.f.topic + `"`,
		"event.content.reason":         `"test"`,
		"internal_metadata.txn_id":     `"r1"`,
		"context.state_group":          "7",
		"context.state_group_deltas.0": "[7,7,[]]",
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("event.redacts").Exists() {
		t.Error("v11 redaction with a top-level redacts")
	}
	if e.Get("internal_metadata.recheck_redaction").Exists() {
		t.Error("recheck_redaction for a sender with redact power")
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,stage" {
		t.Errorf("writes %s", got)
	}
}

func TestRedactionFallbacks(t *testing.T) {
	h := newHarness(t)
	for name, tc := range map[string]struct{ target, body, want string }{
		"create event":    {h.f.create, `{}`, "refused:redact_create"},
		"redacts differs": {h.f.topic, `{"redacts":"$other"}`, "refused:redacts_mismatch"},
		"with relations":  {h.f.topic, `{"org.matrix.msc3912.with_relations":["m.thread"]}`, "with_relations"},
	} {
		h := newHarness(t)
		target := tc.target
		if name == "create event" {
			target = h.f.create
		}
		if name == "redacts differs" || name == "with relations" {
			target = h.f.topic
		}
		_, out := h.put(t, redactReq(target, "r"), "/redact/x/r", tc.body)
		if stopped(out) != tc.want {
			t.Errorf("%s: stopped %q, want %q", name, stopped(out), tc.want)
		}
		if len(h.w.calls) != 0 {
			t.Errorf("%s: wrote %v", name, h.w.calls)
		}
	}
}

func TestServerACLValidator(t *testing.T) {
	for acl, refused := range map[string]bool{
		`{"allow":["*"],"deny":["evil.example"]}`: false,
		`{"allow":[]}`:                     true,
		`{"allow":["*"],"deny":["*.org"]}`: true,
	} {
		st := validateServerACL([]byte(acl), "example.org")
		if (st != nil && st.ref != nil) != refused {
			t.Errorf("%s: %+v", acl, st)
		}
	}
}

// A redaction of an event we do not hold is sent: the writer checks nothing
// either, and applies it when the event arrives.
func TestRedactionOfUnknownEvent(t *testing.T) {
	h := newHarness(t)
	rec, out := h.put(t, redactReq("$unknown", "r1"), "/redact/x/r1", `{}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := gjson.GetBytes(h.p.entries[0], "event.content.redacts").Str; got != "$unknown" {
		t.Errorf("redacts %q", got)
	}
}

// fork makes the room's DAG fork: two state events on the same extremity,
// each the only one in its own group (8 and 9), both now extremities.
func (f *fakeRoom) fork(t *testing.T, a, b [3]string) (string, string) {
	t.Helper()
	base := f.prevs
	stateA, stateB := pdu.StateMap{}, pdu.StateMap{}
	for k, v := range f.state {
		stateA[k], stateB[k] = v, v
	}
	ida := f.addState(t, a[0], a[1], a[2])
	f.prevs = base
	idb := f.addState(t, b[0], b[1], b[2])
	stateA[pdu.StateKey{Type: a[0], StateKey: a[1]}] = ida
	stateB[pdu.StateKey{Type: b[0], StateKey: b[1]}] = idb
	f.groupOf = map[string]int64{ida: 8, idb: 9}
	f.groups = map[int64]pdu.StateMap{8: stateA, 9: stateB}
	f.prevs = []string{ida, idb}
	return ida, idb
}

// A shadow-banned requester is answered with a fake event ID, the same one
// for a retried transaction, and nothing is written.
func TestShadowBannedSend(t *testing.T) {
	h := newHarness(t)
	h.f.token.ShadowBan = true
	rec, out := h.send(t, http.MethodPut, "s1", `{"msgtype":"m.text","body":"hi"}`)
	id := gjson.Get(rec.Body.String(), "event_id").Str
	if out.Result != "shadow_banned" || rec.Code != 200 || len(id) != 44 || id[0] != '$' {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	rec, _ = h.send(t, http.MethodPut, "s1", `{"msgtype":"m.text","body":"hi"}`)
	if again := gjson.Get(rec.Body.String(), "event_id").Str; again != id {
		t.Errorf("retry answered %s, first %s", again, id)
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Errorf("a shadow-banned send wrote: %v", h.w.calls)
	}
	// A refusal still comes first.
	rec, out = h.send(t, http.MethodPut, "s2", `[]`)
	if out.Refused != "not_object" {
		t.Errorf("%+v %s", out, rec.Body)
	}
}

func TestSuspended(t *testing.T) {
	h := newHarness(t)
	h.f.token.Suspended = true
	rec, out := h.send(t, http.MethodPut, "s1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Refused != "suspended" || rec.Code != 403 ||
		rec.Body.String() != `{"errcode":"M_USER_SUSPENDED","error":"Sending messages while account is suspended is not allowed."}` {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	// Its own events it may redact.
	if _, out := h.put(t, redactReq(h.f.topic, "r1"), "/redact/x/r1", `{}`); out.Result != "created" {
		t.Errorf("own redaction: %+v", out)
	}
	h.f.events[h.f.topic].Sender = other
	rec, out = h.put(t, redactReq(h.f.topic, "r2"), "/redact/x/r2", `{}`)
	if out.Refused != "suspended" || !strings.Contains(rec.Body.String(), "You can only redact your own events") {
		t.Errorf("%+v %s", out, rec.Body)
	}
	// Inviting is not; banning is not either, later; leaving is allowed.
	rec, out = h.member(t, memberReq("invite", ""), `{"user_id":"`+other+`"}`)
	if !strings.Contains(rec.Body.String(), "Sending invites while account is suspended") {
		t.Errorf("invite: %+v %s", out, rec.Body)
	}
	rec, out = h.member(t, memberReq("ban", ""), `{"user_id":"`+other+`"}`)
	if !strings.Contains(rec.Body.String(), "Changing membership while account is suspended") {
		t.Errorf("ban: %+v %s", out, rec.Body)
	}
	if rec, out := h.member(t, memberReq("leave", ""), ``); out.Result != "created" {
		t.Errorf("leave: %+v %s", out, rec.Body)
	}
}

// A token without a device: no transaction lookup, and no device in the
// event's metadata or the requester.
func TestTokenWithoutDevice(t *testing.T) {
	h := newHarness(t)
	h.f.token.DeviceID = ""
	h.f.txn = "$old"
	_, out := h.send(t, http.MethodPut, "seen", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "created" {
		t.Fatalf("%+v", out)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if e.Get("internal_metadata.device_id").Exists() || e.Get("requester.device_id").Raw != "null" ||
		e.Get("internal_metadata.txn_id").Str != "seen" {
		t.Errorf("%s %s", e.Get("internal_metadata").Raw, e.Get("requester").Raw)
	}
}

// A puppet token acts as the puppeted user, its owner the authenticated
// entity.
func TestPuppetToken(t *testing.T) {
	h := newHarness(t)
	h.f.token.Puppet, h.f.token.Owner = true, "@admin:example.org"
	if _, out := h.send(t, http.MethodPut, "p1", `{"msgtype":"m.text","body":"hi"}`); out.Result != "created" {
		t.Fatalf("%+v", out)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if e.Get("event.sender").Str != user || e.Get("requester.authenticated_entity").Str != "@admin:example.org" {
		t.Errorf("%s %s", e.Get("event.sender").Raw, e.Get("requester").Raw)
	}
}

func TestGuest(t *testing.T) {
	h := newHarness(t)
	h.f.token.Guest = true
	rec, out := h.put(t, redactReq(h.f.topic, "r1"), "/redact/x/r1", `{}`)
	if out.Refused != "guest" || rec.Body.String() != `{"errcode":"M_GUEST_ACCESS_FORBIDDEN","error":"Guest access not allowed"}` {
		t.Errorf("redact: %+v %s", out, rec.Body)
	}
	rec, out = h.member(t, memberReq("invite", ""), `{"user_id":"`+other+`"}`)
	if out.Refused != "guest" || rec.Body.String() != `{"errcode":"M_FORBIDDEN","error":"Guest access not allowed"}` {
		t.Errorf("invite: %+v %s", out, rec.Body)
	}
	if _, out := h.send(t, http.MethodPut, "g1", `{"msgtype":"m.text","body":"hi"}`); out.Result != "created" {
		t.Errorf("send: %+v", out)
	}
	if gjson.GetBytes(h.p.entries[0], "requester.is_guest").Raw != "true" {
		t.Errorf("requester %s", gjson.GetBytes(h.p.entries[0], "requester").Raw)
	}
}

// Without redact power the auth rules pass any redaction (v3+), and the
// events writer allows only one's own events, which must exist: refused
// here as the writer would refuse them (persist_and_notify_client_events).
func TestRedactionWithoutPower(t *testing.T) {
	for name, tc := range map[string]struct {
		target func(h *harness) string
		status int
		want   string
	}{
		"own event":     {func(h *harness) string { return h.f.topic }, 200, ""},
		"unknown event": {func(*harness) string { return "$unknown" }, 404, `{"errcode":"M_NOT_FOUND","error":"Could not find event $unknown"}`},
		"other's event": {func(h *harness) string {
			ev := *h.f.events[h.f.topic].Event
			ev.Sender = other
			h.f.events["$theirs"] = &pdustore.StoredEvent{Event: &ev, StreamOrdering: 1}
			return "$theirs"
		}, 403, `{"errcode":"M_FORBIDDEN","error":"You don't have permission to redact events"}`},
	} {
		h := newHarness(t)
		h.f.addState(t, "m.room.power_levels", "", `{"events_default":0,"state_default":0}`)
		target := tc.target(h)
		rec, out := h.put(t, redactReq(target, "r1"), "/redact/x/r1", `{}`)
		if rec.Code != tc.status {
			t.Errorf("%s: %d %s %+v", name, rec.Code, rec.Body, out)
			continue
		}
		if tc.status == 200 {
			if !gjson.GetBytes(h.p.entries[0], "internal_metadata.recheck_redaction").Bool() {
				t.Errorf("%s: no recheck_redaction", name)
			}
			continue
		}
		if got := strings.TrimSpace(rec.Body.String()); got != tc.want {
			t.Errorf("%s: %s", name, got)
		}
		if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
			t.Errorf("%s: wrote %v", name, h.w.calls)
		}
	}
}

// Rooms v1 to v3 are served natively. In v1 and v2 the event ID is chosen
// (_create_event_id) and each prev and auth event carries its reference
// hash (add_event_hashes); a redaction's redacts is top-level.
func TestNativeOldRoomVersions(t *testing.T) {
	v1ID := regexp.MustCompile(`^\$\d+[A-Za-z]{5}:example\.org$`)
	for _, version := range []string{"1", "2", "3"} {
		h := newHarnessVersion(t, version)
		rec, out := h.send(t, http.MethodPut, "t1", `{"msgtype":"m.text","body":"hi"}`)
		if out.Result != "created" || rec.Code != 200 {
			t.Fatalf("v%s: %+v %d %s", version, out, rec.Code, rec.Body)
		}
		e := gjson.ParseBytes(h.p.entries[0])
		ev := e.Get("event")
		got := gjson.Get(rec.Body.String(), "event_id").Str
		if version == "3" {
			if ev.Get("event_id").Exists() || ev.Get("prev_events.0").Type != gjson.String {
				t.Errorf("v3: %s", ev.Raw)
			}
			continue
		}
		if !v1ID.MatchString(got) || ev.Get("event_id").Str != got {
			t.Errorf("v%s: event ID %q, in the event %q", version, got, ev.Get("event_id").Str)
		}
		for _, list := range []string{"prev_events", "auth_events"} {
			for _, ref := range ev.Get(list).Array() {
				want, err := build.RefHash(h.f.events[ref.Get("0").Str].Event.JSON, id.RoomVersion(version))
				if err != nil {
					t.Fatal(err)
				}
				if ref.Get("1.sha256").Str != base64.RawStdEncoding.EncodeToString(want) {
					t.Errorf("v%s: %s %s: hash %s", version, list, ref.Get("0").Str, ref.Get("1").Raw)
				}
			}
		}
		// The event persisted parses as what was built, ID and all.
		rv, _ := pdu.LookupRoomVersion(version)
		if _, err := pdu.ParseEvent([]byte(ev.Raw), got, rv, false); err != nil {
			t.Errorf("v%s: %v", version, err)
		}

		rec, out = h.put(t, redactReq(h.f.topic, "r1"), "/redact/x/r1", `{"reason":"x"}`)
		if out.Result != "created" || rec.Code != 200 {
			t.Fatalf("v%s redaction: %+v %d %s", version, out, rec.Code, rec.Body)
		}
		red := gjson.GetBytes(h.p.entries[1], "event")
		if red.Get("redacts").Str != h.f.topic || red.Get("content.redacts").Exists() {
			t.Errorf("v%s redaction: %s", version, red.Raw)
		}
	}
}
