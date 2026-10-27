package txn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/edu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/synapseauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// fakeVerifier accepts a request whose X-Matrix origin is not "forged.example",
// and checks it got the whole body.
type fakeVerifier struct{ calls atomic.Int32 }

func (f *fakeVerifier) Verify(r *http.Request) fedauth.Result {
	f.calls.Add(1)
	_, _ = io.ReadAll(r.Body)
	origin := ""
	if i := strings.Index(r.Header.Get("Authorization"), `origin="`); i >= 0 {
		rest := r.Header.Get("Authorization")[i+len(`origin="`):]
		origin = rest[:strings.IndexByte(rest, '"')]
	}
	if origin == "" || origin == "forged.example" {
		return fedauth.Result{Err: &mautrix.RespError{ErrCode: "M_UNAUTHORIZED", StatusCode: 401}}
	}
	return fedauth.Result{Origin: origin}
}

// fakeUpstream stands in for the Python inbound worker.
type fakeUpstream struct {
	mu     sync.Mutex
	bodies []string
	uris   []string
	status int
	// gate, when set, blocks each forward until it is closed.
	gate chan struct{}
	// onForward runs at the start of each forward.
	onForward func()
}

func (f *fakeUpstream) Forward(w http.ResponseWriter, r *http.Request, _ int64) proxy.Result {
	if f.onForward != nil {
		f.onForward()
	}
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.bodies = append(f.bodies, string(b))
	f.uris = append(f.uris, r.RequestURI)
	status := f.status
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"pdus":{},"from":"synapse"}`))
	return proxy.Result{Status: status}
}

func (f *fakeUpstream) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

type fakeStore struct {
	mu    sync.Mutex
	state map[string]store.TxnState // by origin
	err   error
	calls int
}

func (f *fakeStore) GetTxnState(_ context.Context, origin, _ string) (store.TxnState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return store.TxnState{}, f.err
	}
	return f.state[origin], nil
}

type allowAll struct{}

func (allowAll) Allowed(context.Context, string, string) (bool, error) { return true, nil }

type sender struct {
	mu   sync.Mutex
	sent []string
	fail atomic.Bool
}

func (s *sender) SendEDU(_ context.Context, instance, eduType, origin string, content json.RawMessage) error {
	if s.fail.Load() {
		return errors.New("writer down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, instance+" "+eduType+" "+origin)
	return nil
}

func (s *sender) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

type rig struct {
	h        *Handler
	verifier *fakeVerifier
	up       *fakeUpstream
	store    *fakeStore
	sender   *sender
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{
		verifier: &fakeVerifier{},
		up:       &fakeUpstream{},
		store:    &fakeStore{state: map[string]store.TxnState{}},
		sender:   &sender{},
	}
	router := edu.NewRouter(synapsecfg.Writers{
		Typing: []string{"edu-w"}, Receipts: []string{"edu-w"}, ToDevice: []string{"edu-w"},
		Presence: []string{"edu-w"}, DeviceLists: []string{"edu-w"}, Events: []string{"p"},
	})
	proc := edu.NewProcessor(router, allowAll{}, r.sender, false, zerolog.Nop())
	r.h = NewHandler(Options{
		Verifier:       r.verifier,
		Upstream:       r.up,
		Store:          r.store,
		EDUs:           proc,
		MaxBody:        1 << 20,
		ForwardTimeout: 5 * time.Second,
		Log:            zerolog.Nop(),
	})
	return r
}

const eduOnly = `{"origin":"remote.example","pdus":[],"edus":[
	{"edu_type":"m.typing","content":{"room_id":"!r:x","user_id":"@u:remote.example","typing":true}},
	{"edu_type":"m.presence","content":{"push":[]}},
	{"edu_type":"m.receipt","content":{"!r:x":{"m.read":{}}}}]}`

const withPDU = `{"origin":"remote.example","pdus":[{"type":"m.room.message"}],"edus":[]}`

func send(t *testing.T, h http.Handler, origin, txnID, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/_matrix/federation/v1/send/"+txnID, strings.NewReader(body))
	req.Header.Set("Authorization", `X-Matrix origin="`+origin+`",key="ed25519:a",sig="x"`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestNativeForwardsEDUsAndAnswers(t *testing.T) {
	r := newRig(t)
	rec := send(t, r.h, "remote.example", "t1", eduOnly)

	if rec.Code != 200 || rec.Body.String() != `{"pdus":{}}` {
		t.Fatalf("answer = %d %s", rec.Code, rec.Body)
	}
	if r.up.count() != 0 {
		t.Error("an EDU-only transaction reached Synapse")
	}
	got := r.sender.list()
	// Presence is dropped (track_presence off); typing and receipt forwarded.
	if len(got) != 2 {
		t.Fatalf("forwarded %v, want typing and receipt only", got)
	}
	for _, s := range got {
		if !strings.HasPrefix(s, "edu-w ") || !strings.HasSuffix(s, " remote.example") {
			t.Errorf("forward %q: wrong instance or origin", s)
		}
	}
}

// Everything that is not a clean EDU-only transaction is Synapse's, and
// reaches it byte for byte.
func TestNativeProxiesWhatItDoesNotHandle(t *testing.T) {
	cases := []struct {
		name, origin, body string
		state              store.TxnState
	}{
		{"has PDUs", "remote.example", withPDU, store.TxnState{}},
		{"not an object", "remote.example", `[1,2]`, store.TxnState{}},
		{"edu without type", "remote.example", `{"edus":[{"content":{}}]}`, store.TxnState{}},
		{"edu content not an object", "remote.example", `{"edus":[{"edu_type":"m.typing","content":[]}]}`, store.TxnState{}},
		{"over the EDU limit", "remote.example", `{"edus":[` + strings.Repeat(`{"edu_type":"x","content":{}},`, 100) + `{"edu_type":"x","content":{}}]}`, store.TxnState{}},
		{"typing without room", "remote.example", `{"edus":[{"edu_type":"m.typing","content":{}}]}`, store.TxnState{}},
		{"auth fails", "forged.example", eduOnly, store.TxnState{}},
		{"origin in backoff", "down.example", eduOnly, store.TxnState{RetryLastTS: 12345}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			r.store.state[tc.origin] = tc.state
			rec := send(t, r.h, tc.origin, "t1", tc.body)
			if r.up.count() != 1 {
				t.Fatalf("Synapse got %d requests, want 1", r.up.count())
			}
			if r.up.bodies[0] != tc.body {
				t.Errorf("Synapse got a different body:\n%s", r.up.bodies[0])
			}
			if !strings.Contains(rec.Body.String(), `"from":"synapse"`) {
				t.Errorf("answer %q is not Synapse's", rec.Body)
			}
			if len(r.sender.list()) != 0 {
				t.Errorf("EDUs were forwarded for a proxied transaction: %v", r.sender.list())
			}
		})
	}
}

func TestDatabaseErrorProxies(t *testing.T) {
	r := newRig(t)
	r.store.err = errors.New("db down")
	send(t, r.h, "remote.example", "t1", eduOnly)
	if r.up.count() != 1 || len(r.sender.list()) != 0 {
		t.Error("a transaction whose state could not be read was not left to Synapse")
	}
}

// A transaction Synapse already answered gets Synapse's stored answer, as
// have_responded does (federation_server.py:400), and nothing is redone.
func TestStoredResponseIsReturned(t *testing.T) {
	r := newRig(t)
	r.store.state["remote.example"] = store.TxnState{Previous: &store.StoredResponse{Code: 200, JSON: []byte(`{"pdus":{"$e":{}}}`)}}
	rec := send(t, r.h, "remote.example", "t1", eduOnly)
	if rec.Code != 200 || rec.Body.String() != `{"pdus":{"$e":{}}}` {
		t.Errorf("answer = %d %s", rec.Code, rec.Body)
	}
	if len(r.sender.list()) != 0 || r.up.count() != 0 {
		t.Error("a transaction already answered was processed again")
	}
}

// A retry within 30s gets the first answer without reprocessing.
func TestRetryIsAnsweredFromCache(t *testing.T) {
	r := newRig(t)
	send(t, r.h, "remote.example", "t1", eduOnly)
	first := len(r.sender.list())
	rec := send(t, r.h, "remote.example", "t1", eduOnly)
	if rec.Code != 200 || len(r.sender.list()) != first {
		t.Errorf("retry: %d, forwarded %d more", rec.Code, len(r.sender.list())-first)
	}
}

// A forwarding failure is a 500 so the sender retries, and is not cached: the
// retry is actually retried.
func TestForwardFailureIsRetryable(t *testing.T) {
	r := newRig(t)
	r.sender.fail.Store(true)
	rec := send(t, r.h, "remote.example", "t1", eduOnly)
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "Error handing EDU of type") {
		t.Fatalf("answer = %d %s", rec.Code, rec.Body)
	}
	r.sender.fail.Store(false)
	rec = send(t, r.h, "remote.example", "t1", eduOnly)
	if rec.Code != 200 || len(r.sender.list()) == 0 {
		t.Errorf("the retry was answered from the failed attempt: %d %s", rec.Code, rec.Body)
	}
}

// One transaction per origin at a time -- across the native and proxied paths.
func TestConcurrentTransactionFromSameOriginIs429(t *testing.T) {
	r := newRig(t)
	gate := make(chan struct{})
	r.up.gate = gate

	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- send(t, r.h, "remote.example", "slow", withPDU) }()
	waitFor(t, func() bool { return r.up.count() == 1 })

	rec := send(t, r.h, "remote.example", "next", eduOnly)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("second transaction while one is proxied: %d, want 429", rec.Code)
	}
	// Another origin is unaffected.
	if rec := send(t, r.h, "other.example", "next", eduOnly); rec.Code != 200 {
		t.Errorf("other origin: %d", rec.Code)
	}

	close(gate)
	<-done
	if rec := send(t, r.h, "remote.example", "next", eduOnly); rec.Code != 200 {
		t.Errorf("after the first finished: %d", rec.Code)
	}
}

// A retry of the transaction in progress waits for it rather than being
// refused, and then gets its answer.
func TestRetryOfInProgressTransactionWaits(t *testing.T) {
	r := newRig(t)
	gate := make(chan struct{})
	r.up.gate = gate

	first := make(chan *httptest.ResponseRecorder)
	go func() { first <- send(t, r.h, "remote.example", "t1", withPDU) }()
	waitFor(t, func() bool { return r.up.count() == 1 })

	second := make(chan *httptest.ResponseRecorder)
	go func() { second <- send(t, r.h, "remote.example", "t1", withPDU) }()
	time.Sleep(20 * time.Millisecond)
	close(gate)

	if rec := <-first; rec.Code != 200 {
		t.Errorf("first: %d", rec.Code)
	}
	if rec := <-second; rec.Code != 200 {
		t.Errorf("retry: %d, want it to wait and succeed", rec.Code)
	}
}

func TestNonSendIsProxied(t *testing.T) {
	r := newRig(t)
	for _, target := range []string{
		"/_matrix/federation/v2/send/t1",
		"/_matrix/federation/v1/send/",
		"/_matrix/federation/v1/send/a/b",
		"/_matrix/federation/v1/event/$e",
	} {
		req := httptest.NewRequest(http.MethodPut, target, strings.NewReader(eduOnly))
		req.Header.Set("Authorization", `X-Matrix origin="remote.example",key="k",sig="s"`)
		r.h.ServeHTTP(httptest.NewRecorder(), req)
	}
	req := httptest.NewRequest(http.MethodGet, "/_matrix/federation/v1/send/t1", nil)
	r.h.ServeHTTP(httptest.NewRecorder(), req)

	if r.up.count() != 5 || len(r.sender.list()) != 0 {
		t.Errorf("up=%d sent=%v; everything that is not PUT v1 /send/<id> belongs to Synapse", r.up.count(), r.sender.list())
	}
}

// The path is matched raw and the id decoded after, as Synapse does.
func TestEncodedTxnID(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/_matrix/federation/v1/send/abc%2Fdef/", nil)
	id, version, ok := matchSend(req)
	if !ok || version != "v1" || id != "abc/def" {
		t.Errorf("matchSend = %q %q %v", id, version, ok)
	}
}

func TestBodyOverLimitIsStreamedWhole(t *testing.T) {
	r := newRig(t)
	r.h.maxBody = 16
	send(t, r.h, "remote.example", "t1", eduOnly)
	if r.up.count() != 1 || r.up.bodies[0] != eduOnly {
		t.Error("an oversized body did not reach Synapse intact")
	}
}

func TestClassify(t *testing.T) {
	if edus, _, reason := classify([]byte(`{"origin":"x"}`)); reason != "" || len(edus) != 0 {
		t.Errorf("no pdus, no edus: %v %q (absent lists are empty, units.py:101)", edus, reason)
	}
	if _, _, reason := classify([]byte(`{"pdus":null,"edus":null}`)); reason != "" {
		t.Errorf("null lists: %q", reason)
	}
	if _, _, reason := classify([]byte(`{"pdus":"x"}`)); reason != reasonMalformed {
		t.Errorf("pdus not a list: %q", reason)
	}
	if _, _, reason := classify([]byte(`{"pdus":[` + strings.Repeat(`{},`, 50) + `{}]}`)); reason != reasonLimits {
		t.Errorf("51 PDUs: %q, want the limit (Synapse answers 400)", reason)
	}
	if _, pdus, reason := classify([]byte(`{"pdus":[{"a":1},{"b":2}]}`)); reason != "" || len(pdus) != 2 {
		t.Errorf("pdus: %d %q", len(pdus), reason)
	}
	edus, _, reason := classify([]byte(`{"edus":[{"edu_type":"a","content":{"k":1}},{"edu_type":"b","content":{}}]}`))
	if reason != "" || len(edus) != 2 || edus[0].Type != "a" || edus[1].Type != "b" {
		t.Errorf("order or content lost: %+v %q", edus, reason)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

// At debug level every transaction gets exactly one line saying where it went
// and why; at info, none.
func TestDebugLogsEachTransaction(t *testing.T) {
	r := newRig(t)
	var buf bytes.Buffer
	r.h.log = zerolog.New(&buf).Level(zerolog.DebugLevel)

	send(t, r.h, "remote.example", "t1", eduOnly)
	send(t, r.h, "remote.example", "t2", withPDU)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d log lines for 2 transactions:\n%s", len(lines), buf.String())
	}
	var native, proxied map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &native)
	_ = json.Unmarshal([]byte(lines[1]), &proxied)

	if native["route"] != "native" || native["reason"] != "ok" || native["origin"] != "remote.example" ||
		native["txn_id"] != "t1" || native["verified"] != true {
		t.Errorf("native line: %s", lines[0])
	}
	edus, _ := native["edus"].(string)
	for _, want := range []string{"m.typing:forward@edu-w=1", "m.presence:drop_presence_untracked=1", "m.receipt:forward@edu-w=1"} {
		if !strings.Contains(edus, want) {
			t.Errorf("edus %q lacks %q", edus, want)
		}
	}
	if proxied["route"] != "proxy" || proxied["reason"] != reasonHasPDUs {
		t.Errorf("proxied line: %s", lines[1])
	}

	buf.Reset()
	r.h.log = zerolog.New(&buf).Level(zerolog.InfoLevel)
	send(t, r.h, "remote.example", "t3", eduOnly)
	if buf.Len() != 0 {
		t.Errorf("info level logged a routine transaction: %s", buf.String())
	}
}

type fakePDUServer struct {
	reason  string
	results map[string]json.RawMessage
	planned atomic.Int32
	ran     atomic.Int32
	// slow is how long planning takes; at is the request time it got.
	slow time.Duration
	at   time.Time
}

type fakeRun struct{ f *fakePDUServer }

func (r fakeRun) Run(context.Context) map[string]json.RawMessage {
	r.f.ran.Add(1)
	return r.f.results
}

func (f *fakePDUServer) PlanPDUs(_ context.Context, _ string, pdus []json.RawMessage, at time.Time) (PDURun, string) {
	f.planned.Add(1)
	f.at = at
	time.Sleep(f.slow)
	if f.reason != "" {
		return nil, f.reason
	}
	return fakeRun{f}, ""
}

type fakeResponses struct {
	mu    sync.Mutex
	saved map[string]string
}

func (f *fakeResponses) SetTxnResponse(_ context.Context, origin, txnID string, code int, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved[origin+"/"+txnID] = fmt.Sprintf("%d %s", code, body)
	return nil
}

func pduRig(t *testing.T, ps *fakePDUServer) (*rig, *fakeResponses) {
	t.Helper()
	r := newRig(t)
	resp := &fakeResponses{saved: map[string]string{}}
	r.h.pduServer, r.h.responses = ps, resp
	return r, resp
}

const pduTxn = `{"pdus":[{"room_id":"!r:x"}],"edus":[{"edu_type":"m.typing","content":{"room_id":"!r:x","user_id":"@u:remote.example","typing":true}}]}`

// Write mode answers a PDU transaction itself, with each PDU's entry, in
// canonical JSON, and stores the answer as Synapse does.
func TestWriteModeServesPDUTransactions(t *testing.T) {
	ps := &fakePDUServer{results: map[string]json.RawMessage{
		"$b": json.RawMessage(`{}`),
		"$a": json.RawMessage(`{"errcode":"M_FORBIDDEN","error":"Server is banned from room"}`),
	}}
	r, resp := pduRig(t, ps)
	rec := send(t, r.h, "remote.example", "t1", pduTxn)
	want := `{"pdus":{"$a":{"errcode":"M_FORBIDDEN","error":"Server is banned from room"},"$b":{}}}`
	if rec.Code != 200 || rec.Body.String() != want {
		t.Fatalf("answer %d %s", rec.Code, rec.Body)
	}
	if r.up.count() != 0 {
		t.Error("proxied")
	}
	if len(r.sender.list()) != 1 {
		t.Errorf("EDUs not forwarded: %v", r.sender.list())
	}
	if got := resp.saved["remote.example/t1"]; got != "200 "+want {
		t.Errorf("stored %q", got)
	}
	// EDU-only answers are stored too.
	send(t, r.h, "remote.example", "t2", eduOnly)
	if got := resp.saved["remote.example/t2"]; got != `200 {"pdus":{}}` {
		t.Errorf("EDU-only stored %q", got)
	}
}

func TestWriteModeProxiesWhatItCannotPlan(t *testing.T) {
	ps := &fakePDUServer{reason: "pdu_key_unknown"}
	r, resp := pduRig(t, ps)
	send(t, r.h, "remote.example", "t1", pduTxn)
	if r.up.count() != 1 || ps.ran.Load() != 0 || len(r.sender.list()) != 0 {
		t.Errorf("proxied %d, ran %d, forwarded %v", r.up.count(), ps.ran.Load(), r.sender.list())
	}
	if len(resp.saved) != 0 {
		t.Error("stored an answer Synapse gave")
	}
}

// The sweep nudge sends the upstream an exact copy of the last transaction
// answered and stored here, at most once a minute, and nothing before one is.
func TestNudgeSweepSendsACopyOfAnAnsweredTransaction(t *testing.T) {
	r, _ := pduRig(t, &fakePDUServer{})
	r.h.NudgeSweep()
	if r.up.count() != 0 {
		t.Fatal("a nudge was sent with no answered transaction")
	}
	r.h.nudge.next.Store(0)

	if rec := send(t, r.h, "remote.example", "t1", eduOnly); rec.Code != 200 || r.up.count() != 0 {
		t.Fatalf("not answered here: %d, proxied %d", rec.Code, r.up.count())
	}
	r.h.NudgeSweep()
	waitFor(t, func() bool { return r.up.count() == 1 })
	r.up.mu.Lock()
	body, uri := r.up.bodies[0], r.up.uris[0]
	r.up.mu.Unlock()
	if body != eduOnly || !strings.HasSuffix(uri, "/send/t1") {
		t.Errorf("sent %s %s, want a copy of t1", uri, body)
	}

	r.h.NudgeSweep()
	time.Sleep(50 * time.Millisecond)
	if r.up.count() != 1 {
		t.Errorf("%d nudges inside a minute, want 1", r.up.count())
	}
}

func TestSameJSON(t *testing.T) {
	if !sameJSON([]byte(`{"pdus":{"$a":{"error":"é"}}}`), []byte(`{"pdus": {"$a": {"error": "é"}}}`)) {
		t.Error("the same answer, encoded differently, did not compare equal")
	}
	if sameJSON([]byte(`{"pdus":{}}`), []byte(`{"pdus":{"$a":{}}}`)) {
		t.Error("different answers compared equal")
	}
}

// With Synapse's authentication, what it refuses is answered here, in its
// words, and never reaches the upstream; what its rules accept, though our
// verifier refused it, still goes to Synapse.
func TestAuthRefusalsAnsweredAsSynapse(t *testing.T) {
	r := newRig(t)
	r.h.sauth = synapseauth.New("local.test", nil, func(context.Context) pdu.KeyFetch {
		return func(string, []string, int64) (map[string]pdu.VerifyKey, error) {
			return map[string]pdu.VerifyKey{}, nil
		}
	})
	rec := send(t, r.h, "forged.example", "t1", eduOnly)
	want := `{"errcode":"M_UNAUTHORIZED","error":"Failed to find any key to satisfy: _FetchKeyRequest(server_name='forged.example', minimum_valid_until_ts=`
	if rec.Code != 401 || !strings.HasPrefix(rec.Body.String(), want) || !strings.HasSuffix(rec.Body.String(), `, key_ids=['ed25519:a'])"}`) {
		t.Errorf("no key: %d %s", rec.Code, rec.Body)
	}
	rec = send(t, r.h, "remote.example", "t2", `{"pdus":`)
	if rec.Code != 400 || rec.Body.String() != `{"errcode":"M_NOT_JSON","error":"Content not JSON."}` {
		t.Errorf("not JSON: %d %s", rec.Code, rec.Body)
	}
	if r.up.count() != 0 {
		t.Errorf("%d requests reached Synapse", r.up.count())
	}
}
