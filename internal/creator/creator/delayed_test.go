package creator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
)

func (h *harness) delayed(t *testing.T, path, query, txn, body string, req route.Request) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	method := http.MethodPut
	r := httptest.NewRequest(method, "/_matrix/client/v3/rooms/"+room+path+query, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, req)
}

// A delayed message: the row as add_delayed_event writes it, main told the
// next send time, {"delay_id"} answered, and the same for a retry.
func TestDelayedSend(t *testing.T) {
	h := newHarness(t)
	req := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: "d1"}
	rec, out := h.delayed(t, "/send/m.room.message/d1", "?org.matrix.msc4140.delay=5000", "d1", `{"msgtype":"m.text", "body":"later"}`, req)
	id := gjson.Get(rec.Body.String(), "delay_id").Str
	if out.Result != "delayed" || !strings.HasPrefix(id, "syd_") || len(id) != 24 {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if len(h.w.delayed) != 1 || len(h.p.entries) != 0 {
		t.Fatalf("rows %v, events %d", h.w.delayed, len(h.p.entries))
	}
	row := h.w.delayed[0]
	if row.UserLocalpart != "u" || *row.DeviceID != "DEV" || row.DelayMS != 5000 || row.SendTS != 1700000005000 ||
		row.StateKey != nil || row.Content != `{"msgtype":"m.text","body":"later"}` || row.EventType != "m.room.message" {
		t.Errorf("%+v", row)
	}
	if len(h.main.delayed) != 1 || h.main.delayed[0] != 1700000005000 {
		t.Errorf("main told %v", h.main.delayed)
	}
	rec, out = h.delayed(t, "/send/m.room.message/d1", "?org.matrix.msc4140.delay=5000", "d1", `{"msgtype":"m.text", "body":"later"}`, req)
	if out.Result != "txn_cached" || gjson.Get(rec.Body.String(), "delay_id").Str != id || len(h.w.delayed) != 1 {
		t.Errorf("retry %+v %s", out, rec.Body)
	}
}

func TestDelayedRefusals(t *testing.T) {
	stateReq := route.Request{Endpoint: route.State, Write: true, Room: room, Action: "m.room.topic"}
	memberReq := route.Request{Endpoint: route.State, Write: true, Room: room, Action: "m.room.member", StateKey: user}
	for name, tc := range map[string]struct {
		path, query, body string
		req               route.Request
		want              string
	}{
		"too long": {"/state/m.room.topic/", "?org.matrix.msc4140.delay=86400001", `{"topic":"x"}`, stateReq,
			`400 {"errcode":"ORG.MATRIX.MSC4140_DELAY_TOO_LARGE","error":"The requested delay (86400001ms) exceeds the allowed maximum (86400000ms)"}`},
		"negative": {"/state/m.room.topic/", "?org.matrix.msc4140.delay=-1", `{"topic":"x"}`, stateReq,
			`400 {"errcode":"M_INVALID_PARAM","error":"Query parameter org.matrix.msc4140.delay must be an integer greater than zero."}`},
		"zero": {"/state/m.room.topic/", "?org.matrix.msc4140.delay=0", `{"topic":"x"}`, stateReq,
			`400 {"errcode":"M_INVALID_PARAM","error":"Query parameter org.matrix.msc4140.delay must be an integer greater than zero."}`},
		"not an integer": {"/state/m.room.topic/", "?org.matrix.msc4140.delay=x", `{"topic":"x"}`, stateReq,
			`400 {"errcode":"M_INVALID_PARAM","error":"Query parameter org.matrix.msc4140.delay must be an integer"}`},
		"sticky zero": {"/send/m.room.message/s0", "?org.matrix.msc4354.sticky_duration_ms=0", `{"body":"x"}`,
			route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: "s0"},
			`400 {"errcode":"M_INVALID_PARAM","error":"org.matrix.msc4354.sticky_duration_ms must be strictly positive"}`},
		"bad topic": {"/state/m.room.topic/", "?org.matrix.msc4140.delay=1", `{"topic":1}`, stateReq,
			`400 {"errcode":"M_UNKNOWN","error":"'topic' not a string type"}`},
		"member without membership": {"/state/m.room.member/" + user, "?org.matrix.msc4140.delay=1", `{"displayname":"x"}`, memberReq,
			`400 {"errcode":"M_UNKNOWN","error":"Content has not membership key"}`},
		"member on send": {"/send/m.room.member/m1", "?org.matrix.msc4140.delay=1", `{"membership":"join"}`,
			route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.member", TxnID: "m1"},
			`400 {"errcode":"M_UNKNOWN","error":"'m.room.member' must be state events"}`},
	} {
		h := newHarness(t)
		rec, out := h.delayed(t, tc.path, tc.query, "", tc.body, tc.req)
		if got := itoaStatus(rec.Code) + " " + rec.Body.String(); out.Result != "refused" || got != tc.want {
			t.Errorf("%s: %+v %s", name, out, got)
		}
	}
	// The per-user limit: LimitExceededError, retry_after_ms null.
	h := newHarness(t)
	for i := 0; i < 3; i++ {
		rec, out := h.delayed(t, "/state/m.room.topic/", "?org.matrix.msc4140.delay=1", "", `{"topic":"x"}`, stateReq)
		if i == 2 && (out.Refused != "delayed_limit" ||
			rec.Body.String() != `{"errcode":"M_LIMIT_EXCEEDED","error":"The maximum number of delayed events has been reached.","retry_after_ms":null}`) {
			t.Errorf("%+v %s", out, rec.Body)
		}
	}
	// A delayed member event is kept as sent.
	h = newHarness(t)
	if _, out := h.delayed(t, "/state/m.room.member/"+user, "?org.matrix.msc4140.delay=10", "", `{"membership":"leave"}`, memberReq); out.Result != "delayed" {
		t.Fatalf("%+v", out)
	}
	if row := h.w.delayed[0]; *row.StateKey != user || row.Content != `{"membership":"leave"}` {
		t.Errorf("%+v", row)
	}
}

// Delayed events off (msc4140_enabled false): the parameter is refused
// before anything else of the event is looked at; without it, nothing
// changes.
func TestDelayedDisabled(t *testing.T) {
	h := newHarness(t)
	h.c.o.Delayed.MaxDelayMS = 0
	stateReq := route.Request{Endpoint: route.State, Write: true, Room: room, Action: "m.room.topic"}
	rec, out := h.delayed(t, "/state/m.room.topic/", "?org.matrix.msc4140.delay=-1", "", `{"topic":1}`, stateReq)
	if got := itoaStatus(rec.Code) + " " + rec.Body.String(); out.Refused != "delayed_disabled" ||
		got != `403 {"errcode":"M_FORBIDDEN","error":"Sending delayed events is disabled on this homeserver"}` {
		t.Errorf("%+v %s", out, got)
	}
	if _, out := h.delayed(t, "/state/m.room.topic/", "", "", `{"topic":"x"}`, stateReq); out.Result != "created" {
		t.Errorf("without delay: %+v", out)
	}
}

// v1.164: a delayed event that could not be sent now is refused, in the
// auth rules' words, and nothing is stored. A change to one's own
// membership is not judged until it is sent.
func TestDelayedMustBeSendableNow(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.power_levels", "", `{"users":{"`+other+`":100},"state_default":50}`)
	stateReq := route.Request{Endpoint: route.State, Write: true, Room: room, Action: "m.room.topic"}
	rec, out := h.delayed(t, "/state/m.room.topic/", "?org.matrix.msc4140.delay=1000", "", `{"topic":"x"}`, stateReq)
	if out.Result != "refused" || rec.Code != 403 ||
		rec.Body.String() != `{"errcode":"M_FORBIDDEN","error":"You don't have permission to post that to the room. user_level (0) < send_level (50)"}` {
		t.Errorf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if len(h.w.delayed) != 0 || len(h.main.delayed) != 0 {
		t.Errorf("stored %v, main told %v", h.w.delayed, h.main.delayed)
	}

	memberReq := route.Request{Endpoint: route.State, Write: true, Room: room, Action: "m.room.member", StateKey: user}
	if _, out := h.delayed(t, "/state/m.room.member/"+user, "?org.matrix.msc4140.delay=1000", "", `{"membership":"leave"}`, memberReq); out.Result != "delayed" {
		t.Errorf("own leave: %+v", out)
	}

	// A message is still allowed (events_default is 0).
	sendReq := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: "ok1"}
	if _, out := h.delayed(t, "/send/m.room.message/ok1", "?org.matrix.msc4140.delay=1000", "", `{"msgtype":"m.text","body":"x"}`, sendReq); out.Result != "delayed" {
		t.Errorf("message: %+v", out)
	}
}

// create_event's refusal of a suspended requester, which add now reaches.
func TestDelayedSuspended(t *testing.T) {
	h := newHarness(t)
	h.f.token.Suspended = true
	sendReq := route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: "su1"}
	rec, out := h.delayed(t, "/send/m.room.message/su1", "?org.matrix.msc4140.delay=1000", "", `{"msgtype":"m.text","body":"x"}`, sendReq)
	if out.Result != "refused" || rec.Code != 403 || !strings.Contains(rec.Body.String(), "Sending messages while account is suspended is not allowed.") {
		t.Errorf("%+v %d %s", out, rec.Code, rec.Body)
	}
}
