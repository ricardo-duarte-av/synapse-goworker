package creator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

const other = "@v:example.org"

func memberReq(action, txn string) route.Request {
	return route.Request{Endpoint: route.Membership, Write: true, Room: room, Action: action, TxnID: txn}
}

func (h *harness) member(t *testing.T, req route.Request, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	path := "/_matrix/client/v3/rooms/" + room + "/" + req.Action
	method := http.MethodPost
	if req.Endpoint == route.Join {
		path = "/_matrix/client/v3/join/" + room
	}
	if req.TxnID != "" {
		path += "/" + req.TxnID
		method = http.MethodPut
	}
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, req)
}

func str(s string) *string { return &s }

func TestNativeInvite(t *testing.T) {
	h := newHarness(t)
	h.f.profiles = map[string][2]*string{other: {str("Vee"), nil}}
	rec, out := h.member(t, memberReq("invite", "i1"), `{"user_id":"`+other+`","reason":"come"}`)
	if out.Result != "created" || rec.Code != 200 || rec.Body.String() != "{}" {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,state_group,stage" {
		t.Errorf("writes %s", got)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.type":               `"m.room.member"`,
		"event.state_key":          `"` + other + `"`,
		"event.content":            `{"displayname":"Vee","membership":"invite","reason":"come"}`,
		"extra_users":              `["` + other + `"]`,
		"internal_metadata.txn_id": `"i1"`,
		"context.state_group":      "8",
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("event.unsigned.replaces_state").Exists() {
		t.Error("replaces_state for a first member event")
	}
	// The members auth events: create, power levels, the sender's and the
	// join rules (none here).
	if n := e.Get("event.auth_events.#").Int(); n != 3 {
		t.Errorf("%d auth events", n)
	}

	// The same transaction again answers from the cache, writing nothing.
	h.w.calls = nil
	rec, out = h.member(t, memberReq("invite", "i1"), `{"user_id":"`+other+`","reason":"come"}`)
	if out.Result != "txn_cached" || rec.Body.String() != "{}" || len(h.w.calls) != 0 {
		t.Fatalf("retry: %+v %s %v", out, rec.Body, h.w.calls)
	}
}

func TestJoinWhileJoinedIsANoop(t *testing.T) {
	h := newHarness(t)
	for _, req := range []route.Request{memberReq("join", ""), {Endpoint: route.Join, Write: true, Room: room}} {
		rec, out := h.member(t, req, ``)
		if out.Result != "state_dedup" || rec.Body.String() != `{"room_id":"`+room+`"}` {
			t.Fatalf("%s: %+v %s", req.Endpoint, out, rec.Body)
		}
	}
	if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
		t.Errorf("a no-op join wrote: %v", h.w.calls)
	}
}

func TestJoinWithReasonAddsProfile(t *testing.T) {
	h := newHarness(t)
	h.f.profiles = map[string][2]*string{user: {str("You"), str("mxc://example.org/a")}}
	rec, out := h.member(t, route.Request{Endpoint: route.Join, Write: true, Room: room}, `{"reason":"back",`+
		`"join_authorised_via_users_server":"@x:example.org"}`)
	if out.Result != "created" || rec.Body.String() != `{"room_id":"`+room+`"}` {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if got := e.Get("event.content").Raw; got != `{"avatar_url":"mxc://example.org/a","displayname":"You","membership":"join","reason":"back"}` {
		t.Errorf("content %s", got)
	}
	if got := e.Get("event.unsigned.replaces_state").Str; got == "" {
		t.Error("no replaces_state for a join over a join")
	}
}

func TestKickTellsMain(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.member", other, `{"membership":"join"}`)
	rec, out := h.member(t, memberReq("kick", ""), `{"user_id":"`+other+`"}`)
	if out.Result != "created" || rec.Body.String() != "{}" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if got := e.Get("event.content").Raw; got != `{"membership":"leave"}` {
		t.Errorf("content %s", got)
	}
	if len(h.main.left) != 1 || h.main.left[0] != room+" "+other {
		t.Errorf("main told %v", h.main.left)
	}

	// A ban of someone who already left: main is not told again.
	h2 := newHarness(t)
	h2.f.addState(t, "m.room.member", other, `{"membership":"leave"}`)
	if _, out := h2.member(t, memberReq("ban", ""), `{"user_id":"`+other+`"}`); out.Result != "created" {
		t.Fatalf("ban: %+v", out)
	}
	if len(h2.main.left) != 0 {
		t.Errorf("main told of a ban after a leave: %v", h2.main.left)
	}
}

func TestMembershipFallbacks(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(h *harness)
		req   route.Request
		body  string
		want  string
	}{
		"kick of absent user": {nil, memberReq("kick", ""), `{"user_id":"` + other + `"}`, "refused:kick_absent"},
		"kick of a leave": {func(h *harness) { h.f.addState(t, "m.room.member", other, `{"membership":"leave"}`) },
			memberReq("kick", ""), `{"user_id":"` + other + `"}`, "refused:kick_absent"},
		"unban not banned": {func(h *harness) { h.f.addState(t, "m.room.member", other, `{"membership":"leave"}`) },
			memberReq("unban", ""), `{"user_id":"` + other + `"}`, "refused:unban_not_banned"},
		"invite of banned": {func(h *harness) { h.f.addState(t, "m.room.member", other, `{"membership":"ban"}`) },
			memberReq("invite", ""), `{"user_id":"` + other + `"}`, "refused:target_banned"},
		"missing user_id":       {nil, memberReq("ban", ""), `{}`, "refused:missing_user_id"},
		"bad user_id":           {nil, memberReq("ban", ""), `{"user_id":"nope"}`, "refused:bad_user_id"},
		"user_id number":        {nil, memberReq("ban", ""), `{"user_id":1}`, "refused:bad_user_id"},
		"3pid invite":           {nil, memberReq("invite", ""), `{"medium":"email","address":"a@b"}`, "refused:3pid_invite"},
		"3pid invite, complete": {nil, memberReq("invite", ""), `{"medium":"email","address":"a@b","id_server":"x","id_access_token":"y"}`, "3pid_invite"},
		"third party signed":    {nil, memberReq("join", ""), `{"third_party_signed":{}}`, "third_party_signed"},
		"invite config": {func(h *harness) { h.f.accountData = map[string]bool{other + " " + invitePermissionConfig: true} },
			memberReq("invite", ""), `{"user_id":"` + other + `"}`, "invite_config"},
		"blocked room":  {func(h *harness) { h.f.blocked = true }, memberReq("join", ""), ``, "refused:room_blocked"},
		"not an object": {nil, memberReq("leave", ""), `[]`, "refused:not_object"},
		"restricted join": {func(h *harness) {
			h.f.addState(t, "m.room.join_rules", "", `{"join_rule":"restricted","allow":[]}`)
			h.f.token.UserID = user
		}, memberReq("join", ""), ``, ""},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			_, out := h.member(t, tc.req, tc.body)
			if tc.want == "" {
				// The joined user rejoining a restricted room needs no
				// authorising server: a no-op here.
				if out.Fallback != "" {
					t.Fatalf("fallback %q", out.Fallback)
				}
				return
			}
			if stopped(out) != tc.want {
				t.Fatalf("stopped %q, want %q", stopped(out), tc.want)
			}
			if len(h.w.calls) != 0 || len(h.p.entries) != 0 {
				t.Errorf("a fallback wrote: %v", h.w.calls)
			}
		})
	}
}

func TestInvitesAreRatelimited(t *testing.T) {
	h := newHarness(t)
	h.c.o.Membership.InvitesPerIssuer.Burst = 1
	if _, out := h.member(t, memberReq("invite", ""), `{"user_id":"`+other+`"}`); out.Result != "created" {
		t.Fatalf("first invite: %+v", out)
	}
	rec, out := h.member(t, memberReq("invite", ""), `{"user_id":"@w:example.org"}`)
	if out.Result != "ratelimited" || rec.Code != 429 || gjson.Get(rec.Body.String(), "errcode").Str != "M_LIMIT_EXCEEDED" ||
		rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second invite: %+v %d %s", out, rec.Code, rec.Body)
	}

	// An exempt user is not limited.
	h.f.exempt = true
	if _, out := h.member(t, memberReq("invite", ""), `{"user_id":"@w:example.org"}`); out.Result != "created" {
		t.Fatalf("exempt invite: %+v", out)
	}
}

func TestCallInviteInPublicRoom(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.join_rules", "", `{"join_rule":"public"}`)
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.call.invite/c", strings.NewReader(`{"call_id":"x"}`))
	r.Header.Set("Authorization", "Bearer tok")
	out := h.c.Serve(httptest.NewRecorder(), r, route.Request{Endpoint: route.Send, Room: room, Action: "m.call.invite", TxnID: "c"})
	if out.Refused != "call_invite_public" {
		t.Fatalf("%+v", out)
	}
}

// A repeated key is Python's: the last value.
func TestRepeatedKeyIsTheLast(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.member", other, `{"membership":"join"}`)
	if _, out := h.member(t, memberReq("kick", ""), `{"user_id":"@nobody:example.org","reason":"a","user_id":"`+other+`","reason":"b"}`); out.Result != "created" {
		t.Fatalf("%+v", out)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if e.Get("event.state_key").Str != other || e.Get("event.content").Raw != `{"membership":"leave","reason":"b"}` {
		t.Errorf("%s %s", e.Get("event.state_key").Raw, e.Get("event.content").Raw)
	}
}

// A shadow-banned invite is pretended; other membership goes through.
func TestShadowBannedMembership(t *testing.T) {
	h := newHarness(t)
	h.f.token.ShadowBan = true
	rec, out := h.member(t, memberReq("invite", ""), `{"user_id":"`+other+`"}`)
	if out.Result != "shadow_banned" || rec.Body.String() != "{}" || len(h.p.entries) != 0 {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	h.f.addState(t, "m.room.member", other, `{"membership":"join"}`)
	if _, out := h.member(t, memberReq("kick", ""), `{"user_id":"`+other+`"}`); out.Result != "created" {
		t.Errorf("kick: %+v", out)
	}
	// The requester says so.
	if gjson.GetBytes(h.p.entries[0], "requester.shadow_banned").Raw != "true" {
		t.Errorf("requester %s", gjson.GetBytes(h.p.entries[0], "requester").Raw)
	}
}

// A guest joins only where guest_access is can_join.
func TestGuestJoin(t *testing.T) {
	h := newHarness(t)
	h.f.token.Guest, h.f.token.UserID = true, "@g:example.org"
	rec, out := h.member(t, memberReq("join", ""), ``)
	if out.Refused != "guest" || rec.Code != 403 {
		t.Fatalf("%+v %s", out, rec.Body)
	}
}

func stateMemberReq(target string) route.Request {
	return route.Request{Endpoint: route.State, Write: true, Room: room, Action: "m.room.member", StateKey: target}
}

func (h *harness) stateMember(t *testing.T, target, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/state/m.room.member/"+target, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, stateMemberReq(target))
}

// A per-room nickname through the state servlet: the body is the content,
// the global avatar fills in what it leaves out, and the answer is the
// event ID.
func TestStateMemberPerRoomName(t *testing.T) {
	h := newHarness(t)
	h.f.profiles = map[string][2]*string{user: {str("Global"), str("mxc://example.org/a")}}
	rec, out := h.stateMember(t, user, `{"membership":"join","displayname":"Room nick"}`)
	if out.Result != "created" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	if got := e.Get("event.content").Raw; got != `{"avatar_url":"mxc://example.org/a","displayname":"Room nick","membership":"join"}` {
		t.Errorf("content %s", got)
	}
	if gjson.Get(rec.Body.String(), "event_id").Str != e.Get("event.event_id").Str && gjson.Get(rec.Body.String(), "event_id").Str == "" {
		t.Errorf("answer %s", rec.Body)
	}
}

// update_membership_locked's own no-op: the same sender, membership and
// content, compared as Python compares (true == 1).
func TestStateMemberNoop(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.member", user, `{"membership":"join","displayname":"n","x":1}`)
	old := h.f.state[pdu.StateKey{Type: "m.room.member", StateKey: user}]
	rec, out := h.stateMember(t, user, `{"membership":"join","displayname":"n","x":true}`)
	if out.Result != "membership_noop" || gjson.Get(rec.Body.String(), "event_id").Str != old {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if len(h.p.entries) != 0 {
		t.Error("a no-op wrote")
	}
}

func TestStateMemberRefusals(t *testing.T) {
	long := strings.Repeat("a", 257)
	for body, want := range map[string]string{
		`{}`:                     `400 {"errcode":"M_UNKNOWN","error":"Invalid membership (must be a string)"}`,
		`{"membership":1}`:       `400 {"errcode":"M_UNKNOWN","error":"Invalid membership (must be a string)"}`,
		`{"membership":"dance"}`: `400 {"errcode":"M_UNKNOWN","error":"Invalid membership key"}`,
		`{"membership":"join","displayname":"` + long + `"}`: `400 {"errcode":"M_BAD_JSON","error":"Displayname is too long (max 256)"}`,
		`{"membership":"join","displayname":5}`:              `500 {"errcode":"M_UNKNOWN","error":"Internal server error"}`,
		`{"membership":"join","displayname":0}`:              ``,
		``:                                                   `400 {"errcode":"M_NOT_JSON","error":"Content not JSON."}`,
	} {
		h := newHarness(t)
		rec, out := h.stateMember(t, user, body)
		got := ""
		if out.Refused != "" {
			got = strconv.Itoa(rec.Code) + " " + rec.Body.String()
		}
		if got != want {
			t.Errorf("%.60s: %+v %q, want %q", body, out, got, want)
		}
	}
	h := newHarness(t)
	rec, out := h.stateMember(t, "nobody", `{"membership":"join"}`)
	if out.Refused != "bad_user_id" || !strings.Contains(rec.Body.String(), "Expected UserID string to start with '@'") {
		t.Errorf("%+v %s", out, rec.Body)
	}
}

// A per-room name on /join is served too.
func TestJoinWithPerRoomName(t *testing.T) {
	h := newHarness(t)
	rec, out := h.member(t, route.Request{Endpoint: route.Join, Write: true, Room: room}, `{"displayname":"x"}`)
	if out.Result != "created" {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if got := gjson.GetBytes(h.p.entries[0], "event.content").Raw; got != `{"displayname":"x","membership":"join"}` {
		t.Errorf("content %s", got)
	}
}

type fakeRemote map[string]gjson.Result

func (f fakeRemote) ProfileField(_ context.Context, userID, field string) (gjson.Result, bool) {
	v, ok := f[userID+" "+field]
	return v, ok
}

// A remote invitee's profile comes from their server, a field at a time;
// a failed query leaves the rest out, as Synapse's try does.
func TestRemoteInvite(t *testing.T) {
	for name, tc := range map[string]struct {
		remote fakeRemote
		want   string
	}{
		"both": {fakeRemote{"@x:remote.example displayname": gjson.Parse(`"Ex"`), "@x:remote.example avatar_url": gjson.Parse(`"mxc://remote.example/a"`)},
			`{"avatar_url":"mxc://remote.example/a","displayname":"Ex","membership":"invite"}`},
		"no displayname": {fakeRemote{"@x:remote.example displayname": gjson.Parse(`null`), "@x:remote.example avatar_url": gjson.Result{}},
			`{"membership":"invite"}`},
		"server down": {fakeRemote{}, `{"membership":"invite"}`},
	} {
		h := newHarness(t)
		h.c.o.RemoteProfiles = tc.remote
		if _, out := h.member(t, memberReq("invite", ""), `{"user_id":"@x:remote.example"}`); out.Result != "created" {
			t.Fatalf("%s: %+v", name, out)
		}
		if got := gjson.GetBytes(h.p.entries[0], "event.content").Raw; got != tc.want {
			t.Errorf("%s: %s", name, got)
		}
	}
	h := newHarness(t)
	if _, out := h.member(t, memberReq("invite", ""), `{"user_id":"@x:remote.example"}`); out.Fallback != "remote_profile" {
		t.Errorf("without a federation client: %+v", out)
	}
}

func (h *harness) knock(t *testing.T, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/knock/"+room, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, route.Request{Endpoint: route.Knock, Write: true, Room: room})
}

// A knock on a knockable room this server is in: a local event with the
// knocker's profile and reason, answered with the room.
func TestKnock(t *testing.T) {
	h := newHarness(t)
	h.f.addState(t, "m.room.join_rules", "", `{"join_rule":"knock"}`)
	h.f.token.UserID = "@k:example.org"
	h.f.profiles = map[string][2]*string{"@k:example.org": {str("Kay"), nil}}
	rec, out := h.knock(t, `{"reason":"let me in"}`)
	if out.Result != "created" || rec.Body.String() != `{"room_id":"`+room+`"}` {
		t.Fatalf("%+v %s", out, rec.Body)
	}
	if got := gjson.GetBytes(h.p.entries[0], "event.content").Raw; got != `{"displayname":"Kay","membership":"knock","reason":"let me in"}` {
		t.Errorf("content %s", got)
	}
	// The joined user cannot knock: the auth rules say so.
	h = newHarness(t)
	h.f.addState(t, "m.room.join_rules", "", `{"join_rule":"knock"}`)
	rec, out = h.knock(t, `{}`)
	if out.Refused != "auth_rules" || !strings.Contains(rec.Body.String(), "You cannot knock on a room you are already in") {
		t.Errorf("%+v %s", out, rec.Body)
	}
	// No body is no JSON.
	if rec, out := newHarness(t).knock(t, ``); out.Refused != "not_json" {
		t.Errorf("%+v %s", out, rec.Body)
	}
}
