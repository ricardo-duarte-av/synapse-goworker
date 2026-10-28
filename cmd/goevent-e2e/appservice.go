package main

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/tidwall/gjson"
)

// appservice runs the suite's appservice steps with the goevent-e2e
// registration's as_token: double puppeting of the test account (with and
// without its device), a ghost's join, message and leave, and Synapse's
// refusals of what an appservice may not do. The ghost is always the same
// user, so runs leave no new ones behind.
func (s *suite) appservice(token, tag string) {
	if token == "" {
		s.skip("appservice", "GOEVENT_E2E_AS_TOKEN is not set")
		return
	}
	as := &client{base: s.c.base, token: token, http: s.c.http}
	domain := strings.SplitN(s.me, ":", 2)[1]
	ghost := "@goevent-e2e-ghost:" + domain
	as1 := func(user, extra string) string { return "?user_id=" + url.QueryEscape(user) + extra }
	sendPath := func(name string) string { return s.roomPath("/send/m.room.message/" + esc(s.txn(name))) }
	msg := func(what string) map[string]any {
		return map[string]any{"msgtype": "m.notice", "body": tag + ": " + what}
	}

	// Double puppeting, as bridges do it: the test account, with a ts.
	const ts = "1700000000000"
	r := as.do(http.MethodPut, sendPath("as puppet")+as1(s.me, "&ts="+ts), msg("double puppet"), true)
	id := r.body.Get("event_id").Str
	ev := s.c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
	s.record("appservice double puppet", r.status == 200 && ev.body.Get("sender").Str == s.me &&
		ev.body.Get("origin_server_ts").Raw == ts && ev.body.Get("content.body").Str == tag+": double puppet",
		"%d %s; sender %s ts %s", r.status, id, ev.body.Get("sender").Str, ev.body.Get("origin_server_ts").Raw)
	again := as.do(http.MethodPut, sendPath("as puppet")+as1(s.me, "&ts="+ts), msg("double puppet"), true)
	s.record("appservice txn retry", again.status == 200 && id != "" && again.body.Get("event_id").Str == id,
		"%d %s (want %s)", again.status, again.body.Get("event_id").Str, id)

	// Device masquerading (MSC3202): the suite's own device, whose
	// transactions it then sees as its own.
	r = as.do(http.MethodPut, sendPath("as device")+as1(s.me, "&device_id="+url.QueryEscape(s.device)), msg("device"), true)
	id = r.body.Get("event_id").Str
	ev = s.c.do(http.MethodGet, s.roomPath("/event/"+esc(id)), nil, true)
	s.record("appservice device masquerade", r.status == 200 && ev.body.Get("unsigned.transaction_id").Str == s.txn("as device"),
		"%d %s; transaction_id %q", r.status, id, ev.body.Get("unsigned.transaction_id").Str)

	// A ghost, as a bridge's: registered (Synapse's), invited by the test
	// account, then it joins, speaks and leaves through the worker.
	reg := as.do(http.MethodPost, "/_matrix/client/v3/register", map[string]any{
		"type": "m.login.application_service", "username": "goevent-e2e-ghost", "inhibit_login": true}, true)
	s.record("appservice ghost registered", reg.status == 200 || reg.body.Get("errcode").Str == "M_USER_IN_USE",
		"%d %s", reg.status, reg.raw)
	cur := s.memberEvent(ghost, func(gjson.Result) bool { return true }).Get("content.membership").Str
	if cur != "join" && cur != "invite" {
		inv := s.c.do(http.MethodPost, s.roomPath("/invite"), map[string]any{"user_id": ghost}, true)
		s.record("appservice ghost invited", inv.status == 200, "%d %s", inv.status, inv.raw)
	}
	join := as.do(http.MethodPost, "/_matrix/client/v3/join/"+esc(s.room)+as1(ghost, ""), map[string]any{}, true)
	member := s.memberEvent(ghost, func(e gjson.Result) bool { return e.Get("content.membership").Str == "join" })
	s.record("appservice ghost joins", join.status == 200 && member.Get("sender").Str == ghost, "%d %s; %s", join.status, join.raw, member.Get("content").Raw)
	r = as.do(http.MethodPut, sendPath("as ghost")+as1(ghost, ""), msg("ghost"), true)
	ev = s.c.do(http.MethodGet, s.roomPath("/event/"+esc(r.body.Get("event_id").Str)), nil, true)
	s.record("appservice ghost message", r.status == 200 && ev.body.Get("sender").Str == ghost, "%d %s", r.status, ev.body.Get("sender").Str)
	leave := as.do(http.MethodPost, s.roomPath("/leave")+as1(ghost, ""), map[string]any{}, true)
	member = s.memberEvent(ghost, func(e gjson.Result) bool { return e.Get("content.membership").Str == "leave" })
	s.record("appservice ghost leaves", leave.status == 200 && member.Get("content.membership").Str == "leave", "%d %s", leave.status, leave.raw)

	// What an appservice may not do, in Synapse's words.
	s.expectError("refuse appservice outside its namespace", as.do(http.MethodPut, sendPath("as outside")+as1("@nobody:elsewhere.example", ""),
		msg("x"), true), 403, "M_FORBIDDEN")
	s.expectError("refuse appservice unregistered user", as.do(http.MethodPut, sendPath("as unregistered")+as1("@goevent-e2e-ghost-unregistered:"+domain, ""),
		msg("x"), true), 403, "M_FORBIDDEN")
	s.expectError("refuse appservice unknown device", as.do(http.MethodPut, sendPath("as nodevice")+as1(s.me, "&device_id=NOSUCHDEVICE"),
		msg("x"), true), 400, "M_UNKNOWN_DEVICE")
	s.expectError("refuse appservice bad ts", as.do(http.MethodPut, sendPath("as badts")+as1(s.me, "&ts=-1"), msg("x"), true), 400, "M_INVALID_PARAM")
	s.expectError("refuse appservice sender not in room", as.do(http.MethodPut, sendPath("as sender"), msg("x"), true), 403, "M_FORBIDDEN")
}
