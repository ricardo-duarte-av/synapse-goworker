package creator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
)

// Membership: RoomMembershipRestServlet and JoinRoomAliasServlet
// (rest/client/room.py), then RoomMemberHandler.update_membership and
// update_membership_locked (handlers/room_member.py, v1.163.0), for a local
// membership change in a room this server is in. Remote joins, knocks,
// invite rejections over federation, 3pid invites and anything a module
// would judge are Synapse's.

// Account data types that filter invites (api/constants.py).
const (
	invitePermissionConfig        = "m.invite_permission_config"
	msc4155InvitePermissionConfig = "org.matrix.msc4155.invite_permission_config"
	msc4293RedactEvents           = "org.matrix.msc4293.redact_events"
	authorisingUser               = "join_authorised_via_users_server"
)

// MembershipNotifier is main, told that a user left a room
// (ReplicationUserJoinedLeftRoomRestServlet) or that a delayed event was
// added (ReplicationAddedDelayedEventRestServlet).
type MembershipNotifier interface {
	UserLeftRoom(ctx context.Context, roomID, userID string) error
	DelayedEventAdded(ctx context.Context, nextSendTS int64) error
}

// memberInput is what a membership request asks for.
type memberInput struct {
	// action is the servlet's: join, invite, leave, ban, unban or kick.
	action string
	// membership is the effective membership: kick and unban are leave.
	membership string
	target     string
	// join answers {"room_id"}; the other actions {}; fromState, the
	// state servlet, {"event_id"}.
	join      bool
	fromState bool
	// content is the content being built, and compare the content
	// update_membership_locked compares with the old member event's.
	content map[string]json.RawMessage
	compare []byte
	// leftFrom is the target's membership before a leave, once known.
	leftFrom string
}

// membershipLimiters are update_membership's ratelimiters.
type membershipLimiters struct {
	joinsLocal, invitesPerRoom, invitesPerUser, invitesPerIssuer *ratelimit.Limiter
}

func (c *Creator) limiters() *membershipLimiters {
	c.limitersOnce.Do(func() {
		m := c.o.Membership
		c.lim = &membershipLimiters{
			joinsLocal:       ratelimit.New(m.JoinsLocal, c.o.Now),
			invitesPerRoom:   ratelimit.New(m.InvitesPerRoom, c.o.Now),
			invitesPerUser:   ratelimit.New(m.InvitesPerUser, c.o.Now),
			invitesPerIssuer: ratelimit.New(m.InvitesPerIssuer, c.o.Now),
		}
	})
	return c.lim
}

// serveMembership serves the membership servlets.
func (c *Creator) serveMembership(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	q := r.URL.Query()
	if req.Endpoint == route.Knock && (r.Method != http.MethodPost || req.TxnID != "") {
		// KnockRoomAliasServlet has on_POST only.
		return fallback("method")
	}
	if reason := c.configFallback(); reason != "" {
		// user_may_invite, user_may_join_room, check_event_allowed.
		return fallback(reason)
	}

	body, tooLarge, err := readBody(r)
	switch {
	case err != nil:
		return fallback("read_body")
	case tooLarge:
		return fallback("too_large")
	}

	ctx, cancel := context.WithTimeout(r.Context(), c.o.Timeout)
	defer cancel()
	rq, reason, ref := c.requester(ctx, r)
	switch {
	case ref != nil:
		return c.refuse(w, ref)
	case reason != "":
		return fallback(reason)
	}

	// fetch_or_execute_request: a transaction that succeeded here answers as
	// it did, before anything else is looked at.
	var txnKey string
	if req.TxnID != "" {
		txnKey = rq.txnKey(r)
		if c.txns.seenAt(txnKey, c.o.Now()) {
			writeMembershipAnswer(w, req)
			return Outcome{Result: "txn_cached"}
		}
	}
	if rq.Guest && req.Endpoint == route.Knock {
		// get_user_by_req(allow_guest=False).
		return c.refuse(w, &refusal{reason: "guest", status: http.StatusForbidden, errcode: "M_GUEST_ACCESS_FORBIDDEN",
			msg: "Guest access not allowed"})
	}
	if rq.Guest && req.Endpoint == route.Membership && req.Action != "join" && req.Action != "leave" {
		return c.refuse(w, forbidden("guest", "Guest access not allowed"))
	}

	// The state and knock servlets take no empty body.
	body, st := parseJSONObject(body, req.Endpoint != route.State && req.Endpoint != route.Knock)
	if st != nil {
		return c.end(w, st)
	}
	var ts *int64
	if req.Endpoint == route.State {
		probe := &input{}
		if st := appserviceTS(rq, q, probe); st != nil {
			return c.end(w, st)
		}
		ts = probe.originTS
		if st := parseDelay(q, probe); st != nil {
			return c.end(w, st)
		}
		if probe.delayMS != nil {
			// The state servlet adds a delayed member event as it is.
			sk := req.StateKey
			in := &input{typ: "m.room.member", stateKey: &sk, content: body, room: req.Room, originTS: ts}
			return c.addDelayed(ctx, w, rq, in, *probe.delayMS, "")
		}
	}
	in, st := c.parseMembership(req, rq, body)
	if st != nil {
		return c.end(w, st)
	}
	in.originTS = ts

	if out, limited := c.ratelimitMembership(ctx, w, req, rq, in.member); limited {
		return out
	}
	if rq.ShadowBanned && in.member.action == "invite" {
		// update_membership: ShadowBanError, and the servlet pretends.
		c.shadowPause(ctx)
		if in.member.fromState {
			writeEventID(w, fakeEventID())
		} else {
			writeMembershipAnswer(w, req)
		}
		c.txns.add(txnKey, c.o.Now())
		return Outcome{Result: "shadow_banned"}
	}
	if st := c.checkMembershipRequest(ctx, req, rq, in); st != nil {
		return c.end(w, st)
	}

	unlock := c.locks.lock(req.Room)
	defer unlock()

	p, dup, st := c.prepare(ctx, req, rq, in, gjson.ParseBytes(in.content))
	if st != nil {
		return c.end(w, st)
	}
	if dup != "" {
		c.respond(w, in, dup)
		c.txns.add(txnKey, c.o.Now())
		return Outcome{Result: in.dedupKind}
	}
	out := c.persist(ctx, w, p)
	if out.Result == "created" || out.Result == "writer_dedup" {
		c.txns.add(txnKey, c.o.Now())
	}
	if out.Result == "created" && in.member.membership == "leave" && in.member.leftFrom == "join" && c.o.Main != nil {
		// _user_left_room, on main.
		if err := c.o.Main.UserLeftRoom(context.WithoutCancel(ctx), req.Room, in.member.target); err != nil {
			c.o.Log.Warn().Err(err).Str("room_id", req.Room).Str("user_id", in.member.target).
				Msg("Could not tell main a user left a room")
		}
	}
	return out
}

// writeMembershipAnswer is the servlets' answer: {"room_id"} for a join,
// {} otherwise.
func writeMembershipAnswer(w http.ResponseWriter, req route.Request) {
	out := map[string]string{}
	if req.Endpoint == route.Join || req.Endpoint == route.Knock || req.Action == "join" {
		out["room_id"] = req.Room
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// targetUserID is UserID.from_string(request_body["user_id"]) as the
// membership servlet calls it: Synapse's 400s, its TypeErrors (500), and a
// fallback for what it accepts but is no user ID here.
func targetUserID(v gjson.Result) (string, *stop) {
	start := badRequest("bad_user_id", "Expected UserID string to start with '@'", "M_INVALID_PARAM").stop()
	switch {
	case v.Type == gjson.String:
		if loneSurrogate(v.Raw) {
			return "", fb("surrogate_escape")
		}
		if !strings.HasPrefix(v.Str, "@") {
			return "", start
		}
		if !strings.Contains(v.Str, ":") {
			return "", badRequest("bad_user_id", "Expected UserID of the form '@localname:domain'", "M_INVALID_PARAM").stop()
		}
		if !validUserID(v.Str) {
			return "", fb("bad_user_id")
		}
		return v.Str, nil
	case v.IsArray():
		// len() works; a list never equals "@".
		return "", start
	case v.IsObject():
		if len(v.Map()) == 0 {
			return "", start
		}
		// s[0:1] on a dict.
		return "", internalError("bad_user_id").stop()
	}
	// len() of a number, a bool or None.
	return "", internalError("bad_user_id").stop()
}

// parseMembership is the servlets: RoomMembershipRestServlet's action and
// body, JoinRoomAliasServlet's body as the content, or
// RoomStateEventRestServlet's for m.room.member, whose body is the content
// and its membership the action.
func (c *Creator) parseMembership(req route.Request, rq *requester, body []byte) (*input, *stop) {
	b := gjson.ParseBytes(body)
	m := &memberInput{action: req.Action, target: rq.UserID, content: map[string]json.RawMessage{}}
	whole := func() {
		b.ForEach(func(k, v gjson.Result) bool {
			m.content[k.Str] = json.RawMessage(v.Raw)
			return true
		})
	}

	switch req.Endpoint {
	case route.Join:
		m.action, m.join = "join", true
		whole()
	case route.Knock:
		// KnockRoomAliasServlet: only the reason.
		m.action, m.join = "knock", true
		if v, ok := pdu.Field(b, "reason"); ok {
			m.content["reason"] = json.RawMessage(v.Raw)
		}
	case route.State:
		// `content.get("membership", None)`, then
		// UserID.from_string(state_key).
		v := pdu.Get(b, "membership")
		if v.Type != gjson.String {
			return nil, badRequest("invalid_membership", "Invalid membership (must be a string)").stop()
		}
		key, _ := json.Marshal(req.StateKey)
		target, st := targetUserID(gjson.ParseBytes(key))
		if st != nil {
			return nil, st
		}
		m.action, m.target, m.fromState = v.Str, target, true
		whole()
	default:
		if m.action == "invite" {
			_, medium := pdu.Field(b, "medium")
			_, address := pdu.Field(b, "address")
			if medium && address {
				_, idServer := pdu.Field(b, "id_server")
				_, idToken := pdu.Field(b, "id_access_token")
				if !idServer || !idToken {
					return nil, badRequest("3pid_invite",
						"`id_server` and `id_access_token` are required when doing 3pid invite", "M_MISSING_PARAM").stop()
				}
				return nil, fb("3pid_invite")
			}
		}
		switch m.action {
		case "invite", "ban", "unban", "kick":
			v, ok := pdu.Field(b, "user_id")
			if !ok {
				return nil, badRequest("missing_user_id", "Missing params: ['user_id']", "M_MISSING_PARAM").stop()
			}
			target, st := targetUserID(v)
			if st != nil {
				return nil, st
			}
			m.target = target
		case "join", "leave":
		default:
			return nil, fb("action")
		}
		m.join = m.action == "join"
		if v, ok := pdu.Field(b, "reason"); ok {
			m.content["reason"] = json.RawMessage(v.Raw)
		}
		if c.o.MSC4293 {
			if v, ok := pdu.Field(b, msc4293RedactEvents); ok {
				m.content[msc4293RedactEvents] = json.RawMessage(v.Raw)
			}
		}
	}
	if req.Endpoint != route.State && req.Endpoint != route.Knock {
		if v := pdu.Get(b, "third_party_signed"); v.Exists() && v.Type != gjson.Null {
			return nil, fb("third_party_signed")
		}
	}

	m.membership = m.action
	if m.action == "kick" || m.action == "unban" {
		m.membership = "leave"
	}
	sk := m.target
	return &input{typ: "m.room.member", stateKey: &sk, txn: req.TxnID, room: req.Room, member: m}, nil
}

// membershipList is Membership.LIST.
var membershipList = map[string]bool{"invite": true, "join": true, "knock": true, "leave": true, "ban": true}

// memberContent is the content update_membership_locked builds: per-room
// profiles kept unless the server says otherwise (or the requester is
// shadow-banned), checked for length and, with media limits configured, by
// the media repository; the authorising user dropped; the target's global
// profile for joins and, when configured, invites, where the content does
// not set it.
func (c *Creator) memberContent(ctx context.Context, rq *requester, in *input) *stop {
	m := in.member
	content := m.content
	sn := c.o.Membership.ServerNoticesMXID
	if (!c.o.Membership.AllowPerRoomProfiles && (sn == "" || rq.UserID != sn)) || rq.ShadowBanned {
		delete(content, "displayname")
		delete(content, "avatar_url")
	}
	// `len(content.get(k) or "")`.
	for _, f := range []struct {
		key, msg string
		max      int
	}{{"displayname", "Displayname is too long (max 256)", 256}, {"avatar_url", "Avatar URL is too long (max 1000)", 1000}} {
		v, ok := content[f.key]
		if !ok {
			continue
		}
		n, st := pyLenOrEmpty(gjson.ParseBytes(v))
		if st != nil {
			return st
		}
		if n > f.max {
			return badRequest("profile_too_long", f.msg, "M_BAD_JSON").stop()
		}
	}
	if v, ok := content["avatar_url"]; ok && gjson.ParseBytes(v).Type != gjson.Null && c.o.Membership.AvatarLimits {
		// check_avatar_size_and_mime_type reads the media repository.
		if gjson.ParseBytes(v).Str != "" || gjson.ParseBytes(v).Type != gjson.String {
			return fb("avatar_check")
		}
	}
	delete(content, authorisingUser)
	if !membershipList[m.membership] {
		return badRequest("invalid_membership", "Invalid membership key").stop()
	}
	if m.membership == "join" || m.membership == "knock" || (m.membership == "invite" && c.o.Membership.IncludeProfileDataOnInvite) {
		_, hasDN := content["displayname"]
		_, hasAV := content["avatar_url"]
		if (!hasDN || !hasAV) && !c.isMine(m.target) {
			if c.o.RemoteProfiles == nil {
				return fb("remote_profile")
			}
			// make_query per field; a failure ends the lookups, swallowed.
			for _, f := range []struct {
				key string
				has bool
			}{{"displayname", hasDN}, {"avatar_url", hasAV}} {
				if f.has {
					continue
				}
				v, ok := c.o.RemoteProfiles.ProfileField(ctx, m.target, f.key)
				if !ok {
					break
				}
				if v.Exists() && v.Type != gjson.Null {
					content[f.key] = json.RawMessage(v.Raw)
				}
			}
		} else if !hasDN || !hasAV {
			dn, av, ok, err := c.o.Store.Profile(ctx, m.target)
			if err != nil {
				return fb("db_error")
			}
			// get_displayname and get_avatar_url raise for a missing
			// profile; the try around both leaves the rest out.
			if ok {
				if !hasDN && dn != nil {
					content["displayname"], _ = json.Marshal(*dn)
				}
				if !hasAV && av != nil {
					content["avatar_url"], _ = json.Marshal(*av)
				}
			}
		}
	}
	// The content as update_membership_locked compares it, then as
	// _local_membership_update sends it.
	cmp, err := json.Marshal(content)
	if err != nil {
		return fb("not_object")
	}
	m.compare = cmp
	final := make(map[string]json.RawMessage, len(content)+1)
	for k, v := range content {
		final[k] = v
	}
	final["membership"], _ = json.Marshal(m.membership)
	if in.content, err = json.Marshal(final); err != nil {
		return fb("not_object")
	}
	return nil
}

// pyLenOrEmpty is len(v or ""): 0 for anything falsy, a string's
// characters, a list's or a dict's entries, and len()'s TypeError for the
// rest.
func pyLenOrEmpty(v gjson.Result) (int, *stop) {
	if !pyTruthy(v) {
		return 0, nil
	}
	switch {
	case v.Type == gjson.String:
		if loneSurrogate(v.Raw) {
			return 0, fb("surrogate_escape")
		}
		return utf8.RuneCountInString(v.Str), nil
	case v.IsArray():
		return len(v.Array()), nil
	case v.IsObject():
		n := 0
		v.ForEach(func(_, _ gjson.Result) bool { n++; return true })
		return n, nil
	}
	return 0, internalError("profile_type").stop()
}

// ratelimitMembership is update_membership's ratelimiting: joins by a user
// not yet joined, and invites. A refusal is answered here, as Synapse's 429.
func (c *Creator) ratelimitMembership(ctx context.Context, w http.ResponseWriter, req route.Request, rq *requester, m *memberInput) (Outcome, bool) {
	if m.action != "join" && m.action != "invite" {
		return Outcome{}, false
	}
	if rq.AppService != nil && !rq.AppService.RateLimited {
		// can_do_action: an appservice registered with rate_limited: false.
		return Outcome{}, false
	}
	entity := rq.Entity
	if entity == "" {
		entity = rq.UserID
	}
	// get_ratelimit_for_user(requester.authenticated_entity).
	exempt, err := c.o.Store.RatelimitExempt(ctx, entity)
	if err != nil {
		return fallback("db_error"), true
	}
	if exempt {
		return Outcome{}, false
	}
	lim := c.limiters()
	type check struct {
		l      *ratelimit.Limiter
		key    string
		update bool
	}
	var checks []check
	switch m.action {
	case "join":
		cur, _, err := c.o.Store.LocalMembership(ctx, rq.UserID, req.Room)
		if err != nil {
			return fallback("db_error"), true
		}
		if cur == "join" {
			// Not ratelimited: it would block profile updates.
			return Outcome{}, false
		}
		checks = append(checks, check{lim.joinsLocal, rq.UserID, true})
	case "invite":
		checks = append(checks, check{lim.invitesPerRoom, req.Room, true},
			check{lim.invitesPerUser, m.target, true}, check{lim.invitesPerIssuer, rq.UserID, true})
	}
	for _, ch := range checks {
		if ok, retry := ch.l.CanDo(ch.key, ch.update); !ok {
			return c.limitExceeded(ctx, w, ch.l.Name(), retry), true
		}
	}
	if m.action == "join" {
		// The per-room limiter, update=False: it counts every join the
		// events stream brings, which the database has too.
		ok, retry, err := c.perRoomJoins(ctx, req.Room)
		if err != nil {
			return fallback("db_error"), true
		}
		if !ok {
			return c.limitExceeded(ctx, w, c.o.Membership.JoinsPerRoom.Key, retry), true
		}
	}
	return Outcome{}, false
}

// perRoomJoins replays the room's recent joins into a fresh bucket.
func (c *Creator) perRoomJoins(ctx context.Context, room string) (bool, int64, error) {
	cfg := c.o.Membership.JoinsPerRoom
	window := 60 * time.Second
	if cfg.PerSecond > 0 {
		window += time.Duration(float64(cfg.Burst)/cfg.PerSecond*float64(time.Second)) * 2
	}
	now := c.o.Now()
	ts, err := c.o.Store.RecentJoins(ctx, room, now.Add(-window).UnixMilli())
	if err != nil {
		return false, 0, err
	}
	l := ratelimit.New(cfg, func() time.Time { return now })
	for _, t := range ts {
		l.Record(room, time.UnixMilli(t))
	}
	ok, retry := l.CanDo(room, false)
	return ok, retry, nil
}

// limitExceeded is LimitExceededError as the client sees it, after the half
// second Synapse pauses before answering.
func (c *Creator) limitExceeded(ctx context.Context, w http.ResponseWriter, limiter string, retryMS int64) Outcome {
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
	}
	b, _ := json.Marshal(map[string]any{"errcode": "M_LIMIT_EXCEEDED", "error": "Too Many Requests", "retry_after_ms": retryMS})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.FormatInt(ratelimit.RetryAfterHeader(retryMS), 10))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(b)
	c.o.Log.Info().Str("limiter", limiter).Msg("Membership change ratelimited")
	return Outcome{Result: "ratelimited"}
}

// checkMembershipRequest is update_membership_locked's checks that need no
// room state.
func (c *Creator) checkMembershipRequest(ctx context.Context, req route.Request, rq *requester, in *input) *stop {
	m := in.member
	// Suspension (MSC3823). A join's target is the requester.
	if rq.Suspended && m.action == "invite" {
		return userSuspended("Sending invites while account is suspended is not allowed.").stop()
	}
	if rq.Suspended && m.action == "join" {
		return userSuspended("Joining rooms while account is suspended is not allowed.").stop()
	}
	if rq.Suspended && m.action == "knock" {
		return userSuspended("Knocking on rooms while account is suspended is not allowed.").stop()
	}
	if st := c.memberContent(ctx, rq, in); st != nil {
		return st
	}
	sn := c.o.Membership.ServerNoticesMXID
	if sn != "" && rq.UserID == sn {
		return fb("server_notices")
	}
	if m.membership != "leave" && m.membership != "ban" {
		blocked, err := c.o.Store.RoomBlocked(ctx, req.Room)
		if err != nil {
			return fb("db_error")
		}
		if blocked {
			return (&refusal{reason: "room_blocked", status: http.StatusForbidden, errcode: "M_UNKNOWN",
				msg: "This room has been blocked on this server"}).stop()
		}
	}
	if m.membership == "invite" {
		if sn != "" && m.target == sn {
			return (&refusal{reason: "server_notices", status: http.StatusForbidden, errcode: "M_UNKNOWN",
				msg: "Cannot invite this user"}).stop()
		}
		if !rq.Admin {
			if c.o.Membership.BlockNonAdminInvites {
				return forbidden("invites_blocked", "Invites have been disabled on this server").stop()
			}
			// get_invite_config_for_user: any rules at all are Synapse's
			// to apply.
			types := []string{invitePermissionConfig}
			if c.o.MSC4155 {
				types = append(types, msc4155InvitePermissionConfig)
			}
			for _, t := range types {
				_, ok, err := c.o.Store.AccountData(ctx, m.target, t)
				if err != nil {
					return fb("db_error")
				}
				if ok {
					return fb("invite_config")
				}
			}
		}
	}
	if sn != "" {
		if m.target == sn {
			return fb("server_notices")
		}
		// is_server_notice_room: invites there cannot be rejected, and
		// joining it needs no consent.
		cur, _, err := c.o.Store.LocalMembership(ctx, sn, req.Room)
		if err != nil {
			return fb("db_error")
		}
		// A profile change's join is served there too: for a join the room
		// changes nothing but consent, which is configFallback's, and the
		// background propagation has no main to hand one room to.
		if cur == "join" && !in.profileJoin {
			return fb("server_notice_room")
		}
	}
	return nil
}

// checkMembership is update_membership_locked's checks against the state
// before the event, then _local_membership_update's transaction lookup. A
// reason means Synapse's; dup is the event a transaction already made.
func (c *Creator) checkMembership(ctx context.Context, req route.Request, rq *requester, in *input, rv *pdu.RoomVersion, before pdu.StateMap) (dup string, st *stop) {
	m := in.member
	ids := []string{}
	for k, id := range before {
		if (k.Type == "m.room.member" && (c.isMine(k.StateKey) || k.StateKey == m.target)) || k.Type == "m.room.join_rules" && k.StateKey == "" {
			ids = append(ids, id)
		}
	}
	evs, bad, err := c.o.Store.GetStoredEvents(ctx, ids)
	if err != nil {
		return "", fb("db_error")
	}
	membershipOf := func(user string) (string, bool) {
		id, ok := before[pdu.StateKey{Type: "m.room.member", StateKey: user}]
		if !ok {
			return "", false
		}
		se := evs[id]
		if se == nil || bad[id] != nil {
			// get_event(allow_none=True) found nothing: Synapse goes on as if
			// there were no old membership, we do not.
			return "", true
		}
		v := pdu.Get(se.Content, "membership")
		if v.Type != gjson.String {
			return "", true
		}
		return v.Str, true
	}

	// _is_host_in_room.
	inRoom := false
	for k := range before {
		if k.Type == "m.room.member" && c.isMine(k.StateKey) {
			if ms, _ := membershipOf(k.StateKey); ms == "join" {
				inRoom = true
				break
			}
		}
	}
	if !inRoom {
		return "", fb("host_not_in_room")
	}

	old, hasOld := membershipOf(m.target)
	notInRoom := forbidden("kick_absent", "The target user is not in the room").stop()
	switch {
	case hasOld && old == "":
		return "", fb("old_membership_unknown")
	case !hasOld && m.action == "kick":
		return "", notInRoom
	case hasOld && m.action == "unban" && old != "ban":
		return "", (&refusal{reason: "unban_not_banned", status: http.StatusForbidden, errcode: "M_BAD_STATE",
			msg: "Cannot unban user who was not banned (membership=" + old + ")"}).stop()
	case old == "ban" && m.action != "ban" && m.action != "unban" && m.action != "leave":
		return "", (&refusal{reason: "target_banned", status: http.StatusForbidden, errcode: "M_BAD_STATE",
			msg: "Cannot " + m.action + " user who was banned"}).stop()
	}
	if hasOld {
		// The same sender, membership and content (compared as Python
		// dicts): nothing to do, the old event is the answer.
		oldID := before[pdu.StateKey{Type: "m.room.member", StateKey: m.target}]
		if se := evs[oldID]; se != nil && se.Sender == rq.UserID && old == m.membership &&
			pyEqual(gjson.ParseBytes(m.compare), se.Content) {
			in.dedupKind = "membership_noop"
			return oldID, nil
		}
	}
	if (old == "ban" || old == "leave") && m.action == "kick" {
		return "", notInRoom
	}
	m.leftFrom = old

	if m.membership == "join" && rq.Guest {
		// _can_guest_join.
		ok, err := c.guestCanJoin(ctx, before)
		if err != nil {
			return "", fb("db_error")
		}
		if !ok {
			return "", forbidden("guest", "Guest access not allowed").stop()
		}
	}
	if m.membership == "join" {
		// _should_perform_remote_join: restricted rooms need an authorising
		// user unless the user is already joined or invited.
		if restrictedJoinRules(rv, before, evs) && old != "join" && old != "invite" {
			return "", fb("restricted_join")
		}
	}

	// _local_membership_update: get_event_id_from_transaction.
	if in.txn != "" && rq.DeviceID != "" {
		id, err := c.o.Store.TxnEvent(ctx, req.Room, rq.UserID, rq.DeviceID, in.txn)
		switch {
		case err == nil:
			in.dedupKind = "txn_dedup"
			return id, nil
		case !errors.Is(err, pdustore.ErrNotFound):
			return "", fb("db_error")
		}
	}
	return "", nil
}

// guestCanJoin is _can_guest_join: the room's guest_access is can_join.
func (c *Creator) guestCanJoin(ctx context.Context, before pdu.StateMap) (bool, error) {
	id, ok := before[pdu.StateKey{Type: "m.room.guest_access"}]
	if !ok {
		return false, nil
	}
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{id})
	if err != nil {
		return false, err
	}
	ev := evs[id]
	if ev == nil {
		return false, nil
	}
	v := pdu.Get(ev.Content, "guest_access")
	return v.Type == gjson.String && v.Str == "can_join", nil
}

// restrictedJoinRules is has_restricted_join_rules, on the events loaded.
func restrictedJoinRules(rv *pdu.RoomVersion, before pdu.StateMap, evs map[string]*pdustore.StoredEvent) bool {
	if !rv.RestrictedJoinRule {
		return false
	}
	id, ok := before[pdu.StateKey{Type: "m.room.join_rules"}]
	if !ok {
		return false
	}
	se := evs[id]
	if se == nil {
		// Not loaded: treated as restricted, which falls back.
		return true
	}
	rule := pdu.Get(se.Content, "join_rule")
	if rule.Type == gjson.String && rule.Str == "restricted" {
		return true
	}
	return rv.KnockRestrictedJoinRule && rule.Type == gjson.String && rule.Str == "knock_restricted"
}

func (c *Creator) isMine(userID string) bool {
	i := strings.IndexByte(userID, ':')
	return i >= 0 && userID[i+1:] == c.o.ServerName
}

// validUserID is UserID.from_string's acceptance, made strict: a sigil, a
// non-empty localpart and a domain.
func validUserID(s string) bool {
	if len(s) < 2 || s[0] != '@' {
		return false
	}
	i := strings.IndexByte(s, ':')
	return i > 1 && i < len(s)-1
}

// respond answers a request with its event.
func (c *Creator) respond(w http.ResponseWriter, in *input, eventID string) {
	if in.member == nil || in.member.fromState {
		writeEventID(w, eventID)
		return
	}
	out := map[string]string{}
	if in.member.join {
		out["room_id"] = in.room
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// txnCache is HttpTransactionCache for the membership servlets: a
// transaction that succeeded on this process answers the same for 30
// minutes. Failures are not kept.
type txnCache struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

const txnCacheTTL = 30 * time.Minute

func (t *txnCache) seenAt(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.seen[key]
	return ok && now.Sub(at) < txnCacheTTL
}

func (t *txnCache) add(key string, now time.Time) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen == nil {
		t.seen = map[string]time.Time{}
	}
	for k, at := range t.seen {
		if now.Sub(at) >= txnCacheTTL {
			delete(t.seen, k)
		}
	}
	t.seen[key] = now
}
