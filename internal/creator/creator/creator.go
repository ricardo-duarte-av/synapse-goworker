// Package creator serves /send, /state and /redact natively: it builds the
// event as Synapse's event creator does, writes what the creator writes itself
// (push actions staging, room participation, the state group deletion bump, a
// state event's new state group), and hands the event to the room's events
// writer over send_events (EventCreationHandler.create_and_send_nonmember_event,
// handlers/message.py, and the servlets in rest/client/room.py, v1.163.0).
//
// Anything it does not reproduce exactly is left to Synapse: the request is
// proxied, untouched, and the reason counted
// (goevent_native_fallback_total). That decision is always made before the
// first write, so a proxied request leaves nothing behind. A request Synapse
// would refuse is refused here, with Synapse's status, errcode and words,
// at the point in Synapse's order where Synapse refuses it
// (goevent_native_refusals_total); where the words cannot be reproduced
// exactly, the request is Synapse's.
//
// Each native event is verified afterwards against what the events writer
// persisted (goevent_native_verify_total).
package creator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/crypto/canonicaljson"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/build"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/pushcalc"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/writer"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/appservice"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/stateres"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

var (
	native = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_native_total",
		Help: "Requests served natively, by endpoint and result.",
	}, []string{"endpoint", "result"})
	fallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_native_fallback_total",
		Help: "Requests in native mode left to Synapse, by endpoint and reason. Nothing was written for them.",
	}, []string{"endpoint", "reason"})
	verifies = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_native_verify_total",
		Help: "Checks of natively created events against what the events writer persisted, by check and result.",
	}, []string{"check", "result"})
	nativeSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goevent_native_seconds",
		Help:    "Time to serve a native request, by endpoint and result.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"endpoint", "result"})
)

// Query parameters that change what /send does (rest/client/room.py).
const (
	StickyParam = "org.matrix.msc4354.sticky_duration_ms"
	DelayParam  = "org.matrix.msc4140.delay"
)

// MaxBody is the largest body read: Synapse refuses events over 64 KiB.
const MaxBody = 65536

// Store is what the creator reads.
type Store interface {
	TxnEvent(ctx context.Context, roomID, userID, deviceID, txnID string) (string, error)
	RoomVersion(ctx context.Context, roomID string) (string, error)
	PartialState(ctx context.Context, roomID string) (bool, error)
	PrevEventsForRoom(ctx context.Context, roomID string) ([]string, error)
	RoomMaxStream(ctx context.Context, roomID string) (int64, error)
	Depths(ctx context.Context, ids []string) (map[string]int64, error)
	StateGroupForEvent(ctx context.Context, id string) (int64, error)
	StateForGroup(ctx context.Context, group int64) (pdu.StateMap, error)
	StateDelta(ctx context.Context, group int64) (prev int64, hasPrev bool, delta pdu.StateMap, err error)
	GetStoredEvents(ctx context.Context, ids []string) (map[string]*pdustore.StoredEvent, map[string]error, error)
	RejectionColumn(ctx context.Context, eventID string) (*string, error)
	EventRoom(ctx context.Context, eventID string) (string, error)
	IsRelationTarget(ctx context.Context, eventID string) (bool, error)
	HasRelation(ctx context.Context, eventID string) (bool, error)
	HasAnnotated(ctx context.Context, parent, eventType, sender, key string) (bool, error)

	// State resolution, across several prev events.
	stateres.Store

	// Membership.
	Profile(ctx context.Context, userID string) (displayname, avatarURL *string, ok bool, err error)
	JoinedRooms(ctx context.Context, userID string) ([]string, error)
	RoomBlocked(ctx context.Context, roomID string) (bool, error)
	LocalMembership(ctx context.Context, userID, roomID string) (membership, eventID string, err error)
	RatelimitExempt(ctx context.Context, userID string) (bool, error)
	RatelimitOverride(ctx context.Context, userID string) (found bool, perSecond, burst *int64, err error)
	RecentJoins(ctx context.Context, roomID string, sinceMS int64) ([]int64, error)
	AccountData(ctx context.Context, userID, typ string) (content []byte, ok bool, err error)
	// The requester's flags, and an appservice's or a profile target's.
	UserFlags(ctx context.Context, userID string) (exists, admin, suspended bool, err error)
	ShadowBanned(ctx context.Context, userID string) (bool, error)
	DeviceExists(ctx context.Context, userID, deviceID string) (bool, error)
}

// Writer is what the creator writes.
type Writer interface {
	BumpDeletion(ctx context.Context, groups []int64) ([]int64, error)
	SetRoomParticipation(ctx context.Context, userID, roomID string) error
	StagePush(ctx context.Context, eventID string, rows []writer.PushRow) error
	UnstagePush(ctx context.Context, eventID string) error
	StoreStateGroup(ctx context.Context, eventID, roomID string, prev int64, delta []writer.StateRow, full func() ([]writer.StateRow, error)) (int64, error)
	StoreFullStateGroup(ctx context.Context, eventID, roomID string, rows []writer.StateRow) (int64, error)
	AddDelayedEvent(ctx context.Context, ev writer.DelayedEvent, limit int) (int64, error)
	// The profile propagations not yet finished (goevent_profile_propagations).
	SaveProfileJob(ctx context.Context, j writer.ProfileJob) error
	CheckpointProfileJob(ctx context.Context, userID, jobID, roomID string) error
	DeleteProfileJob(ctx context.Context, userID, jobID string) error
	ProfileJobs(ctx context.Context) ([]writer.ProfileJob, error)
}

// Authenticator is get_user_by_req (platform/clientauth), which also marks
// a token used and records the request for user_ips, as Synapse does.
type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (clientauth.Verdict, error)
}

// AppServices are the registrations, by ID.
type AppServices interface {
	ByID(id string) *appservice.Service
	All() []*appservice.Service
}

// RemoteProfiles is the federation profile query.
type RemoteProfiles interface {
	ProfileField(ctx context.Context, userID, field string) (gjson.Result, bool)
}

// Persister is the events writer, over replication.
type Persister interface {
	SendClientEvents(ctx context.Context, instance string, entries []json.RawMessage) (int64, string, error)
}

// Options configure a Creator.
type Options struct {
	Store      Store
	Writer     Writer
	Persister  Persister
	Key        *build.Key
	Push       *pushcalc.Calculator
	ServerName string
	// Auth identifies requesters.
	Auth Authenticator
	// AppServices are the registrations: a requester's, by the ID its
	// verdict names.
	AppServices AppServices
	// EventsWriter is the instance that persists a room's events
	// (stream_writers.events).
	EventsWriter func(roomID string) string
	// Sticky is experimental_features.msc4354_enabled.
	Sticky bool
	// RedactionAllowedPeriod is redaction_allowed_period being set, which
	// limits local redactions; they are then Synapse's.
	RedactionAllowedPeriod bool
	// Membership is what update_membership reads from homeserver.yaml.
	Membership synapsecfg.Membership
	// MSC3848 is msc3848_enabled: auth refusals carry their new errcode.
	MSC3848 bool
	// MSC4293 and MSC4155 are msc4293_enabled (redact_events on kicks and
	// bans) and msc4155_enabled (invite filtering).
	MSC4293, MSC4155 bool
	// Main is told when a local user leaves a room they were joined to.
	Main MembershipNotifier

	// ProfileWriter, at ProfileWriterInstance (the first events writer),
	// stores profile fields. MSC4069 is
	// msc4069_profile_inhibit_propagation. SyncPropagation runs a profile
	// change's join events before answering (tests).
	ProfileWriter         ProfileWriter
	ProfileWriterInstance string
	MSC4069               bool
	SyncPropagation       bool
	// Delayed is delayed events' settings.
	Delayed synapsecfg.Delayed
	// Policy asks rooms' policy servers to sign events (MSC4284); nil
	// leaves rooms with one to Synapse.
	Policy PolicySigner
	// RemoteProfiles queries remote servers for profiles; nil leaves
	// remote targets' membership to Synapse.
	RemoteProfiles RemoteProfiles
	// ShadowPause, in tests, replaces the pause before a shadow-banned
	// answer.
	ShadowPause func(time.Duration) time.Duration
	// Timeout bounds one request's work, the persister's included.
	Timeout time.Duration
	Log     zerolog.Logger
	Now     func() time.Time
}

// Creator serves native requests.
type Creator struct {
	o     Options
	locks roomLocks
	txns  txnCache
	// answers are the event IDs transactions were answered with
	// (HttpTransactionCache).
	answers answerCache

	limitersOnce sync.Once
	lim          *membershipLimiters
	msgOnce      sync.Once
	msgLimiter   *ratelimit.Limiter
	// verify is replaced in tests.
	verify func(v pending)
	// profiles are the profile changes being propagated.
	profiles profileJobs
}

// New builds a Creator.
func New(o Options) *Creator {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Timeout <= 0 {
		o.Timeout = 60 * time.Second
	}
	c := &Creator{o: o}
	c.verify = func(v pending) { go c.check(v) }
	return c
}

// Outcome is what happened to a request.
type Outcome struct {
	// Fallback is why the request must be proxied; nothing was written to
	// the response or the database. Empty when the request was served.
	Fallback string
	// Result labels a served request.
	Result string
	// Refused is why a request was refused, in Synapse's words.
	Refused string
}

func fallback(reason string) Outcome { return Outcome{Fallback: reason} }

// Serves reports whether an endpoint can be native.
func Serves(ep route.Endpoint) bool {
	switch ep {
	case route.Send, route.State, route.Redact, route.Membership, route.Join, route.Knock:
		return true
	}
	return false
}

// Serve serves /send (PUT with a transaction, or POST), PUT /state and
// /redact (PUT with a transaction, or POST). r.Body is left readable from the
// start, so a fallback can forward the request.
func (c *Creator) Serve(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	start := c.o.Now()
	var out Outcome
	if req.Endpoint == route.Membership || req.Endpoint == route.Join || req.Endpoint == route.Knock ||
		(req.Endpoint == route.State && req.Action == "m.room.member" && r.Method == http.MethodPut) {
		// RoomStateEventRestServlet hands m.room.member to update_membership.
		out = c.serveMembership(w, r, req)
	} else {
		out = c.serve(w, r, req)
	}
	ep := string(req.Endpoint)
	if out.Fallback != "" {
		fallbacks.WithLabelValues(ep, out.Fallback).Inc()
	} else {
		if out.Refused != "" {
			refusals.WithLabelValues(ep, out.Refused).Inc()
		}
		native.WithLabelValues(ep, out.Result).Inc()
		nativeSeconds.WithLabelValues(ep, out.Result).Observe(c.o.Now().Sub(start).Seconds())
	}
	return out
}

// input is what a request asks for: Synapse's event_dict.
type input struct {
	typ string
	// stateKey is nil for a message event.
	stateKey *string
	content  []byte
	// redacts is the redacted event, for a redaction.
	redacts *string
	txn     string
	sticky  *int64
	// room is the room the request names.
	room string
	// member is set for a membership change.
	member *memberInput
	// dedupKind labels an answer with an existing event.
	dedupKind string
	// originTS is an appservice's ts.
	originTS *int64
	// delayMS is a delayed event's delay.
	delayMS *int64
	// noRatelimit is update_membership's ratelimit=False: the events
	// writer does not ratelimit it either.
	noRatelimit bool
	// profileJoin is a profile change's join into one of the user's rooms
	// (_update_join_states_task).
	profileJoin bool
}

func (c *Creator) serve(w http.ResponseWriter, r *http.Request, req route.Request) Outcome {
	q := r.URL.Query()
	if req.Endpoint == route.State && r.Method != http.MethodPut {
		return fallback("method")
	}
	if reason := c.configFallback(); reason != "" {
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

	// The servlet: get_user_by_req, then parse_json_object_from_request.
	rq, reason, ref := c.requester(ctx, r)
	switch {
	case ref != nil:
		return c.refuse(w, ref)
	case reason != "":
		return fallback(reason)
	case rq.Guest && req.Endpoint == route.Redact:
		// get_user_by_req(allow_guest=False).
		return c.refuse(w, &refusal{reason: "guest", status: http.StatusForbidden, errcode: "M_GUEST_ACCESS_FORBIDDEN",
			msg: "Guest access not allowed"})
	}
	// fetch_or_execute_request: what this process answered the
	// transaction with, in the last 30 minutes.
	var txnKey string
	if req.TxnID != "" {
		txnKey = rq.txnKey(r)
		if body, ok := c.answers.get(txnKey, c.o.Now()); ok {
			writeJSON(w, []byte(body))
			return Outcome{Result: "txn_cached"}
		}
	}
	body, st := parseJSONObject(body, false)
	if st != nil {
		return c.end(w, st)
	}
	in, st := c.parse(ctx, req, rq, body, q)
	if st != nil {
		return c.end(w, st)
	}
	if in.delayMS != nil {
		// _parse_request_for_delayed_event_delay: DelayedEventsHandler.add,
		// before anything that creates an event.
		return c.addDelayed(ctx, w, rq, in, *in.delayMS, txnKey)
	}
	content := gjson.ParseBytes(in.content)
	if rq.ShadowBanned && in.redacts == nil {
		// create_and_send_nonmember_event, before its transaction lookup.
		return c.shadowBanned(ctx, w, txnKey)
	}

	unlock := c.locks.lock(req.Room)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()

	// get_event_from_transaction: a transaction already used answers with
	// its event. /state has no transaction, and nor has a requester without
	// a device.
	if in.txn != "" && rq.DeviceID != "" {
		id, err := c.o.Store.TxnEvent(ctx, req.Room, rq.UserID, rq.DeviceID, in.txn)
		switch {
		case err == nil:
			c.respond(w, in, id)
			c.answers.add(txnKey, eventBody(id), c.o.Now())
			return Outcome{Result: "txn_dedup"}
		case !errors.Is(err, pdustore.ErrNotFound):
			return fallback("db_error")
		}
	}
	if rq.ShadowBanned {
		// A redaction: the servlet looked the transaction up first. The
		// pause is not the room's.
		locked = false
		unlock()
		return c.shadowBanned(ctx, w, txnKey)
	}

	p, dup, st := c.prepare(ctx, req, rq, in, content)
	if st != nil {
		return c.end(w, st)
	}
	if dup != "" {
		// deduplicate_state_event: the same state, from the same sender, is
		// already in the room; that event is the answer.
		c.respond(w, in, dup)
		return Outcome{Result: in.dedupKind}
	}

	// Past here, writes. Everything Synapse could refuse has been checked.
	out := c.persist(ctx, w, p)
	if out.Result == "created" || out.Result == "writer_dedup" {
		c.answers.add(txnKey, eventBody(p.answer), c.o.Now())
	}
	return out
}

// parseDelay is _parse_request_for_delayed_event_delay.
func parseDelay(q map[string][]string, in *input) *stop {
	v, ok := q[DelayParam]
	if !ok || len(v) == 0 {
		return nil
	}
	n, st := parseIntegerParam(DelayParam, v[0])
	if st != nil {
		return st
	}
	in.delayMS = &n
	return nil
}

// eventBody is {"event_id": id}.
func eventBody(id string) string {
	b, _ := json.Marshal(map[string]string{"event_id": id})
	return string(b)
}

// appserviceTS is an appservice's ts query parameter: the event's
// origin_server_ts.
func appserviceTS(rq *requester, q map[string][]string, in *input) *stop {
	if rq.AppService == nil {
		return nil
	}
	v, ok := q["ts"]
	if !ok || len(v) == 0 {
		return nil
	}
	n, st := parseIntegerParam("ts", v[0])
	if st != nil {
		return st
	}
	in.originTS = &n
	return nil
}

// configFallback is what homeserver.yaml configures that the native path
// does not reproduce: module callbacks (third-party rules, spam checks),
// auth blocking and the privacy policy.
func (c *Creator) configFallback() string {
	switch m := c.o.Membership; {
	case m.Modules:
		return "modules"
	case m.AuthBlocking:
		return "auth_blocking"
	case m.ConsentRequired:
		return "consent"
	}
	return ""
}

// parse is the servlet: what the request asks for, or why it is refused or
// Synapse's.
func (c *Creator) parse(ctx context.Context, req route.Request, rq *requester, body []byte, q map[string][]string) (*input, *stop) {
	in := &input{typ: req.Action, content: body, txn: req.TxnID, room: req.Room}
	if req.Endpoint == route.Send || req.Endpoint == route.State {
		// `if requester.app_service_id: origin_server_ts =
		// parse_integer(request, "ts")`.
		if st := appserviceTS(rq, q, in); st != nil {
			return nil, st
		}
	}
	switch req.Endpoint {
	case route.Send:
		if c.o.Sticky {
			if v, ok := q[StickyParam]; ok && len(v) > 0 {
				n, s := parseIntegerParam(StickyParam, v[0])
				if s != nil {
					return nil, s
				}
				in.sticky = &n
			}
		}
		if st := parseDelay(q, in); st != nil {
			return nil, st
		}
		if req.Action == "m.room.member" && in.delayMS == nil {
			// create_and_send_nonmember_event.
			return nil, (&refusal{reason: "type_member", status: http.StatusInternalServerError, errcode: "M_UNKNOWN",
				msg: "Tried to send member event through non-member codepath"}).stop()
		}
	case route.State:
		if req.Action == "m.room.member" {
			// update_membership, not the non-member path.
			return nil, fb("type_member")
		}
		sk := req.StateKey
		in.stateKey, in.txn = &sk, ""
		if st := parseDelay(q, in); st != nil {
			return nil, st
		}
	case route.Redact:
		content := gjson.ParseBytes(body)
		rvID, err := c.o.Store.RoomVersion(ctx, req.Room)
		if err != nil {
			return nil, fb("unknown_room")
		}
		rv, ok := pdu.LookupRoomVersion(rvID)
		if !ok {
			return nil, fb("room_version")
		}
		if rq.Suspended {
			if st := c.suspendedRedaction(ctx, rq, req.Action); st != nil {
				return nil, st
			}
		}
		// MSC2174: "redacts" in content and content["redacts"] != event_id.
		if r, ok := pdu.Field(content, "redacts"); rv.UpdatedRedactionRules && ok && !(r.Type == gjson.String && r.Str == req.Action) {
			return nil, badRequest("redacts_mismatch",
				"Cannot provide a redacts value incoherent with the event_id of the URL parameter", "M_INVALID_PARAM").stop()
		}
		if c.o.RedactionAllowedPeriod {
			return nil, fb("redaction_period")
		}
		if pdu.Get(content, "org.matrix.msc3912.with_relations").Exists() {
			// Synapse strips it and redacts the relations too.
			return nil, fb("with_relations")
		}
		target := req.Action
		in.typ, in.redacts = "m.room.redaction", &target
	default:
		return nil, fb("endpoint")
	}
	return in, nil
}

// requester identifies the caller as get_user_by_req does
// (api/auth/internal.py), with platform/clientauth: a refusal is Synapse's
// answer, a reason means the request is Synapse's. clientauth has already
// marked the token used and recorded the request for user_ips.
func (c *Creator) requester(ctx context.Context, r *http.Request) (*requester, string, *refusal) {
	if _, _, ok := accessToken(r); !ok {
		// A token that is not ASCII: Synapse's 500.
		return nil, "token_encoding", nil
	}
	v, err := c.o.Auth.Authenticate(ctx, r)
	switch {
	case err != nil:
		return nil, "db_error", nil
	case v.Rejection != nil:
		return nil, "", authRefusal(v.Rejection)
	case !v.Valid:
		return nil, "auth_unknown", nil
	case v.Source != clientauth.SourceNative:
		// /whoami's: a guest's macaroon, delegated auth, account_validity,
		// a module, an appservice regex RE2 cannot compile. Its verdict
		// lacks what the requester needs.
		return nil, "auth_whoami", nil
	}
	if v.AppServiceID != "" {
		return c.appserviceRequester(ctx, v)
	}
	_, admin, suspended, err := c.o.Store.UserFlags(ctx, v.UserID)
	if err != nil {
		return nil, "db_error", nil
	}
	banned, err := c.o.Store.ShadowBanned(ctx, v.UserID)
	if err != nil {
		return nil, "db_error", nil
	}
	return &requester{UserID: v.UserID, DeviceID: v.DeviceID, TokenID: v.TokenID, Admin: admin, Entity: v.Entity,
		Guest: v.IsGuest, ShadowBanned: banned, Suspended: suspended}, "", nil
}

// appserviceRequester is the rest of get_appservice_user, once clientauth
// has checked the namespace, the user and the device: create_requester.
func (c *Creator) appserviceRequester(ctx context.Context, v clientauth.Verdict) (*requester, string, *refusal) {
	var as *appservice.Service
	if c.o.AppServices != nil {
		as = c.o.AppServices.ByID(v.AppServiceID)
	}
	if as == nil {
		return nil, "appservice_unknown", nil
	}
	// create_requester: UserID.from_string.
	key, _ := json.Marshal(v.UserID)
	if _, st := targetUserID(gjson.ParseBytes(key)); st != nil {
		if st.ref != nil {
			return nil, "", st.ref
		}
		return nil, st.fallback, nil
	}
	_, admin, suspended, err := c.o.Store.UserFlags(ctx, v.UserID)
	if err != nil {
		return nil, "db_error", nil
	}
	return &requester{UserID: v.UserID, DeviceID: v.DeviceID, AppService: as, Entity: v.UserID, Admin: admin, Suspended: suspended}, "", nil
}

// requester is Requester.serialize (rust/src/types/mod.rs).
type requester struct {
	UserID string
	// DeviceID is "" for a token without one (the admin API mints them).
	DeviceID string
	TokenID  int64
	// Entity is the token's own user: UserID's puppeteer for a puppet
	// token.
	Entity       string
	Guest        bool
	ShadowBanned bool
	// AppService is the appservice whose token it is.
	AppService *appservice.Service
	// Admin is is_server_admin and Suspended get_user_suspended_status;
	// neither is serialized.
	Admin     bool
	Suspended bool
	// NoToken is create_requester's requester, made by the server (a
	// profile change's join events): access_token_id is None.
	NoToken bool
}

// appServiceID is app_service_id: null for a user's own token.
func (rq *requester) appServiceID() any {
	if rq.AppService == nil {
		return nil
	}
	return rq.AppService.ID
}

func (rq *requester) MarshalJSON() ([]byte, error) {
	var device any
	if rq.DeviceID != "" {
		device = rq.DeviceID
	}
	entity := rq.Entity
	if entity == "" {
		entity = rq.UserID
	}
	var token any = rq.TokenID
	if rq.AppService != nil || rq.NoToken {
		token = nil
	}
	return json.Marshal(map[string]any{
		"user_id":              rq.UserID,
		"access_token_id":      token,
		"is_guest":             rq.Guest,
		"scope":                []string{},
		"shadow_banned":        rq.ShadowBanned,
		"device_id":            device,
		"app_server_id":        rq.appServiceID(),
		"authenticated_entity": entity,
	})
}

// txnKey is HttpTransactionCache's key: the path, and the user and device,
// or the guest, or the token for a requester without a device.
func (rq *requester) txnKey(r *http.Request) string {
	switch {
	case rq.Guest:
		return r.URL.EscapedPath() + "\x00guest\x00" + rq.UserID
	case rq.AppService != nil:
		return r.URL.EscapedPath() + "\x00appservice\x00" + rq.AppService.ID
	case rq.DeviceID != "":
		return r.URL.EscapedPath() + "\x00user\x00" + rq.UserID + "\x00" + rq.DeviceID
	}
	return r.URL.EscapedPath() + "\x00user_admin\x00" + strconv.FormatInt(rq.TokenID, 10)
}

// prepared is an event ready to persist.
type prepared struct {
	req   route.Request
	in    *input
	rq    *requester
	rv    *pdu.RoomVersion
	ev    *pdu.Event
	built *build.Event
	ts    int64
	// group is the state group before the event: sb's, or the one stored
	// for it.
	group     int64
	sb        *stateBefore
	before    pdu.StateMap
	replaces  string
	maxStream int64
	push      map[string]pushcalc.Row
	// after is the state group after the event, once stored.
	after int64
	// answer is the event ID the request was answered with.
	answer string
}

// prepare builds and checks the event, in the order Synapse checks it
// (_create_and_send_nonmember_event_locked, create_event,
// create_new_client_event, handle_new_client_event). A stop is a refusal
// or a fallback; dup is the event a state event duplicates.
func (c *Creator) prepare(ctx context.Context, req route.Request, rq *requester, in *input, content gjson.Result) (p *prepared, dup string, st *stop) {
	rvID, err := c.o.Store.RoomVersion(ctx, req.Room)
	if err != nil {
		return nil, "", fb("unknown_room")
	}
	rv, ok := pdu.LookupRoomVersion(rvID)
	if !ok {
		return nil, "", fb("room_version")
	}
	if partial, err := c.o.Store.PartialState(ctx, req.Room); err != nil {
		return nil, "", fb("db_error")
	} else if partial {
		return nil, "", fb("partial_state")
	}
	if in.member == nil {
		// check_local_user_in_room.
		ms, _, err := c.o.Store.LocalMembership(ctx, rq.UserID, req.Room)
		if err != nil {
			return nil, "", fb("db_error")
		}
		if ms != "join" {
			return nil, "", forbidden("not_in_room", fmt.Sprintf("User %s not in room %s", rq.UserID, req.Room)).stop()
		}
		// create_event.
		if rq.Suspended {
			if in.redacts == nil {
				return nil, "", userSuspended("Sending messages while account is suspended is not allowed.").stop()
			}
			if !rv.UpdatedRedactionRules {
				// event_dict["content"]["redacts"]: a KeyError.
				return nil, "", internalError("suspended").stop()
			}
			if st := c.suspendedRedaction(ctx, rq, *in.redacts); st != nil {
				return nil, "", st
			}
		}
		if st := validateBuilder(in, content, req.Room); st != nil {
			return nil, "", st
		}
	}
	if in.redacts != nil && rv.UpdatedRedactionRules {
		// RoomRedactEventRestServlet: MSC2174 puts redacts in the content.
		b, err := withField(in.content, "redacts", *in.redacts)
		if err != nil {
			return nil, "", fb("not_object")
		}
		in.content, content = b, gjson.ParseBytes(b)
	}

	// The stream position first: the push check reads members as of now.
	maxStream, err := c.o.Store.RoomMaxStream(ctx, req.Room)
	if err != nil {
		return nil, "", fb("db_error")
	}
	prevs, err := c.o.Store.PrevEventsForRoom(ctx, req.Room)
	if err != nil {
		return nil, "", fb("db_error")
	}
	if len(prevs) == 0 {
		return nil, "", fb("prev_events_0")
	}
	if in.typ == "m.room.create" {
		if in.stateKey != nil {
			// create_new_client_event: the room has prev events.
			return nil, "", badRequest("type_create", "Cannot resend m.room.create event", "M_INVALID_PARAM").stop()
		}
		return nil, "", fb("type_create")
	}
	// calculate_context_info: the state before, resolved across the prev
	// events when they disagree.
	sb, st := c.resolveStateBefore(ctx, req.Room, rv, prevs)
	if st != nil {
		return nil, "", st
	}
	before := sb.state
	if in.member != nil {
		id, st := c.checkMembership(ctx, req, rq, in, rv, before)
		if st != nil {
			return nil, "", st
		}
		if id != "" {
			return nil, id, nil
		}
		// create_event.
		if rq.Suspended && in.member.membership != "leave" {
			return nil, "", userSuspended("Changing membership while account is suspended is not allowed.").stop()
		}
	}

	// validate_new, then _validate_event_relation and the call invite
	// check.
	if st := c.validateNew(in, in.content, content); st != nil {
		return nil, "", st
	}
	if st := c.checkRelation(ctx, req, rq, in.typ, content); st != nil {
		return nil, "", st
	}
	if in.typ == "m.call.invite" {
		if public, err := c.publicRoom(ctx, before); err != nil {
			return nil, "", fb("db_error")
		} else if public {
			return nil, "", forbidden("call_invite_public", "Call invites are not allowed in public rooms.").stop()
		}
	}

	depths, err := c.o.Store.Depths(ctx, prevs)
	if err != nil || len(depths) != len(prevs) {
		return nil, "", fb("db_error")
	}
	prevDepths := make([]int64, len(prevs))
	for i, id := range prevs {
		prevDepths[i] = depths[id]
	}
	proto := &pdu.Event{Type: in.typ, Sender: rq.UserID, RoomID: req.Room, StateKey: in.stateKey, Content: content, Room: rv}
	authIDs, err := pdu.ComputeAuthEvents(rv, proto, before)
	if err != nil {
		return nil, "", fb("auth_events")
	}
	ts := c.o.Now().UnixMilli()
	originTS := ts
	if in.originTS != nil {
		originTS = *in.originTS
	}
	bp := build.Proto{
		RoomVersion:      id.RoomVersion(rvID),
		RoomID:           req.Room,
		Sender:           rq.UserID,
		Type:             in.typ,
		StateKey:         in.stateKey,
		Content:          jsontext.Value(in.content),
		Redacts:          in.redacts,
		PrevEvents:       prevs,
		AuthEvents:       authIDs,
		Depth:            build.Depth(prevDepths),
		OriginServerTS:   originTS,
		StickyDurationMS: in.sticky,
	}
	if rv.EventFormat == pdu.FormatV1V2 {
		// builder.py: the origin picks the event ID, and each prev and auth
		// event is listed with its reference hash (add_event_hashes).
		bp.EventID = build.NewV1EventID(ts, c.o.Key.ServerName)
		if bp.RefHashes, err = c.refHashes(ctx, rv, prevs, authIDs); err != nil {
			return nil, "", fb("ref_hashes")
		}
	}
	built, err := build.Build(bp, c.o.Key)
	if err != nil {
		return nil, "", fb("build")
	}

	// calculate_context_info: a state event replacing another says so.
	replaces := ""
	if in.stateKey != nil {
		if r := before[pdu.StateKey{Type: in.typ, StateKey: *in.stateKey}]; r != built.ID {
			replaces = r
		}
	}
	withUnsigned, err := withUnsignedFields(built.JSON, ts, replaces)
	if err != nil {
		return nil, "", fb("build")
	}
	// The whole event's canonical JSON: ours, past the content.
	if err := pdu.CheckPDUJSON(withUnsigned, rv); err != nil {
		return nil, "", fb("invalid_json")
	}
	// MSC4284: the room's policy server signs every event the non-member
	// path sends, before it is handled.
	if in.member == nil {
		if st := c.policyCheck(ctx, built, withUnsigned, rv, before, in); st != nil {
			return nil, "", st
		}
	}

	// handle_new_client_event: dedup, then the auth rules.
	if in.stateKey != nil {
		if id, err := c.duplicate(ctx, before, in, rq); err != nil {
			return nil, "", fb("db_error")
		} else if id != "" {
			in.dedupKind = "state_dedup"
			return nil, id, nil
		}
	}
	if in.redacts != nil {
		if st := c.checkRedaction(ctx, req, *in.redacts); st != nil {
			return nil, "", st
		}
	}

	// validate_event_for_room_version: _check_size_limits.
	if se := pdu.CheckPDUSize(withUnsigned); se != nil {
		return nil, "", tooLarge(se).stop()
	}
	ev, err := pdu.ParseEvent(built.JSON, built.ID, rv, false)
	if err != nil {
		return nil, "", fb("build")
	}
	if se := pdu.CheckFieldSizes(ev); se != nil {
		return nil, "", tooLarge(se).stop()
	}

	if err := c.checkAuth(ctx, ev); err != nil {
		return nil, "", c.authRefusal(err)
	}
	if ev.RedactOwnOnly && in.redacts != nil {
		if st := c.checkOwnRedaction(ctx, ev, *in.redacts); st != nil {
			return nil, "", st
		}
	}

	var rows map[string]pushcalc.Row
	if c.o.Push != nil {
		if rows, err = c.o.Push.Rows(ctx, ev, before); err != nil {
			return nil, "", fb("push")
		}
	}

	return &prepared{req: req, in: in, rq: rq, rv: rv, ev: ev, built: built, ts: ts, group: sb.group,
		sb: sb, before: before, replaces: replaces, maxStream: maxStream, push: rows}, "", nil
}

// tooLarge is EventSizeError: 413 M_TOO_LARGE.
func tooLarge(se *pdu.SizeError) *refusal {
	return &refusal{reason: "too_large", status: http.StatusRequestEntityTooLarge, errcode: "M_TOO_LARGE", msg: se.Msg}
}

// authRefusal is check_auth_rules_from_context's AuthError, as the client
// sees it. Anything else the auth checks raise, and a message not written as
// Synapse writes it, is Synapse's.
func (c *Creator) authRefusal(err error) *stop {
	var ae *pdu.AuthError
	if !errors.As(err, &ae) || ae.Inexact {
		return fb("auth_rules")
	}
	f := &refusal{reason: "auth_rules", status: ae.Status, errcode: "M_FORBIDDEN", msg: ae.Msg}
	if ae.Unstable != "" && c.o.MSC3848 {
		f.extra = map[string]any{"org.matrix.msc3848.unstable.errcode": ae.Unstable}
	}
	return f.stop()
}

// duplicate is deduplicate_state_event: the event of this key in the state
// before, when it has the same sender and canonical content.
func (c *Creator) duplicate(ctx context.Context, before pdu.StateMap, in *input, rq *requester) (string, error) {
	prevID, ok := before[pdu.StateKey{Type: in.typ, StateKey: *in.stateKey}]
	if !ok {
		return "", nil
	}
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{prevID})
	if err != nil {
		return "", err
	}
	prev := evs[prevID]
	if prev == nil || prev.Sender != rq.UserID {
		return "", nil
	}
	a, errA := canonicaljson.CanonicalJSON(in.content)
	b, errB := canonicaljson.CanonicalJSON([]byte(prev.Content.Raw))
	if errA != nil || errB != nil || !bytes.Equal(a, b) {
		return "", nil
	}
	return prevID, nil
}

// refHashes is get_event_reference_hashes for the events a v1/v2 event
// lists: rejected ones too (allow_rejected), each as stored.
func (c *Creator) refHashes(ctx context.Context, rv *pdu.RoomVersion, idLists ...[]string) (map[string][]byte, error) {
	var ids []string
	for _, l := range idLists {
		ids = append(ids, l...)
	}
	evs, bad, err := c.o.Store.GetStoredEvents(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(ids))
	for _, e := range ids {
		se := evs[e]
		if se == nil || bad[e] != nil {
			return nil, fmt.Errorf("event %s unavailable", e)
		}
		if out[e], err = build.RefHash(se.Event.JSON, id.RoomVersion(rv.ID)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// withField sets one top-level field of a JSON object.
func withField(raw []byte, key string, value any) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	m[key] = b
	return json.Marshal(m)
}

// checkRedaction is the refusals persist_and_notify_client_events makes on
// the events writer, made here before any write: a redaction is never sent
// that the writer would refuse. The writer ratelimits first (rc_message, the
// one limiter that counts): a refusal given here is not counted there.
func (c *Creator) checkRedaction(ctx context.Context, req route.Request, target string) *stop {
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{target})
	if err != nil {
		return fb("db_error")
	}
	orig := evs[target]
	if orig == nil || orig.Reason != "" {
		// get_event(allow_rejected=False, allow_none=True): nothing to
		// check; the redaction waits for its target.
		return nil
	}
	switch {
	case orig.Type == "m.room.create":
		return forbidden("redact_create", "Redacting create events is not permitted").stop()
	case orig.RoomID != req.Room:
		return badRequest("redact_other_room", "Cannot redact event from a different room").stop()
	case orig.Type == "m.room.server_acl":
		return forbidden("redact_acl", "Redacting server ACL events is not permitted").stop()
	}
	return nil
}

// checkOwnRedaction is the rest of the writer's redaction checks, when
// check_redaction answered True (no redact power): the target must exist,
// not rejected, and be the sender's own.
func (c *Creator) checkOwnRedaction(ctx context.Context, ev *pdu.Event, target string) *stop {
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{target})
	if err != nil {
		return fb("db_error")
	}
	orig := evs[target]
	if orig == nil || orig.Reason != "" {
		return (&refusal{reason: "redact_not_found", status: http.StatusNotFound, errcode: "M_NOT_FOUND",
			msg: "Could not find event " + target}).stop()
	}
	if orig.Sender != ev.Sender {
		return forbidden("redact_not_own", "You don't have permission to redact events").stop()
	}
	return nil
}

// persist writes what the creator writes, then sends the event to its
// writer: _persist_events.
func (c *Creator) persist(ctx context.Context, w http.ResponseWriter, p *prepared) Outcome {
	room := p.req.Room
	// check_state_groups_and_bump_deletion on what the state relies on: a
	// resolution's inputs, or the one group.
	need := p.sb.inputs
	if len(need) == 0 {
		need = []int64{p.sb.group}
	}
	missing, err := c.o.Writer.BumpDeletion(ctx, need)
	if err != nil {
		return fallback("db_error")
	}
	if len(missing) > 0 {
		return fallback("state_group_deleted")
	}
	if p.sb.group == 0 {
		// calculate_context_info stores a group for resolved state that no
		// group holds.
		var g int64
		if p.sb.hasPrev {
			g, err = c.o.Writer.StoreStateGroup(ctx, p.ev.ID, room, p.sb.prev, stateRows(p.sb.delta),
				func() ([]writer.StateRow, error) { return stateRows(p.sb.state), nil })
		} else {
			g, err = c.o.Writer.StoreFullStateGroup(ctx, p.ev.ID, room, stateRows(p.sb.state))
		}
		if err != nil {
			c.o.Log.Warn().Err(err).Str("room_id", room).Msg("Could not store the resolved state")
			return fallback("state_group")
		}
		p.group = g
	}
	// handle_new_client_event.
	if !p.ev.IsState() && (p.ev.Type == "m.room.message" || p.ev.Type == "m.room.encrypted") {
		if err := c.o.Writer.SetRoomParticipation(ctx, p.rq.UserID, room); err != nil {
			return fallback("db_error")
		}
	}

	// UnpersistedEventContext.persist: a state event gets a new group, a
	// delta of exactly itself on the group before.
	after := p.group
	if p.ev.IsState() {
		key := p.ev.Key()
		g, err := c.o.Writer.StoreStateGroup(ctx, p.ev.ID, room, p.group,
			[]writer.StateRow{{Type: key.Type, StateKey: key.StateKey, EventID: p.ev.ID}},
			func() ([]writer.StateRow, error) {
				rows := make([]writer.StateRow, 0, len(p.before)+1)
				for k, v := range p.before {
					if k != key {
						rows = append(rows, writer.StateRow{Type: k.Type, StateKey: k.StateKey, EventID: v})
					}
				}
				return append(rows, writer.StateRow{Type: key.Type, StateKey: key.StateKey, EventID: p.ev.ID}), nil
			})
		if err != nil {
			return fallback("state_group")
		}
		after = g
	}
	p.after = after

	rows := make([]writer.PushRow, 0, len(p.push))
	for _, r := range p.push {
		rows = append(rows, writer.PushRow{UserID: r.UserID, Actions: r.Actions, Notif: r.Notif,
			Highlight: r.Highlight, Unread: r.Unread, ThreadID: r.ThreadID})
	}
	if err := c.o.Writer.StagePush(ctx, p.ev.ID, rows); err != nil {
		_ = c.o.Writer.UnstagePush(context.WithoutCancel(ctx), p.ev.ID)
		return fallback("db_error")
	}

	entry, err := c.entry(p)
	if err != nil {
		_ = c.o.Writer.UnstagePush(context.WithoutCancel(ctx), p.ev.ID)
		return fallback("serialize")
	}
	instance := c.o.EventsWriter(room)
	_, persisted, err := c.o.Persister.SendClientEvents(ctx, instance, []json.RawMessage{entry})
	if err != nil {
		_ = c.o.Writer.UnstagePush(context.WithoutCancel(ctx), p.ev.ID)
		var he *replication.HTTPError
		if errors.As(err, &he) {
			// The writer's refusal, as Synapse relays it (to_synapse_error).
			c.o.Log.Info().Int("status", he.Status).Str("room_id", room).Msg("Events writer refused a native event")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(he.Status)
			_, _ = io.WriteString(w, he.Body)
			return Outcome{Result: "writer_" + strconv.Itoa(he.Status)}
		}
		c.o.Log.Error().Err(err).Str("room_id", room).Str("writer", instance).Msg("Could not persist a native event")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = fmt.Fprintf(w, `{"errcode":"M_UNKNOWN","error":"Failed to talk to %s process"}`, instance)
		return Outcome{Result: "writer_unreachable"}
	}

	if persisted != "" && persisted != p.ev.ID {
		// Deduplicated by the writer: an event of the same transaction
		// was persisted first. That is the answer.
		p.answer = persisted
		c.respond(w, p.in, persisted)
		return Outcome{Result: "writer_dedup"}
	}
	p.answer = p.ev.ID
	c.respond(w, p.in, p.ev.ID)
	c.verify(pending{p: p})
	return Outcome{Result: "created"}
}

// entry is ReplicationSendEventsRestServlet._serialize_payload's entry for
// the event.
func (c *Creator) entry(p *prepared) (json.RawMessage, error) {
	ev, err := withUnsignedFields(p.built.JSON, p.ts, p.replaces)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{}
	if p.rq.DeviceID != "" {
		meta["device_id"] = p.rq.DeviceID
	}
	if p.in.txn != "" {
		meta["txn_id"] = p.in.txn
	}
	if p.ev.RecheckRedaction {
		// check_redaction set it; the writer checks the sender again.
		meta["recheck_redaction"] = true
	}
	// _build_state_group_deltas: (before, after) first, then the group
	// before's own edge when its entry has one.
	deltas := []stateGroupDelta{{From: p.group, To: p.group, Delta: pdu.StateMap{}}}
	due := []any{}
	if p.ev.IsState() {
		key := p.ev.Key()
		deltas[0] = stateGroupDelta{From: p.group, To: p.after, Delta: pdu.StateMap{key: p.ev.ID}}
		due = []any{[3]string{key.Type, key.StateKey, p.ev.ID}}
	}
	if p.sb.hasPrev {
		deltas = append(deltas, stateGroupDelta{From: p.sb.prev, To: p.group, Delta: p.sb.delta})
	}
	// _local_membership_update notifies the target too.
	extra := []string{}
	if p.in.member != nil {
		extra = append(extra, p.in.member.target)
	}
	return json.Marshal(map[string]any{
		"event":                json.RawMessage(ev),
		"room_version":         p.rv.ID,
		"event_format_version": p.rv.EventFormat,
		"internal_metadata":    meta,
		"outlier":              false,
		"rejected_reason":      nil,
		"context": map[string]any{
			"state_group":              p.after,
			"state_group_before_event": p.group,
			// str | None (events/snapshot.py): None, never False. The
			// writer stores it in events.rejection_reason as it is, and a
			// False there is the text 'false'.
			"rejected":                 nil,
			"state_group_deltas":       deltas,
			"state_delta_due_to_event": due,
			"app_service_id":           p.rq.appServiceID(),
			"partial_state":            false,
		},
		"requester":   p.rq,
		"ratelimit":   !p.in.noRatelimit,
		"extra_users": extra,
	})
}

// stateGroupDelta is _encode_state_group_delta's (prev, group, [[type,
// state_key, event_id], ...]).
type stateGroupDelta struct {
	From, To int64
	Delta    pdu.StateMap
}

func (d stateGroupDelta) MarshalJSON() ([]byte, error) {
	rows := make([][3]string, 0, len(d.Delta))
	for k, v := range d.Delta {
		rows = append(rows, [3]string{k.Type, k.StateKey, v})
	}
	return json.Marshal([]any{d.From, d.To, rows})
}

// checkRelation is _validate_event_relation.
func (c *Creator) checkRelation(ctx context.Context, req route.Request, rq *requester, eventType string, content gjson.Result) *stop {
	rel := pdu.Get(content, "m.relates_to")
	if !rel.Exists() || !pyTruthy(rel) || !rel.IsObject() {
		return nil
	}
	relType, parent := pdu.Get(rel, "rel_type"), pdu.Get(rel, "event_id")
	if relType.Type != gjson.String || parent.Type != gjson.String {
		return nil
	}
	if loneSurrogate(rel.Raw) {
		return fb("surrogate_escape")
	}
	room, err := c.o.Store.EventRoom(ctx, parent.Str)
	switch {
	case err == nil:
		if room != req.Room {
			return badRequest("relation_other_room", "Relations must be in the same room").stop()
		}
	case errors.Is(err, pdustore.ErrNotFound):
		target, err := c.o.Store.IsRelationTarget(ctx, parent.Str)
		if err != nil {
			return fb("db_error")
		}
		if !target {
			return badRequest("relation_unknown", "Can't send relation to unknown event").stop()
		}
	default:
		return fb("db_error")
	}
	switch relType.Str {
	case "m.annotation":
		key := pdu.Get(rel, "key")
		if key.Type != gjson.String {
			return badRequest("annotation_key", "Missing aggregation key").stop()
		}
		if utf8.RuneCountInString(key.Str) > 500 {
			return badRequest("annotation_key", "Aggregation key is too long").stop()
		}
		dup, err := c.o.Store.HasAnnotated(ctx, parent.Str, eventType, rq.UserID, key.Str)
		if err != nil {
			return fb("db_error")
		}
		if dup {
			return badRequest("duplicate_annotation", "Can't send same reaction twice", "M_DUPLICATE_ANNOTATION").stop()
		}
	case "m.thread":
		has, err := c.o.Store.HasRelation(ctx, parent.Str)
		if err != nil {
			return fb("db_error")
		}
		if has {
			return badRequest("thread_from_relation", "Cannot start threads from an event with a relation").stop()
		}
	}
	return nil
}

// checkAuth runs the auth rules against the event's auth events.
func (c *Creator) checkAuth(ctx context.Context, ev *pdu.Event) error {
	stored, bad, err := c.o.Store.GetStoredEvents(ctx, ev.AuthEvents)
	if err != nil {
		return err
	}
	byID := map[string]*pdu.Event{}
	list := make([]*pdu.Event, 0, len(ev.AuthEvents))
	for _, id := range ev.AuthEvents {
		se := stored[id]
		if bad[id] != nil || se == nil {
			return fmt.Errorf("auth event %s unavailable", id)
		}
		byID[id] = se.Event
		list = append(list, se.Event)
	}
	if err := pdu.CheckStateIndependentAuthRules(ev, byID); err != nil {
		return err
	}
	return pdu.CheckStateDependentAuthRules(ev, list)
}

// withUnsignedFields adds unsigned as Synapse fills it before sending the
// event: age_ts (create_local_event_from_event_dict, for an event with no
// age) and, for a state event replacing another, replaces_state
// (calculate_context_info). The event as get_pdu_json sends it.
func withUnsignedFields(raw []byte, ts int64, replaces string) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	u := map[string]any{"age_ts": ts}
	if replaces != "" {
		u["replaces_state"] = replaces
	}
	b, err := json.Marshal(u)
	if err != nil {
		return nil, err
	}
	m["unsigned"] = b
	return json.Marshal(m)
}

func writeEventID(w http.ResponseWriter, eventID string) {
	b, _ := json.Marshal(map[string]string{"event_id": eventID})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// readBody reads up to MaxBody and leaves r.Body readable from the start.
func readBody(r *http.Request) (body []byte, tooLarge bool, err error) {
	if r.Body == nil {
		return nil, false, nil
	}
	head, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
	r.Body = readCloser{io.MultiReader(bytes.NewReader(head), r.Body), r.Body}
	return head, len(head) > MaxBody, err
}

type readCloser struct {
	io.Reader
	io.Closer
}

// pyTruthy is bool(v) for a decoded JSON value.
func pyTruthy(v gjson.Result) bool {
	switch v.Type {
	case gjson.Null, gjson.False:
		return false
	case gjson.True:
		return true
	case gjson.Number:
		return v.Num != 0
	case gjson.String:
		return v.Str != ""
	}
	if v.IsArray() {
		return len(v.Array()) > 0
	}
	if v.IsObject() {
		return len(v.Map()) > 0
	}
	return false
}

// roomLocks serialises event creation per room, as the creator's limiter
// does (_create_and_send_nonmember_event_locked).
type roomLocks struct {
	mu    sync.Mutex
	rooms map[string]*roomLock
}

type roomLock struct {
	mu   sync.Mutex
	refs int
}

func (l *roomLocks) lock(room string) func() {
	l.mu.Lock()
	if l.rooms == nil {
		l.rooms = map[string]*roomLock{}
	}
	rl := l.rooms[room]
	if rl == nil {
		rl = &roomLock{}
		l.rooms[room] = rl
	}
	rl.refs++
	l.mu.Unlock()

	rl.mu.Lock()
	return func() {
		rl.mu.Unlock()
		l.mu.Lock()
		rl.refs--
		if rl.refs == 0 {
			delete(l.rooms, room)
		}
		l.mu.Unlock()
	}
}
