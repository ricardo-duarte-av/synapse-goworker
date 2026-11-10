package accountdata

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// Endpoints, for metrics and dispatch.
const (
	epGlobal       = "account_data"
	epRoom         = "room_account_data"
	epGlobalDelete = "msc3391_account_data"
	epRoomDelete   = "msc3391_room_account_data"
	epTags         = "tags"
	epTag          = "tag"
)

// Account data types the servlets refuse to set.
const (
	typeFullyRead = "m.fully_read"
	typePushRules = "m.push_rules"
)

// maxTagLength is MAX_TAG_LENGTH, in UTF-8 bytes.
const maxTagLength = 255

type route struct {
	endpoint string
	// args, percent-encoded: user, then room, then type or tag.
	args []string
}

// parse is the servlets' client_patterns (r0, v3, unstable; each argument
// [^/]*, anchored), and the MSC3391 servlets', unstable only.
func parse(p string) (route, bool) {
	rest, ok := strings.CutPrefix(p, "/_matrix/client/")
	if !ok {
		return route{}, false
	}
	version, rest, ok := strings.Cut(rest, "/")
	if !ok {
		return route{}, false
	}
	switch version {
	case "r0", "v3":
	case "unstable":
		if after, ok := strings.CutPrefix(rest, "org.matrix.msc3391/"); ok {
			seg := strings.Split(after, "/")
			switch {
			case len(seg) == 4 && seg[0] == "user" && seg[2] == "account_data":
				return route{epGlobalDelete, []string{seg[1], seg[3]}}, true
			case len(seg) == 6 && seg[0] == "user" && seg[2] == "rooms" && seg[4] == "account_data":
				return route{epRoomDelete, []string{seg[1], seg[3], seg[5]}}, true
			}
			return route{}, false
		}
	default:
		return route{}, false
	}
	seg := strings.Split(rest, "/")
	if len(seg) < 4 || seg[0] != "user" {
		return route{}, false
	}
	switch {
	case len(seg) == 4 && seg[2] == "account_data":
		return route{epGlobal, []string{seg[1], seg[3]}}, true
	case len(seg) == 6 && seg[2] == "rooms" && seg[4] == "account_data":
		return route{epRoom, []string{seg[1], seg[3], seg[5]}}, true
	case len(seg) == 5 && seg[2] == "rooms" && seg[4] == "tags":
		return route{epTags, []string{seg[1], seg[3]}}, true
	case len(seg) == 6 && seg[2] == "rooms" && seg[4] == "tags":
		return route{epTag, []string{seg[1], seg[3], seg[5]}}, true
	}
	return route{}, false
}

var methods = map[string][]string{
	epGlobal:       {http.MethodPut, http.MethodGet},
	epRoom:         {http.MethodPut, http.MethodGet},
	epGlobalDelete: {http.MethodDelete},
	epRoomDelete:   {http.MethodDelete},
	epTags:         {http.MethodGet},
	epTag:          {http.MethodPut, http.MethodDelete},
}

// Match is whether a request is one of ours. The MSC3391 servlets exist
// only while it is enabled; anything else (another method included) goes
// on to main, which answers as Synapse does.
func (a *App) Match(method, escapedPath string) bool {
	rt, ok := parse(escapedPath)
	if !ok {
		return false
	}
	if (rt.endpoint == epGlobalDelete || rt.endpoint == epRoomDelete) && !a.msc3391() {
		return false
	}
	for _, m := range methods[rt.endpoint] {
		if m == method {
			return true
		}
	}
	return false
}

// Handler serves what Match accepts.
func (a *App) Handler() http.Handler { return http.HandlerFunc(a.serve) }

// answer is a request's result: an error, or a body already rendered.
type answer struct {
	outcome string
	err     *clientreq.Error
	body    []byte
}

func (a *App) serve(w http.ResponseWriter, r *http.Request) {
	rt, _ := parse(r.URL.EscapedPath())
	if !a.writing {
		requests.WithLabelValues(rt.endpoint, "not_writer").Inc()
		a.notWriter.Do(func() {
			a.Log.Warn().Msg("an account data request reached this process, which did not start as the " +
				"account-data writer; it goes to main. nginx and homeserver.yaml disagree, or goworker was not restarted")
		})
		a.Fallback.ServeHTTP(w, r)
		return
	}
	if rt.endpoint == epGlobal && r.Method == http.MethodGet && a.PushRules == nil &&
		clientreq.Unquote(rt.args[1]) == typePushRules {
		requests.WithLabelValues(rt.endpoint, "push_rules_to_main").Inc()
		a.Fallback.ServeHTTP(w, r)
		return
	}
	// With server announcement rooms configured (Synapse 1.165), TagServlet
	// lets their synthetic members tag them and refuses to remove
	// m.server_notice from them; neither is reproduced here. Main checks,
	// then writes through our add_tag_to_room/remove_tag_from_room
	// replication endpoints as usual.
	if rt.endpoint == epTag && a.Synapse().Announcements.Configured() {
		requests.WithLabelValues(rt.endpoint, "server_announcements_to_main").Inc()
		a.Fallback.ServeHTTP(w, r)
		return
	}
	ans := a.handle(r.Context(), r, rt)
	requests.WithLabelValues(rt.endpoint, ans.outcome).Inc()
	if ans.err != nil || ans.body == nil {
		clientreq.Write(w, ans.err, nil)
		return
	}
	clientreq.WriteRaw(w, ans.body)
}

func (a *App) handle(ctx context.Context, r *http.Request, rt route) answer {
	v, cerr, err := clientreq.User(ctx, a.Auth, r, false)
	if err != nil {
		a.Log.Error().Err(err).Msg("account data: cannot validate a token")
		return answer{outcome: "auth_unavailable", err: cerr}
	}
	if cerr != nil {
		return answer{outcome: "auth_rejected", err: cerr}
	}
	args := make([]string, len(rt.args))
	for i, s := range rt.args {
		args[i] = clientreq.Unquote(s)
	}
	userID := args[0]
	if userID != v.UserID {
		return answer{outcome: "other_user", err: forbidden(otherUserMessage(rt.endpoint, r.Method))}
	}
	switch rt.endpoint {
	case epGlobal:
		if r.Method == http.MethodGet {
			return a.getGlobal(ctx, userID, args[1])
		}
		return a.putGlobal(ctx, r, userID, args[1])
	case epGlobalDelete:
		if cerr := checkCanSet(args[1]); cerr != nil {
			return answer{outcome: "refused_type", err: cerr}
		}
		return a.done(a.RemoveUser(ctx, CallerClient, userID, args[1]))
	case epRoom, epRoomDelete:
		roomID, dataType := args[1], args[2]
		if !ValidRoomID(roomID) {
			return answer{outcome: "bad_room", err: clientreq.New(400, "M_INVALID_PARAM", roomID+" is not a valid room ID")}
		}
		switch {
		case r.Method == http.MethodGet:
			return a.getRoom(ctx, userID, roomID, dataType)
		case rt.endpoint == epRoomDelete:
			if cerr := checkCanSet(dataType); cerr != nil {
				return answer{outcome: "refused_type", err: cerr}
			}
			return a.done(a.RemoveRoom(ctx, CallerClient, userID, roomID, dataType))
		}
		return a.putRoom(ctx, r, userID, roomID, dataType)
	case epTags:
		return a.getTags(ctx, userID, args[1])
	case epTag:
		roomID, tag := args[1], args[2]
		if r.Method == http.MethodDelete {
			return a.done(a.RemoveTag(ctx, CallerClient, userID, roomID, tag))
		}
		return a.putTag(ctx, r, userID, roomID, tag)
	}
	return answer{outcome: "unknown", err: clientreq.Internal}
}

func otherUserMessage(endpoint, method string) string {
	switch {
	case endpoint == epTags:
		return "Cannot get tags for other users."
	case endpoint == epTag:
		// TagServlet says "add" for DELETE too.
		return "Cannot add tags for other users."
	case method == http.MethodDelete:
		return "Cannot delete account data for other users."
	case method == http.MethodGet:
		return "Cannot get account data for other users."
	}
	return "Cannot add account data for other users."
}

func forbidden(msg string) *clientreq.Error { return clientreq.New(403, "M_FORBIDDEN", msg) }

// checkCanSet is _check_can_set_account_data_type.
func checkCanSet(dataType string) *clientreq.Error {
	switch dataType {
	case typeFullyRead:
		return clientreq.New(405, "M_BAD_JSON", "Cannot set m.fully_read through this API."+
			" Use /rooms/!roomId:server.name/read_markers")
	case typePushRules:
		return clientreq.New(405, "M_BAD_JSON", "Cannot set m.push_rules through this API. Use /pushrules")
	}
	return nil
}

// readBody is parse_json_object_from_request, keeping the bytes in their
// order for json_encoder; empty says the body is {}.
func readBody(r *http.Request) (raw json.RawMessage, empty bool, cerr *clientreq.Error) {
	raw, obj, cerr := clientreq.ReadObjectRaw(r.Body)
	if cerr != nil {
		return nil, false, cerr
	}
	return raw, len(obj) == 0, nil
}

func (a *App) putGlobal(ctx context.Context, r *http.Request, userID, dataType string) answer {
	if cerr := checkCanSet(dataType); cerr != nil {
		return answer{outcome: "refused_type", err: cerr}
	}
	body, empty, cerr := readBody(r)
	if cerr != nil {
		return answer{outcome: "bad_json", err: cerr}
	}
	if a.msc3391() && empty {
		return a.done(a.RemoveUser(ctx, CallerClient, userID, dataType))
	}
	return a.done(a.AddUser(ctx, CallerClient, userID, dataType, body))
}

func (a *App) putRoom(ctx context.Context, r *http.Request, userID, roomID, dataType string) answer {
	if cerr := checkCanSet(dataType); cerr != nil {
		return answer{outcome: "refused_type", err: cerr}
	}
	body, empty, cerr := readBody(r)
	if cerr != nil {
		return answer{outcome: "bad_json", err: cerr}
	}
	if a.msc3391() && empty {
		return a.done(a.RemoveRoom(ctx, CallerClient, userID, roomID, dataType))
	}
	return a.done(a.AddRoom(ctx, CallerClient, userID, roomID, dataType, body))
}

func (a *App) putTag(ctx context.Context, r *http.Request, userID, roomID, tag string) answer {
	if len(tag) > maxTagLength {
		return answer{outcome: "tag_too_long", err: clientreq.New(400, "M_INVALID_PARAM", "tag parameter's length is over 255 bytes")}
	}
	member, err := a.Store.HasMembership(ctx, userID, roomID)
	if err != nil {
		a.Log.Error().Err(err).Msg("account data: membership")
		return answer{outcome: "db_error", err: clientreq.Internal}
	}
	if !member {
		return answer{outcome: "not_member", err: forbidden("User " + userID + " has no membership in room " + roomID)}
	}
	body, _, cerr := readBody(r)
	if cerr != nil {
		return answer{outcome: "bad_json", err: cerr}
	}
	return a.done(a.AddTag(ctx, CallerClient, userID, roomID, tag, body))
}

// done renders a write's result: {} or the error Synapse raises.
func (a *App) done(_ int64, err error) answer {
	switch {
	case err == nil:
		return answer{outcome: "ok"}
	case errors.Is(err, ErrIgnoreSelf):
		return answer{outcome: "ignore_self", err: clientreq.New(400, "M_INVALID_PARAM", ErrIgnoreSelf.Error())}
	}
	a.Log.Error().Err(err).Msg("account data: write failed")
	return answer{outcome: "error", err: clientreq.Internal}
}

func notFound(msg string) *clientreq.Error { return clientreq.New(404, "M_NOT_FOUND", msg) }

func (a *App) getGlobal(ctx context.Context, userID, dataType string) answer {
	var (
		data json.RawMessage
		err  error
	)
	if dataType == typePushRules {
		data, err = a.PushRules(ctx, userID)
	} else {
		data, err = a.Store.Global(ctx, userID, dataType)
	}
	if err != nil {
		a.Log.Error().Err(err).Msg("account data: read failed")
		return answer{outcome: "db_error", err: clientreq.Internal}
	}
	return a.render(data, "Account data not found")
}

func (a *App) getRoom(ctx context.Context, userID, roomID, dataType string) answer {
	data := json.RawMessage(`{}`) // room push rules are not supported
	if dataType != typePushRules {
		var err error
		if data, err = a.Store.Room(ctx, userID, roomID, dataType); err != nil {
			a.Log.Error().Err(err).Msg("account data: read failed")
			return answer{outcome: "db_error", err: clientreq.Internal}
		}
	}
	return a.render(data, "Room account data not found")
}

// render is the GET's answer: 404 for none (and, with MSC3391, for {}),
// else the content as db_to_json then json_encoder give it back.
func (a *App) render(data json.RawMessage, missing string) answer {
	if data == nil {
		return answer{outcome: "not_found", err: notFound(missing)}
	}
	if a.msc3391() && isEmptyObject(data) {
		return answer{outcome: "not_found", err: notFound(missing)}
	}
	enc, err := pyjson.Encode(data)
	if err != nil {
		a.Log.Error().Err(err).Msg("account data: stored content does not encode")
		return answer{outcome: "error", err: clientreq.Internal}
	}
	return answer{outcome: "ok", body: []byte(enc)}
}

func isEmptyObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal(raw, &m) == nil && m != nil && len(m) == 0
}

func (a *App) getTags(ctx context.Context, userID, roomID string) answer {
	tags, err := a.Store.Tags(ctx, userID, roomID)
	if err != nil {
		a.Log.Error().Err(err).Msg("account data: tags")
		return answer{outcome: "db_error", err: clientreq.Internal}
	}
	// {"tags": {tag: content}}: a dict built in the table's order.
	var b strings.Builder
	b.WriteString(`{"tags":{`)
	for i, t := range tags {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(t.Name)
		b.Write(k)
		b.WriteByte(':')
		b.Write(t.Content)
	}
	b.WriteString(`}}`)
	enc, err := pyjson.Encode([]byte(b.String()))
	if err != nil {
		a.Log.Error().Err(err).Msg("account data: stored tag does not encode")
		return answer{outcome: "error", err: clientreq.Internal}
	}
	return answer{outcome: "ok", body: []byte(enc)}
}

// ValidRoomID is RoomID.is_valid.
func ValidRoomID(s string) bool { return clientreq.ValidRoomID(s) }
