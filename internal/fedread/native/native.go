// Package native computes an endpoint's answer from our own implementation.
//
// The request path and the sweeps (cmd/*sweep) call the same dispatch and
// error mapping, so they cannot disagree about what "our answer" is.
package native

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/e2ekeys"
)

// Resolver computes native answers.
//
// State is the odd one out: it writes its answer rather than returning it,
// because a /state response can reach 97MB here and returning one would defeat
// the point of implementing the endpoint at all.
type Resolver interface {
	StateIDs(ctx context.Context, origin, roomID, eventID string) (*matrixstate.StateIDsResponse, error)
	Event(ctx context.Context, origin, serverName, eventID string) (*matrixstate.TransactionResponse, error)
	GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*matrixstate.MissingEventsResponse, error)
	UserKeys(ctx context.Context, users []string, devices map[string][]string) ([]byte, error)
	UserDevices(ctx context.Context, userID string) ([]byte, error)
	KeyQueryOptions() matrixstate.KeyQueryOptions
	State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error)
}

// Request identifies what to answer.
type Request struct {
	Endpoint string
	// Method is the request's; only POST is /user/keys/query's.
	Method  string
	Origin  string
	RoomID  string
	EventID string
	// Param is the trailing path parameter as it arrived, percent-encoded:
	// /user/devices' user ID, which Synapse unquotes itself.
	Param string
	// Body is the raw request body, for endpoints whose answer depends on it.
	// Carried as bytes rather than parsed, so every caller parses it
	// identically.
	Body []byte
}

// MissingEventsRequest is the POST body of /get_missing_events.
//
// Synapse defaults limit to 10 and the lists to empty, and caps the limit at
// 20 inside the handler. A body that will not parse is a client error, not an
// internal one.
type MissingEventsRequest struct {
	EarliestEvents []string `json:"earliest_events"`
	LatestEvents   []string `json:"latest_events"`
	Limit          *int     `json:"limit"`
}

// NotReproduced is an answer left to Synapse on purpose, by reason. It
// falls back as any error does, but is not a fault.
type NotReproduced struct{ Reason string }

func (e NotReproduced) Error() string { return "left to Synapse: " + e.Reason }

// stateDagField is StateDag.GET_MISSING_EVENTS_FIELD (MSC4242): a body
// setting it to anything Python finds true makes Synapse walk the state DAG
// (on_get_missing_events_state_dag) instead, whatever the room and config.
const stateDagField = "org.matrix.msc4242.state_dag"

// Meta carries facts about how an answer was produced that its body cannot
// express.
//
// Currently just the one, but it exists as a struct because the alternative --
// a bare bool threaded through two call sites -- would have to be widened the
// moment a second such fact appears, and each widening is a chance for a
// caller to be missed.
type Meta struct {
	// WalkTruncated reports that /get_missing_events stopped at the limit, so
	// its answer is one of several valid ones and cannot be compared strictly.
	WalkTruncated bool
}

// Answer computes one response.
//
// The returned status is what Synapse would return, including its error
// statuses; err is reserved for failures that mean we could not produce an
// answer at all, which on the request path is what triggers falling back to
// the proxy.
func Answer(ctx context.Context, r Resolver, serverName string, request Request) (body []byte, status int, meta Meta, err error) {
	switch request.Endpoint {
	case "state_ids":
		resp, err := r.StateIDs(ctx, request.Origin, request.RoomID, request.EventID)
		if err != nil {
			return matrixErrorMeta(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, Meta{}, err
		}
		return body, http.StatusOK, Meta{}, nil

	case "user_keys_query":
		return userKeysQuery(ctx, r, serverName, request)

	case "user_devices":
		return userDevices(ctx, r, serverName, request)

	case "get_missing_events":
		var body MissingEventsRequest
		if len(request.Body) > 0 {
			if err := json.Unmarshal(request.Body, &body); err != nil {
				return matrixErrorMeta(matrixstate.ErrBadJSON())
			}
		}
		if pyTrue(gjson.GetBytes(request.Body, gjson.Escape(stateDagField))) {
			return nil, 0, Meta{}, NotReproduced{Reason: "state_dag"}
		}
		limit := 10
		if body.Limit != nil {
			limit = *body.Limit
		}
		resp, err := r.GetMissingEvents(ctx, request.Origin, serverName, request.RoomID,
			body.EarliestEvents, body.LatestEvents, limit)
		if err != nil {
			return matrixErrorMeta(err)
		}
		out, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, Meta{}, err
		}
		return out, http.StatusOK, Meta{WalkTruncated: resp.WalkTruncated}, nil

	case "event":
		resp, err := r.Event(ctx, request.Origin, serverName, request.EventID)
		if errors.Is(err, matrixstate.ErrEventNotFound) {
			// Synapse answers an unknown event with 404 and an empty body,
			// not a JSON error.
			return nil, http.StatusNotFound, Meta{}, nil
		}
		if err != nil {
			return matrixErrorMeta(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, Meta{}, err
		}
		return body, http.StatusOK, Meta{}, nil
	}
	return nil, 0, Meta{}, fmt.Errorf("native: no implementation for %q", request.Endpoint)
}

// matrixError turns a Matrix error into the body and status it is served with,
// leaving anything else as an internal failure.
func matrixError(err error) ([]byte, int, error) {
	var me *matrixstate.MatrixError
	if errors.As(err, &me) {
		body, mErr := json.Marshal(me)
		if mErr != nil {
			return nil, 0, mErr
		}
		return body, me.Status, nil
	}
	return nil, 0, err
}

// matrixErrorMeta is matrixError in the four-value shape Answer returns.
func matrixErrorMeta(err error) ([]byte, int, Meta, error) {
	body, status, internal := matrixError(err)
	return body, status, Meta{}, internal
}

// pyTrue is bool() of a JSON value: false for missing, null, false, 0, "",
// [] and {}.
func pyTrue(v gjson.Result) bool {
	switch v.Type {
	case gjson.Null, gjson.False:
		return false
	case gjson.Number:
		return v.Float() != 0
	case gjson.String:
		return v.Str != ""
	case gjson.JSON:
		empty := true
		v.ForEach(func(_, _ gjson.Result) bool { empty = false; return false })
		return !empty
	}
	return true
}

// notHosted is on_federation_query_client_keys' refusal of a user not ours.
var notHosted = []byte(`{"errcode":"M_UNKNOWN","error":"User is not hosted on this homeserver"}`)

// userKeysQuery is FederationClientKeysQueryServlet.on_POST over
// on_federation_query_client_keys: content["device_keys"] (absent, empty),
// every user ours or a 400, then the keys. Left to Synapse: another method,
// a body whose shape Synapse fails on or answers in its own words (a
// device_keys that is not an object, a user ID UserID.from_string refuses, a
// device list that is not a list of strings), and under MSC3984 a user an
// appservice owns exclusively.
func userKeysQuery(ctx context.Context, r Resolver, serverName string, request Request) ([]byte, int, Meta, error) {
	if request.Method != "POST" {
		return nil, 0, Meta{}, NotReproduced{Reason: "method"}
	}
	root := gjson.ParseBytes(request.Body)
	if len(request.Body) == 0 || !json.Valid(request.Body) || !root.IsObject() {
		return nil, 0, Meta{}, NotReproduced{Reason: "body"}
	}
	dk := root.Get("device_keys")
	if dk.Exists() && !dk.IsObject() {
		return nil, 0, Meta{}, NotReproduced{Reason: "body"}
	}
	var users []string
	devices := map[string][]string{}
	bad := false
	dk.ForEach(func(k, v gjson.Result) bool {
		var ids []string
		switch {
		case v.Type == gjson.Null:
		case v.IsArray():
			for _, e := range v.Array() {
				if e.Type != gjson.String {
					bad = true
					return false
				}
				ids = append(ids, e.Str)
			}
		default:
			bad = true
			return false
		}
		if _, seen := devices[k.Str]; !seen {
			users = append(users, k.Str)
		}
		devices[k.Str] = ids
		return true
	})
	if bad {
		return nil, 0, Meta{}, NotReproduced{Reason: "body"}
	}
	for _, u := range users {
		// UserID.from_string raises in its own words.
		if !strings.HasPrefix(u, "@") || !strings.Contains(u, ":") {
			return nil, 0, Meta{}, NotReproduced{Reason: "user_id"}
		}
	}
	for _, u := range users {
		if _, domain, _ := strings.Cut(u, ":"); domain != serverName {
			return notHosted, http.StatusBadRequest, Meta{}, nil
		}
	}
	if o := r.KeyQueryOptions(); o.MSC3984 {
		if o.ExclusiveUnknown {
			return nil, 0, Meta{}, NotReproduced{Reason: "appservice_namespaces"}
		}
		for _, u := range users {
			if o.ExclusiveUsers != nil && o.ExclusiveUsers.MatchString(u) {
				return nil, 0, Meta{}, NotReproduced{Reason: "appservice_keys"}
			}
		}
	}
	body, err := r.UserKeys(ctx, users, devices)
	if err != nil {
		return nil, 0, Meta{}, err
	}
	return body, http.StatusOK, Meta{}, nil
}

// UserID.from_string's refusals (types/__init__.py), as SynapseError bodies.
var (
	userIDNoSigil = []byte(`{"errcode":"M_INVALID_PARAM","error":"Expected UserID string to start with '@'"}`)
	userIDNoColon = []byte(`{"errcode":"M_INVALID_PARAM","error":"Expected UserID of the form '@localname:domain'"}`)
)

// userDevices is FederationUserDevicesQueryServlet.on_GET over
// on_federation_query_user_devices (handlers/device.py, v1.164.1): the
// path's user ID unquoted, UserID.from_string's 400s, a user not ours
// Synapse's 400, then the user's devices. Left to Synapse: another method
// (HEAD included, which it answers as GET without a body), a parameter Go
// and urllib.parse.unquote might decode differently (a malformed escape,
// bytes that are not UTF-8), a device list stream position we do not
// know, a key_json Synapse fails on, and under MSC3984 a user an
// appservice owns exclusively.
func userDevices(ctx context.Context, r Resolver, serverName string, request Request) ([]byte, int, Meta, error) {
	if request.Method != "GET" {
		return nil, 0, Meta{}, NotReproduced{Reason: "method"}
	}
	userID, err := url.PathUnescape(request.Param)
	if err != nil || !utf8.ValidString(userID) {
		return nil, 0, Meta{}, NotReproduced{Reason: "user_id"}
	}
	if !strings.HasPrefix(userID, "@") {
		return userIDNoSigil, http.StatusBadRequest, Meta{}, nil
	}
	_, domain, ok := strings.Cut(userID, ":")
	if !ok {
		return userIDNoColon, http.StatusBadRequest, Meta{}, nil
	}
	if domain != serverName {
		return notHosted, http.StatusBadRequest, Meta{}, nil
	}
	if o := r.KeyQueryOptions(); o.MSC3984 {
		if o.ExclusiveUnknown {
			return nil, 0, Meta{}, NotReproduced{Reason: "appservice_namespaces"}
		}
		if o.ExclusiveUsers != nil && o.ExclusiveUsers.MatchString(userID) {
			return nil, 0, Meta{}, NotReproduced{Reason: "appservice_keys"}
		}
	}
	body, err := r.UserDevices(ctx, userID)
	switch {
	case errors.Is(err, matrixstate.ErrNoDeviceStream):
		return nil, 0, Meta{}, NotReproduced{Reason: "stream_position"}
	case errors.Is(err, e2ekeys.ErrUnsplicable):
		return nil, 0, Meta{}, NotReproduced{Reason: "key_json"}
	case err != nil:
		return nil, 0, Meta{}, err
	}
	return body, http.StatusOK, Meta{}, nil
}
