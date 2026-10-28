// Package route classifies the client requests nginx sends to the
// event_creator worker.
//
// The nginx patterns (proxy_host/2.conf) are wider than the servlets behind
// them: "rooms/.*/send" also matches nothing Synapse serves, and
// "rooms/.*/state/" catches GETs of a single state event, which read. So a
// path is matched again here, against Synapse's own servlet patterns
// (rest/client/room.py, profile.py, knock.py), and anything that does not
// match exactly is Other, which is always proxied.
package route

import (
	"net/http"
	"net/url"
	"strings"
)

// Endpoint is one group of servlets, served in one mode.
type Endpoint string

const (
	// Send is PUT/POST rooms/{room}/send/{type}[/{txn}].
	Send Endpoint = "send"
	// State is PUT/POST rooms/{room}/state/{type}[/{key}].
	State Endpoint = "state"
	// Redact is PUT/POST rooms/{room}/redact/{event}[/{txn}].
	Redact Endpoint = "redact"
	// Membership is POST/PUT rooms/{room}/(join|invite|leave|ban|unban|kick).
	Membership Endpoint = "membership"
	// Join is POST/PUT join/{room_or_alias}[/{txn}].
	Join Endpoint = "join"
	// Knock is POST/PUT knock/{room_or_alias}[/{txn}].
	Knock Endpoint = "knock"
	// Profile is everything under profile/.
	Profile Endpoint = "profile"
	// Other matches nothing above.
	Other Endpoint = "other"
)

// Endpoints is every endpoint that has a mode, in a stable order.
var Endpoints = []Endpoint{Send, State, Redact, Membership, Join, Knock, Profile}

// Request is what a path says about the request.
type Request struct {
	Endpoint Endpoint
	// Write is false for a read that shares a path prefix with a write: a
	// GET of a state event, or of a profile.
	Write bool
	// Room is the room ID or alias from the path, unescaped. For Join and
	// Knock it may be an alias.
	Room string
	// Action is the membership action, the event type for Send and State,
	// or the redacted event ID.
	Action string
	// StateKey is State's state key ("" when absent).
	StateKey string
	// TxnID is the transaction ID for Send, Redact, Join, Knock and
	// Membership's PUT form.
	TxnID string
	// FieldPath is a Profile path ProfileFieldRestServlet serves; User is
	// its user ID and Action its field name, either possibly empty.
	FieldPath bool
	User      string
}

var membershipActions = map[string]bool{
	"join": true, "invite": true, "leave": true, "ban": true, "unban": true, "kick": true,
}

// Classify reads an escaped request path. It must be the escaped form:
// room IDs and event IDs are percent-encoded segments, and splitting the
// decoded path would cut an ID containing an encoded slash.
func Classify(method, escapedPath string) Request {
	other := Request{Endpoint: Other}

	rest, ok := strings.CutPrefix(escapedPath, "/_matrix/client/")
	if !ok {
		return other
	}
	version, rest, ok := strings.Cut(rest, "/")
	if !ok {
		return other
	}
	switch version {
	case "api", "r0", "v3", "unstable":
	default:
		return other
	}
	if version == "api" {
		v1, after, ok := strings.Cut(rest, "/")
		if !ok || v1 != "v1" {
			return other
		}
		rest = after
	}

	seg := strings.Split(rest, "/")
	parts := make([]string, len(seg))
	for i, s := range seg {
		u, err := url.PathUnescape(s)
		if err != nil {
			return other
		}
		parts[i] = u
	}
	write := method == http.MethodPut || method == http.MethodPost

	switch parts[0] {
	case "profile":
		// Every method: profile writes fan out to every room the user is
		// in, so the whole endpoint is one mode.
		r := Request{Endpoint: Profile, Write: method != http.MethodGet && method != http.MethodHead}
		// ProfileFieldRestServlet: displayname and avatar_url under every
		// prefix (client_patterns, v1=True), any other field under v3 only.
		if len(parts) == 3 && (parts[2] == "displayname" || parts[2] == "avatar_url" || version == "v3") {
			r.FieldPath, r.User, r.Action = true, parts[1], parts[2]
		}
		return r

	case "join", "knock":
		ep := Join
		if parts[0] == "knock" {
			ep = Knock
		}
		// register_txn_path: POST /join/{id}, PUT /join/{id}/{txn}.
		switch {
		case len(parts) == 2 && method == http.MethodPost:
			return Request{Endpoint: ep, Write: true, Room: parts[1]}
		case len(parts) == 3 && method == http.MethodPut:
			return Request{Endpoint: ep, Write: true, Room: parts[1], TxnID: parts[2]}
		}
		return other

	case "rooms":
		if len(parts) < 3 || parts[1] == "" {
			return other
		}
		room, verb, args := parts[1], parts[2], parts[3:]

		if membershipActions[verb] {
			switch {
			case len(args) == 0 && method == http.MethodPost:
				return Request{Endpoint: Membership, Write: true, Room: room, Action: verb}
			case len(args) == 1 && method == http.MethodPut:
				return Request{Endpoint: Membership, Write: true, Room: room, Action: verb, TxnID: args[0]}
			}
			return other
		}

		switch verb {
		case "send":
			switch {
			case len(args) == 1 && method == http.MethodPost:
				return Request{Endpoint: Send, Write: true, Room: room, Action: args[0]}
			case len(args) == 2 && method == http.MethodPut:
				return Request{Endpoint: Send, Write: true, Room: room, Action: args[0], TxnID: args[1]}
			}
		case "redact":
			switch {
			case len(args) == 1 && method == http.MethodPost:
				return Request{Endpoint: Redact, Write: true, Room: room, Action: args[0]}
			case len(args) == 2 && method == http.MethodPut:
				return Request{Endpoint: Redact, Write: true, Room: room, Action: args[0], TxnID: args[1]}
			}
		case "state":
			// rooms/{room}/state/{type}[/{key}]; GET reads.
			if len(args) == 1 || len(args) == 2 {
				r := Request{Endpoint: State, Write: write, Room: room, Action: args[0]}
				if len(args) == 2 {
					r.StateKey = args[1]
				}
				return r
			}
		}
	}
	return other
}
