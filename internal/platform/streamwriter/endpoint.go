package streamwriter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

// Endpoint is get_repl_stream_updates (replication/http/streams.py): the
// updates of a stream this process writes between two tokens, for a
// follower that missed them. Only a stream not in the database is fetched
// this way; for the rest a follower reads the database itself.
func (w *Writer) Endpoint() replserver.Endpoint {
	return replserver.Endpoint{
		Name:     "get_repl_stream_updates",
		Method:   http.MethodGet,
		PathArgs: []string{"stream_name"},
		// CACHE is left at its default, True, so the path ends in a txn_id
		// even for this GET (_base.py:396); WAIT_FOR_STREAMS is False.
		Cache:  true,
		Handle: w.serveUpdates,
	}
}

func (w *Writer) serveUpdates(_ context.Context, req replserver.Request) (int, map[string]any, error) {
	s := w.Stream(req.Args["stream_name"])
	if s == nil {
		return 0, nil, &replserver.Error{Code: 400, Errcode: "M_UNKNOWN", Msg: "Unknown stream"}
	}
	from, err := queryInt(req.Query, "from_token")
	if err != nil {
		return 0, nil, err
	}
	upto, err := queryInt(req.Query, "upto_token")
	if err != nil {
		return 0, nil, err
	}
	// get_updates_since
	updates, limited := []Update(nil), false
	if from != upto {
		updates, upto, limited = s.UpdatesSince(from, upto, TargetRowCount)
	}
	rows := make([][2]any, 0, len(updates))
	for _, u := range updates {
		rows = append(rows, [2]any{u.Token, json.RawMessage(u.Row)})
	}
	return 200, map[string]any{"updates": rows, "upto_token": upto, "limited": limited}, nil
}

// queryInt is parse_integer(request, name, required=True): Python's int()
// of the first value, which allows surrounding whitespace, a sign and
// underscores between digits, and no negatives.
func queryInt(q url.Values, name string) (int64, error) {
	vs, ok := q[name]
	if !ok || len(vs) == 0 {
		return 0, &replserver.Error{Code: 400, Errcode: "M_MISSING_PARAM",
			Msg: "Missing required integer query parameter " + name}
	}
	n, ok := pyInt(vs[0])
	if !ok {
		return 0, &replserver.Error{Code: 400, Errcode: "M_INVALID_PARAM",
			Msg: "Query parameter " + name + " must be an integer"}
	}
	if n < 0 {
		return 0, &replserver.Error{Code: 400, Errcode: "M_INVALID_PARAM",
			Msg: "Query parameter " + name + " must be a positive integer."}
	}
	return n, nil
}

func pyInt(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	sign := ""
	if s != "" && (s[0] == '+' || s[0] == '-') {
		sign, s = s[:1], s[1:]
	}
	if s == "" || s[0] == '_' || s[len(s)-1] == '_' || strings.Contains(s, "__") {
		return 0, false
	}
	digits := strings.ReplaceAll(s, "_", "")
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(sign+digits, 10, 64)
	return n, err == nil
}
