package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// POST /keys/query: KeyQueryServlet over E2eKeysHandler.query_devices
// (rest/client/keys.py, handlers/e2e_keys.py, v1.164.1).
//
// Answered here: local users' devices and every user's cross-signing keys,
// from the database; remote users whose device lists this server tracks,
// from device_lists_remote_cache; and remote servers in backoff, as the
// failure Synapse records without asking them. Left to Synapse: anything
// that needs a federation query (a user whose list is not cached, or whose
// cached cross-signing keys are half there), a cached user's device that is
// not in the cache (Synapse's lookup raises), a body or timeout Synapse
// would refuse, and with MSC3984 a local user an appservice owns
// exclusively (Synapse asks the appservice).

// KeysDeps are what a key query reads beyond Deps.
type KeysDeps struct {
	// MSC4263 is msc4263_limit_key_queries_to_users_who_share_rooms: only
	// users sharing a room with the requester, joined or invited, are asked
	// about.
	MSC4263 bool
	// MSC3984 is msc3984_appservice_key_query: a local user in an exclusive
	// appservice namespace is asked of the appservice too.
	MSC3984 bool
	// ExclusiveUsers is the exclusive user namespaces (appservice.
	// ExclusiveUsers), nil when there are none; ExclusiveUnknown, that they
	// could not be read, so every MSC3984 query is Synapse's.
	ExclusiveUsers   *regexp.Regexp
	ExclusiveUnknown bool
}

// maxKeysQueryBody bounds what is read before deciding; a larger body is
// Synapse's.
const maxKeysQueryBody = 1 << 20

// notReadyForRetry is _NOT_READY_FOR_RETRY_FAILURE.
var notReadyForRetry = json.RawMessage(`{"status":503,"message":"Not ready for retry"}`)

var timeoutParam = regexp.MustCompile(`^[0-9]+$`)

// KeysQuery serves POST /keys/query.
func KeysQuery(d Deps, k KeysDeps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		setEndpoint(ann, "keys_query")
		if k.MSC3984 && k.ExclusiveUnknown {
			handBack(w, r, d, ann, "appservice_namespaces")
			return
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}
		// parse_integer(request, "timeout"): only the federation queries use
		// it, but a bad one is refused.
		if t, present := r.URL.Query()["timeout"]; present && (len(t) == 0 || !timeoutParam.MatchString(t[0])) {
			handBack(w, r, d, ann, "timeout")
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxKeysQueryBody+1))
		// Whatever happens next, main can still be handed the whole body.
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		if err != nil {
			refuse(w, ann, http.StatusInternalServerError, *internalError(d, "read body", err))
			return
		}
		if len(body) > maxKeysQueryBody {
			handBack(w, r, d, ann, "too_large")
			return
		}
		query, ok := parseKeysQuery(body)
		if !ok {
			handBack(w, r, d, ann, "body")
			return
		}

		resp, back, err := queryDevices(r.Context(), d, k, verdict, query)
		switch {
		case err != nil:
			refuse(w, ann, http.StatusInternalServerError, *internalError(d, "keys query", err))
		case back != "":
			handBack(w, r, d, ann, back)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(resp)
		}
	})
}

// keysQuery is device_keys, in the order of its keys, a repeated key
// keeping its first place and its last value, as a Python dict does.
type keysQuery struct {
	users   []string
	devices map[string][]string
}

// parseKeysQuery is parse_json_object_from_request and the servlet's
// checks: false for anything Synapse would refuse.
func parseKeysQuery(body []byte) (keysQuery, bool) {
	q := keysQuery{devices: map[string][]string{}}
	if !json.Valid(body) {
		return q, false
	}
	root := gjson.ParseBytes(body)
	dk := root.Get("device_keys")
	if !root.IsObject() || !dk.IsObject() {
		return q, false
	}
	ok := true
	dk.ForEach(func(key, v gjson.Result) bool {
		if !v.IsArray() {
			ok = false
			return false
		}
		ids := []string{}
		for _, e := range v.Array() {
			if e.Type != gjson.String {
				ok = false
				return false
			}
			ids = append(ids, e.Str)
		}
		if _, seen := q.devices[key.Str]; !seen {
			q.users = append(q.users, key.Str)
		}
		q.devices[key.Str] = ids
		return true
	})
	return q, ok
}

func queryDevices(ctx context.Context, d Deps, k KeysDeps, verdict clientauth.Verdict, query keysQuery) ([]byte, string, error) {
	from := verdict.UserID

	// filter_device_key_query: invalid user IDs are dropped, and with MSC4263
	// those who share no room with the requester, joined or invited.
	var valid []string
	for _, u := range query.users {
		if validUserID(u) {
			valid = append(valid, u)
		}
	}
	if k.MSC4263 && len(valid) > 0 {
		allowed, err := d.Store.UsersSharingARoomJoinedOrInvited(ctx, from, valid)
		if err != nil {
			return nil, "", err
		}
		kept := valid[:0]
		for _, u := range valid {
			if allowed[u] {
				kept = append(kept, u)
			}
		}
		valid = kept
	}
	var local, remote []string
	for _, u := range valid {
		if isMine(u, d.ServerName) {
			local = append(local, u)
		} else {
			remote = append(remote, u)
		}
	}
	// appservice_handler.query_keys asks the appservice owning a local user
	// exclusively; its answer is merged over ours.
	if k.MSC3984 && k.ExclusiveUsers != nil {
		for _, u := range local {
			if k.ExclusiveUsers.MatchString(u) {
				return nil, "appservice_keys", nil
			}
		}
	}

	results := map[string]map[string]json.RawMessage{}
	failures := map[string]json.RawMessage{}

	// query_local_devices.
	if len(local) > 0 {
		var all []string
		var pairs []store.UserDevice
		for _, u := range local {
			results[u] = map[string]json.RawMessage{}
			if len(query.devices[u]) == 0 {
				all = append(all, u)
			}
			for _, dev := range query.devices[u] {
				pairs = append(pairs, store.UserDevice{UserID: u, DeviceID: dev})
			}
		}
		keys, err := d.Store.DeviceKeys(ctx, all, pairs)
		if err != nil {
			return nil, "", err
		}
		var signed []store.UserDevice
		for u, devs := range keys {
			for dev, k := range devs {
				if k.KeyJSON != nil {
					signed = append(signed, store.UserDevice{UserID: u, DeviceID: dev})
				}
			}
		}
		sigs, err := d.Store.DeviceSelfCrossSignatures(ctx, signed)
		if err != nil {
			return nil, "", err
		}
		for u, devs := range keys {
			if results[u] == nil {
				continue
			}
			for dev, k := range devs {
				if k.KeyJSON == nil {
					continue
				}
				body := k.KeyJSON
				for _, s := range sigs[store.UserDevice{UserID: u, DeviceID: dev}] {
					if body, err = sjson.SetBytes(body, "signatures."+gjson.Escape(u)+"."+gjson.Escape(s.KeyID), s.Signature); err != nil {
						return nil, "", err
					}
				}
				unsigned := `{}`
				if k.DisplayName != nil {
					unsigned = `{"device_display_name":` + string(jsonString(*k.DisplayName)) + `}`
				}
				if body, err = sjson.SetRawBytes(body, "unsigned", []byte(unsigned)); err != nil {
					return nil, "", err
				}
				results[u][dev] = body
			}
		}
	}

	// get_cross_signing_keys_from_cache, with from's signatures.
	master, selfSigning, userSigning, err := crossSigningKeys(ctx, d, valid, from)
	if err != nil {
		return nil, "", err
	}

	// The remote users this server tracks, from its cache.
	if len(remote) > 0 {
		var whole []string
		wholeSet := map[string]bool{}
		for _, u := range remote {
			if len(query.devices[u]) == 0 {
				whole = append(whole, u)
				wholeSet[u] = true
			}
		}
		cached, err := d.Store.UsersWithCachedDevices(ctx, remote)
		if err != nil {
			return nil, "", err
		}
		notInCache := map[string]bool{}
		for _, u := range remote {
			if !cached[u] {
				notInCache[u] = true
			}
		}
		remoteResults := map[string]map[string][]byte{}
		for _, u := range whole {
			if !cached[u] {
				continue
			}
			if remoteResults[u], err = d.Store.CachedDevices(ctx, u); err != nil {
				return nil, "", err
			}
		}
		for _, u := range remote {
			if !cached[u] || wholeSet[u] {
				continue
			}
			for _, dev := range query.devices[u] {
				content, found, err := d.Store.CachedDevice(ctx, u, dev)
				if err != nil {
					return nil, "", err
				}
				if !found {
					return nil, "uncached_device", nil
				}
				if remoteResults[u] == nil {
					remoteResults[u] = map[string][]byte{}
				}
				remoteResults[u][dev] = content
			}
		}

		// Cached users this server no longer shares a room with are asked
		// again.
		cachedUsers := make([]string, 0, len(remoteResults))
		for u := range remoteResults {
			cachedUsers = append(cachedUsers, u)
		}
		still, err := d.Store.UsersServerSharesRoomWith(ctx, cachedUsers)
		if err != nil {
			return nil, "", err
		}
		for _, u := range cachedUsers {
			if !still[u] {
				notInCache[u] = true
				delete(remoteResults, u)
			}
		}

		for u, devs := range remoteResults {
			if results[u] == nil {
				results[u] = map[string]json.RawMessage{}
			}
			for dev, content := range devs {
				keys := gjson.GetBytes(content, "keys")
				if !pyTruthy(keys) {
					continue
				}
				if !keys.IsObject() {
					return nil, "cached_device", nil
				}
				out := []byte(keys.Raw)
				name := gjson.GetBytes(content, "device_display_name")
				existing := keys.Get("unsigned")
				switch {
				case !existing.Exists():
					out, err = sjson.SetRawBytes(out, "unsigned", []byte(`{}`))
				case !existing.IsObject() && pyTruthy(name):
					return nil, "cached_device", nil
				}
				if err != nil {
					return nil, "", err
				}
				if pyTruthy(name) {
					if out, err = sjson.SetRawBytes(out, `unsigned.device_display_name`, []byte(name.Raw)); err != nil {
						return nil, "", err
					}
				}
				results[u][dev] = out
			}
		}

		// Both cross-signing keys or neither: one alone is asked for.
		for _, u := range remote {
			if (master[u] != nil) != (selfSigning[u] != nil) {
				notInCache[u] = true
			}
		}

		// Destinations in backoff are not asked; the rest would be.
		var dests []string
		seen := map[string]bool{}
		for _, u := range remote {
			if !notInCache[u] {
				continue
			}
			_, domain, _ := strings.Cut(u, ":")
			if !seen[domain] {
				seen[domain] = true
				dests = append(dests, domain)
			}
		}
		if len(dests) > 0 {
			down, err := d.Store.DestinationsNotReady(ctx, dests, time.Now().UnixMilli(), 60*1000)
			if err != nil {
				return nil, "", err
			}
			for _, dest := range dests {
				if !down[dest] {
					return nil, "federation", nil
				}
				failures[dest] = notReadyForRetry
			}
		}
	}

	resp := map[string]any{
		"device_keys":       results,
		"failures":          failures,
		"master_keys":       master,
		"self_signing_keys": selfSigning,
		"user_signing_keys": userSigning,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(resp); err != nil {
		return nil, "", err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), "", nil
}

// crossSigningKeys is get_cross_signing_keys_from_cache over
// get_e2e_cross_signing_keys_bulk(user_ids, from_user_id): everyone's
// master and self-signing keys, from's own user-signing key, each with the
// signatures from made on it.
func crossSigningKeys(ctx context.Context, d Deps, users []string, from string) (
	master, selfSigning, userSigning map[string]json.RawMessage, err error) {

	master, selfSigning, userSigning = map[string]json.RawMessage{}, map[string]json.RawMessage{}, map[string]json.RawMessage{}
	keys, err := d.Store.CrossSigningKeys(ctx, users)
	if err != nil {
		return nil, nil, nil, err
	}
	// _get_e2e_cross_signing_signatures_txn: each key is found by its user
	// and the last value of its "keys", its public key.
	type slot struct{ user, typ string }
	bySigned := map[store.UserDevice]slot{}
	var targets []store.UserDevice
	for u, byType := range keys {
		for typ, data := range byType {
			pub := ""
			gjson.GetBytes(data, "keys").ForEach(func(_, v gjson.Result) bool {
				pub = v.String()
				return true
			})
			t := store.UserDevice{UserID: u, DeviceID: pub}
			if _, dup := bySigned[t]; !dup {
				targets = append(targets, t)
			}
			bySigned[t] = slot{u, typ}
		}
	}
	if from != "" && len(targets) > 0 {
		sigs, err := d.Store.CrossSignaturesBy(ctx, from, targets)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, s := range sigs {
			at := bySigned[s.Target]
			data := keys[at.user][at.typ]
			if data, err = sjson.SetBytes(data, "signatures."+gjson.Escape(from)+"."+gjson.Escape(s.KeyID), s.Signature.Signature); err != nil {
				return nil, nil, nil, err
			}
			keys[at.user][at.typ] = data
		}
	}
	for u, byType := range keys {
		if k, ok := byType["master"]; ok {
			master[u] = k
		}
		if k, ok := byType["self_signing"]; ok {
			selfSigning[u] = k
		}
	}
	if k, ok := keys[from]["user_signing"]; ok && from != "" {
		userSigning[from] = k
	}
	return master, selfSigning, userSigning, nil
}

// pyTruthy is Python's truth of a JSON value: false for a missing key,
// null, false, 0, "", [] and {}.
func pyTruthy(v gjson.Result) bool {
	switch v.Type {
	case gjson.Null:
		return false
	case gjson.False:
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
