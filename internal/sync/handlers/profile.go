package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
)

// GET /profile/{userId} and /profile/{userId}/{field}: ProfileRestServlet
// and ProfileFieldRestServlet (rest/client/profile.py, v1.164.1), over
// ProfileHandler and the profiles table.

// ProfileDeps are what a profile lookup reads beyond Deps.
type ProfileDeps struct {
	Settings synapsecfg.Profile
	// Limiter is the process's rc_profile limiter.
	Limiter *ratelimit.Limiter
	// MSC4133Enabled adds the unstable field path.
	MSC4133Enabled bool
	// Modules is a module being configured: its ratelimit callbacks may
	// override rc_profile, so every lookup is Synapse's.
	Modules bool
	// Pause is how long a ratelimited lookup waits before its 429 (Synapse:
	// half a second, against tight retry loops).
	Pause time.Duration
}

// The paths, matched against the raw path as Synapse matches them, each
// group then unquoted.
var (
	profilePath = regexp.MustCompile(
		`^/_matrix/client/(?:api/v1|r0|v3|unstable)/profile/([^/]*)$`)
	// displayname and avatar_url under every prefix (client_patterns, v1).
	profileNamedFieldPath = regexp.MustCompile(
		`^/_matrix/client/(?:api/v1|r0|v3|unstable)/profile/([^/]*)/(displayname|avatar_url)$`)
	profileV3FieldPath = regexp.MustCompile(
		`^/_matrix/client/v3/profile/([^/]*)/([^/]*)$`)
	profileMSC4133Path = regexp.MustCompile(
		`^/_matrix/client/unstable/uk\.tcpip\.msc4133/profile/([^/]*)/([^/]*)$`)
)

// namespacedGrammar is NAMESPACED_GRAMMAR, matched as Python's re.match with
// `$`, which also matches before a final newline.
var namespacedGrammar = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,254}\n?$`)

const maxCustomFieldLen = 255 // MAX_CUSTOM_FIELD_LEN, bytes

// Profile serves a profile lookup of a local user. Left to Synapse: a
// remote user (a federation query), a configured module, and a path whose
// parts do not unquote.
func Profile(d Deps, p ProfileDeps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		raw := r.URL.EscapedPath()

		var userID, field string
		isField, ok := false, true
		if m := profilePath.FindStringSubmatch(raw); m != nil {
			setEndpoint(ann, "profile")
			userID, ok = unquote(m[1], ok)
		} else if m := fieldPathMatch(raw, p.MSC4133Enabled); m != nil {
			setEndpoint(ann, "profile_field")
			isField = true
			userID, ok = unquote(m[1], ok)
			field, ok = unquote(m[2], ok)
		} else {
			ok = false
		}
		if !ok {
			handBack(w, r, d, ann, "path")
			return
		}
		if p.Modules {
			handBack(w, r, d, ann, "modules")
			return
		}

		// _auth_and_ratelimit_profile_lookup.
		var requester *clientauth.Verdict
		if p.Settings.RequireAuth || hasAccessToken(r) {
			v, ok := authenticate(w, r, d, ann)
			if !ok {
				return
			}
			if p.Settings.RequireAuth && v.IsGuest {
				refuse(w, ann, http.StatusForbidden, matrixerr.Error{ErrCode: "M_GUEST_ACCESS_FORBIDDEN",
					Error: "Guest access not allowed"})
				return
			}
			requester = &v
		}
		if limited, retryMS, err := profileRatelimit(r.Context(), d, p, requester, r); err != nil {
			refuse(w, ann, http.StatusInternalServerError, *internalError(d, "ratelimit", err))
			return
		} else if limited {
			limitExceeded(r.Context(), w, ann, p.Pause, retryMS)
			return
		}

		if !validUserID(userID) {
			refuse(w, ann, http.StatusBadRequest, matrixerr.Error{ErrCode: matrixerr.CodeInvalidParam,
				Error: "Invalid user id"})
			return
		}
		if isField {
			switch {
			case field == "":
				refuse(w, ann, http.StatusBadRequest, matrixerr.Error{ErrCode: matrixerr.CodeInvalidParam,
					Error: "Field name too short"})
				return
			case len(field) > maxCustomFieldLen:
				refuse(w, ann, http.StatusBadRequest, matrixerr.Error{ErrCode: "M_KEY_TOO_LARGE",
					Error: "Field name too long"})
				return
			case !namespacedGrammar.MatchString(field):
				refuse(w, ann, http.StatusBadRequest, matrixerr.Error{ErrCode: matrixerr.CodeInvalidParam,
					Error: "Field name does not follow Common Namespaced Identifier Grammar"})
				return
			}
		}
		if !isMine(userID, d.ServerName) {
			handBack(w, r, d, ann, "remote_user")
			return
		}

		ctx := r.Context()
		// check_profile_query_allowed (MSC1301).
		if p.Settings.LimitToSharedRooms && requester != nil && requester.UserID != userID {
			shared, err := d.Store.ShareARoom(ctx, requester.UserID, userID)
			if err != nil {
				refuse(w, ann, http.StatusInternalServerError, *internalError(d, "shared rooms", err))
				return
			}
			if !shared {
				refuse(w, ann, http.StatusForbidden, matrixerr.Error{ErrCode: matrixerr.CodeForbidden,
					Error: "Profile isn't available"})
				return
			}
		}

		var body []byte
		var status int
		var mxErr *matrixerr.Error
		if isField {
			body, status, mxErr = profileField(ctx, d, userID, field)
		} else {
			body, status, mxErr = wholeProfile(ctx, d, userID)
		}
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func fieldPathMatch(raw string, msc4133 bool) []string {
	if m := profileNamedFieldPath.FindStringSubmatch(raw); m != nil {
		return m
	}
	if m := profileV3FieldPath.FindStringSubmatch(raw); m != nil {
		return m
	}
	if msc4133 {
		return profileMSC4133Path.FindStringSubmatch(raw)
	}
	return nil
}

// hasAccessToken is Auth.has_access_token: any Authorization header, or an
// access_token parameter, even an empty one.
func hasAccessToken(r *http.Request) bool {
	_, inQuery := r.URL.Query()["access_token"]
	return inQuery || len(r.Header.Values("Authorization")) > 0
}

// validUserID is UserID.is_valid: "@", a localpart, ":", and a server name
// parse_and_validate_server_name accepts.
func validUserID(s string) bool {
	if !strings.HasPrefix(s, "@") {
		return false
	}
	_, domain, ok := strings.Cut(s[1:], ":")
	return ok && pdu.ValidServerName(domain)
}

// profileRatelimit is the rc_profile limiter's ratelimit(requester, key):
// the requester's user ID, or else the client's address. An unlimited
// appservice, and an entity whose ratelimit_override has no rate, are not
// limited.
func profileRatelimit(ctx context.Context, d Deps, p ProfileDeps, requester *clientauth.Verdict,
	r *http.Request) (bool, int64, error) {

	if p.Limiter == nil {
		return false, 0, nil
	}
	key := clientauth.ClientIP(r)
	if requester != nil {
		key = requester.UserID
		if requester.AppServiceID != "" && requester.AppServiceUnlimited {
			return false, 0, nil
		}
		entity := requester.Entity
		if entity == "" {
			entity = requester.UserID
		}
		exempt, err := d.Store.RatelimitExempt(ctx, entity)
		if err != nil {
			return false, 0, err
		}
		if exempt {
			return false, 0, nil
		}
	}
	ok, retry := p.Limiter.CanDo(key, true)
	return !ok, retry, nil
}

// limitExceeded is LimitExceededError, after its pause.
func limitExceeded(ctx context.Context, w http.ResponseWriter, ann *server.Annotation, pause time.Duration, retryMS int64) {
	select {
	case <-time.After(pause):
	case <-ctx.Done():
	}
	annotateRefusal(ann, http.StatusTooManyRequests, "M_LIMIT_EXCEEDED")
	b, _ := json.Marshal(map[string]any{"errcode": "M_LIMIT_EXCEEDED", "error": "Too Many Requests",
		"retry_after_ms": retryMS})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.FormatInt(ratelimit.RetryAfterHeader(retryMS), 10))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(b)
}

var profileNotFound = matrixerr.Error{ErrCode: matrixerr.CodeNotFound, Error: "Profile was not found"}

// wholeProfile is ProfileHandler.get_profile for a local user:
// get_profileinfo, then get_profile_fields, which fails a user without a
// profiles row with its StoreError as it is.
func wholeProfile(ctx context.Context, d Deps, userID string) ([]byte, int, *matrixerr.Error) {
	prof, found, err := d.Store.ProfileRow(ctx, userID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "profile", err)
	}
	if !found {
		return nil, http.StatusNotFound, &matrixerr.Error{ErrCode: matrixerr.CodeUnknown,
			Error: "No row found (profiles)"}
	}
	var fields []gjson.Result
	if len(prof.Fields) > 0 {
		gjson.ParseBytes(prof.Fields).ForEach(func(k, v gjson.Result) bool {
			fields = append(fields, k, v)
			return true
		})
	}
	if prof.DisplayName == nil && prof.AvatarURL == nil && len(fields) == 0 {
		return nil, http.StatusNotFound, &profileNotFound
	}

	// ret, then ret.update(extra_fields): a field of the same name as one
	// already set keeps its place and takes the new value.
	var keys []string
	values := map[string]json.RawMessage{}
	set := func(k string, v json.RawMessage) {
		if _, ok := values[k]; !ok {
			keys = append(keys, k)
		}
		values[k] = v
	}
	if prof.DisplayName != nil {
		set("displayname", jsonString(*prof.DisplayName))
	}
	if prof.AvatarURL != nil {
		set("avatar_url", jsonString(*prof.AvatarURL))
	}
	for i := 0; i < len(fields); i += 2 {
		set(fields[i].Str, compact(fields[i+1].Raw))
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(jsonString(k))
		buf.WriteByte(':')
		buf.Write(values[k])
	}
	buf.WriteByte('}')
	return buf.Bytes(), http.StatusOK, nil
}

// profileField is ProfileFieldRestServlet.on_GET's answer for a local user.
// displayname and avatar_url: {} when unset; another field: 404 unless set.
// A user without a profiles row is "Profile was not found" for each.
func profileField(ctx context.Context, d Deps, userID, field string) ([]byte, int, *matrixerr.Error) {
	if field == "displayname" || field == "avatar_url" {
		prof, found, err := d.Store.ProfileRow(ctx, userID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "profile", err)
		}
		if !found {
			return nil, http.StatusNotFound, &profileNotFound
		}
		v := prof.DisplayName
		if field == "avatar_url" {
			v = prof.AvatarURL
		}
		if v == nil {
			return []byte(`{}`), http.StatusOK, nil
		}
		return []byte(`{` + string(jsonString(field)) + `:` + string(jsonString(*v)) + `}`), http.StatusOK, nil
	}
	localpart, _, _ := strings.Cut(strings.TrimPrefix(userID, "@"), ":")
	row, exists, value, err := d.Store.ProfileField(ctx, localpart, field)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "profile field", err)
	}
	if !row || !exists {
		return nil, http.StatusNotFound, &profileNotFound
	}
	return []byte(`{` + string(jsonString(field)) + `:` + string(compact(string(value))) + `}`), http.StatusOK, nil
}

// jsonString encodes s without escaping <, > and &, as Synapse's encoder.
func jsonString(s string) json.RawMessage {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// compact is jsonb's text without its spaces.
func compact(raw string) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(raw)); err != nil {
		return json.RawMessage(raw)
	}
	return buf.Bytes()
}
