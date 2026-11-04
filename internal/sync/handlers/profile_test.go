package handlers

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// The C1 fixture, with profiles:
//
//	alice  a display name, no avatar, three custom fields
//	bob    a row with nothing set
//	carol  no row at all
//
// bob and alice are both joined to !w; eve shares no room with alice.
func newProfileFixture(t *testing.T, s synapsecfg.Profile) (*stateFixture, func(user, path string) (int, []byte)) {
	t.Helper()
	f := newStateFixture(t)
	f.exec(`INSERT INTO profiles (user_id, full_user_id, displayname, avatar_url, fields) VALUES
		('alice', '@alice:example.org', 'Alice <A&B>', NULL, '{"m.status": {"text": "away"}, "a.b": 1, "x.null": null}'),
		('bob', '@bob:example.org', NULL, NULL, NULL)`)
	f.exec(`INSERT INTO current_state_events (event_id, room_id, type, state_key, membership) VALUES
		('$wa', '!w:example.org', 'm.room.member', '@alice:example.org', 'join'),
		('$wb', '!w:example.org', 'm.room.member', '@bob:example.org', 'join')`)
	if s.Rc.Burst == 0 {
		s.Rc = synapsecfg.Ratelimit{Key: "rc_profile", PerSecond: 1, Burst: 500}
	}
	h := Profile(f.deps, ProfileDeps{Settings: s, Limiter: ratelimit.New(s.Rc, nil)})
	return f, func(user, path string) (int, []byte) { return serve(h, user, path) }
}

func TestProfile(t *testing.T) {
	f, get := newProfileFixture(t, synapsecfg.Profile{})
	const p = "/_matrix/client/v3/profile/"
	for _, c := range []struct {
		name, user, path string
		code             int
		body             string
	}{
		{"whole", "bob", p + "%40alice%3Aexample.org", 200,
			`{"displayname":"Alice <A&B>","a.b":1,"x.null":null,"m.status":{"text":"away"}}`},
		{"whole, anonymous", "", p + "@alice:example.org", 200,
			`{"displayname":"Alice <A&B>","a.b":1,"x.null":null,"m.status":{"text":"away"}}`},
		{"whole, nothing set", "alice", p + "@bob:example.org", 404, `{"errcode":"M_NOT_FOUND","error":"Profile was not found"}`},
		{"whole, no row", "alice", p + "@carol:example.org", 404, `{"errcode":"M_UNKNOWN","error":"No row found (profiles)"}`},
		{"displayname", "bob", p + "@alice:example.org/displayname", 200, `{"displayname":"Alice <A&B>"}`},
		{"displayname, r0", "bob", "/_matrix/client/r0/profile/@alice:example.org/displayname", 200, `{"displayname":"Alice <A&B>"}`},
		{"avatar_url unset", "bob", p + "@alice:example.org/avatar_url", 200, `{}`},
		{"displayname, no row", "bob", p + "@carol:example.org/displayname", 404, `{"errcode":"M_NOT_FOUND","error":"Profile was not found"}`},
		{"custom", "bob", p + "@alice:example.org/a.b", 200, `{"a.b":1}`},
		{"custom object", "bob", p + "@alice:example.org/m.status", 200, `{"m.status":{"text":"away"}}`},
		{"custom null", "bob", p + "@alice:example.org/x.null", 200, `{"x.null":null}`},
		{"custom unset", "bob", p + "@alice:example.org/x.y", 404, `{"errcode":"M_NOT_FOUND","error":"Profile was not found"}`},
		{"custom, row without fields", "bob", p + "@bob:example.org/x.y", 404, `{"errcode":"M_NOT_FOUND","error":"Profile was not found"}`},
		{"custom, no row", "bob", p + "@carol:example.org/x.y", 404, `{"errcode":"M_NOT_FOUND","error":"Profile was not found"}`},
		{"invalid user", "bob", p + "alice", 400, `{"errcode":"M_INVALID_PARAM","error":"Invalid user id"}`},
		{"invalid server name", "bob", p + "@alice:bad_name", 400, `{"errcode":"M_INVALID_PARAM","error":"Invalid user id"}`},
		{"field too short", "bob", p + "@alice:example.org/", 400, `{"errcode":"M_INVALID_PARAM","error":"Field name too short"}`},
		{"field grammar", "bob", p + "@alice:example.org/Nope", 400,
			`{"errcode":"M_INVALID_PARAM","error":"Field name does not follow Common Namespaced Identifier Grammar"}`},
		{"field too long", "bob", p + "@alice:example.org/" + strings.Repeat("a", 256), 400, `{"errcode":"M_KEY_TOO_LARGE","error":"Field name too long"}`},
	} {
		code, body := get(c.user, c.path)
		if code != c.code || (c.body != "" && string(body) != c.body) {
			t.Errorf("%s: %d %s", c.name, code, body)
		}
	}

	f.handedIn = nil
	for _, path := range []string{
		p + "@someone:elsewhere.org",                                               // remote
		p + "@someone:elsewhere.org/displayname",                                   // remote
		"/_matrix/client/r0/profile/@alice:example.org/a.b",                        // custom fields are v3's
		"/_matrix/client/unstable/uk.tcpip.msc4133/profile/@alice:example.org/a.b", // MSC4133 off
	} {
		if code, body := get("bob", path); code != 299 {
			t.Errorf("%s: %d %s", path, code, body)
		}
	}
	if len(f.handedIn) != 4 {
		t.Errorf("handed back %v", f.handedIn)
	}
}

func TestProfileRequireAuthAndSharedRooms(t *testing.T) {
	_, get := newProfileFixture(t, synapsecfg.Profile{RequireAuth: true, LimitToSharedRooms: true})
	const alice = "/_matrix/client/v3/profile/@alice:example.org/displayname"
	if code, body := get("", alice); code != 401 || !strings.Contains(string(body), "M_MISSING_TOKEN") {
		t.Errorf("anonymous: %d %s", code, body)
	}
	if code, _ := get("bob", alice); code != 200 {
		t.Errorf("bob shares !w: %d", code)
	}
	if code, _ := get("alice", alice); code != 200 {
		t.Errorf("one's own: %d", code)
	}
	if code, body := get("eve", alice); code != 403 || string(body) != `{"errcode":"M_FORBIDDEN","error":"Profile isn't available"}` {
		t.Errorf("eve: %d %s", code, body)
	}
}

func TestProfileRatelimit(t *testing.T) {
	f, get := newProfileFixture(t, synapsecfg.Profile{Rc: synapsecfg.Ratelimit{Key: "rc_profile", PerSecond: 0.001, Burst: 2}})
	const alice = "/_matrix/client/v3/profile/@alice:example.org/displayname"
	for i := 0; i < 2; i++ {
		if code, _ := get("bob", alice); code != 200 {
			t.Fatalf("%d: %d", i, code)
		}
	}
	code, body := get("bob", alice)
	if code != http.StatusTooManyRequests || !strings.HasPrefix(string(body), `{"errcode":"M_LIMIT_EXCEEDED","error":"Too Many Requests","retry_after_ms":`) {
		t.Fatalf("%d %s", code, body)
	}
	// Per user: alice has her own bucket; and an override without a rate
	// exempts.
	if code, _ := get("alice", alice); code != 200 {
		t.Errorf("alice: %d", code)
	}
	f.exec(`INSERT INTO ratelimit_override (user_id, messages_per_second, burst_count) VALUES ('@bob:example.org', 0, 0)`)
	if code, _ := get("bob", alice); code != 200 {
		t.Errorf("exempt bob: %d", code)
	}
}
