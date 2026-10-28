package handlers

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

//go:embed testdata/roomstate_schema.sql
var roomStateSchema string

// The room, and who is in it how:
//
//	alice  joined (and a server admin)
//	bob    joined, then left; the topic changed after, and the room got a name
//	carol  invited
//	dave   left, and forgot the room
//	eve    nothing; !w is world_readable, !r is not
//	frank  left !o by an outlier leave, which has no state group
//
// !p is partial-state.
type stateFixture struct {
	h        http.Handler
	handedIn []string
}

func newStateFixture(t *testing.T) *stateFixture {
	t.Helper()
	admin := os.Getenv("GOSYNC_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip(`GOSYNC_TEST_ADMIN_DSN not set (a throwaway PostgreSQL, e.g. "host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse")`)
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("GOSYNC_TEST_ADMIN_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	ac, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("roomstate_test_%d", time.Now().UnixNano())
	if _, err := ac.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = ac.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		_ = ac.Close(context.Background())
	})
	// A keyword/value DSN: a repeated keyword's last value wins.
	dsn := admin + " dbname=" + name
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, roomStateSchema); err != nil {
		t.Fatal(err)
	}

	stream := int64(0)
	ev := func(room, id, typ, stateKey, sender, content string) {
		t.Helper()
		stream++
		body := fmt.Sprintf(`{"type":%q,"room_id":%q,"sender":%q,"state_key":%q,"content":%s,"origin_server_ts":%d,"depth":%d,"auth_events":[],"prev_events":[],"hashes":{"sha256":"x"},"signatures":{}}`,
			typ, room, sender, stateKey, content, 1700000000000+stream, stream)
		if stateKey == "-" {
			body = strings.Replace(body, `"state_key":"-",`, "", 1)
		}
		sk := any(stateKey)
		if stateKey == "-" {
			sk = nil
		}
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO events (topological_ordering, event_id, type, room_id, processed, outlier, depth,
				origin_server_ts, received_ts, sender, stream_ordering, state_key, instance_name)
				VALUES ($1, $2, $3, $4, true, false, $1, $5, $5, $6, $1, $7, 'master')`,
				[]any{stream, id, typ, room, 1700000000000 + stream, sender, sk}},
			{`INSERT INTO event_json (event_id, room_id, internal_metadata, json, format_version)
				VALUES ($1, $2, '{}', $3, 3)`, []any{id, room, body}},
		} {
			if _, err := conn.Exec(ctx, q.sql, q.args...); err != nil {
				t.Fatalf("%s: %v", id, err)
			}
		}
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}

	const r, w, o, p = "!r:example.org", "!w:example.org", "!o:example.org", "!p:example.org"
	for _, room := range []string{r, w, o, p} {
		exec(`INSERT INTO rooms (room_id, room_version) VALUES ($1, '10')`, room)
	}
	ev(r, "$create", "m.room.create", "", "@alice:example.org", `{"room_version":"10"}`)
	ev(r, "$alice", "m.room.member", "@alice:example.org", "@alice:example.org", `{"membership":"join"}`)
	ev(r, "$hv", "m.room.history_visibility", "", "@alice:example.org", `{"history_visibility":"shared"}`)
	ev(r, "$topic1", "m.room.topic", "", "@alice:example.org", `{"topic":"first"}`)
	ev(r, "$bobjoin", "m.room.member", "@bob:example.org", "@bob:example.org", `{"membership":"join"}`)
	ev(r, "$bobleave", "m.room.member", "@bob:example.org", "@bob:example.org", `{"membership":"leave"}`)
	ev(r, "$topic2", "m.room.topic", "", "@alice:example.org", `{"topic":"second"}`)
	ev(r, "$name", "m.room.name", "", "@alice:example.org", `{"name":"secret"}`)
	ev(r, "$redact", "m.room.redaction", "-", "@alice:example.org", `{"redacts":"$name"}`)
	ev(r, "$carol", "m.room.member", "@carol:example.org", "@alice:example.org", `{"membership":"invite"}`)
	ev(r, "$davejoin", "m.room.member", "@dave:example.org", "@dave:example.org", `{"membership":"join"}`)
	ev(r, "$daveleave", "m.room.member", "@dave:example.org", "@dave:example.org", `{"membership":"leave"}`)
	ev(r, "$redact2", "m.room.redaction", "-", "@alice:example.org", `{"redacts":"$name"}`)
	// Two redactions of $name: Synapse takes the first row its unordered
	// SELECT returns -- the one inserted first -- not the earliest event.
	exec(`INSERT INTO redactions (event_id, redacts, recheck) VALUES ('$redact2', '$name', false), ('$redact', '$name', false)`)
	// MSC4293: mallory's ban redacts what mallory sent.
	ev(r, "$malstate", "x.mallory", "", "@mallory:example.org", `{"spam":true}`)
	ev(r, "$malban", "m.room.member", "@mallory:example.org", "@alice:example.org", `{"membership":"ban","reason":"spam"}`)
	exec(`INSERT INTO room_ban_redactions (room_id, user_id, redacting_event_id) VALUES ($1, '@mallory:example.org', '$malban')`, r)

	current := map[[2]string]string{
		{"m.room.create", ""}: "$create", {"m.room.member", "@alice:example.org"}: "$alice",
		{"m.room.history_visibility", ""}: "$hv", {"m.room.member", "@bob:example.org"}: "$bobleave",
		{"m.room.topic", ""}: "$topic2", {"m.room.name", ""}: "$name",
		{"m.room.member", "@carol:example.org"}: "$carol", {"m.room.member", "@dave:example.org"}: "$daveleave",
		{"x.mallory", ""}: "$malstate", {"m.room.member", "@mallory:example.org"}: "$malban",
	}
	for k, id := range current {
		exec(`INSERT INTO current_state_events (event_id, room_id, type, state_key) VALUES ($1, $2, $3, $4)`,
			id, r, k[0], k[1])
	}
	exec(`UPDATE current_state_events c SET membership = m.m FROM (VALUES
		('$alice','join'),('$bobleave','leave'),('$carol','invite'),('$daveleave','leave')) m(e, m) WHERE c.event_id = m.e`)

	// State after bob left: group 1 in full; group 2, a delta on it.
	exec(`INSERT INTO state_groups (id, room_id, event_id) VALUES (1, $1, '$bobleave'), (2, $1, '$topic2')`, r)
	for _, row := range [][3]string{{"m.room.create", "", "$create"}, {"m.room.member", "@alice:example.org", "$alice"},
		{"m.room.history_visibility", "", "$hv"}, {"m.room.topic", "", "$topic1"}, {"m.room.member", "@bob:example.org", "$bobleave"}} {
		exec(`INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (1, $1, $2, $3, $4)`, r, row[0], row[1], row[2])
	}
	exec(`INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (2, $1, 'm.room.topic', '', '$topic2')`, r)
	exec(`INSERT INTO state_group_edges (state_group, prev_state_group) VALUES (2, 1)`)
	exec(`INSERT INTO event_to_state_groups (event_id, state_group) VALUES ('$bobleave', 1), ('$topic2', 2)`)

	for _, m := range [][4]any{{"$alice", "@alice:example.org", "join", 0}, {"$bobjoin", "@bob:example.org", "join", 0},
		{"$bobleave", "@bob:example.org", "leave", 0}, {"$carol", "@carol:example.org", "invite", 0},
		{"$davejoin", "@dave:example.org", "join", 1}, {"$daveleave", "@dave:example.org", "leave", 1}} {
		exec(`INSERT INTO room_memberships (event_id, user_id, sender, room_id, membership, forgotten) VALUES ($1, $2, $2, $3, $4, $5)`,
			m[0], m[1], r, m[2], m[3])
	}
	for _, m := range [][3]string{{"@alice:example.org", "$alice", "join"}, {"@bob:example.org", "$bobleave", "leave"},
		{"@carol:example.org", "$carol", "invite"}, {"@dave:example.org", "$daveleave", "leave"}} {
		exec(`INSERT INTO local_current_membership (room_id, user_id, event_id, membership) VALUES ($1, $2, $3, $4)`, r, m[0], m[1], m[2])
	}

	ev(w, "$wcreate", "m.room.create", "", "@alice:example.org", `{"room_version":"10"}`)
	ev(w, "$whv", "m.room.history_visibility", "", "@alice:example.org", `{"history_visibility":"world_readable"}`)
	exec(`INSERT INTO current_state_events (event_id, room_id, type, state_key) VALUES ('$wcreate', $1, 'm.room.create', ''), ('$whv', $1, 'm.room.history_visibility', '')`, w)

	ev(o, "$frankleave", "m.room.member", "@frank:example.org", "@frank:example.org", `{"membership":"leave"}`)
	exec(`UPDATE events SET outlier = true WHERE event_id = '$frankleave'`)
	exec(`INSERT INTO room_memberships (event_id, user_id, sender, room_id, membership) VALUES ('$frankleave', '@frank:example.org', '@frank:example.org', $1, 'leave')`, o)
	exec(`INSERT INTO local_current_membership (room_id, user_id, event_id, membership) VALUES ($1, '@frank:example.org', '$frankleave', 'leave')`, o)

	ev(p, "$pjoin", "m.room.member", "@alice:example.org", "@alice:example.org", `{"membership":"join"}`)
	exec(`INSERT INTO partial_state_rooms (room_id, join_event_id) VALUES ($1, '$pjoin')`, p)

	for i, u := range []string{"alice", "bob", "carol", "dave", "eve", "frank"} {
		exec(`INSERT INTO users (name, is_guest, admin) VALUES ($1, 0, $2)`, "@"+u+":example.org", map[bool]int{true: 1}[u == "alice"])
		exec(`INSERT INTO access_tokens (id, user_id, device_id, token, used) VALUES ($1, $2, 'DEV', $3, true)`,
			i+1, "@"+u+":example.org", u+"_tok")
	}

	st, err := store.Open(ctx, store.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	auth, err := clientauth.New(ctx, clientauth.Config{
		ServerName: "example.org",
		DSN:        dsn,
		Whoami:     clientauth.WhoamiConfig{URL: "http://synapse.invalid", Timeout: time.Second},
		Log:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auth.Close)
	f := &stateFixture{}
	f.h = RoomState(Deps{
		Store:      st,
		Auth:       auth,
		Log:        zerolog.Nop(),
		ServerName: "example.org",
		Fallback: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.handedIn = append(f.handedIn, r.URL.Path)
			w.WriteHeader(299)
		}),
	})
	return f
}

func (f *stateFixture) get(t *testing.T, user, path string) (int, []byte) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("Authorization", "Bearer "+user+"_tok")
	w := httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	return w.Code, w.Body.Bytes()
}

func ids(t *testing.T, body []byte) []string {
	t.Helper()
	var events []map[string]any
	if err := json.Unmarshal(body, &events); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e["event_id"].(string)
		if u, _ := e["unsigned"].(map[string]any); u != nil {
			if _, ok := u["membership"]; ok {
				t.Errorf("%s carries unsigned.membership: FilteredEvent.state has none", out[i])
			}
		}
		if e["user_id"] != e["sender"] {
			t.Errorf("%s is not in the v1 format (user_id %v)", out[i], e["user_id"])
		}
	}
	return out
}

func sorted(xs ...string) []string { sort.Strings(xs); return xs }

func TestRoomState(t *testing.T) {
	f := newStateFixture(t)
	const base = "/_matrix/client/v3/rooms/%21r%3Aexample.org/state"

	t.Run("joined: current state", func(t *testing.T) {
		code, body := f.get(t, "alice", base)
		got := ids(t, body)
		sort.Strings(got)
		want := sorted("$create", "$alice", "$hv", "$bobleave", "$topic2", "$name", "$carol", "$daveleave",
			"$malstate", "$malban")
		if code != 200 || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%d %v; want %v", code, got, want)
		}
	})
	t.Run("joined: one key, content and event", func(t *testing.T) {
		code, body := f.get(t, "alice", base+"/m.room.topic")
		if code != 200 || string(body) != `{"topic":"second"}` {
			t.Fatalf("%d %s", code, body)
		}
		code, body = f.get(t, "alice", base+"/m.room.topic/?format=event")
		var e map[string]any
		_ = json.Unmarshal(body, &e)
		if code != 200 || e["event_id"] != "$topic2" || e["user_id"] != nil {
			t.Fatalf("%d %s: want $topic2 in the v2 format", code, body)
		}
	})
	t.Run("an encoded state key", func(t *testing.T) {
		code, body := f.get(t, "alice", base+"/m.room.member/%40alice%3Aexample.org")
		if code != 200 || string(body) != `{"membership":"join"}` {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("redacted state is pruned", func(t *testing.T) {
		code, body := f.get(t, "alice", base+"/m.room.name")
		if code != 200 || string(body) != `{}` {
			t.Fatalf("%d %s", code, body)
		}
		_, body = f.get(t, "alice", base+"/m.room.name?format=event")
		var e struct {
			Unsigned struct {
				RedactedBy string `json:"redacted_by"`
			} `json:"unsigned"`
		}
		_ = json.Unmarshal(body, &e)
		if e.Unsigned.RedactedBy != "$redact2" {
			t.Fatalf("redacted by %q, want $redact2, the first row: %s", e.Unsigned.RedactedBy, body)
		}
	})
	t.Run("MSC4293: redacted by a ban", func(t *testing.T) {
		code, body := f.get(t, "alice", base+"/x.mallory")
		if code != 200 || string(body) != `{}` {
			t.Fatalf("%d %s", code, body)
		}
		_, body = f.get(t, "alice", base+"/x.mallory?format=event")
		if !strings.Contains(string(body), `"redacted_by":"$malban"`) {
			t.Fatalf("%s", body)
		}
	})
	t.Run("left: the state at the leave", func(t *testing.T) {
		code, body := f.get(t, "bob", base)
		got := ids(t, body)
		// (type, state_key) order, as the state-group query returns it.
		want := []string{"$create", "$hv", "$alice", "$bobleave", "$topic1"}
		if code != 200 || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%d %v; want %v", code, got, want)
		}
		if code, body := f.get(t, "bob", base+"/m.room.topic"); code != 200 || string(body) != `{"topic":"first"}` {
			t.Fatalf("%d %s", code, body)
		}
		if code, body := f.get(t, "bob", base+"/m.room.name"); code != 404 ||
			!strings.Contains(string(body), `"errcode":"M_NOT_FOUND","error":"Event not found."`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("invited, or forgotten: refused", func(t *testing.T) {
		for _, u := range []string{"carol", "dave", "eve"} {
			code, body := f.get(t, u, base)
			want := fmt.Sprintf(`{"errcode":"M_FORBIDDEN","error":"User @%s:example.org not in room !r:example.org, and room previews are disabled"}`, u)
			if code != 403 || string(body) != want {
				t.Errorf("%s: %d %s", u, code, body)
			}
		}
	})
	t.Run("world_readable: anyone", func(t *testing.T) {
		code, body := f.get(t, "eve", "/_matrix/client/r0/rooms/%21w%3Aexample.org/state")
		got := ids(t, body)
		sort.Strings(got)
		if code != 200 || fmt.Sprint(got) != fmt.Sprint(sorted("$wcreate", "$whv")) {
			t.Fatalf("%d %v", code, got)
		}
	})
	t.Run("a bad format", func(t *testing.T) {
		code, body := f.get(t, "alice", base+"/m.room.topic?format=raw")
		if code != 400 || string(body) != `{"errcode":"M_INVALID_PARAM","error":"Query parameter 'format' must be one of ['content', 'event']"}` {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("handed back", func(t *testing.T) {
		f.handedIn = nil
		for _, c := range []struct{ user, path string }{
			{"alice", "/_matrix/client/v3/rooms/%21p%3Aexample.org/state"}, // partial state
			{"frank", "/_matrix/client/v3/rooms/%21o%3Aexample.org/state"}, // outlier leave
			{"alice", base + "/a/b/c"},                                     // not Synapse's path
		} {
			if code, body := f.get(t, c.user, c.path); code != 299 {
				t.Errorf("%s: %d %s; want it handed back", c.path, code, body)
			}
		}
		if len(f.handedIn) != 3 {
			t.Fatalf("handed back %v", f.handedIn)
		}
	})
}
