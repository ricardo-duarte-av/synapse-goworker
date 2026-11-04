package slidingsync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

const profilesSchema = `
CREATE TABLE profiles (user_id text NOT NULL, displayname text, avatar_url text, full_user_id text, fields jsonb);
CREATE TABLE current_state_events (event_id text NOT NULL, room_id text NOT NULL, type text NOT NULL,
	state_key text NOT NULL, membership text, event_stream_ordering bigint);
CREATE TABLE local_current_membership (room_id text NOT NULL, user_id text NOT NULL, event_id text NOT NULL,
	membership text NOT NULL, event_stream_ordering bigint);
CREATE TABLE profile_updates (stream_id bigint PRIMARY KEY, instance_name text NOT NULL, user_id text NOT NULL,
	action text NOT NULL, affected_fields jsonb, inserted_ts bigint NOT NULL);
CREATE TABLE profile_updates_per_user (stream_id bigint NOT NULL, user_id text NOT NULL, inserted_ts bigint NOT NULL);

INSERT INTO profiles VALUES
	('alice', 'Alice', NULL, '@alice:x', NULL),
	('bob', 'Bob', 'mxc://b', '@bob:x', '{"m.status": {"text": "away"}}'),
	('carol', 'Carol', NULL, '@carol:x', NULL);
INSERT INTO current_state_events VALUES
	('$a', '!lazy', 'm.room.member', '@alice:x', 'join', 1), ('$a2', '!full', 'm.room.member', '@alice:x', 'join', 2);
INSERT INTO local_current_membership VALUES
	('!full', '@alice:x', '$a2', 'join', 2), ('!full', '@bob:x', '$b', 'join', 3);
INSERT INTO profile_updates VALUES
	(2, 'm', '@bob:x', 'update', '["avatar_url", "m.status"]', 0),
	(3, 'm', '@dave:x', 'joined_room', NULL, 0),
	(4, 'm', '@dave:x', 'left_room', NULL, 0),
	(5, 'm', '@erin:x', 'joined_room', NULL, 0);
INSERT INTO profile_updates_per_user VALUES (2, '@alice:x', 0), (3, '@alice:x', 0), (4, '@alice:x', 0), (5, '@alice:x', 0);
UPDATE profiles SET avatar_url = NULL WHERE user_id = 'bob';`

func profilesStore(t *testing.T) *store.Store {
	t.Helper()
	admin := os.Getenv("GOSYNC_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip("GOSYNC_TEST_ADMIN_DSN not set")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("GOSYNC_TEST_ADMIN_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	ac, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("profiles_test_%d", time.Now().UnixNano())
	if _, err := ac.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = ac.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		_ = ac.Close(context.Background())
	})
	dsn := admin + " dbname=" + name
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, profilesSchema); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, store.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

// !lazy lazy-loads members and sent a timeline from carol; !full does not,
// and bob is in it.
func profilesInputs(from *streamtoken.Token) extensionInputs {
	return extensionInputs{
		UserID: "@alice:x",
		Request: &Request{
			Lists: map[string]List{
				"l": {CommonRoomParameters: CommonRoomParameters{RequiredState: [][2]string{{"m.room.member", "$LAZY"}}}},
				"f": {CommonRoomParameters: CommonRoomParameters{RequiredState: [][2]string{{"m.room.name", ""}}}},
			},
		},
		From: from,
		Now:  streamtoken.Token{ProfileUpdates: 5},
		Lists: map[string]ListResult{
			"l": {Ops: []ListOp{{Op: "SYNC", RoomIDs: []string{"!lazy"}}}},
			"f": {Ops: []ListOp{{Op: "SYNC", RoomIDs: []string{"!full"}}}},
		},
		Rooms: map[string]*RoomResult{
			"!lazy": {Timeline: []json.RawMessage{json.RawMessage(`{"sender":"@carol:x"}`)}},
		},
	}
}

func TestProfilesExtension(t *testing.T) {
	d := Deps{Store: profilesStore(t), ProfileUpdatesInSync: true}
	ctx := context.Background()
	render := func(v *profilesJSON) gjson.Result {
		b, _ := json.Marshal(v)
		return gjson.ParseBytes(b)
	}

	t.Run("initial: lazy room senders, full room members, oneself", func(t *testing.T) {
		v, err := profilesExtension(ctx, d, profilesInputs(nil), &ProfilesExtension{Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		r := render(v)
		if r.Get(`users.@carol:x.updated.displayname`).Str != "Carol" ||
			r.Get(`users.@bob:x.updated.m\.status.text`).Str != "away" ||
			r.Get(`users.@alice:x.updated.displayname`).Str != "Alice" || len(r.Get("users").Map()) != 3 {
			t.Fatalf("%s", r.Raw)
		}
	})
	t.Run("incremental: removals, a leave overriding a join, a joiner with no profile", func(t *testing.T) {
		from := streamtoken.Token{ProfileUpdates: 1}
		v, err := profilesExtension(ctx, d, profilesInputs(&from), &ProfilesExtension{Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		r := render(v)
		bob := r.Get(`users.@bob:x`)
		if bob.Get("updated").Raw != `{"m.status":{"text":"away"}}` || bob.Get("removed").Raw != `["avatar_url"]` {
			t.Errorf("bob: %s", bob.Raw)
		}
		if r.Get(`users.@dave:x`).Raw != `null` || r.Get(`users.@erin:x`).Raw != `null` {
			t.Errorf("dave and erin: %s", r.Raw)
		}
		// carol is mentioned by a lazy room: every field.
		if r.Get(`users.@carol:x.updated.displayname`).Str != "Carol" {
			t.Errorf("carol: %s", r.Raw)
		}
		if r.Get(`users.@alice:x`).Exists() {
			t.Errorf("alice changed nothing: %s", r.Raw)
		}
	})
	t.Run("fields: only those; an empty list, none at first", func(t *testing.T) {
		v, _ := profilesExtension(ctx, d, profilesInputs(nil), &ProfilesExtension{Enabled: true, Fields: []string{"m.status"}})
		if r := render(v); len(r.Get("users").Map()) != 1 || !r.Get(`users.@bob:x.updated.m\.status`).Exists() {
			t.Errorf("%s", r.Raw)
		}
		if v, _ := profilesExtension(ctx, d, profilesInputs(nil), &ProfilesExtension{Enabled: true, Fields: []string{}}); v != nil {
			t.Errorf("empty fields: %+v", v)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		if v, _ := profilesExtension(ctx, d, profilesInputs(nil), &ProfilesExtension{}); v != nil {
			t.Errorf("%+v", v)
		}
	})
}

// An absent fields list means all fields; an empty one, none.
func TestProfilesFieldsAbsentOrEmpty(t *testing.T) {
	for body, wantNil := range map[string]bool{
		`{"extensions":{"org.matrix.msc4262.profiles":{"enabled":true}}}`:             true,
		`{"extensions":{"org.matrix.msc4262.profiles":{"enabled":true,"fields":[]}}}`: false,
	} {
		req, err := ParseRequest([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if got := req.Extensions.Profiles.Fields == nil; got != wantNil {
			t.Errorf("%s: fields nil %v", body, got)
		}
	}
}
