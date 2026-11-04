package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lazyload"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// The C1 fixture, with profiles and profile_updates for alice to receive:
//
//	alice  joined !r and !w; "Alice", m.status
//	bob    joined !w; changes his display name (2)
//	carol  joins a room alice is in (3); has a display name
//	dave   leaves the last room alice shares with him (4)
//	frank  changes a field alice does not ask for (5)
func newProfileUpdatesFixture(t *testing.T) Deps {
	t.Helper()
	f := newStateFixture(t)
	x := f.exec
	x(`INSERT INTO profiles (user_id, full_user_id, displayname, avatar_url, fields) VALUES
		('alice', '@alice:example.org', 'Alice', NULL, '{"m.status": {"text": "here"}}'),
		('bob', '@bob:example.org', 'Bobby', 'mxc://b', NULL),
		('carol', '@carol:example.org', 'Carol', NULL, '{"x.other": 1}'),
		('frank', '@frank:example.org', 'Frank', NULL, '{"x.other": 2}')`)
	x(`INSERT INTO current_state_events (event_id, room_id, type, state_key, membership) VALUES
		('$wa', '!w:example.org', 'm.room.member', '@alice:example.org', 'join'),
		('$wb', '!w:example.org', 'm.room.member', '@bob:example.org', 'join')`)
	x(`INSERT INTO local_current_membership (room_id, user_id, event_id, membership) VALUES
		('!w:example.org', '@alice:example.org', '$wa', 'join'), ('!w:example.org', '@bob:example.org', '$wb', 'join')`)
	x(`INSERT INTO profile_updates (stream_id, instance_name, user_id, action, affected_fields, inserted_ts) VALUES
		(2, 'm', '@bob:example.org', 'update', '["displayname"]', 0),
		(3, 'm', '@carol:example.org', 'joined_room', NULL, 0),
		(4, 'm', '@dave:example.org', 'left_room', NULL, 0),
		(5, 'm', '@frank:example.org', 'update', '["x.other"]', 0)`)
	x(`INSERT INTO profile_updates_per_user (stream_id, user_id, inserted_ts) VALUES
		(2, '@alice:example.org', 0), (3, '@alice:example.org', 0), (4, '@alice:example.org', 0), (5, '@alice:example.org', 0)`)
	d := f.deps
	d.ProfileUpdatesInSync = true
	d.ProfileFieldsSent = lazyload.New(0, 0)
	return d
}

func profileFilter(t *testing.T, doc string) *filter.Collection {
	t.Helper()
	c, err := filter.NewInline(json.RawMessage(doc), filter.Features{MSC4429: true})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSyncProfileUpdates(t *testing.T) {
	d := newProfileUpdatesFixture(t)
	ctx := context.Background()
	f := profileFilter(t, `{"org.matrix.msc4429.profile_fields":{"ids":["displayname","m.status"]}}`)
	tok := func(n int64) streamtoken.Token { return streamtoken.Token{ProfileUpdates: n} }
	render := func(m map[string]any) gjson.Result {
		b, _ := json.Marshal(m)
		return gjson.ParseBytes(b)
	}

	t.Run("initial: who shares a room, and oneself", func(t *testing.T) {
		got, err := syncProfileUpdates(ctx, d, f, "@alice:example.org", "DEV", nil, tok(5), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := render(got)
		if r.Get(`@alice:example\.org.profile_updates.m\.status.text`).Str != "here" ||
			r.Get(`@bob:example\.org.profile_updates.displayname`).Str != "Bobby" ||
			r.Get(`@bob:example\.org.profile_updates.avatar_url`).Exists() || len(r.Map()) != 2 {
			t.Fatalf("%s", r.Raw)
		}
	})
	t.Run("incremental: changes, a join, a leave", func(t *testing.T) {
		since := tok(1)
		got, err := syncProfileUpdates(ctx, d, f, "@alice:example.org", "DEV", &since, tok(5), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := render(got)
		if r.Get(`@bob:example\.org.profile_updates`).Raw != `{"displayname":"Bobby"}` ||
			r.Get(`@carol:example\.org.profile_updates`).Raw != `{"displayname":"Carol"}` ||
			r.Get(`@dave:example\.org.profile_updates`).Raw != `null` ||
			r.Get(`@frank:example\.org`).Exists() {
			t.Fatalf("%s", r.Raw)
		}
	})
	t.Run("incremental, lazy: a mentioned user in full, once per device", func(t *testing.T) {
		lazy := profileFilter(t, `{"room":{"state":{"lazy_load_members":true}},"org.matrix.msc4429.profile_fields":{"ids":["displayname","m.status"]}}`)
		joined := map[string]any{"!w:example.org": map[string]any{
			"timeline": map[string]any{"events": []json.RawMessage{json.RawMessage(`{"sender":"@alice:example.org"}`)}},
		}}
		since := tok(5)
		got, err := syncProfileUpdates(ctx, d, lazy, "@alice:example.org", "DEV", &since, tok(5), joined, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if r := render(got); r.Get(`@alice:example\.org.profile_updates.displayname`).Str != "Alice" {
			t.Fatalf("%s", r.Raw)
		}
		again, _ := syncProfileUpdates(ctx, d, lazy, "@alice:example.org", "DEV", &since, tok(5), joined, nil, nil)
		if again != nil {
			t.Fatalf("sent twice: %v", again)
		}
		other, _ := syncProfileUpdates(ctx, d, lazy, "@alice:example.org", "OTHER", &since, tok(5), joined, nil, nil)
		if other == nil {
			t.Fatal("another device was not sent it")
		}
	})
	t.Run("not asked for, or off", func(t *testing.T) {
		if got, _ := syncProfileUpdates(ctx, d, profileFilter(t, `{}`), "@alice:example.org", "DEV", nil, tok(5), nil, nil, nil); got != nil {
			t.Errorf("no profile_fields: %v", got)
		}
		off := d
		off.ProfileUpdatesInSync = false
		if got, _ := syncProfileUpdates(ctx, off, f, "@alice:example.org", "DEV", nil, tok(5), nil, nil, nil); got != nil {
			t.Errorf("off: %v", got)
		}
	})
}
