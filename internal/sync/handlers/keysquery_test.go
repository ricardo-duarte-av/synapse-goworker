package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// The C1 fixture, with keys:
//
//	alice  DEV (keys, "Phone", signed by her self-signing key), HID
//	       (hidden), NOKEY (no keys); all three cross-signing keys, and her
//	       signature on bob's master key
//	bob    no devices; master and self-signing keys
//	@r:remote.org      tracked: RD cached, and joined to a room
//	@half:remote.org   tracked, joined, but only a master key cached
//	@gone:down.org     tracked, but in no room; down.org is backing off
//	@un:far.org        not tracked
func newKeysFixture(t *testing.T) (*stateFixture, func(user, body string) (int, []byte)) {
	t.Helper()
	f := newStateFixture(t)
	x := f.exec
	x(`INSERT INTO devices (user_id, device_id, display_name, hidden) VALUES
		('@alice:example.org', 'DEV', 'Phone', false), ('@alice:example.org', 'HID', NULL, true),
		('@alice:example.org', 'NOKEY', NULL, false)`)
	x(`INSERT INTO e2e_device_keys_json (user_id, device_id, ts_added_ms, key_json) VALUES
		('@alice:example.org', 'DEV', 1, '{"user_id":"@alice:example.org","device_id":"DEV","algorithms":["m.olm.v1.curve25519-aes-sha2"],"keys":{"ed25519:DEV":"devpub"},"signatures":{"@alice:example.org":{"ed25519:DEV":"self"}},"unsigned":{"stale":true}}'),
		('@alice:example.org', 'HID', 1, '{"device_id":"HID"}')`)
	x(`INSERT INTO e2e_cross_signing_keys (user_id, keytype, keydata, stream_id) VALUES
		('@alice:example.org', 'master', '{"user_id":"@alice:example.org","usage":["master"],"keys":{"ed25519:amk":"amk"}}', 1),
		('@alice:example.org', 'self_signing', '{"user_id":"@alice:example.org","usage":["self_signing"],"keys":{"ed25519:assk":"assk"}}', 2),
		('@alice:example.org', 'user_signing', '{"user_id":"@alice:example.org","usage":["user_signing"],"keys":{"ed25519:ausk":"ausk"}}', 3),
		('@bob:example.org', 'master', '{"user_id":"@bob:example.org","usage":["master"],"keys":{"ed25519:old":"old"}}', 4),
		('@bob:example.org', 'master', '{"user_id":"@bob:example.org","usage":["master"],"keys":{"ed25519:bmk":"bmk"}}', 5),
		('@bob:example.org', 'self_signing', '{"user_id":"@bob:example.org","usage":["self_signing"],"keys":{"ed25519:bssk":"bssk"}}', 6),
		('@r:remote.org', 'master', '{"user_id":"@r:remote.org","keys":{"ed25519:rmk":"rmk"}}', 7),
		('@r:remote.org', 'self_signing', '{"user_id":"@r:remote.org","keys":{"ed25519:rssk":"rssk"}}', 8),
		('@half:remote.org', 'master', '{"user_id":"@half:remote.org","keys":{"ed25519:hmk":"hmk"}}', 9)`)
	x(`INSERT INTO e2e_cross_signing_signatures (user_id, key_id, target_user_id, target_device_id, signature) VALUES
		('@alice:example.org', 'ed25519:assk', '@alice:example.org', 'DEV', 'cross'),
		('@alice:example.org', 'ed25519:ausk', '@bob:example.org', 'bmk', 'alice-on-bob')`)
	x(`INSERT INTO device_lists_remote_extremeties (user_id, stream_id) VALUES
		('@r:remote.org', '5'), ('@half:remote.org', '6'), ('@gone:down.org', '7')`)
	x(`INSERT INTO device_lists_remote_cache (user_id, device_id, content) VALUES
		('@r:remote.org', 'RD', '{"device_id":"RD","keys":{"device_id":"RD","keys":{"ed25519:RD":"rd"}},"device_display_name":"R phone"}'),
		('@r:remote.org', 'EMPTY', '{"device_id":"EMPTY","keys":{}}'),
		('@gone:down.org', 'G', '{"device_id":"G","keys":{"device_id":"G"}}')`)
	x(`INSERT INTO current_state_events (event_id, room_id, type, state_key, membership) VALUES
		('$rj', '!w:example.org', 'm.room.member', '@r:remote.org', 'join'),
		('$hj', '!w:example.org', 'm.room.member', '@half:remote.org', 'join')`)
	now := time.Now().UnixMilli()
	x(`INSERT INTO destinations (destination, retry_last_ts, retry_interval, failure_ts) VALUES
		('down.org', $1, 3600000, $1), ('far.org', $1 - 7200000, 60000, $1)`, now)

	h := KeysQuery(f.deps, KeysDeps{})
	return f, func(user, body string) (int, []byte) {
		r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/keys/query", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+user+"_tok")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code, w.Body.Bytes()
	}
}

func TestKeysQuery(t *testing.T) {
	f, post := newKeysFixture(t)

	t.Run("local, tracked remote, a remote in backoff", func(t *testing.T) {
		code, body := post("alice", `{"device_keys":{"@alice:example.org":[],"@bob:example.org":[],"@r:remote.org":[],"nope":[],"@gone:down.org":[]}}`)
		b := gjson.ParseBytes(body)
		if code != 200 {
			t.Fatalf("%d %s", code, body)
		}
		dev := b.Get(`device_keys.@alice:example\.org.DEV`)
		if dev.Get(`signatures.@alice:example\.org.ed25519:DEV`).Str != "self" ||
			dev.Get(`signatures.@alice:example\.org.ed25519:assk`).Str != "cross" ||
			dev.Get("unsigned").Raw != `{"device_display_name":"Phone"}` {
			t.Errorf("DEV: %s", dev.Raw)
		}
		if n := len(b.Get(`device_keys.@alice:example\.org`).Map()); n != 1 {
			t.Errorf("alice's devices: %s (HID is hidden, NOKEY has no keys)", b.Get(`device_keys.@alice:example\.org`).Raw)
		}
		if b.Get(`device_keys.@bob:example\.org`).Raw != `{}` {
			t.Errorf("bob: %s", b.Get(`device_keys.@bob:example\.org`).Raw)
		}
		rd := b.Get(`device_keys.@r:remote\.org`)
		if rd.Get("RD.unsigned").Raw != `{"device_display_name":"R phone"}` || rd.Get("EMPTY").Exists() {
			t.Errorf("remote: %s", rd.Raw)
		}
		if b.Get(`device_keys.@gone:down\.org`).Exists() || b.Get(`device_keys.nope`).Exists() {
			t.Errorf("gone or nope: %s", b.Get("device_keys").Raw)
		}
		if b.Get("failures").Raw != `{"down.org":{"status":503,"message":"Not ready for retry"}}` {
			t.Errorf("failures: %s", b.Get("failures").Raw)
		}
		if b.Get(`master_keys.@bob:example\.org.keys`).Raw != `{"ed25519:bmk":"bmk"}` ||
			b.Get(`master_keys.@bob:example\.org.signatures.@alice:example\.org.ed25519:ausk`).Str != "alice-on-bob" {
			t.Errorf("bob's master: %s", b.Get("master_keys").Raw)
		}
		if !b.Get(`self_signing_keys.@r:remote\.org`).Exists() || !b.Get(`self_signing_keys.@alice:example\.org`).Exists() {
			t.Errorf("self-signing: %s", b.Get("self_signing_keys").Raw)
		}
		if us := b.Get("user_signing_keys").Map(); len(us) != 1 || !us["@alice:example.org"].Exists() {
			t.Errorf("user-signing: %s", b.Get("user_signing_keys").Raw)
		}
	})
	t.Run("one device", func(t *testing.T) {
		code, body := post("alice", `{"device_keys":{"@alice:example.org":["DEV","NOKEY","nope"],"@r:remote.org":["RD"]}}`)
		b := gjson.ParseBytes(body)
		if code != 200 || len(b.Get(`device_keys.@alice:example\.org`).Map()) != 1 ||
			!b.Get(`device_keys.@r:remote\.org.RD`).Exists() {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("someone else: no signatures of alice's, no user-signing key", func(t *testing.T) {
		code, body := post("eve", `{"device_keys":{"@bob:example.org":[]}}`)
		b := gjson.ParseBytes(body)
		if code != 200 || b.Get(`master_keys.@bob:example\.org.signatures`).Exists() ||
			b.Get("user_signing_keys").Raw != `{}` || b.Get("failures").Raw != `{}` {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("handed back", func(t *testing.T) {
		f.handedIn = nil
		for _, body := range []string{
			`{"device_keys":{"@un:far.org":[]}}`,         // a federation query
			`{"device_keys":{"@half:remote.org":[]}}`,    // half its cross-signing keys
			`{"device_keys":{"@r:remote.org":["NOPE"]}}`, // a device not cached
			`{"device_keys":{"@alice:example.org":"DEV"}}`,
			`{"device_keys":[]}`,
			`{}`,
			`not json`,
		} {
			if code, resp := post("alice", body); code != 299 {
				t.Errorf("%s: %d %s", body, code, resp)
			}
		}
		if len(f.handedIn) != 7 {
			t.Fatalf("handed back %v", f.handedIn)
		}
	})
	t.Run("main is handed the whole body", func(t *testing.T) {
		var got string
		d := f.deps
		d.Fallback = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			got = string(b)
		})
		const body = `{"device_keys":{"@un:far.org":[]}}`
		r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/keys/query", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer alice_tok")
		KeysQuery(d, KeysDeps{}).ServeHTTP(httptest.NewRecorder(), r)
		if got != body {
			t.Fatalf("main got %q", got)
		}
	})
	t.Run("MSC4263: only users sharing a room, joined or invited", func(t *testing.T) {
		// alice is joined to !r; carol is invited there; bob left it and
		// shares nothing with alice in current state.
		h := KeysQuery(f.deps, KeysDeps{MSC4263: true})
		r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/keys/query",
			strings.NewReader(`{"device_keys":{"@alice:example.org":[],"@carol:example.org":[],"@bob:example.org":[]}}`))
		r.Header.Set("Authorization", "Bearer alice_tok")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		dk := gjson.GetBytes(w.Body.Bytes(), "device_keys")
		if w.Code != 200 || !dk.Get(`@alice:example\.org`).Exists() || !dk.Get(`@carol:example\.org`).Exists() ||
			dk.Get(`@bob:example\.org`).Exists() || gjson.GetBytes(w.Body.Bytes(), `master_keys.@bob:example\.org`).Exists() {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
	})
	t.Run("MSC3984: a user an appservice owns is Synapse's", func(t *testing.T) {
		f.handedIn = nil
		h := KeysQuery(f.deps, KeysDeps{MSC3984: true, ExclusiveUsers: regexp.MustCompile(`^(?:(@bob:.*))`)})
		for body, back := range map[string]bool{
			`{"device_keys":{"@bob:example.org":[]}}`:   true,
			`{"device_keys":{"@alice:example.org":[]}}`: false,
		} {
			r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/keys/query", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer alice_tok")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if (w.Code == 299) != back {
				t.Errorf("%s: %d %s", body, w.Code, w.Body)
			}
		}
	})
}
