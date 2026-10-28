package clientauth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

func request(target string, header map[string][]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	for k, v := range header {
		r.Header[k] = v
	}
	return r
}

func TestTokenFromRequest(t *testing.T) {
	for _, c := range []struct {
		name   string
		target string
		auth   []string
		token  string
		reject string
	}{
		{"header", "/sync", []string{"Bearer abc"}, "abc", ""},
		{"query", "/sync?access_token=abc", nil, "abc", ""},
		{"first query value", "/sync?access_token=a&access_token=b", nil, "a", ""},
		{"empty query value is a token", "/sync?access_token=", nil, "", ""},
		{"neither", "/sync", nil, "", "Missing access token"},
		{"both", "/sync?access_token=abc", []string{"Bearer abc"}, "", "Mixing Authorization headers and access_token query parameters."},
		{"both, query empty", "/sync?access_token=", []string{"Bearer abc"}, "", "Mixing Authorization headers and access_token query parameters."},
		{"two headers", "/sync", []string{"Bearer a", "Bearer b"}, "", "Too many Authorization headers."},
		{"two spaces", "/sync", []string{"Bearer  abc"}, "", "Invalid Authorization header."},
		{"lower case scheme", "/sync", []string{"bearer abc"}, "", "Invalid Authorization header."},
		{"not bearer", "/sync", []string{"Basic abc"}, "", "Invalid Authorization header."},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := map[string][]string{}
			if c.auth != nil {
				h["Authorization"] = c.auth
			}
			token, rej := TokenFromRequest(request(c.target, h))
			if c.reject == "" {
				if rej != nil || token != c.token {
					t.Fatalf("got %q, %v; want %q", token, rej, c.token)
				}
				return
			}
			if rej == nil {
				t.Fatalf("accepted %q; want %q", token, c.reject)
			}
			var body map[string]any
			_ = json.Unmarshal(rej.Body, &body)
			if rej.Status != 401 || body["errcode"] != "M_MISSING_TOKEN" || body["error"] != c.reject {
				t.Fatalf("got %d %s", rej.Status, rej.Body)
			}
		})
	}
}

func TestClientIP(t *testing.T) {
	r := request("/sync", map[string][]string{"X-Forwarded-For": {" 203.0.113.9 , 10.0.0.1", "198.51.100.1"}})
	if got := ClientIP(r); got != "203.0.113.9" {
		t.Fatalf("got %q", got)
	}
	r = request("/sync", nil)
	r.RemoteAddr = "192.0.2.4:5555"
	if got := ClientIP(r); got != "192.0.2.4" {
		t.Fatalf("got %q", got)
	}
}

func TestUserAgentIsLastHeaderAsASCII(t *testing.T) {
	r := request("/sync", map[string][]string{"User-Agent": {"first", "Élément/1"}})
	if got := userAgent(r); got != "��lément/1"[0:0]+"��l��ment/1" {
		t.Fatalf("got %q", got)
	}
}

func registration(extra string) []byte {
	return []byte(`
id: bridge
url: http://bridge
as_token: AS_TOKEN
hs_token: HS_TOKEN
sender_localpart: bridgebot
namespaces:
  users:
    - exclusive: true
      regex: '@bridge_.*:example\.org'
    - exclusive: false
      regex: '@shared'
` + extra)
}

func TestParseAppService(t *testing.T) {
	as, why, err := parseAppService("example.org", registration(""))
	if err != nil || why != "" {
		t.Fatal(err, why)
	}
	for user, want := range map[string]bool{
		"@bridgebot:example.org":  true, // the sender
		"@bridge_1:example.org":   true,
		"@bridge_1:example.orgX":  true, // re.match: a prefix suffices
		"@sharedwhatever:else":    true, // a non-exclusive namespace counts
		"@alice:example.org":      false,
		"x@bridge_1:example.org":  false, // anchored at the start
		"@bridgebot:example.org2": false,
	} {
		if got := as.interestedInUser(user); got != want {
			t.Errorf("%s: got %v, want %v", user, got, want)
		}
	}

	_, why, err = parseAppService("example.org", []byte(strings.Replace(string(registration("")),
		`'@shared'`, `'@(?!admin)'`, 1)))
	if err != nil || why == "" {
		t.Fatalf("a lookahead should be valid but not native: %v %q", err, why)
	}

	for _, bad := range []string{
		strings.Replace(string(registration("")), "as_token: AS_TOKEN\n", "", 1),
		strings.Replace(string(registration("")), "sender_localpart: bridgebot", "sender_localpart: 'bridge bot'", 1),
		strings.Replace(string(registration("")), "exclusive: false\n", "", 1),
	} {
		if _, _, err := parseAppService("example.org", []byte(bad)); err == nil {
			t.Errorf("accepted:\n%s", bad)
		}
	}

	as, _, err = parseAppService("example.org", registration("ip_range_whitelist: ['10.0.0.0/8', '192.0.2.7']\n"))
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]bool{"10.1.2.3": true, "192.0.2.7": true, "192.0.2.8": false, "::ffff:10.0.0.1": true, "nonsense": false} {
		if got := as.allowsIP(ip); got != want {
			t.Errorf("%s: got %v, want %v", ip, got, want)
		}
	}
}

type fakePublisher struct {
	mu    sync.Mutex
	lines []string
	chans []string
}

func (f *fakePublisher) Publish(_ context.Context, channel, line string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, line)
	f.chans = append(f.chans, channel)
	return nil
}

func (f *fakePublisher) wait(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		got := append([]string(nil), f.lines...)
		f.mu.Unlock()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUserIPRateLimitAndFormat(t *testing.T) {
	pub := &fakePublisher{}
	u := newUserIPs(pub, "example.org", zerolog.Nop())
	defer u.close()
	u.insert("@a:example.org", "tok", "203.0.113.1", "Element/1", "DEV")
	u.insert("@a:example.org", "tok", "203.0.113.1", "Element/1", "DEV") // within two minutes
	u.insert("@a:example.org", "tok", "203.0.113.2", "Element/1", "")    // another address
	u.insert("@a:example.org", "tok", "203.0.113.3", "sync-v3-proxy-", "DEV")
	got := pub.wait(t, 2)
	time.Sleep(20 * time.Millisecond)
	if len(pub.lines) != 2 {
		t.Fatalf("got %d lines: %q", len(pub.lines), pub.lines)
	}
	if pub.chans[0] != "example.org/USER_IP" {
		t.Fatalf("channel %q", pub.chans[0])
	}
	name, rest, _ := strings.Cut(got[0], " ")
	user, args, _ := strings.Cut(rest, " ")
	var a []any
	if name != "USER_IP" || user != "@a:example.org" || json.Unmarshal([]byte(args), &a) != nil || len(a) != 5 {
		t.Fatalf("line %q", got[0])
	}
	if a[0] != "tok" || a[1] != "203.0.113.1" || a[2] != "Element/1" || a[3] != "DEV" {
		t.Fatalf("args %v", a)
	}
	if !strings.Contains(got[1], `,null,`) {
		t.Fatalf("no device should be null: %q", got[1])
	}
}

// --- against a throwaway PostgreSQL ---

// throwaway makes a database with the columns Synapse's schema gives the
// tables read here, and drops it afterwards.
//
//	CLIENTAUTH_TEST_ADMIN_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func throwaway(t *testing.T) (dsn string, conn *pgx.Conn) {
	t.Helper()
	admin := os.Getenv("CLIENTAUTH_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip("CLIENTAUTH_TEST_ADMIN_DSN not set")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("CLIENTAUTH_TEST_ADMIN_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	ac, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("clientauth_test_%d", time.Now().UnixNano())
	if _, err := ac.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = ac.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		_ = ac.Close(context.Background())
	})
	// A keyword/value DSN: a repeated keyword's last value wins.
	dsn = admin + " dbname=" + name
	conn, err = pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	// synapse/storage/schema/main/full_schemas/72 and the deltas since.
	for _, q := range []string{
		`CREATE TABLE users (name text, password_hash text, creation_ts bigint,
			admin smallint DEFAULT 0 NOT NULL, upgrade_ts bigint, is_guest smallint DEFAULT 0 NOT NULL,
			appservice_id text, consent_version text, consent_server_notice_sent text, user_type text,
			deactivated smallint DEFAULT 0 NOT NULL, shadow_banned boolean, consent_ts bigint,
			approved boolean, locked boolean DEFAULT false NOT NULL, suspended boolean DEFAULT false NOT NULL,
			UNIQUE (name))`,
		`CREATE TABLE access_tokens (id bigint NOT NULL PRIMARY KEY, user_id text NOT NULL,
			device_id text, token text NOT NULL UNIQUE, valid_until_ms bigint, puppets_user_id text,
			last_validated bigint, refresh_token_id bigint, used boolean)`,
		`CREATE TABLE devices (user_id text NOT NULL, device_id text NOT NULL, display_name text,
			last_seen bigint, ip text, user_agent text, hidden boolean DEFAULT false,
			UNIQUE (user_id, device_id))`,
		`INSERT INTO users (name, is_guest) VALUES ('@alice:example.org', 0), ('@admin:example.org', 0),
			('@locked:example.org', 0), ('@bridge_1:example.org', 0), ('@guest:example.org', 1)`,
		`UPDATE users SET locked = true WHERE name = '@locked:example.org'`,
		`INSERT INTO access_tokens (id, user_id, device_id, token, valid_until_ms, puppets_user_id, used) VALUES
			(1, '@alice:example.org', 'ALICE', 'alice_tok', NULL, NULL, NULL),
			(2, '@alice:example.org', 'OLD', 'expired_tok', 1000, NULL, true),
			(3, '@locked:example.org', 'L', 'locked_tok', NULL, NULL, true),
			(4, '@admin:example.org', NULL, 'puppet_tok', NULL, '@alice:example.org', true),
			(5, '@alice:example.org', 'LATER', 'future_tok', 99999999999999, NULL, false),
			(6, '@alice:example.org', 'W', 'whoami_tok', NULL, NULL, true)`,
		`INSERT INTO devices (user_id, device_id, hidden) VALUES
			('@bridge_1:example.org', 'BRIDGEDEV', false), ('@bridge_1:example.org', 'HIDDEN', true)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	return dsn, conn
}

type fixture struct {
	a      *Authenticator
	conn   *pgx.Conn
	pub    *fakePublisher
	whoami *int
}

func newFixture(t *testing.T, ca synapsecfg.ClientAuth) fixture {
	t.Helper()
	dsn, conn := throwaway(t)
	dir := t.TempDir()
	reg := filepath.Join(dir, "bridge.yaml")
	if err := os.WriteFile(reg, registration(""), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	synapse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.Header.Get("Authorization") {
		case "Bearer guest_macaroon":
			_, _ = w.Write([]byte(`{"user_id":"@guest:example.org","device_id":"guest_device","is_guest":true}`))
			return
		case "Bearer whoami_tok":
			_, _ = w.Write([]byte(`{"user_id":"@alice:example.org","device_id":"W"}`))
			return
		}
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed.","soft_logout":false}`))
	}))
	t.Cleanup(synapse.Close)
	pub := &fakePublisher{}
	a, err := New(context.Background(), Config{
		ServerName:      "example.org",
		Synapse:         ca,
		AppServiceFiles: []string{reg},
		DSN:             dsn,
		MaxConns:        2,
		Whoami:          WhoamiConfig{URL: synapse.URL, PositiveTTL: time.Minute, NegativeTTL: time.Minute, Timeout: 5 * time.Second},
		Publisher:       pub,
		Log:             zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	if ok, why := a.Native(); ok == (ca != synapsecfg.ClientAuth{}) {
		t.Fatalf("native %v (%s) for %+v", ok, why, ca)
	}
	return fixture{a: a, conn: conn, pub: pub, whoami: &calls}
}

func (f fixture) auth(t *testing.T, target, token string) Verdict {
	t.Helper()
	r := request(target, map[string][]string{
		"Authorization":   {"Bearer " + token},
		"X-Forwarded-For": {"203.0.113.5"},
		"User-Agent":      {"Element/1.0"},
	})
	v, err := f.a.Authenticate(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func wantRejected(t *testing.T, v Verdict, status int, body string) {
	t.Helper()
	if v.Valid || v.Rejection == nil {
		t.Fatalf("accepted: %+v", v)
	}
	var got, want map[string]any
	_ = json.Unmarshal(v.Rejection.Body, &got)
	_ = json.Unmarshal([]byte(body), &want)
	if v.Rejection.Status != status || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %d %s; want %d %s", v.Rejection.Status, v.Rejection.Body, status, body)
	}
}

func TestUserTokens(t *testing.T) {
	f := newFixture(t, synapsecfg.ClientAuth{})
	ctx := context.Background()

	v := f.auth(t, "/sync", "alice_tok")
	if !v.Valid || v.UserID != "@alice:example.org" || v.DeviceID != "ALICE" || v.TokenID != 1 || v.Source != SourceNative {
		t.Fatalf("%+v", v)
	}
	var used *bool
	if err := f.conn.QueryRow(ctx, `SELECT used FROM access_tokens WHERE id = 1`).Scan(&used); err != nil || used == nil || !*used {
		t.Fatalf("token not marked used: %v %v", used, err)
	}
	lines := f.pub.wait(t, 1)
	if len(lines) != 1 || !strings.HasPrefix(lines[0], `USER_IP @alice:example.org ["alice_tok","203.0.113.5","Element/1.0","ALICE",`) {
		t.Fatalf("USER_IP %q", lines)
	}

	wantRejected(t, f.auth(t, "/sync", "expired_tok"), 401,
		`{"errcode":"M_UNKNOWN_TOKEN","error":"Access token has expired","soft_logout":true}`)
	wantRejected(t, f.auth(t, "/sync", "locked_tok"), 401,
		`{"errcode":"M_USER_LOCKED","error":"User account has been locked","soft_logout":true}`)
	if v := f.auth(t, "/sync", "future_tok"); !v.Valid {
		t.Fatalf("a token valid until the future: %+v", v)
	}

	// An admin puppeting alice: alice is the user, the admin the entity
	// recorded (track_puppeted_user_ips is off).
	v = f.auth(t, "/sync", "puppet_tok")
	if !v.Valid || v.UserID != "@alice:example.org" || v.TokenID != 4 || v.Entity != "@admin:example.org" {
		t.Fatalf("%+v", v)
	}
	lines = f.pub.wait(t, 3)
	if !strings.HasPrefix(lines[len(lines)-1], "USER_IP @admin:example.org ") {
		t.Fatalf("USER_IP %q", lines)
	}

	if *f.whoami != 0 {
		t.Fatalf("/whoami asked %d times about native tokens", *f.whoami)
	}

	// Not in access_tokens: Synapse decides, and its answer is passed
	// through as sent.
	v = f.auth(t, "/sync", "guest_macaroon")
	if !v.Valid || !v.IsGuest || v.UserID != "@guest:example.org" || v.Source != SourceWhoami {
		t.Fatalf("%+v", v)
	}
	v = f.auth(t, "/sync", "nonsense")
	if v.Rejection == nil || string(v.Rejection.Body) != `{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed.","soft_logout":false}` {
		t.Fatalf("%+v", v)
	}
}

func TestInvalidations(t *testing.T) {
	f := newFixture(t, synapsecfg.ClientAuth{})
	ctx := context.Background()
	if v := f.auth(t, "/sync", "alice_tok"); !v.Valid {
		t.Fatal(v)
	}

	// Logout: the row goes, then the caches stream says so.
	if _, err := f.conn.Exec(ctx, `DELETE FROM access_tokens WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if v := f.auth(t, "/sync", "alice_tok"); !v.Valid {
		t.Fatal("cached until invalidated, as Synapse's cache is")
	}
	f.a.handle(`RDATA caches master 4711 ["get_user_by_access_token",["alice_tok"],1761600000000]`)
	if v := f.auth(t, "/sync", "alice_tok"); v.Valid {
		t.Fatal("a deleted token still accepted after its invalidation")
	}

	// Unlock.
	if _, err := f.conn.Exec(ctx, `UPDATE users SET locked = false WHERE name = '@locked:example.org'`); err != nil {
		t.Fatal(err)
	}
	f.a.handle(`RDATA caches master batch ["get_user_locked_status",["@locked:example.org"],1761600000000]`)
	if v := f.auth(t, "/sync", "locked_tok"); !v.Valid {
		t.Fatalf("unlocked user refused: %+v", v)
	}

	// Keys null: the whole cache.
	if _, err := f.conn.Exec(ctx, `UPDATE users SET locked = true WHERE name = '@locked:example.org'`); err != nil {
		t.Fatal(err)
	}
	f.a.handle(`RDATA caches master 4712 ["get_user_locked_status",null,1761600000000]`)
	if v := f.auth(t, "/sync", "locked_tok"); v.Valid {
		t.Fatal("locked user accepted after a whole-cache invalidation")
	}
}

func TestAppServices(t *testing.T) {
	f := newFixture(t, synapsecfg.ClientAuth{})

	v := f.auth(t, "/sync", "AS_TOKEN")
	if !v.Valid || v.UserID != "@bridgebot:example.org" || v.AppServiceID != "bridge" || v.DeviceID != "" {
		t.Fatalf("%+v", v)
	}
	// The sender needs no users row.
	if v := f.auth(t, "/sync?user_id=@bridgebot:example.org", "AS_TOKEN"); !v.Valid {
		t.Fatalf("%+v", v)
	}
	v = f.auth(t, "/sync?user_id=@bridge_1:example.org&device_id=BRIDGEDEV", "AS_TOKEN")
	if !v.Valid || v.UserID != "@bridge_1:example.org" || v.DeviceID != "BRIDGEDEV" {
		t.Fatalf("%+v", v)
	}
	wantRejected(t, f.auth(t, "/sync?user_id=@alice:example.org", "AS_TOKEN"), 403,
		`{"errcode":"M_FORBIDDEN","error":"Application service cannot masquerade as this user (@alice:example.org)."}`)
	wantRejected(t, f.auth(t, "/sync?user_id=@bridge_2:example.org", "AS_TOKEN"), 403,
		`{"errcode":"M_FORBIDDEN","error":"Application service has not registered this user (@bridge_2:example.org)"}`)
	wantRejected(t, f.auth(t, "/sync?user_id=@bridge_1:example.org&device_id=HIDDEN", "AS_TOKEN"), 400,
		`{"errcode":"M_UNKNOWN_DEVICE","error":"Application service trying to use a device that doesn't exist ('HIDDEN' for @bridge_1:example.org)"}`)

	// track_appservice_user_ips is off: nothing recorded.
	time.Sleep(20 * time.Millisecond)
	if n := len(f.pub.wait(t, 0)); n != 0 {
		t.Fatalf("%d USER_IP for an appservice", n)
	}
	if *f.whoami != 0 {
		t.Fatalf("/whoami asked %d times", *f.whoami)
	}
}

func TestNotNativeWhenSynapseDefersElsewhere(t *testing.T) {
	for _, ca := range []synapsecfg.ClientAuth{{Delegated: true}, {AccountValidity: true}, {Modules: 1}} {
		a, err := New(context.Background(), Config{
			Synapse: ca,
			Whoami:  WhoamiConfig{URL: "http://synapse.invalid"},
			Log:     zerolog.Nop(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if ok, why := a.Native(); ok || why == "" {
			t.Errorf("%+v: native", ca)
		}
		a.Close()
	}
}

// Not native, /whoami decides; the token's ID still comes from
// access_tokens, as Synapse's requester has it.
func TestWhoamiVerdictsCarryTheTokenID(t *testing.T) {
	f := newFixture(t, synapsecfg.ClientAuth{AccountValidity: true})
	v := f.auth(t, "/sync", "whoami_tok")
	if !v.Valid || v.Source != SourceWhoami || v.TokenID != 6 {
		t.Fatalf("%+v", v)
	}
	// A macaroon has no row: None, as in Synapse.
	if v := f.auth(t, "/sync", "guest_macaroon"); !v.Valid || v.TokenID != 0 {
		t.Fatalf("%+v", v)
	}
	if *f.whoami != 2 {
		t.Fatalf("/whoami asked %d times", *f.whoami)
	}
}
