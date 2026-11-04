package e2ekeys

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tidwall/gjson"
)

const schema = `
CREATE TABLE devices (user_id text NOT NULL, device_id text NOT NULL, display_name text, hidden boolean DEFAULT false);
CREATE TABLE e2e_device_keys_json (user_id text NOT NULL, device_id text NOT NULL, ts_added_ms bigint NOT NULL, key_json text NOT NULL);
CREATE TABLE e2e_cross_signing_keys (user_id text NOT NULL, keytype text NOT NULL, keydata text NOT NULL, stream_id bigint NOT NULL);
CREATE TABLE e2e_cross_signing_signatures (user_id text NOT NULL, key_id text NOT NULL, target_user_id text NOT NULL,
	target_device_id text NOT NULL, signature text NOT NULL);
INSERT INTO devices VALUES ('@a:x', 'D', 'Phone <1>', false);
INSERT INTO e2e_device_keys_json VALUES ('@a:x', 'D', 1, '{"device_id":"D","keys":{"ed25519:D":"d"},"unsigned":{"old":1}}');
INSERT INTO e2e_cross_signing_keys VALUES
	('@a:x', 'master', '{"user_id":"@a:x","keys":{"ed25519:am":"am"}}', 1),
	('@a:x', 'self_signing', '{"user_id":"@a:x","keys":{"ed25519:as":"as"}}', 2),
	('@a:x', 'user_signing', '{"user_id":"@a:x","keys":{"ed25519:au":"au"}}', 3);
INSERT INTO e2e_cross_signing_signatures VALUES
	('@a:x', 'ed25519:as', '@a:x', 'D', 'self-cross'),
	('@a:x', 'ed25519:D', '@a:x', 'am', 'device-on-master');`

func testPool(t *testing.T) *pgxpool.Pool {
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
	name := fmt.Sprintf("e2ekeys_test_%d", time.Now().UnixNano())
	if _, err := ac.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = ac.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		_ = ac.Close(context.Background())
	})
	p, err := pgxpool.New(ctx, admin+" dbname="+name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if _, err := p.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	return p
}

// What a server is told (from_user_id None, no display names) and what a
// user is (their own signatures, their user-signing key, names).
func TestLocalAndCrossSigning(t *testing.T) {
	q := PoolQuerier(testPool(t))
	ctx := context.Background()
	users := []string{"@a:x", "@none:x"}

	srv, err := Local(ctx, q, users, map[string][]string{}, false)
	if err != nil {
		t.Fatal(err)
	}
	d := gjson.ParseBytes(srv["@a:x"]["D"])
	if d.Get("unsigned").Raw != `{}` || d.Get(`signatures.@a:x.ed25519:as`).Str != "self-cross" || len(srv["@none:x"]) != 0 || srv["@none:x"] == nil {
		t.Errorf("server: %v", srv)
	}
	usr, _ := Local(ctx, q, users, map[string][]string{"@a:x": {"D", "nope"}}, true)
	if gjson.GetBytes(usr["@a:x"]["D"], "unsigned").Raw != `{"device_display_name":"Phone <1>"}` {
		t.Errorf("user: %s", usr["@a:x"]["D"])
	}

	m, s, u, err := CrossSigning(ctx, q, users, "")
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(m["@a:x"], "signatures").Exists() || s["@a:x"] == nil || len(u) != 0 {
		t.Errorf("server: %s %s %v", m["@a:x"], s["@a:x"], u)
	}
	m, _, u, _ = CrossSigning(ctx, q, users, "@a:x")
	if gjson.GetBytes(m["@a:x"], `signatures.@a:x.ed25519:D`).Str != "device-on-master" || u["@a:x"] == nil {
		t.Errorf("owner: %s %v", m["@a:x"], u)
	}
}
