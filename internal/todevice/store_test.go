package todevice

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// Against a throwaway PostgreSQL with Synapse's schema, never the live one:
//
//	TODEVICE_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func TestStoreAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("TODEVICE_TEST_DSN")
	if dsn == "" {
		t.Skip("TODEVICE_TEST_DSN not set")
	}
	if strings.Contains(dsn, "/var/sockets") {
		t.Fatal("TODEVICE_TEST_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	db, err := Open(ctx, dsn, 4, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tag := time.Now().UnixNano()
	u := fmt.Sprintf("@td%d:hs.test", tag)
	dest := fmt.Sprintf("d%d.test", tag)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.pool.Exec(ctx, q, args...); err != nil {
			t.Fatal(q, err)
		}
	}
	t.Cleanup(func() {
		for _, q := range []string{`DELETE FROM device_inbox WHERE user_id = $1`, `DELETE FROM devices WHERE user_id = $1`,
			`DELETE FROM device_lists_remote_resync WHERE user_id = $1`} {
			_, _ = db.pool.Exec(context.Background(), q, u)
		}
		_, _ = db.pool.Exec(context.Background(), `DELETE FROM device_federation_outbox WHERE destination = $1`, dest)
		_, _ = db.pool.Exec(context.Background(), `DELETE FROM device_federation_inbox WHERE origin = $1`, dest)
	})
	for _, d := range []struct {
		id     string
		hidden bool
	}{{"A", false}, {"B", false}, {"XSIGN", true}} {
		exec(`INSERT INTO devices (user_id, device_id, hidden) VALUES ($1, $2, $3)`, u, d.id, d.hidden)
	}
	base := int64(900_000_000) + tag%1_000_000*10
	inbox := func(id int64) []string {
		t.Helper()
		rows, err := db.pool.Query(ctx, `SELECT device_id || '=' || message_json || '@' || coalesce(instance_name, '')
			FROM device_inbox WHERE user_id = $1 AND stream_id = $2 ORDER BY device_id`, u, id)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			_ = rows.Scan(&s)
			out = append(out, s)
		}
		return out
	}

	// "*" is every visible device; named devices only those that exist.
	if err := db.AddLocal(ctx, "gw", base+1, []UserMessages{{UserID: u, Devices: []string{"*"}, ByID: map[string]Message{"*": {JSON: `{"m":1}`}}}}); err != nil {
		t.Fatal(err)
	}
	if got := inbox(base + 1); !slices.Equal(got, []string{`A={"m":1}@gw`, `B={"m":1}@gw`}) {
		t.Errorf("wildcard %v", got)
	}
	if err := db.AddLocal(ctx, "gw", base+2, []UserMessages{{UserID: u, Devices: []string{"B", "XSIGN", "GONE"},
		ByID: map[string]Message{"B": {JSON: `{"b":1}`}, "XSIGN": {JSON: `{}`}, "GONE": {JSON: `{}`, NotObject: true}}}}); err != nil {
		t.Fatal(err)
	}
	if got := inbox(base + 2); !slices.Equal(got, []string{`B={"b":1}@gw`}) {
		t.Errorf("named %v", got)
	}
	// A non-object content for an existing device fails the transaction.
	if err := db.AddLocal(ctx, "gw", base+3, []UserMessages{{UserID: u, Devices: []string{"A"},
		ByID: map[string]Message{"A": {JSON: `{"content":1}`, NotObject: true}}}}); err == nil {
		t.Error("non-object stored")
	}

	if err := db.AddRemote(ctx, "gw", base+4, dest, `{"edu":1}`, 5); err != nil {
		t.Fatal(err)
	}
	stored, err := db.AddFromRemote(ctx, "gw", base+5, dest, "m1", []UserMessages{{UserID: u, Devices: []string{"A"}, ByID: map[string]Message{"A": {JSON: `{"r":1}`}}}}, 6)
	if err != nil || !stored {
		t.Fatal(stored, err)
	}
	stored, err = db.AddFromRemote(ctx, "gw", base+6, dest, "m1", []UserMessages{{UserID: u, Devices: []string{"A"}, ByID: map[string]Message{"A": {JSON: `{"r":2}`}}}}, 7)
	if err != nil || stored || len(inbox(base+6)) != 0 {
		t.Fatal("repeat stored", stored, err)
	}
	var inst *string
	_ = db.pool.QueryRow(ctx, `SELECT instance_name FROM device_federation_inbox WHERE origin = $1`, dest).Scan(&inst)
	if inst != nil {
		t.Errorf("device_federation_inbox.instance_name written: %s", *inst)
	}

	// The stream: each entity at its greatest ID, sorted, cut by ID count.
	ups, upto, limited, err := db.Updates(ctx, base, base+6, 100)
	if err != nil || limited || upto != base+6 {
		t.Fatal(upto, limited, err)
	}
	var mine []string
	for _, x := range ups {
		if strings.Contains(string(x.Row), fmt.Sprint(tag)) {
			mine = append(mine, fmt.Sprintf("%d %s", x.Token-base, x.Row))
		}
	}
	if want := []string{`4 ["` + dest + `"]`, `5 ["` + u + `"]`}; !slices.Equal(mine, want) {
		t.Errorf("updates %v, want %v", mine, want)
	}
	ups, upto, limited, err = db.Updates(ctx, base, base+6, 2)
	if err != nil || !limited || upto != base+2 {
		t.Errorf("limited: %v %d %v %v", ups, upto, limited, err)
	}

	if err := db.MarkStale(ctx, u, 10); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkStale(ctx, u, 20); err != nil {
		t.Fatal(err)
	}
	var ts int64
	_ = db.pool.QueryRow(ctx, `SELECT added_ts FROM device_lists_remote_resync WHERE user_id = $1`, u).Scan(&ts)
	if ts != 10 {
		t.Errorf("added_ts %d (the first is kept)", ts)
	}
	if ok, err := db.SharesRoom(ctx, u); err != nil || ok {
		t.Error(ok, err)
	}
}
