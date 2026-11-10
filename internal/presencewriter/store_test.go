package presencewriter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Against a throwaway PostgreSQL with Synapse's schema, never the live one:
//
//	PRESENCEWRITER_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func TestStoreAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("PRESENCEWRITER_TEST_DSN")
	if dsn == "" {
		t.Skip("PRESENCEWRITER_TEST_DSN not set")
	}
	if strings.Contains(dsn, "/var/sockets") {
		t.Fatal("PRESENCEWRITER_TEST_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	db, err := Open(ctx, dsn, 2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tag := time.Now().UnixNano()
	a := fmt.Sprintf("@pa%d:hs.test", tag)
	b := fmt.Sprintf("@pb%d:hs.test", tag)
	base := int64(800_000_000) + tag%1_000_000*10
	t.Cleanup(func() {
		_, _ = db.pool.Exec(context.Background(), `DELETE FROM presence_stream WHERE user_id = ANY($1)`, []string{a, b})
		_, _ = db.pool.Exec(context.Background(), `DELETE FROM ratelimit_override WHERE user_id = ANY($1)`, []string{a, b})
	})
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.pool.Exec(ctx, q, args...); err != nil {
			t.Fatal(q, err)
		}
	}
	// a: an old row and a current online one; b: offline, with NULLs.
	exec(`INSERT INTO presence_stream (stream_id, user_id, state, last_active_ts, last_federation_update_ts,
		last_user_sync_ts, status_msg, currently_active, instance_name) VALUES
		($1, $2, 'online', 1, 2, 3, 'old', true, 'old-writer'),
		($3, $2, 'unavailable', 10, 20, 30, 'caf'||chr(233), NULL, 'old-writer'),
		($4, $5, 'offline', NULL, NULL, NULL, NULL, false, 'old-writer')`, base+1, a, base+2, base+3, b)

	active, err := db.Active(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine []State
	for _, s := range active {
		if s.UserID == a || s.UserID == b {
			mine = append(mine, s)
		}
	}
	if len(mine) != 2 {
		t.Fatalf("active %+v", mine)
	}

	rows, upto, limited, err := db.Updates(ctx, base, base+3, 100)
	if err != nil || limited || upto != base+3 || len(rows) != 3 {
		t.Fatalf("updates %v %d %v %v", rows, upto, limited, err)
	}
	// json_encoder's ensure_ascii.
	if got := string(rows[1].Row); got != fmt.Sprintf(`[%q,"unavailable",10,20,30,"caf`+"\\"+`u00e9",null]`, a) {
		t.Errorf("row %s", got)
	}
	if got := string(rows[2].Row); got != fmt.Sprintf(`[%q,"offline",null,null,null,null,false]`, b) {
		t.Errorf("row %s", got)
	}
	rows, upto, limited, err = db.Updates(ctx, base, base+3, 2)
	if err != nil || !limited || upto != base+2 || len(rows) != 2 {
		t.Fatalf("limited updates %d %d %v %v", len(rows), upto, limited, err)
	}

	// Marking a offline: its older rows go, the new one is ours.
	s := State{UserID: a, State: "offline", LastActiveTS: ptr(int64(10)), LastFederationUpdateTS: ptr(int64(20)), LastUserSyncTS: ptr(int64(30))}
	if err := db.Persist(ctx, "gw", []int64{base + 5}, []State{s}); err != nil {
		t.Fatal(err)
	}
	var got []string
	r, err := db.pool.Query(ctx, `SELECT stream_id::text || ' ' || state || ' ' || coalesce(status_msg, 'NULL') || ' ' ||
		currently_active::text || ' ' || instance_name || ' ' || last_active_ts::text
		FROM presence_stream WHERE user_id = $1 ORDER BY stream_id`, a)
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var line string
		if err := r.Scan(&line); err != nil {
			t.Fatal(err)
		}
		got = append(got, line)
	}
	if want := fmt.Sprintf("%d offline NULL false gw 10", base+5); len(got) != 1 || got[0] != want {
		t.Errorf("rows %v, want %s", got, want)
	}

	if ex, err := db.RatelimitExempt(ctx, a); err != nil || ex {
		t.Errorf("exempt without a row: %v %v", ex, err)
	}
	exec(`INSERT INTO ratelimit_override (user_id, messages_per_second, burst_count) VALUES ($1, 0, 0), ($2, 5, 5)`, a, b)
	if ex, _ := db.RatelimitExempt(ctx, a); !ex {
		t.Error("a zero rate is exempt")
	}
	if ex, _ := db.RatelimitExempt(ctx, b); ex {
		t.Error("a rate is not exempt")
	}
}

func ptr[T any](v T) *T { return &v }
