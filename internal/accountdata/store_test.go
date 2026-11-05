package accountdata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
)

// Against a throwaway PostgreSQL with Synapse's schema, never the live one:
//
//	ACCOUNTDATA_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("ACCOUNTDATA_TEST_DSN")
	if dsn == "" {
		t.Skip("ACCOUNTDATA_TEST_DSN not set")
	}
	if strings.Contains(dsn, "/var/sockets") {
		t.Fatal("ACCOUNTDATA_TEST_DSN must be a throwaway database, not the live one")
	}
	db, err := Open(context.Background(), dsn, 4, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

func TestStoreAgainstPostgres(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	tag := time.Now().UnixNano()
	me := fmt.Sprintf("@me%d:hs.test", tag)
	room := fmt.Sprintf("!ad%d:hs.test", tag)
	a, b, c := fmt.Sprintf("@a%d:x", tag), fmt.Sprintf("@b%d:x", tag), fmt.Sprintf("@c%d:x", tag)
	t.Cleanup(func() {
		for _, q := range []string{`DELETE FROM account_data WHERE user_id = $1`, `DELETE FROM room_account_data WHERE user_id = $1`,
			`DELETE FROM room_tags WHERE user_id = $1`, `DELETE FROM room_tags_revisions WHERE user_id = $1`,
			`DELETE FROM ignored_users WHERE ignorer_user_id = $1`} {
			_, _ = db.pool.Exec(context.Background(), q, me)
		}
	})
	id := int64(800_000_000) + tag%1_000_000*10
	next := func() int64 { id++; return id }
	var content string
	var sid int64
	row := func(q string, args ...any) {
		t.Helper()
		if err := db.pool.QueryRow(ctx, q, args...).Scan(&content, &sid); err != nil {
			t.Fatal(q, err)
		}
	}
	ignoredSet := func() []string {
		t.Helper()
		rows, err := db.pool.Query(ctx, `SELECT ignored_user_id FROM ignored_users WHERE ignorer_user_id = $1 ORDER BY 1`, me)
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
	cacheKeys := func(rows []cachestream.Row) []string {
		var out []string
		for _, r := range rows {
			out = append(out, r.CacheFunc+":"+strings.Join(r.Keys, ","))
			var n int
			if err := db.pool.QueryRow(ctx, `SELECT count(*) FROM cache_invalidation_stream_by_instance
				WHERE stream_id = $1 AND instance_name = 'gw' AND cache_func = $2 AND keys = $3`, r.StreamID, r.CacheFunc, r.Keys).Scan(&n); err != nil || n != 1 {
				t.Errorf("caches row %+v not stored: %d %v", r, n, err)
			}
		}
		slices.Sort(out)
		return out
	}

	// A plain type: upsert.
	first := next()
	if rows, err := db.AddUser(ctx, "gw", first, me, "m.direct", `{"x":1}`); err != nil || rows != nil {
		t.Fatal(rows, err)
	}
	second := next()
	if _, err := db.AddUser(ctx, "gw", second, me, "m.direct", `{"x":2}`); err != nil {
		t.Fatal(err)
	}
	row(`SELECT content, stream_id FROM account_data WHERE user_id = $1 AND account_data_type = 'm.direct'`, me)
	if content != `{"x":2}` || sid != second {
		t.Errorf("%s %d", content, sid)
	}
	var inst *string
	_ = db.pool.QueryRow(ctx, `SELECT instance_name FROM account_data WHERE user_id = $1`, me).Scan(&inst)
	if inst != nil {
		t.Errorf("instance_name written: %v (Synapse leaves it NULL)", *inst)
	}

	// The ignored list: the table follows it, and the caches stream says so.
	rows, err := db.AddUser(ctx, "gw", next(), me, TypeIgnoredUserList, `{"ignored_users":{"`+a+`":{},"`+b+`":{}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := ignoredSet(); !slices.Equal(got, []string{a, b}) {
		t.Errorf("ignored %v", got)
	}
	if got := cacheKeys(rows); !slices.Equal(got, []string{"ignored_by:" + a, "ignored_by:" + b, "ignored_users:" + me}) {
		t.Errorf("caches %v", got)
	}
	rows, err = db.AddUser(ctx, "gw", next(), me, TypeIgnoredUserList, `{"ignored_users":{"`+b+`":{},"`+c+`":{}},"other":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := ignoredSet(); !slices.Equal(got, []string{b, c}) {
		t.Errorf("ignored %v", got)
	}
	if got := cacheKeys(rows); !slices.Equal(got, []string{"ignored_by:" + a, "ignored_by:" + c, "ignored_users:" + me}) {
		t.Errorf("caches %v", got)
	}
	// Unchanged: the content is stored, nothing is invalidated.
	unchanged := next()
	rows, err = db.AddUser(ctx, "gw", unchanged, me, TypeIgnoredUserList, `{"ignored_users":{"`+c+`":{},"`+b+`":{}}}`)
	if err != nil || rows != nil {
		t.Fatal(rows, err)
	}
	row(`SELECT content, stream_id FROM account_data WHERE user_id = $1 AND account_data_type = $2`, me, TypeIgnoredUserList)
	if sid != unchanged {
		t.Errorf("unchanged list not stored: %d", sid)
	}
	// Ignoring oneself rolls everything back.
	if _, err := db.AddUser(ctx, "gw", next(), me, TypeIgnoredUserList, `{"ignored_users":{"`+me+`":{}}}`); !errors.Is(err, ErrIgnoreSelf) {
		t.Fatal(err)
	}
	row(`SELECT content, stream_id FROM account_data WHERE user_id = $1 AND account_data_type = $2`, me, TypeIgnoredUserList)
	if sid != unchanged {
		t.Errorf("self-ignore was not rolled back: %d", sid)
	}

	// Removes: once, then nothing to remove.
	removed := next()
	updated, rows, err := db.RemoveUser(ctx, "gw", removed, me, TypeIgnoredUserList)
	if err != nil || !updated {
		t.Fatal(updated, err)
	}
	if got := ignoredSet(); len(got) != 0 {
		t.Errorf("ignored after remove %v", got)
	}
	if got := cacheKeys(rows); !slices.Equal(got, []string{"ignored_by:" + b, "ignored_by:" + c, "ignored_users:" + me}) {
		t.Errorf("caches %v", got)
	}
	row(`SELECT content, stream_id FROM account_data WHERE user_id = $1 AND account_data_type = $2`, me, TypeIgnoredUserList)
	if content != "{}" || sid != removed {
		t.Errorf("%s %d", content, sid)
	}
	if updated, rows, err := db.RemoveUser(ctx, "gw", next(), me, TypeIgnoredUserList); err != nil || updated || rows != nil {
		t.Errorf("second remove: %v %v %v", updated, rows, err)
	}

	// Room account data.
	if err := db.AddRoom(ctx, next(), me, room, "m.fully_read", `{"event_id":"$e"}`); err != nil {
		t.Fatal(err)
	}
	got, err := db.Room(ctx, me, room, "m.fully_read")
	if err != nil || string(got) != `{"event_id":"$e"}` {
		t.Errorf("%s %v", got, err)
	}
	if updated, err := db.RemoveRoom(ctx, next(), me, room, "m.fully_read"); err != nil || !updated {
		t.Error(updated, err)
	}
	if updated, err := db.RemoveRoom(ctx, next(), me, room, "m.fully_read"); err != nil || updated {
		t.Error(updated, err)
	}

	// Tags and their revision.
	t1 := next()
	if err := db.AddTag(ctx, t1, me, room, "u.work", `{"order":1}`); err != nil {
		t.Fatal(err)
	}
	t2 := next()
	if err := db.AddTag(ctx, t2, me, room, "u.home", `{}`); err != nil {
		t.Fatal(err)
	}
	var rev int64
	_ = db.pool.QueryRow(ctx, `SELECT stream_id FROM room_tags_revisions WHERE user_id = $1 AND room_id = $2`, me, room).Scan(&rev)
	if rev != t2 {
		t.Errorf("revision %d, want %d", rev, t2)
	}
	t3 := next()
	if err := db.RemoveTag(ctx, t3, me, room, "u.work"); err != nil {
		t.Fatal(err)
	}
	tags, err := db.Tags(ctx, me, room)
	if err != nil || len(tags) != 1 || tags[0].Name != "u.home" {
		t.Errorf("%v %v", tags, err)
	}
	if ok, err := db.HasMembership(ctx, me, room); err != nil || ok {
		t.Error(ok, err)
	}

	// The stream: every table, merged, and limited.
	ups, upto, limited, err := db.Updates(ctx, first-1, id, 100)
	if err != nil || limited || upto != id {
		t.Fatal(upto, limited, err)
	}
	var seen []string
	for _, u := range ups {
		if strings.Contains(string(u.Row), me) {
			seen = append(seen, fmt.Sprintf("%d %s", u.Token, u.Row))
		}
	}
	// Each table holds a row's latest ID only: m.direct at second, the
	// ignored list at removed, the room data at its remove, the tags at t3.
	if len(seen) != 4 || !strings.Contains(seen[len(seen)-1], `"m.tag"]`) || !strings.Contains(seen[0], `null,"m.direct"]`) {
		t.Errorf("updates %v", seen)
	}
	ups, upto, limited, err = db.Updates(ctx, first-1, id, 1)
	if err != nil || !limited || len(ups) != 1 || upto != ups[0].Token {
		t.Errorf("limited: %v %d %v %v", ups, upto, limited, err)
	}
}

// A concurrent first tag of one user in one room loses its write, as in
// Synapse, where the caught IntegrityError leaves an aborted transaction
// that psycopg2 then commits as a silent ROLLBACK.
func TestTagRevisionRace(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	me := fmt.Sprintf("@race%d:hs.test", time.Now().UnixNano())
	room := "!race:hs.test"
	t.Cleanup(func() {
		_, _ = db.pool.Exec(context.Background(), `DELETE FROM room_tags WHERE user_id = $1`, me)
		_, _ = db.pool.Exec(context.Background(), `DELETE FROM room_tags_revisions WHERE user_id = $1`, me)
	})
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, qUpsertTag, me, room, "u.mine", `{}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE room_tags_revisions SET stream_id = 1 WHERE user_id = $1 AND room_id = $2`, me, room); err != nil {
		t.Fatal(err)
	}
	// The other writer's revision commits in between.
	if _, err := db.pool.Exec(ctx, `INSERT INTO room_tags_revisions (user_id, room_id, stream_id) VALUES ($1, $2, 2)`, me, room); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO room_tags_revisions (user_id, room_id, stream_id) VALUES ($1, $2, 1)`, me, room)
	if !isUniqueViolation(err) {
		t.Fatalf("not a unique violation: %v", err)
	}
	_ = tx.Rollback(ctx)
	// Through the store: the write is reported done and nothing is kept.
	if err := db.tagTxn(ctx, func(pgx.Tx) error { return errLostRevision }); err != nil {
		t.Errorf("lost revision reported: %v", err)
	}
	var n int
	_ = db.pool.QueryRow(ctx, `SELECT count(*) FROM room_tags WHERE user_id = $1`, me).Scan(&n)
	if n != 0 {
		t.Errorf("tag kept: %d", n)
	}
}
