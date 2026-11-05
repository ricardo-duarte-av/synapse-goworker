package streamid

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeDB struct {
	mu       sync.Mutex
	seq      int64
	pos      map[string]int64
	stale    []string
	block    chan struct{} // NextVal waits on it when set
	failNext bool
}

func (f *fakeDB) DeleteStale(_ context.Context, _ string, writers []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for inst := range f.pos {
		if inst != writers[0] {
			f.stale = append(f.stale, inst)
			delete(f.pos, inst)
		}
	}
	return nil
}
func (f *fakeDB) LastValue(context.Context, string) (int64, error) { return f.seq, nil }
func (f *fakeDB) NextVal(context.Context, string) (int64, error) {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return 0, errors.New("down")
	}
	f.seq++
	return f.seq, nil
}
func (f *fakeDB) UpsertPosition(_ context.Context, _, inst string, pos int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pos > f.pos[inst] {
		f.pos[inst] = pos
	}
	return nil
}

func open(t *testing.T, db *fakeDB) *Generator {
	t.Helper()
	g, err := Open(context.Background(), db, Options{Stream: "receipts", Instance: "gw", Sequence: "receipts_sequence", Writers: []string{"gw"}})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestOpen(t *testing.T) {
	db := &fakeDB{seq: 100, pos: map[string]int64{"old-writer": 90, "gw": 50}}
	g := open(t, db)
	// The sequence, not our (lagging) row; the old writer's row gone.
	if g.Position() != 100 || len(db.stale) != 1 || db.stale[0] != "old-writer" {
		t.Fatalf("position %d, stale %v", g.Position(), db.stale)
	}
	if _, err := Open(context.Background(), db, Options{Stream: "receipts", Instance: "gw", Writers: []string{"gw", "other"}}); err == nil {
		t.Fatal("opened with two writers")
	}
}

func TestPositionWaitsForTheLowestUnfinished(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{seq: 10, pos: map[string]int64{}}
	g := open(t, db)
	a, _ := g.Next(ctx) // 11
	b, _ := g.Next(ctx) // 12
	c, _ := g.Next(ctx) // 13

	_ = c.Finish(ctx, true)
	if g.Position() != 10 {
		t.Fatalf("passed unfinished 11 and 12: %d", g.Position())
	}
	_ = a.Finish(ctx, true)
	if g.Position() != 11 {
		t.Fatalf("after 11: %d", g.Position())
	}
	// A failure finishes its ID too: 12 no longer holds the stream.
	_ = b.Finish(ctx, false)
	if g.Position() != 13 {
		t.Fatalf("after 12 failed: %d", g.Position())
	}
	// Only successes write stream_positions, at the position then: 11, until
	// the next success.
	if db.pos["gw"] != 11 {
		t.Fatalf("stream_positions %v", db.pos)
	}
	d, _ := g.Next(ctx)
	_ = d.Finish(ctx, true)
	if db.pos["gw"] != 14 {
		t.Fatalf("stream_positions after 14: %v", db.pos)
	}
	// Finishing twice changes nothing.
	_ = a.Finish(ctx, false)
	if g.Position() != 14 {
		t.Fatal("finished twice")
	}
}

func TestAnIDBeingFetchedHoldsThePosition(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{seq: 10, pos: map[string]int64{}}
	g := open(t, db)
	a, _ := g.Next(ctx) // 11

	db.block = make(chan struct{})
	fetched := make(chan *ID)
	go func() { id, _ := g.Next(ctx); fetched <- id }()
	// Wait until the second fetch is under way.
	for {
		g.mu.Lock()
		n := len(g.inFlight)
		g.mu.Unlock()
		if n > 0 {
			break
		}
	}
	_ = a.Finish(ctx, true)
	// 11 finished, and the fetch began with 11 the highest seen: its ID is
	// above 11, so 11 is safe -- but not past it.
	if g.Position() != 11 {
		t.Fatalf("position %d", g.Position())
	}
	close(db.block)
	b := <-fetched
	db.block = nil
	if b.Value != 12 || g.Position() != 11 {
		t.Fatalf("b=%d position=%d", b.Value, g.Position())
	}
	_ = b.Finish(ctx, true)
	if g.Position() != 12 {
		t.Fatalf("after b: %d", g.Position())
	}
}

func TestAFailedFetchHoldsNothing(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{seq: 10, pos: map[string]int64{}, failNext: true}
	g := open(t, db)
	if _, err := g.Next(ctx); err == nil {
		t.Fatal("no error")
	}
	a, _ := g.Next(ctx)
	_ = a.Finish(ctx, true)
	if g.Position() != 11 || len(g.inFlight) != 0 {
		t.Fatalf("position %d, in flight %v", g.Position(), g.inFlight)
	}
}

func TestConcurrent(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{seq: 0, pos: map[string]int64{}}
	g := open(t, db)
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := g.Next(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			// The position never reaches an unfinished ID.
			if p := g.Position(); p >= id.Value {
				t.Errorf("position %d reached unfinished %d", p, id.Value)
			}
			_ = id.Finish(ctx, i%7 != 0)
		}()
	}
	wg.Wait()
	if g.Position() != 200 {
		t.Fatalf("position %d after 200", g.Position())
	}
}

// Against a throwaway PostgreSQL with Synapse's schema:
//
//	STREAMID_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func TestPG(t *testing.T) {
	dsn := os.Getenv("STREAMID_TEST_DSN")
	if dsn == "" {
		t.Skip("STREAMID_TEST_DSN not set")
	}
	if strings.Contains(dsn, "/var/sockets") {
		t.Fatal("STREAMID_TEST_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const stream, seq = "streamid_test", "streamid_test_sequence"
	for _, q := range []string{
		`DROP SEQUENCE IF EXISTS ` + seq, `CREATE SEQUENCE ` + seq,
		`DELETE FROM stream_positions WHERE stream_name = '` + stream + `'`,
		`INSERT INTO stream_positions VALUES ('` + stream + `', 'old-writer', 5)`,
		`SELECT setval('` + seq + `', 41)`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM stream_positions WHERE stream_name = '`+stream+`'`)
		_, _ = pool.Exec(ctx, `DROP SEQUENCE IF EXISTS `+seq)
	})
	g, err := Open(ctx, PG{pool}, Options{Stream: stream, Instance: "gw", Sequence: seq, Writers: []string{"gw"}})
	if err != nil {
		t.Fatal(err)
	}
	if g.Position() != 41 {
		t.Fatalf("position %d", g.Position())
	}
	id, err := g.Next(ctx)
	if err != nil || id.Value != 42 {
		t.Fatalf("next %v %v", id, err)
	}
	if err := id.Finish(ctx, true); err != nil {
		t.Fatal(err)
	}
	var rows []string
	r, _ := pool.Query(ctx, `SELECT instance_name || '=' || stream_id FROM stream_positions WHERE stream_name = $1`, stream)
	for r.Next() {
		var s string
		_ = r.Scan(&s)
		rows = append(rows, s)
	}
	r.Close()
	if len(rows) != 1 || rows[0] != "gw=42" {
		t.Fatalf("stream_positions %v", rows)
	}
	// Never lowered.
	if err := (PG{pool}).UpsertPosition(ctx, stream, "gw", 7); err != nil {
		t.Fatal(err)
	}
	var v int64
	_ = pool.QueryRow(ctx, `SELECT stream_id FROM stream_positions WHERE stream_name = $1`, stream).Scan(&v)
	if v != 42 {
		t.Fatalf("lowered to %d", v)
	}
}
