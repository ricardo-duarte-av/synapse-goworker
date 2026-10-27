package statecache

import (
	"context"
	"testing"
)

type fakeDB struct {
	full   map[int64]Map
	prev   map[int64]int64
	delta  map[int64]Map
	reads  int
	deltas int
}

func (f *fakeDB) Delta(_ context.Context, g int64) (int64, bool, Map, error) {
	f.deltas++
	p, ok := f.prev[g]
	return p, ok, f.delta[g], nil
}

func (f *fakeDB) Full(_ context.Context, g int64) (Map, error) {
	f.reads++
	return f.full[g], nil
}

var (
	create = Key{"m.room.create", ""}
	alice  = Key{"m.room.member", "@alice:a"}
)

func TestLoadIsOnceThenFromMemory(t *testing.T) {
	db := &fakeDB{full: map[int64]Map{1: {create: "$c"}}}
	c := New(1 << 20)
	c.SetArmed(true)
	for i, want := range []string{Full, Hit, Hit} {
		m, how, err := c.Load(context.Background(), 1, db)
		if err != nil || how != want || m[create] != "$c" {
			t.Fatalf("load %d: %v %q %v, want %q", i, m, how, err, want)
		}
	}
	if db.reads != 1 {
		t.Errorf("read the group %d times", db.reads)
	}
}

// A group whose prev is cached is rebuilt from the prev and its delta; the
// prev's map is not touched, since it is shared.
func TestLoadRebuildsFromACachedPrev(t *testing.T) {
	db := &fakeDB{
		full:  map[int64]Map{1: {create: "$c"}},
		prev:  map[int64]int64{2: 1},
		delta: map[int64]Map{2: {alice: "$join"}},
	}
	c := New(1 << 20)
	c.SetArmed(true)
	base, _, _ := c.Load(context.Background(), 1, db)
	m, how, err := c.Load(context.Background(), 2, db)
	if err != nil || how != Delta || m[create] != "$c" || m[alice] != "$join" {
		t.Fatalf("rebuilt %v %q %v", m, how, err)
	}
	if len(base) != 1 {
		t.Errorf("the cached prev was modified: %v", base)
	}
	if db.reads != 1 {
		t.Errorf("read %d groups in full, want only the first", db.reads)
	}
}

// Disarmed (the replication stream is not being followed), nothing is
// cached: a group purged meanwhile must not be served from memory.
func TestDisarmedCachesNothing(t *testing.T) {
	db := &fakeDB{full: map[int64]Map{1: {create: "$c"}}}
	c := New(1 << 20)
	c.SetArmed(false)
	for range 2 {
		if _, how, _ := c.Load(context.Background(), 1, db); how != Full {
			t.Fatalf("disarmed load was %q", how)
		}
	}
	if db.deltas != 0 {
		t.Error("a disarmed cache looked up deltas it could not use")
	}
	c.SetArmed(true)
	c.Load(context.Background(), 1, db)
	c.SetArmed(false)
	if _, ok := c.Get(1); ok {
		t.Error("disarming kept a group")
	}
}

func TestBoundedByBytes(t *testing.T) {
	m := Map{create: "$c"}
	c := New(SizeOf(m) * 2)
	c.SetArmed(true)
	for g := int64(1); g <= 3; g++ {
		c.Add(g, Map{create: "$c"})
	}
	if _, ok := c.Get(1); ok {
		t.Error("the oldest group outlived the bound")
	}
	if _, ok := c.Get(3); !ok {
		t.Error("the newest group was not kept")
	}
}
