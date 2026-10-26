package lru

import "testing"

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	c := New[string, int](2)
	c.Add("a", 1)
	c.Add("b", 2)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should still be present")
	}
	// "a" was just used, so "b" is the eviction candidate.
	c.Add("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("a should have survived")
	}
	if c.Len() != 2 {
		t.Errorf("Len = %d, want 2", c.Len())
	}
}

func TestUpdateDoesNotGrow(t *testing.T) {
	c := New[string, int](2)
	c.Add("a", 1)
	c.Add("a", 2)
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
	if v, _ := c.Get("a"); v != 2 {
		t.Errorf("value = %d, want 2", v)
	}
}

// A max of zero disables caching, which is a supported configuration: it makes
// "is the cache hiding a bug?" answerable.
func TestZeroMaxDisablesCaching(t *testing.T) {
	c := New[string, int](0)
	c.Add("a", 1)
	if _, ok := c.Get("a"); ok {
		t.Error("a disabled cache must not retain anything")
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0", c.Len())
	}
}

func TestPurge(t *testing.T) {
	c := New[string, int](4)
	c.Add("a", 1)
	c.Add("b", 2)
	c.Purge()
	if c.Len() != 0 {
		t.Errorf("Len = %d after Purge, want 0", c.Len())
	}
	if _, ok := c.Get("a"); ok {
		t.Error("Purge should have removed a")
	}
	// Still usable afterwards.
	c.Add("c", 3)
	if _, ok := c.Get("c"); !ok {
		t.Error("cache should work after Purge")
	}
}

// A nil cache is a usable no-op, so a caller need not special-case it.
func TestNilCacheIsSafe(t *testing.T) {
	var c *Cache[string, int]
	c.Add("a", 1)
	if _, ok := c.Get("a"); ok {
		t.Error("a nil cache holds nothing")
	}
	c.Purge()
	if c.Len() != 0 {
		t.Error("a nil cache has no entries")
	}
}

func TestConcurrentUse(t *testing.T) {
	c := New[int, int](64)
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 500; i++ {
				c.Add(w*1000+i, i)
				c.Get(i)
			}
		}(w)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if c.Len() > 64 {
		t.Errorf("Len = %d, want at most 64", c.Len())
	}
}
