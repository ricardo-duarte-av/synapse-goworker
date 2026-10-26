package lazyload

import (
	"testing"
	"time"
)

func TestRecordAndSent(t *testing.T) {
	c := New(10, time.Minute)
	m := c.For(Key{UserID: "@a:x", DeviceID: "D"})
	if got := m.Sent("@b:x"); got != "" {
		t.Fatalf("a fresh cache should remember nothing, got %q", got)
	}
	m.Record("@b:x", "$e1")
	if got := m.Sent("@b:x"); got != "$e1" {
		t.Fatalf("Sent = %q, want $e1", got)
	}
	// A newer membership for the same user replaces the old one, which is what
	// makes the dedupe check `cache.Sent(u) == id` rather than a set lookup.
	m.Record("@b:x", "$e2")
	if got := m.Sent("@b:x"); got != "$e2" {
		t.Fatalf("Sent = %q, want $e2", got)
	}
}

func TestDevicesDoNotShareACache(t *testing.T) {
	c := New(10, time.Minute)
	c.For(Key{UserID: "@a:x", DeviceID: "D1"}).Record("@b:x", "$e1")
	if got := c.For(Key{UserID: "@a:x", DeviceID: "D2"}).Sent("@b:x"); got != "" {
		t.Fatalf("a second device should start empty, got %q", got)
	}
}

func TestClearForgetsEverything(t *testing.T) {
	c := New(10, time.Minute)
	m := c.For(Key{UserID: "@a:x"})
	m.Record("@b:x", "$e1")
	m.Clear()
	if got := m.Sent("@b:x"); got != "" {
		t.Fatalf("Clear should have forgotten the member, got %q", got)
	}
}

func TestExpiryIsFromCreationNotLastUse(t *testing.T) {
	// Synapse builds this cache without reset_expiry_on_get, so a client that
	// syncs continuously still loses its cache on schedule and is re-sent every
	// lazy-loaded member. Touching it must not extend its life.
	c := New(10, time.Minute)
	now := time.Now()
	c.now = func() time.Time { return now }

	key := Key{UserID: "@a:x", DeviceID: "D"}
	c.For(key).Record("@b:x", "$e1")

	now = now.Add(50 * time.Second)
	if got := c.For(key).Sent("@b:x"); got != "$e1" {
		t.Fatalf("cache should survive within the TTL, got %q", got)
	}

	// Still 60s after creation, despite the touch above.
	now = now.Add(11 * time.Second)
	if got := c.For(key).Sent("@b:x"); got != "" {
		t.Fatalf("cache should have expired, got %q", got)
	}
}

func TestNilCacheIsUsable(t *testing.T) {
	// Deps.LazyLoad is nil wherever lazy loading is not configured, and every
	// call site would otherwise need a guard.
	var c *Cache
	m := c.For(Key{UserID: "@a:x"})
	m.Record("@b:x", "$e1")
	m.Clear()
	if got := m.Sent("@b:x"); got != "" {
		t.Fatalf("a nil cache should remember nothing, got %q", got)
	}
}
