// Package lazyload remembers which member events have already been sent to a
// client, so lazy-loaded syncs do not send them again.
//
// This is server-side state with no representation in the database or in any
// token, which makes it the one part of a /sync response that a second
// implementation cannot reproduce from the same inputs: our cache and
// Synapse's are filled by different traffic and will disagree. Two things keep
// that bounded. An initial sync clears the cache before using it, so it is
// always deterministic. And the whole per-device cache expires 30 minutes
// after it was created -- not after it was last used -- so both sides
// periodically forget everything and re-send.
//
// Port of the `lazy_loaded_members_cache` in synapse/handlers/sync.py.
package lazyload

import (
	"sync"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lru"
)

// DefaultMaxPerDevice is how many members one device's cache remembers.
//
// Synapse asks for 100 and then scales it by `caches.global_factor`, which
// defaults to 0.5 -- so the effective size on a default deployment is 50, not
// 100. Getting this wrong is not a correctness bug but a bandwidth one: too
// small and members are re-sent, too large and we withhold members Synapse
// would have sent.
const DefaultMaxPerDevice = 50

// DefaultTTL is how long a device's cache lives.
//
// Measured from creation, not from last use: Synapse's ExpiringCache is built
// here without `reset_expiry_on_get`, so a continuously syncing client still
// loses its cache every half hour and is sent every lazy-loaded member again.
const DefaultTTL = 30 * time.Minute

// Key identifies one client session.
//
// The device ID is part of it because two devices of the same user have seen
// different things; a token with no device (an appservice, an old token) shares
// one cache under the empty string, as Synapse shares one under None.
type Key struct {
	UserID   string
	DeviceID string
}

// Cache holds one member cache per device.
type Cache struct {
	mu           sync.Mutex
	entries      map[Key]*Members
	maxPerDevice int
	ttl          time.Duration
	lastPrune    time.Time
	now          func() time.Time
}

// Members is one device's record of the members it has been sent.
type Members struct {
	mu      sync.Mutex
	lru     *lru.Cache[string, string]
	created time.Time
}

// New builds a cache. A non-positive size or TTL takes the default.
func New(maxPerDevice int, ttl time.Duration) *Cache {
	if maxPerDevice <= 0 {
		maxPerDevice = DefaultMaxPerDevice
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Cache{
		entries:      map[Key]*Members{},
		maxPerDevice: maxPerDevice,
		ttl:          ttl,
		now:          time.Now,
	}
}

// For returns the member cache for one session, creating it if needed.
func (c *Cache) For(key Key) *Members {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.pruneLocked(now)

	m, ok := c.entries[key]
	if ok && now.Sub(m.created) < c.ttl {
		return m
	}
	m = &Members{lru: lru.New[string, string](c.maxPerDevice), created: now}
	c.entries[key] = m
	return m
}

// pruneLocked drops expired sessions.
//
// Synapse runs this on a timer at half the expiry interval. Doing it on access
// instead avoids a goroutine, and the rate limit keeps a busy server from
// walking the whole map on every request.
func (c *Cache) pruneLocked(now time.Time) {
	if now.Sub(c.lastPrune) < c.ttl/2 {
		return
	}
	c.lastPrune = now
	for k, m := range c.entries {
		if now.Sub(m.created) >= c.ttl {
			delete(c.entries, k)
		}
	}
}

// Len reports how many sessions are cached, for metrics and tests.
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Sent reports the member event last sent to this session for a user, or "".
func (m *Members) Sent(userID string) string {
	if m == nil {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, _ := m.lru.Get(userID)
	return v
}

// Record notes that a member event has been sent.
func (m *Members) Record(userID, eventID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lru.Add(userID, eventID)
}

// Clear forgets everything.
//
// An initial sync does this before computing its state block: the client is
// starting over and has no memory of what it was sent before, so neither may
// we. This is also what makes an initial lazy-loading sync reproducible.
func (m *Members) Clear() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lru.Purge()
}
