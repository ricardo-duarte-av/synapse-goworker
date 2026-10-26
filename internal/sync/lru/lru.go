// Package lru provides a small bounded cache.
//
// Entry-bounded rather than byte-bounded, deliberately: everything cached here
// is a filtered state view, which is a handful of entries. gopro-worker bounds
// its caches in megabytes because it caches *whole* state maps, where one
// entry can be tens of megabytes. That does not apply to anything here, and an
// entry bound is simpler to reason about.
package lru

import (
	"container/list"
	"sync"
)

// Cache is a bounded, concurrency-safe LRU.
type Cache[K comparable, V any] struct {
	mu    sync.Mutex
	max   int
	items map[K]*list.Element
	order *list.List // front = most recently used

	// disarmed makes every Get miss and every Add a no-op, without losing the
	// configured size. A cache whose invalidations arrive over replication is
	// only trustworthy while replication is connected; when it drops there is
	// no way to know what changed underneath us, so the cache stops answering
	// rather than answering stale.
	disarmed bool

	hits, misses, evictions uint64
}

// Stats reports cumulative counters and the current size.
type Stats struct {
	Hits, Misses, Evictions uint64
	Entries                 int
	Armed                   bool
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// New returns a cache holding at most max entries. A max of zero or less
// disables caching entirely, which is a supported configuration rather than a
// mistake: it makes a "does the cache hide a bug?" question answerable.
func New[K comparable, V any](max int) *Cache[K, V] {
	return &Cache[K, V]{
		max:   max,
		items: make(map[K]*list.Element),
		order: list.New(),
	}
}

// Get returns the value for a key.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V
	if c == nil || c.max <= 0 {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed {
		c.misses++
		return zero, false
	}
	elem, ok := c.items[key]
	if !ok {
		c.misses++
		return zero, false
	}
	c.hits++
	c.order.MoveToFront(elem)
	return elem.Value.(*entry[K, V]).value, true
}

// Add stores a value, evicting the least recently used entry if needed.
func (c *Cache[K, V]) Add(key K, value V) {
	if c == nil || c.max <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed {
		return
	}
	if elem, ok := c.items[key]; ok {
		elem.Value.(*entry[K, V]).value = value
		c.order.MoveToFront(elem)
		return
	}
	c.items[key] = c.order.PushFront(&entry[K, V]{key: key, value: value})
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*entry[K, V]).key)
		c.evictions++
	}
}

// Remove drops one key, for a targeted invalidation.
func (c *Cache[K, V]) Remove(key K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.order.Remove(elem)
		delete(c.items, key)
	}
}

// SetArmed enables or disables the cache, purging it when disabling.
//
// Purging on the way down rather than on the way up: while disarmed the cache
// holds nothing, so there is no window in which a stale entry could be served
// by a code path that forgot to check.
func (c *Cache[K, V]) SetArmed(armed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disarmed = !armed
	if !armed {
		c.items = make(map[K]*list.Element)
		c.order.Init()
	}
}

// Stats reports counters for the metrics collector.
func (c *Cache[K, V]) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits: c.hits, Misses: c.misses, Evictions: c.evictions,
		Entries: c.order.Len(), Armed: !c.disarmed,
	}
}

// Purge empties the cache.
//
// Everything cached here is derived from immutable data -- a state group's
// contents are fixed once written -- so this is not needed for correctness on
// the write path. It exists for the replication subscriber to call when it
// loses its connection and can no longer see rooms being purged or events
// deleted underneath us.
func (c *Cache[K, V]) Purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[K]*list.Element)
	c.order.Init()
}

// Len reports how many entries are held.
func (c *Cache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
