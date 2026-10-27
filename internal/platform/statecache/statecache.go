// Package statecache is the worker's one cache of room state: a state group's
// full state, (type, state_key) -> event ID.
//
// A state group is immutable once written, so its state can be cached for as
// long as it exists. The federation reads (/state, /state_ids) and inbound
// write mode (auth, state resolution, push) both need whole groups, often the
// same ones -- the rooms remote servers ask about are the rooms they send
// into -- and one byte-bounded cache serves both, so a group is read from the
// database once and held once.
//
// Maps handed out are shared: callers must never modify one. Both callers
// already build a new map whenever they need a changed one.
//
// Groups do stop existing, when a room's history is purged or the room is
// deleted. Synapse announces that on the replication stream, so the cache is
// armed only while the stream is followed, and purged on those announcements
// (see the federation reads' replication package, which drives it).
package statecache

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cache"
)

// Key is a state key: an event type and its state_key.
type Key struct {
	Type     string
	StateKey string
}

// Map is a state group's full state. Never modify one taken from the cache.
type Map = map[Key]string

// Cache holds state groups' full state, bounded by bytes.
type Cache struct {
	lru *cache.LRU[int64, Map]
}

// New builds a cache of at most maxBytes. Whoever follows the replication
// stream disarms it until the stream is confirmed (the federation reads'
// subscriber does so at start).
func New(maxBytes int64) *Cache {
	return &Cache{lru: cache.NewLRU[int64, Map]("state_groups", maxBytes, SizeOf)}
}

// SizeOf approximates a state map's footprint: the strings, plus a per-entry
// constant for map overhead.
func SizeOf(m Map) int64 {
	var n int64
	for k, v := range m {
		n += int64(len(k.Type) + len(k.StateKey) + len(v) + 64)
	}
	return n
}

// Get returns a group's state if it is cached.
func (c *Cache) Get(group int64) (Map, bool) {
	if c == nil {
		return nil, false
	}
	return c.lru.Get(group)
}

// Add caches a group's state. The map must not be modified afterwards.
func (c *Cache) Add(group int64, m Map) {
	if c != nil {
		c.lru.Add(group, m)
	}
}

// Loader reads state groups from the database.
type Loader interface {
	// Delta is a group's prev group and its difference from it; hasPrev is
	// false for a group stored in full.
	Delta(ctx context.Context, group int64) (prev int64, hasPrev bool, delta Map, err error)
	// Full resolves a group's whole state.
	Full(ctx context.Context, group int64) (Map, error)
}

// How a group was got, for the callers' metrics.
const (
	Hit   = "hit"   // cached
	Delta = "delta" // rebuilt from a cached prev group and its delta
	Full  = "full"  // read from the database
)

// Load returns a group's state, from the cache if it can: whole, or rebuilt
// from its cached prev group and the (small) delta stored for it, which in a
// busy room is the common case and costs one indexed read instead of a walk
// of the whole chain.
func (c *Cache) Load(ctx context.Context, group int64, db Loader) (Map, string, error) {
	if m, ok := c.Get(group); ok {
		return m, Hit, nil
	}
	if c != nil && c.lru.Armed() {
		prev, hasPrev, delta, err := db.Delta(ctx, group)
		if err != nil {
			return nil, "", err
		}
		if hasPrev {
			if base, ok := c.Get(prev); ok {
				m := make(Map, len(base)+len(delta))
				for k, v := range base {
					m[k] = v
				}
				for k, v := range delta {
					m[k] = v
				}
				c.Add(group, m)
				return m, Delta, nil
			}
		}
	}
	m, err := db.Full(ctx, group)
	if err != nil {
		return nil, "", err
	}
	c.Add(group, m)
	return m, Full, nil
}

// SetArmed turns the cache on or off; off empties it.
func (c *Cache) SetArmed(armed bool) {
	if c != nil {
		c.lru.SetArmed(armed)
	}
}

// Purge empties the cache, leaving it armed.
func (c *Cache) Purge() {
	if c != nil {
		c.lru.Purge()
	}
}

// Stats is a snapshot for metrics.
func (c *Cache) Stats() cache.Stats { return c.lru.Stats() }
