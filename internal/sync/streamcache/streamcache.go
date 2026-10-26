// Package streamcache answers "has entity X changed since stream position P?"
// from memory, so that a sync pass can skip the query that would answer it.
//
// A port of Synapse's `synapse/util/caches/stream_change_cache.py`. The
// asymmetry is the whole design and is worth stating before anything else:
//
//	A false positive is free.  A false negative is a lost event.
//
// Saying "changed" when nothing did costs one query that would have run
// anyway. Saying "unchanged" when something did means a client never learns
// about it, and nothing downstream will notice. Every decision below resolves
// in that direction, and so should every future one.
//
// The cache is a complete record of changes *above* its horizon, and knows
// nothing below it. `earliest` is that horizon: a question about a position at
// or below it is answered "changed" without looking, and an entity absent from
// a cache asked about a position above it provably did not change. That is
// what makes "unknown entity" mean "unchanged" rather than "no idea" -- a
// reading that is only sound because of the horizon, so the horizon may never
// move backwards.
package streamcache

import (
	"sort"
	"sync"
)

// Cache tracks the position of the most recent change to each entity.
//
// The entity is a room ID or a user ID depending on the stream, but nothing
// here cares which.
type Cache struct {
	mu   sync.Mutex
	name string

	// max bounds the number of distinct *positions* held, not the number of
	// entities, matching Synapse. One position can name many entities.
	max int

	byPos     map[int64]map[string]struct{}
	positions []int64 // keys of byPos, ascending
	entityPos map[string]int64

	// earliest is the horizon: the cache cannot answer for any position at or
	// below it. Never decreases.
	earliest int64

	// disarmed makes every question answer "changed" and every update a no-op.
	// While replication is down we cannot see changes happening, so the only
	// safe answer is the one that costs a query.
	disarmed bool

	hits, misses, evictions uint64
}

// Stats reports counters and horizon for the metrics collector.
type Stats struct {
	Hits, Misses, Evictions uint64
	Entities                int
	Positions               int
	Earliest                int64
	Armed                   bool
}

// New returns a cache that knows nothing below currentPos, DISARMED.
//
// Disarmed until Arm is called, and that is not a detail. A cache constructed
// armed at horizon 0 with nothing in it claims to hold a complete record of all
// history and therefore answers "unchanged" to every question -- the exact
// false negative this package exists to prevent, arrived at by doing nothing.
//
// It is reachable, which is why it is guarded rather than merely documented: a
// deployment with replication disabled never fires the connect callback that
// prefills and arms these, and a Store built for a test or a one-off tool never
// prefills at all. Found on 2026-09-03 when a test's membership gate silently
// reported no changes.
//
// A max of zero or less is a supported configuration, not a mistake: it makes
// "does this cache hide a bug?" answerable by turning it off. Such a cache
// holds nothing and therefore answers "changed" to everything.
func New(name string, currentPos int64, max int) *Cache {
	return &Cache{
		name:      name,
		max:       max,
		byPos:     make(map[int64]map[string]struct{}),
		entityPos: make(map[string]int64),
		earliest:  currentPos,
		disarmed:  true,
	}
}

// Prefill seeds the cache from one query per stream at startup and sets the
// horizon to the oldest position that query could see.
//
// Without this every cache starts with earliest == now, every question falls
// below the horizon, and every gate says "changed" for as long as it takes the
// live stream to fill it. The failure is silent -- the queries simply never go
// away -- which is why EarliestKnownPosition is exported and reported as a
// metric.
func (c *Cache) Prefill(entries map[string]int64, minPos int64) {
	if c == nil || c.max <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed {
		return
	}
	if minPos > c.earliest {
		c.earliest = minPos
	}
	for entity, pos := range entries {
		c.entityHasChangedLocked(entity, pos)
	}
}

// EntityHasChanged records a change, and is the only writer.
//
// Positions at or below the horizon are dropped: the cache already answers
// "changed" for them, so recording them would only consume space to reach the
// same conclusion. This is also the guard that makes a bug like feeding
// position 0 for every row of a replication batch merely useless rather than
// actively wrong -- 0 is below every real horizon.
func (c *Cache) EntityHasChanged(entity string, pos int64) {
	if c == nil || c.max <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed {
		return
	}
	c.entityHasChangedLocked(entity, pos)
}

func (c *Cache) entityHasChangedLocked(entity string, pos int64) {
	if pos <= c.earliest {
		return
	}
	if old, ok := c.entityPos[entity]; ok {
		if old >= pos {
			return
		}
		if set := c.byPos[old]; set != nil {
			delete(set, entity)
			if len(set) == 0 {
				delete(c.byPos, old)
				c.removePosition(old)
			}
		}
	}
	set, ok := c.byPos[pos]
	if !ok {
		set = make(map[string]struct{})
		c.byPos[pos] = set
		c.insertPosition(pos)
	}
	set[entity] = struct{}{}
	c.entityPos[entity] = pos
	c.evictLocked()
}

// HasEntityChanged reports whether the entity may have changed after pos.
//
// True is always a safe answer; false is a claim. It is only returned when the
// question is above the horizon AND either the entity is unknown (so it
// provably did not change in a range the cache covers completely) or its last
// change is at or before pos.
func (c *Cache) HasEntityChanged(entity string, pos int64) bool {
	if c == nil || c.max <= 0 {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed || pos < c.earliest {
		c.misses++
		return true
	}
	last, ok := c.entityPos[entity]
	if !ok {
		c.hits++
		return false
	}
	if pos < last {
		c.misses++
		return true
	}
	c.hits++
	return false
}

// EntitiesChanged returns the subset of entities that changed after pos,
// preserving the caller's order.
//
// Below the horizon it returns everything it was given, which is exactly the
// caller's un-optimised behaviour.
//
// Synapse has two implementations of this and picks between them on a size
// heuristic; both compute the same answer, and only the direct one is ported.
// The other exists to avoid rebuilding a large Python set, which is not a cost
// that applies here.
func (c *Cache) EntitiesChanged(entities []string, pos int64) []string {
	if c == nil || c.max <= 0 {
		return entities
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed || pos < c.earliest || len(c.byPos) == 0 {
		c.misses++
		return entities
	}
	c.hits++
	changed := make([]string, 0, len(entities))
	for _, e := range entities {
		if last, ok := c.entityPos[e]; ok && last > pos {
			changed = append(changed, e)
		}
	}
	return changed
}

// HasAnyEntityChanged reports whether anything at all changed after pos.
//
// This is the gate for a query whose cost does not depend on which entity
// changed -- the presence query being the reason it exists.
func (c *Cache) HasAnyEntityChanged(pos int64) bool {
	if c == nil || c.max <= 0 {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed || pos < c.earliest {
		c.misses++
		return true
	}
	if len(c.positions) == 0 {
		c.misses++
		return false
	}
	c.hits++
	return pos < c.positions[len(c.positions)-1]
}

// MaxPosOfLastChange returns an upper bound on the position of the entity's
// last change, if the cache knows one.
//
// Sliding sync's room-list sort is the caller: it needs an ordering key per
// room, and this supplies it without the per-room "last event before token"
// query. The bound is an upper one because the cache records the position of a
// change, not of the newest event the *user* may see, so a caller that must not
// over-report has to re-check against its own token.
func (c *Cache) MaxPosOfLastChange(entity string) (int64, bool) {
	if c == nil || c.max <= 0 {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disarmed {
		return 0, false
	}
	pos, ok := c.entityPos[entity]
	return pos, ok
}

// EarliestKnownPosition returns the horizon.
//
// Exported because a cache whose horizon has climbed above the positions
// callers actually ask about is useless in a way that produces no error and no
// wrong answer -- only queries that never went away. Report it.
func (c *Cache) EarliestKnownPosition() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.earliest
}

// AllEntitiesChanged drops everything and moves the horizon to pos.
//
// For the case where we know something changed but not what: a purge, a room
// deletion, or a replication gap we cannot reconstruct.
func (c *Cache) AllEntitiesChanged(pos int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked(pos)
}

// Arm enables the cache with a fresh horizon.
//
// Called on replication connect, after seeding positions. Arming empties the
// cache first: whatever it held predates the outage and cannot be trusted.
func (c *Cache) Arm(currentPos int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked(currentPos)
	c.disarmed = false
}

// Disarm makes every question answer "changed" until Arm is called.
//
// Called on replication drop. Emptying on the way down rather than the way up
// means there is no window in which a stale entry could be served by a path
// that forgot to check.
func (c *Cache) Disarm() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disarmed = true
	c.resetLocked(c.earliest)
}

// Armed reports whether the cache is answering.
//
// A cache configured to hold nothing is not armed. It answers "changed" to
// everything, which is indistinguishable from disarmed to every caller, and
// reporting it as armed would put a flat, healthy-looking line on the dashboard
// for a cache that is doing nothing at all.
func (c *Cache) Armed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.disarmed && c.max > 0
}

// Name is the cache's label, for metrics.
func (c *Cache) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// Stats reports counters for the metrics collector.
func (c *Cache) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits: c.hits, Misses: c.misses, Evictions: c.evictions,
		Entities: len(c.entityPos), Positions: len(c.positions),
		Earliest: c.earliest, Armed: !c.disarmed && c.max > 0,
	}
}

func (c *Cache) resetLocked(pos int64) {
	c.byPos = make(map[int64]map[string]struct{})
	c.entityPos = make(map[string]int64)
	c.positions = c.positions[:0]
	if pos > c.earliest {
		c.earliest = pos
	}
}

// evictLocked drops the oldest positions until the size bound holds, raising
// the horizon to whatever was dropped.
//
// Raising the horizon is not incidental: it is what keeps "unknown entity means
// unchanged" true after an eviction. Dropping entries without moving it would
// turn every evicted entity into a silent false negative.
func (c *Cache) evictLocked() {
	for len(c.positions) > c.max {
		k := c.positions[0]
		c.positions = c.positions[1:]
		for entity := range c.byPos[k] {
			if c.entityPos[entity] == k {
				delete(c.entityPos, entity)
			}
		}
		delete(c.byPos, k)
		if k > c.earliest {
			c.earliest = k
		}
		c.evictions++
	}
}

// insertPosition keeps positions sorted. Replication delivers positions in
// increasing order, so the append case is the one that runs.
func (c *Cache) insertPosition(pos int64) {
	if n := len(c.positions); n == 0 || c.positions[n-1] < pos {
		c.positions = append(c.positions, pos)
		return
	}
	i := sort.Search(len(c.positions), func(i int) bool { return c.positions[i] >= pos })
	c.positions = append(c.positions, 0)
	copy(c.positions[i+1:], c.positions[i:])
	c.positions[i] = pos
}

func (c *Cache) removePosition(pos int64) {
	i := sort.Search(len(c.positions), func(i int) bool { return c.positions[i] >= pos })
	if i < len(c.positions) && c.positions[i] == pos {
		c.positions = append(c.positions[:i], c.positions[i+1:]...)
	}
}
