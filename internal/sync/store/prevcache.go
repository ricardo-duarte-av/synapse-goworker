package store

import (
	"container/list"
	"encoding/json"
	"sync"
	"time"
)

// prevCache stands in for the one property of Synapse's event cache
// (*getEvent*) that reaches a response: an event fetched with
// get_prev_content=True has prev_content and prev_sender written into the
// unsigned of the cached object (get_events_as_list: "This mutates the
// cached event"), so every later read of that event in the same process --
// a state block, which never asks for prev content -- carries them too,
// until the entry leaves the cache.
//
// So an event this process has served in a timeline (AttachPrevContent) is
// remembered with its previous content, and a state event found here is
// given it (AttachCachedPrevContent). Bounded as Synapse's cache is:
// event_cache_size entries, read-refreshed, dropped after cache_entry_ttl
// unread while expire_caches is on, and dropped when the event is redacted
// (the cache entry is invalidated) or its room purged. Also bounded in
// bytes, as every cache here is: a big room's power levels are large.
type prevCache struct {
	mu       sync.Mutex
	ll       *list.List
	items    map[string]*list.Element
	bytes    int64
	maxBytes int64
	maxCount int64
	ttl      time.Duration
	now      func() time.Time
}

type prevEntry struct {
	eventID string
	content json.RawMessage
	sender  string
	read    time.Time
}

func (e *prevEntry) size() int64 { return int64(len(e.eventID) + len(e.content) + len(e.sender) + 96) }

// defaultPrevCacheBytes bounds the cache when nothing else does.
const defaultPrevCacheBytes = 64 << 20

func newPrevCache() *prevCache {
	return &prevCache{ll: list.New(), items: map[string]*list.Element{}, maxBytes: defaultPrevCacheBytes,
		maxCount: 10240, ttl: 30 * time.Minute, now: time.Now}
}

// SetEventCache sizes the cache as homeserver.yaml sizes Synapse's:
// entries (event_cache_size) and expiry (0: none). maxBytes <= 0 keeps the
// default byte bound.
func (s *Store) SetEventCache(entries int64, expiry time.Duration, maxBytes int64) {
	c := s.prev()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxCount, c.ttl = entries, expiry
	if maxBytes > 0 {
		c.maxBytes = maxBytes
	}
	c.evictLocked()
}

func (s *Store) prev() *prevCache {
	s.prevOnce.Do(func() {
		if s.prevContent == nil {
			s.prevContent = newPrevCache()
		}
	})
	return s.prevContent
}

func (c *prevCache) get(eventID string) (*prevEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[eventID]
	if !ok {
		return nil, false
	}
	e := el.Value.(*prevEntry)
	now := c.now()
	if c.ttl > 0 && now.Sub(e.read) > c.ttl {
		c.removeLocked(el)
		return nil, false
	}
	e.read = now
	c.ll.MoveToFront(el)
	return e, true
}

func (c *prevCache) add(eventID string, content json.RawMessage, sender string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[eventID]; ok {
		c.removeLocked(el)
	}
	e := &prevEntry{eventID: eventID, content: content, sender: sender, read: c.now()}
	if c.maxCount <= 0 || e.size() > c.maxBytes {
		return
	}
	c.items[eventID] = c.ll.PushFront(e)
	c.bytes += e.size()
	c.evictLocked()
}

func (c *prevCache) remove(eventID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[eventID]; ok {
		c.removeLocked(el)
	}
}

func (c *prevCache) purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.items = map[string]*list.Element{}
	c.bytes = 0
}

func (c *prevCache) removeLocked(el *list.Element) {
	e := el.Value.(*prevEntry)
	c.ll.Remove(el)
	delete(c.items, e.eventID)
	c.bytes -= e.size()
}

func (c *prevCache) evictLocked() {
	for c.ll.Len() > 0 && (int64(c.ll.Len()) > c.maxCount || c.bytes > c.maxBytes) {
		c.removeLocked(c.ll.Back())
	}
}

// ForgetPrevContent drops an event from the cache: it was redacted, and
// Synapse invalidates its cached copy (_invalidate_caches_for_event).
func (s *Store) ForgetPrevContent(eventID string) { s.prev().remove(eventID) }
