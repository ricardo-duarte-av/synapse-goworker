package replserver

import (
	"container/list"
	"context"
	"encoding/json"
	"sync"
	"time"
)

// responseCache is ResponseCache as _base.py uses it: one answer per
// (endpoint, txn_id), shared by every request that arrives while it is
// being computed, kept for CacheTimeout after it completes, and forgotten at
// once if it failed (util/caches/response_cache.py _set). Synapse keys each
// endpoint's cache by txn_id alone (_base.py:418: "we probably also want to
// use the other PATH_ARGS"); one cache per endpoint is the same thing.
//
// Unlike Synapse's it is bounded by size: past maxBytes the oldest completed
// answers go first.
type responseCache struct {
	maxBytes int64
	now      func() time.Time

	mu      sync.Mutex
	entries map[cacheKey]*cacheEntry
	// done holds completed entries, oldest first.
	done  *list.List
	bytes int64
}

type cacheKey struct{ endpoint, txnID string }

type cacheEntry struct {
	key   cacheKey
	ready chan struct{}
	// Set before ready closes.
	status  int
	body    map[string]json.RawMessage
	err     error
	size    int64
	expires time.Time
	elem    *list.Element
}

func newResponseCache(maxBytes int64, now func() time.Time) *responseCache {
	return &responseCache{maxBytes: maxBytes, now: now, entries: map[cacheKey]*cacheEntry{}, done: list.New()}
}

// wrap returns the answer for (endpoint, txnID), calling compute only when
// there is none, finished or running. outcome is miss, hit or joined. A
// request whose caller hangs up while it waits on another's computation
// stops waiting; the computation goes on for the others.
func (c *responseCache) wrap(ctx context.Context, endpoint, txnID string,
	compute func() (int, map[string]json.RawMessage, error),
) (status int, body map[string]json.RawMessage, outcome string, err error) {
	key := cacheKey{endpoint, txnID}
	c.mu.Lock()
	c.expireLocked()
	if e, ok := c.entries[key]; ok {
		c.mu.Unlock()
		select {
		case <-e.ready:
			if e.err != nil {
				// It failed after we found it and before it was removed:
				// the failure is the answer, as it is for every request
				// that was waiting on it.
				return 0, nil, "joined", e.err
			}
			return e.status, e.body, "hit", nil
		default:
		}
		select {
		case <-e.ready:
			return e.status, e.body, "joined", e.err
		case <-ctx.Done():
			return 0, nil, "joined", ctx.Err()
		}
	}
	e := &cacheEntry{key: key, ready: make(chan struct{})}
	c.entries[key] = e
	c.mu.Unlock()

	e.status, e.body, e.err = compute()

	c.mu.Lock()
	if e.err != nil {
		delete(c.entries, key)
	} else {
		e.size = entrySize(key, e.body)
		e.expires = c.now().Add(CacheTimeout)
		e.elem = c.done.PushBack(e)
		c.bytes += e.size
		c.evictLocked()
	}
	cacheBytes.Set(float64(c.bytes))
	c.mu.Unlock()
	close(e.ready)
	return e.status, e.body, "miss", e.err
}

func (c *responseCache) expireLocked() {
	now := c.now()
	for front := c.done.Front(); front != nil; front = c.done.Front() {
		e := front.Value.(*cacheEntry)
		if e.expires.After(now) {
			break
		}
		c.removeLocked(e)
	}
	cacheBytes.Set(float64(c.bytes))
}

func (c *responseCache) evictLocked() {
	for c.bytes > c.maxBytes && c.done.Len() > 0 {
		c.removeLocked(c.done.Front().Value.(*cacheEntry))
	}
}

func (c *responseCache) removeLocked(e *cacheEntry) {
	c.done.Remove(e.elem)
	c.bytes -= e.size
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
}

// entrySize is the answer's bytes and its keys, and a fixed allowance for
// the entry and its map.
func entrySize(key cacheKey, body map[string]json.RawMessage) int64 {
	n := int64(len(key.endpoint)+len(key.txnID)) + 256
	for k, v := range body {
		n += int64(len(k)+len(v)) + 48
	}
	return n
}
