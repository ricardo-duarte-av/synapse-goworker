package store

import (
	"context"
	"sync"
)

// A request cache memoises store results for the life of one sync pass.
//
// It exists because the same question gets asked several times while one
// response is assembled -- "what was the last event before `since` in this
// room?" is asked by the prev_batch calculation, by the lazy-loaded state
// lookup, and by the newly-joined probe, and each ask is a round trip to a
// database that is answering it identically.
//
// Scoped to a sync PASS, not to the HTTP request. A long poll holds one
// request context for up to five minutes across many passes, and a membership
// answer cached at the top of that is stale by the end of it. Each pass
// installs its own.
//
// Nothing here changes what a response contains. Two identical queries in one
// request could already disagree -- the pool runs at read committed and each
// query sees its own snapshot -- so collapsing them makes a response more
// internally consistent, never less.

type reqCacheKey struct{}

// maxRequestCacheEntries bounds a single pass's memo.
//
// The natural bound is the pass itself, which is short. This is only here so
// that a bug producing unbounded distinct keys costs memory it can be seen
// spending rather than all of it.
const maxRequestCacheEntries = 8192

type requestCache struct {
	mu sync.Mutex
	m  map[string]any
}

// WithRequestCache returns a context carrying a fresh memo.
//
// Called at the top of each sync pass. A context without one is not an error:
// memo falls through to the loader, so store methods work unchanged from
// tests, from /events, and from anywhere that has not opted in.
func WithRequestCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, reqCacheKey{}, &requestCache{m: map[string]any{}})
}

// memo returns the cached value for key, or calls load and caches it.
//
// Deliberately not singleflight: two concurrent callers both run the query,
// and the second overwrites the first with an identical value. Rooms are built
// ten at a time, so a collision is possible, but a shared in-flight promise
// would put one room's query on another room's error path and give a cancelled
// request the power to fail its neighbours. The duplicate this drops is the
// sequential one, which is the one there is most of.
//
// Errors are never cached. A failed query is worth retrying, and remembering
// the failure would turn one transient error into a request-wide one.
func memo[T any](ctx context.Context, key string, load func() (T, error)) (T, error) {
	c, _ := ctx.Value(reqCacheKey{}).(*requestCache)
	if c == nil {
		return load()
	}

	c.mu.Lock()
	cached, ok := c.m[key]
	c.mu.Unlock()
	if ok {
		return cached.(T), nil
	}

	out, err := load()
	if err != nil {
		return out, err
	}

	c.mu.Lock()
	if len(c.m) < maxRequestCacheEntries {
		c.m[key] = out
	}
	c.mu.Unlock()
	return out, nil
}
