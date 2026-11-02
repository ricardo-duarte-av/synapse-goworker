package fedclient

import (
	"math/rand"
	"sync"
	"time"

	"maunium.net/go/mautrix/federation"
)

// Server resolution cache periods, from Synapse's
// http/federation/well_known_resolver.py:45.
//
// The split between them is the substance. A server that delegates through
// .well-known is telling us where it lives and how long that answer is good
// for, and re-asking every few minutes is pure waste -- 24 hours is the spec's
// own default. A server we could NOT get a delegation from is a different
// claim: it means "as far as we can tell right now, this server is where its
// own name points", and that is exactly the answer that goes stale badly.
const (
	// resolveDelegatedTTL is how long a .well-known answer is trusted when the
	// response carries no cache headers of its own.
	resolveDelegatedTTL = 24 * time.Hour
	// resolveNoDelegationTTL is how long to trust a resolution reached WITHOUT
	// a delegation. Shorter than Synapse's WELL_KNOWN_INVALID_CACHE_PERIOD
	// (an hour), on purpose (operator, 2026-11-02): a .well-known that timed
	// out looks exactly like one that does not exist, and Synapse only tells
	// them apart by having seen a delegation since it started. After a
	// restart that memory is gone, so one blip at start pinned
	// continuwuity.org to an unreachable :8448 (2026-11-02). Re-asking costs
	// one GET per destination we are talking to, every five minutes.
	resolveNoDelegationTTL = 5 * time.Minute
	// resolveDownTTL is the same, for a server that has delegated recently and
	// did not this time -- so its .well-known is probably down rather than
	// gone. Synapse's WELL_KNOWN_DOWN_CACHE_PERIOD, and short on purpose: this
	// is the case where the cached answer is most likely to be WRONG.
	resolveDownTTL = 2 * time.Minute
	// resolveRememberDelegation is how long we remember that a server has
	// delegated before, which is what tells the two cases above apart.
	resolveRememberDelegation = 2 * time.Hour

	// resolveMinTTL and resolveMaxTTL clamp a delegation's own cache headers.
	resolveMinTTL = 5 * time.Minute
	resolveMaxTTL = 48 * time.Hour

	// resolveJitter spreads expiries so a thousand destinations resolved in one
	// burst do not all expire in the same second.
	resolveJitter = 0.1

	// resolveSweepEvery bounds how often expired entries are dropped. Without
	// a sweep the map only ever grows: mautrix's own cache never removes an
	// expired entry, it just declines to return it.
	resolveSweepEvery = 5 * time.Minute
)

// Resolutions is the process's one cache of server name resolutions, which
// every federation client shares: a resolution found wrong by one part is
// forgotten for all of them.
var Resolutions = NewResolveCache()

// ResolveCache is a federation.ResolutionCache applying Synapse's TTL policy,
// shortened for resolutions reached without a delegation.
//
// mautrix's own cache keeps every resolution for the full 24 hours regardless
// of how it was reached, and that is the case worth fixing. When a delegating
// server's .well-known is briefly unreachable, resolution falls through to SRV
// and then to port 8448 on the bare name -- a perfectly successful resolution,
// to the wrong place -- and caching THAT for a day means a day of failed
// deliveries to a server that was only briefly misconfigured. Synapse re-checks
// such a server in two minutes.
//
// A resolution we then cannot connect through is forgotten (Forget, by
// ForgetOnFailure), whatever its TTL: the next request resolves afresh.
//
// Failures are deliberately not cached here. A resolution failure surfaces as a
// send failure, which puts the destination into the per-destination backoff in
// internal/retry -- so it is already not retried for at least
// destination_min_retry_interval. A second timer would be a second backoff, and
// two implementations of a backoff disagree under exactly the conditions that
// make a backoff matter.
type ResolveCache struct {
	mu      sync.Mutex
	entries map[string]federation.ResolvedServerName
	// delegatedUntil remembers that a server HAS delegated, which is how a
	// missing .well-known is told apart from a broken one.
	delegatedUntil map[string]time.Time
	lastSweep      time.Time

	// now and jitter are injectable so the policy can be tested without
	// sleeping through a day.
	now    func() time.Time
	jitter func() float64
}

// NewResolveCache is an empty cache.
func NewResolveCache() *ResolveCache {
	return &ResolveCache{
		entries:        map[string]federation.ResolvedServerName{},
		delegatedUntil: map[string]time.Time{},
		now:            time.Now,
		jitter: func() float64 {
			return 1 - resolveJitter + rand.Float64()*2*resolveJitter
		},
	}
}

// StoreResolution records a resolution with a TTL that depends on how it was
// reached.
//
// Whether a delegation was used is inferred from HostHeader, which mautrix
// leaves equal to the server name unless .well-known returned a different one
// (resolution.go:57,81). A .well-known that returns the server's own name is
// therefore treated as no delegation, which costs one extra fetch an hour for
// such a server and is never wrong about where to send.
func (c *ResolveCache) StoreResolution(res *federation.ResolvedServerName) {
	if res == nil || res.ServerName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	ttl := res.Expires.Sub(now)

	if res.HostHeader != "" && res.HostHeader != res.ServerName {
		// A delegation. Trust its own cache period, clamped.
		c.delegatedUntil[res.ServerName] = now.Add(resolveRememberDelegation)
		if ttl <= 0 {
			ttl = resolveDelegatedTTL
		}
		ttl = min(max(ttl, resolveMinTTL), resolveMaxTTL)
	} else {
		limit := resolveNoDelegationTTL
		if until, ok := c.delegatedUntil[res.ServerName]; ok && until.After(now) {
			// It usually delegates and did not this time, so this resolution is
			// a fallback and is the one most likely to be wrong.
			limit = resolveDownTTL
		}
		ttl = min(ttl, limit)
		if ttl <= 0 {
			ttl = limit
		}
	}

	stored := *res
	stored.Expires = now.Add(time.Duration(float64(ttl) * c.jitter()))
	c.entries[res.ServerName] = stored

	c.sweepLocked(now)
}

// LoadResolution returns a cached resolution, or nil when there is none that is
// still valid. It never returns an error: an error here fails the request
// outright (httpclient.go:84) rather than falling back to resolving.
func (c *ResolveCache) LoadResolution(serverName string) (*federation.ResolvedServerName, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	res, ok := c.entries[serverName]
	if !ok {
		return nil, nil
	}
	if !res.Expires.After(c.now()) {
		delete(c.entries, serverName)
		return nil, nil
	}
	out := res
	return &out, nil
}

// Forget drops serverName's resolution, so the next request resolves it
// again. The delegation it may have had is still remembered.
func (c *ResolveCache) Forget(serverName string) {
	c.mu.Lock()
	delete(c.entries, serverName)
	c.mu.Unlock()
}

// sweepLocked drops expired entries, at most once every resolveSweepEvery.
func (c *ResolveCache) sweepLocked(now time.Time) {
	if now.Sub(c.lastSweep) < resolveSweepEvery {
		return
	}
	c.lastSweep = now
	for name, res := range c.entries {
		if !res.Expires.After(now) {
			delete(c.entries, name)
		}
	}
	for name, until := range c.delegatedUntil {
		if !until.After(now) {
			delete(c.delegatedUntil, name)
		}
	}
}
