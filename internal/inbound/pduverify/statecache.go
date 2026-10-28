package pduverify

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/statecache"
)

// DefaultStateCacheMB bounds a Verifier's state cache when none is shared
// with it, as in tests and offline tools.
const DefaultStateCacheMB = 128

// groupCache is the Verifier's view of the worker's state cache: state
// groups' full state, shared with the federation reads.
//
// Bounded by bytes, which is to say by state entries, never by group count:
// one big room's state is 150k entries, 31MB as a Go map. The first PDU
// checker cached up to 2,000 groups and copied the state per event, and
// reached 1.7GB RSS. A group whose prev is cached is rebuilt from the prev and
// its delta rather than read whole.
type groupCache struct {
	states *statecache.Cache
}

// SetStateCache shares the worker's state cache with this Verifier.
func (s *Verifier) SetStateCache(c *statecache.Cache) { s.groups = &groupCache{states: c} }

// loader reads groups for the cache.
type loader struct{ st *pdustore.Store }

func (l loader) Delta(ctx context.Context, g int64) (int64, bool, statecache.Map, error) {
	return l.st.StateDelta(ctx, g)
}

func (l loader) Full(ctx context.Context, g int64) (statecache.Map, error) {
	return l.st.StateForGroup(ctx, g)
}

// get returns a group's full state. The map is shared: never modify it.
func (c *groupCache) get(ctx context.Context, st *pdustore.Store, g int64) (pdu.StateMap, error) {
	m, how, err := c.states.Load(ctx, g, loader{st})
	if err != nil {
		return nil, err
	}
	stateCacheLoads.WithLabelValues(how).Inc()
	return m, nil
}

var stateCacheLoads = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goinbound_pdu_state_cache_loads_total",
	Help: "State groups write mode needed, by how they were got: hit (cached), delta (rebuilt from a cached prev group) or full (read from the database).",
}, []string{"how"})
