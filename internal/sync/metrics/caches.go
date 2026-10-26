package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/lru"
)

// RegisterCaches publishes the derived caches at scrape time.
//
// A collector rather than counters incremented at the call sites: the counters
// already live inside each cache, where they are updated under the same lock
// as the map they describe. Mirroring them into Prometheus counters would give
// two sources of truth for one fact, and the interesting failure -- a cache
// that is never hit -- is exactly when the mirror is least likely to be
// noticed drifting.
//
// Safe to call once at startup; the function is invoked on every scrape.
func RegisterCaches(stats func() map[string]lru.Stats) {
	prometheus.MustRegister(&cacheCollector{stats: stats})
}

type cacheCollector struct {
	stats func() map[string]lru.Stats
}

var (
	cacheHits = prometheus.NewDesc(
		"gosync_cache_hits_total", "Derived cache hits, by cache.", []string{"cache"}, nil)
	cacheMisses = prometheus.NewDesc(
		"gosync_cache_misses_total", "Derived cache misses, by cache.", []string{"cache"}, nil)
	cacheEvictions = prometheus.NewDesc(
		"gosync_cache_evictions_total", "Derived cache evictions, by cache.", []string{"cache"}, nil)
	cacheEntries = prometheus.NewDesc(
		"gosync_cache_entries", "Entries held per derived cache.", []string{"cache"}, nil)
	// A DISARMED cache reports no hits and no entries, which on a hit-rate
	// panel is indistinguishable from a cache nothing asks for. This is the
	// series that tells them apart, and the one to alert on: a disarmed cache
	// means replication is down and invalidations are not arriving.
	cacheArmed = prometheus.NewDesc(
		"gosync_cache_armed", "1 when a derived cache is serving, 0 when disarmed.", []string{"cache"}, nil)
)

func (c *cacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cacheHits
	ch <- cacheMisses
	ch <- cacheEvictions
	ch <- cacheEntries
	ch <- cacheArmed
}

func (c *cacheCollector) Collect(ch chan<- prometheus.Metric) {
	if c.stats == nil {
		return
	}
	for name, st := range c.stats() {
		ch <- prometheus.MustNewConstMetric(cacheHits, prometheus.CounterValue, float64(st.Hits), name)
		ch <- prometheus.MustNewConstMetric(cacheMisses, prometheus.CounterValue, float64(st.Misses), name)
		ch <- prometheus.MustNewConstMetric(cacheEvictions, prometheus.CounterValue, float64(st.Evictions), name)
		ch <- prometheus.MustNewConstMetric(cacheEntries, prometheus.GaugeValue, float64(st.Entries), name)
		armed := 0.0
		if st.Armed {
			armed = 1
		}
		ch <- prometheus.MustNewConstMetric(cacheArmed, prometheus.GaugeValue, armed, name)
	}
}
