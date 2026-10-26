package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamcache"
)

// RegisterStreamCaches publishes the stream-change caches at scrape time.
//
// Same shape as RegisterCaches and for the same reason -- the counters live
// inside the cache, under the lock that protects what they describe -- but with
// one series the derived caches do not need and this one cannot do without.
func RegisterStreamCaches(stats func() map[string]streamcache.Stats) {
	prometheus.MustRegister(&streamCacheCollector{stats: stats})
}

type streamCacheCollector struct {
	stats func() map[string]streamcache.Stats
}

var (
	streamCacheHits = prometheus.NewDesc(
		"gosync_stream_cache_hits_total",
		"Stream-change cache questions answered from memory, by stream.",
		[]string{"stream"}, nil)
	streamCacheMisses = prometheus.NewDesc(
		"gosync_stream_cache_misses_total",
		"Stream-change cache questions that fell below the horizon or found a change, by stream.",
		[]string{"stream"}, nil)
	streamCacheEvictions = prometheus.NewDesc(
		"gosync_stream_cache_evictions_total",
		"Stream positions dropped to stay within the size bound, by stream.",
		[]string{"stream"}, nil)
	streamCacheEntities = prometheus.NewDesc(
		"gosync_stream_cache_entities",
		"Distinct entities tracked, by stream.", []string{"stream"}, nil)
	streamCachePositions = prometheus.NewDesc(
		"gosync_stream_cache_positions",
		"Distinct stream positions held, by stream. This is what the size bound limits.",
		[]string{"stream"}, nil)
	// The series that makes a useless cache visible.
	//
	// A cache too small for its stream has this climb until it passes the
	// `since` positions clients actually send. Every question then falls below
	// it, every gate answers "changed", and the queries come back -- with no
	// error, no wrong answer, and no symptom except a graph nobody was
	// watching. Compare it against the stream's current position: a gap that
	// keeps shrinking means the cache is too small.
	streamCacheEarliest = prometheus.NewDesc(
		"gosync_stream_cache_earliest_position",
		"Oldest stream position the cache can answer for. Questions at or below it always say 'changed'.",
		[]string{"stream"}, nil)
	streamCacheArmed = prometheus.NewDesc(
		"gosync_stream_cache_armed",
		"1 when a stream cache is gating, 0 when disarmed and every gate says 'changed'.",
		[]string{"stream"}, nil)
)

func (c *streamCacheCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		streamCacheHits, streamCacheMisses, streamCacheEvictions,
		streamCacheEntities, streamCachePositions, streamCacheEarliest, streamCacheArmed,
	} {
		ch <- d
	}
}

func (c *streamCacheCollector) Collect(ch chan<- prometheus.Metric) {
	if c.stats == nil {
		return
	}
	for name, st := range c.stats() {
		armed := 0.0
		if st.Armed {
			armed = 1
		}
		ch <- prometheus.MustNewConstMetric(streamCacheHits, prometheus.CounterValue, float64(st.Hits), name)
		ch <- prometheus.MustNewConstMetric(streamCacheMisses, prometheus.CounterValue, float64(st.Misses), name)
		ch <- prometheus.MustNewConstMetric(streamCacheEvictions, prometheus.CounterValue, float64(st.Evictions), name)
		ch <- prometheus.MustNewConstMetric(streamCacheEntities, prometheus.GaugeValue, float64(st.Entities), name)
		ch <- prometheus.MustNewConstMetric(streamCachePositions, prometheus.GaugeValue, float64(st.Positions), name)
		ch <- prometheus.MustNewConstMetric(streamCacheEarliest, prometheus.GaugeValue, float64(st.Earliest), name)
		ch <- prometheus.MustNewConstMetric(streamCacheArmed, prometheus.GaugeValue, armed, name)
	}
}
