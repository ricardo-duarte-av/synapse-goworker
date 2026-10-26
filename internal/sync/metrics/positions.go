package metrics

import "github.com/prometheus/client_golang/prometheus"

// RegisterStreamPositions publishes the replication position of each stream at
// scrape time.
//
// On its own this is mildly interesting -- it says the worker is keeping up.
// Paired with gosync_stream_cache_earliest_position it is the series that makes
// a useless stream cache visible: the difference between the two is how far
// back that cache can answer, and a cache too small for its stream has it
// shrink towards zero. At zero every question falls below the horizon, every
// gate answers "changed", and the queries the cache exists to skip come back
// with no error, no wrong answer, and no other symptom.
//
// A collector rather than a gauge updated on every row: the subscriber already
// holds these under the lock that protects them, and mirroring would give two
// sources of truth for one fact on the hottest path in the process.
func RegisterStreamPositions(positions func() map[string]int64) {
	prometheus.MustRegister(&positionCollector{positions: positions})
}

type positionCollector struct {
	positions func() map[string]int64
}

var streamPosition = prometheus.NewDesc(
	"gosync_replication_position",
	"Latest position seen on each replication stream.",
	[]string{"stream"}, nil)

func (c *positionCollector) Describe(ch chan<- *prometheus.Desc) { ch <- streamPosition }

func (c *positionCollector) Collect(ch chan<- prometheus.Metric) {
	if c.positions == nil {
		return
	}
	for stream, pos := range c.positions() {
		ch <- prometheus.MustNewConstMetric(streamPosition, prometheus.GaugeValue, float64(pos), stream)
	}
}
