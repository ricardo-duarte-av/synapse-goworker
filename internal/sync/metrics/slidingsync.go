package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// SlidingSyncResponses counts responses by what happened, and it is the
	// first thing to look at when sliding sync misbehaves.
	//
	// The outcomes are `immediate` (there was news without waiting), `woken`
	// (parked, then woken by it), `empty` (nothing to say, either because the
	// client asked for no timeout or because the timeout was still running),
	// `timed_out` (parked to the deadline with nothing), `unknown_pos`, and
	// `client_gone` -- the caller hung up mid-request, which for a long poll is
	// ordinary rather than a fault and is why abandoned requests must not be
	// counted as 5xx.
	//
	// **A worker answering `immediate` on nearly every request has a broken
	// emptiness rule**, not a busy server. That is not hypothetical: on
	// 2026-09-03 this endpoint treated any present extension as news, so the
	// long poll never waited and SchildiChat was answered about ten times a
	// second per connection. With this metric the shape is obvious in one
	// glance -- `immediate` at the request rate and `timed_out` at zero -- and
	// without it, it took reading an nginx access log.
	SlidingSyncResponses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_sliding_sync_responses_total",
		Help: "Sliding sync responses, by outcome.",
	}, []string{"outcome"})

	// SlidingSyncRooms is how many rooms each response described.
	//
	// Reading it beside the response rate says whether connection tracking is
	// working: after the first response for a connection, a quiet poll should
	// describe ZERO rooms. A steady stream of full windows means every
	// response is re-sending everything, which is the difference between a
	// sliding sync and a very expensive /sync.
	SlidingSyncRooms = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gosync_sliding_sync_rooms",
		Help:    "Rooms described per sliding sync response.",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100, 250, 500},
	})

	// SlidingSyncResponseBytes is the encoded response size.
	//
	// A client controls its own window and required_state, so it controls this
	// almost entirely. Worth watching because the expensive failure is not a
	// slow query but a client that asks for a thousand rooms with `["*","*"]`.
	SlidingSyncResponseBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gosync_sliding_sync_response_bytes",
		Help:    "Encoded sliding sync response size.",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 8),
	})

	// SlidingSyncRowsWritten counts rows written to the connection store.
	//
	// This endpoint is the only one that writes, and the write is proportional
	// to the connection's room count rather than to what changed: each new
	// position copies the previous one's rows forward. Measured on the live
	// database before any of this ran: ~725 stream rows plus ~248 room-config
	// rows per position for a 654-room connection, and Element X holds three
	// connections per device.
	//
	// So the number to watch is rows per response, not rows per second. A
	// quiet poll should write NOTHING at all.
	SlidingSyncRowsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_sliding_sync_rows_written_total",
		Help: "Rows written to the sliding sync connection store, by table.",
	}, []string{"table"})

	// SlidingSyncPositionsMinted counts new connection positions.
	//
	// One per response that changed something. Compared against
	// gosync_sliding_sync_responses_total this is the cheapest check that the
	// "reuse the position when nothing changed" short-circuit still works: if
	// they track each other, it does not.
	SlidingSyncPositionsMinted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gosync_sliding_sync_positions_minted_total",
		Help: "New sliding sync connection positions written.",
	})

	// SlidingSyncConnectionsReaped counts connections collected for being
	// unused.
	//
	// Should be small and steady. Zero for ever means the reaper is not
	// running, and nothing else prunes a connection whose client stopped
	// coming back -- so the six tables would grow without bound.
	SlidingSyncConnectionsReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gosync_sliding_sync_connections_reaped_total",
		Help: "Sliding sync connections deleted for being unused.",
	})
)

// RegisterSlidingSyncStore publishes the connection store's size at scrape
// time.
//
// A collector because these are `SELECT count(*)`: asking on every request
// would put six queries on the hot path to answer a question only a dashboard
// asks. The tables are small by design -- reading a position prunes the others
// -- so a row count that climbs is the reaper failing or the prune not
// happening, and both are invisible any other way.
func RegisterSlidingSyncStore(counts func() (SlidingStoreCounts, error)) {
	prometheus.MustRegister(&slidingStoreCollector{counts: counts})
}

// SlidingStoreCounts is the connection store's size.
//
// Declared here rather than taking internal/slidingstore's own type, because
// that package counts rows by incrementing the counters above -- so depending
// on it from here would be a cycle. The caller adapts, which is one line in
// main.go.
type SlidingStoreCounts struct {
	Connections int64
	Positions   int64
	// Rows maps a table name to its row count.
	Rows map[string]int64
}

type slidingStoreCollector struct {
	counts func() (SlidingStoreCounts, error)
}

var (
	slidingConnections = prometheus.NewDesc(
		"gosync_sliding_sync_connections",
		"Sliding sync connections held.", nil, nil)
	slidingPositions = prometheus.NewDesc(
		"gosync_sliding_sync_positions",
		"Sliding sync connection positions held. Normally close to the connection count: "+
			"reading a position deletes the others on its connection.", nil, nil)
	slidingRows = prometheus.NewDesc(
		"gosync_sliding_sync_store_rows",
		"Rows held in the sliding sync connection store, by table.", []string{"table"}, nil)
	slidingStoreUp = prometheus.NewDesc(
		"gosync_sliding_sync_store_up",
		"1 when the connection store answered the last scrape.", nil, nil)
)

func (c *slidingStoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- slidingConnections
	ch <- slidingPositions
	ch <- slidingRows
	ch <- slidingStoreUp
}

func (c *slidingStoreCollector) Collect(ch chan<- prometheus.Metric) {
	if c.counts == nil {
		return
	}
	counts, err := c.counts()
	if err != nil {
		// Reported rather than swallowed: a scrape that cannot reach the
		// connection store is a scrape whose other numbers are stale, and an
		// absent series looks the same as a zero one.
		ch <- prometheus.MustNewConstMetric(slidingStoreUp, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(slidingStoreUp, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(slidingConnections, prometheus.GaugeValue,
		float64(counts.Connections))
	ch <- prometheus.MustNewConstMetric(slidingPositions, prometheus.GaugeValue,
		float64(counts.Positions))
	for table, n := range counts.Rows {
		ch <- prometheus.MustNewConstMetric(slidingRows, prometheus.GaugeValue, float64(n), table)
	}
}
