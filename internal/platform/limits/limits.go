// Package limits bounds how much work the worker takes on at once.
//
// Goroutines are cheap and the worker spawns them freely; what each one
// holds is not. A /sync long-poll holds a few KB, an initial sync of a large
// account holds hundreds of MB, a streamed /state holds a batch of a room
// with 150k state events. Each kind of work therefore has its own Gate, sized
// for what one unit costs, and work past it waits or is refused rather than
// queuing without bound.
package limits

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Config is the `limits:` block. Zero takes the default; the defaults are
// well above what production has needed, so they bind only in an overload.
type Config struct {
	// SyncRequests bounds concurrent client sync requests, long-polls
	// included. Past it, 503 with Retry-After.
	SyncRequests int `yaml:"sync_requests"`
	// InitialSyncs bounds concurrent syncs without a since token, the
	// expensive kind (209s and 1.5GB for a 654-room account). Past it,
	// requests wait for a slot.
	InitialSyncs int `yaml:"initial_syncs"`
	// FederationReads bounds concurrent /event, /state_ids and
	// /get_missing_events answers. Past it, a short wait, then 429.
	FederationReads int `yaml:"federation_reads"`
	// StateStreams bounds concurrent streamed /state answers, up to 97MB
	// each. Past it, requests wait for a slot.
	StateStreams int `yaml:"state_streams"`
	// InboundTransactions bounds /send transactions being handled at once
	// (each origin is one at a time already). Past it, requests wait.
	InboundTransactions int `yaml:"inbound_transactions"`
	// SenderDestinations bounds the per-destination transmission loops.
	SenderDestinations int `yaml:"sender_destinations"`
	// SenderCatchUp bounds destinations being caught up at once.
	SenderCatchUp int `yaml:"sender_catchup"`
}

// WithDefaults fills unset fields.
func (c Config) WithDefaults() Config {
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	def(&c.SyncRequests, 20000)
	def(&c.InitialSyncs, 4)
	def(&c.FederationReads, 256)
	def(&c.StateStreams, 4)
	def(&c.InboundTransactions, 64)
	def(&c.SenderDestinations, 2000)
	def(&c.SenderCatchUp, 32)
	return c
}

// Validate refuses negative bounds.
func (c Config) Validate() error {
	for name, v := range map[string]int{
		"sync_requests": c.SyncRequests, "initial_syncs": c.InitialSyncs,
		"federation_reads": c.FederationReads, "state_streams": c.StateStreams,
		"inbound_transactions": c.InboundTransactions,
		"sender_destinations":  c.SenderDestinations, "sender_catchup": c.SenderCatchUp,
	} {
		if v < 0 {
			return fmt.Errorf("config: limits.%s must not be negative", name)
		}
	}
	return nil
}

var (
	inUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_limit_in_use",
		Help: "Units of work currently holding a slot, by limit.",
	}, []string{"limit"})
	capacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_limit_capacity",
		Help: "Slots available, by limit.",
	}, []string{"limit"})
	waited = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goworker_limit_wait_seconds",
		Help:    "Time waited for a slot, by limit. Zero waits are observed too, so the quantiles mean something.",
		Buckets: []float64{.001, .01, .1, .5, 1, 5, 15, 60},
	}, []string{"limit"})
	refused = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_limit_refused_total",
		Help: "Work refused because its limit was full, by limit.",
	}, []string{"limit"})
)

// Gate is a counting semaphore with metrics.
type Gate struct {
	name  string
	slots chan struct{}
}

// NewGate builds a gate of n slots.
func NewGate(name string, n int) *Gate {
	capacity.WithLabelValues(name).Set(float64(n))
	inUse.WithLabelValues(name).Set(0)
	return &Gate{name: name, slots: make(chan struct{}, n)}
}

// Acquire waits for a slot until ctx ends. It reports false, having
// counted a refusal, when ctx ends first.
func (g *Gate) Acquire(ctx context.Context) bool {
	start := time.Now()
	select {
	case g.slots <- struct{}{}:
		waited.WithLabelValues(g.name).Observe(time.Since(start).Seconds())
		inUse.WithLabelValues(g.name).Inc()
		return true
	case <-ctx.Done():
		refused.WithLabelValues(g.name).Inc()
		return false
	}
}

// TryAcquire takes a slot only if one is free now.
func (g *Gate) TryAcquire() bool {
	select {
	case g.slots <- struct{}{}:
		waited.WithLabelValues(g.name).Observe(0)
		inUse.WithLabelValues(g.name).Inc()
		return true
	default:
		refused.WithLabelValues(g.name).Inc()
		return false
	}
}

// Release returns a slot.
func (g *Gate) Release() {
	<-g.slots
	inUse.WithLabelValues(g.name).Dec()
}

// Name is the gate's metric label.
func (g *Gate) Name() string { return g.name }

// Busy answers a request refused by a full gate, in Synapse's error shape.
// status is 503 for clients and 429 for federation, which backs off on it.
func Busy(w http.ResponseWriter, status int, retryAfter time.Duration) {
	ms := retryAfter.Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.FormatInt(int64(retryAfter.Seconds()+0.999), 10))
	w.WriteHeader(status)
	code := "M_UNKNOWN"
	if status == http.StatusTooManyRequests {
		code = "M_LIMIT_EXCEEDED"
	}
	_, _ = fmt.Fprintf(w, `{"errcode":%q,"error":"Server is busy","retry_after_ms":%d}`, code, ms)
}
