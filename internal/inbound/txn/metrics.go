package txn

import (
	"encoding/json"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// Routes a request can take.
const (
	routeNative = "native"
	routeProxy  = "proxy"
	// routeAbandoned is a request whose sender went away before it was
	// answered. Routine for federation traffic, and not a failure of ours.
	routeAbandoned = "abandoned"
)

// marshalError is Synapse's error body, cs_error as canonical JSON.
func marshalError(errcode, msg string) ([]byte, error) {
	b, err := json.Marshal(struct {
		ErrCode string `json:"errcode"`
		Error   string `json:"error"`
	}{errcode, msg})
	if err != nil {
		return nil, err
	}
	return pdu.Canonical(b)
}

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goinbound_requests_total",
		Help: "Requests by route (native, proxy, abandoned) and outcome or proxy reason.",
	}, []string{"route", "reason"})
	duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goinbound_request_duration_seconds",
		Help:    "Time to answer a request, by route.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"route"})
	upstreamErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goinbound_upstream_errors_total",
		Help: "Requests Synapse could not be reached for.",
	})
)
