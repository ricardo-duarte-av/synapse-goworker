package clientauth

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Whole-process metrics: authentication is shared by every client-API part.
var (
	nativeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_client_auth_native",
		Help: "1 while access tokens are checked here; 0 while every one goes to Synapse's /whoami.",
	})
	verdicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_client_auth_verdicts_total",
		Help: "Requests authenticated, by verdict (accepted, rejected, unknown) and where the answer came from (native, whoami).",
	}, []string{"verdict", "source"})
	queries = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goworker_client_auth_query_seconds",
		Help:    "Database round trips made to authenticate, by query (token, user, device, mark_used).",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1},
	}, []string{"query"})
	invalidations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_client_auth_invalidations_total",
		Help: "Cached lookups dropped on Synapse's word, by cache function; purge is every cache, on a reconnect.",
	}, []string{"cache_func"})
	userIPRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_client_auth_user_ip_total",
		Help: "USER_IP commands, by outcome (sent, failed, dropped).",
	}, []string{"outcome"})
)
