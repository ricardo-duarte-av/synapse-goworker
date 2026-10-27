package matrixstate

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// authChainFallbacks counts auth chains computed by walking event_auth because
// the chain cover index did not describe the events.
//
// The fallback is correct but much slower. There is deliberately no room_id
// label: this server has over a thousand rooms, and a per-room label would make
// the series count grow without bound. The room is logged instead.
var authChainFallbacks = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "gopro",
	Name:      "auth_chain_index_fallback_total",
	Help:      "Auth chains computed by recursive walk because the chain cover index was incomplete.",
})

// stateIDsFlights counts /state_ids requests by how their answer was obtained.
//
// "computed" ran the state and auth-chain resolution; "joined" waited on an
// identical computation already in flight; "abandoned" stopped waiting because
// its own request ended first. A burst shows as joined climbing while computed
// stays near the number of distinct events asked about.
var stateIDsFlights = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gopro_state_ids_flights_total",
	Help: "/state_ids requests by whether they computed the answer, joined an identical computation in flight, or stopped waiting.",
}, []string{"role"})
