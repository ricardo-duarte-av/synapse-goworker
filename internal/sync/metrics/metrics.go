// Package metrics defines the worker's Prometheus instrumentation.
//
// Labels are deliberately low-cardinality. Per-user or per-room labels would
// be the obvious thing to want on a sync worker and are exactly what must not
// be done: this homeserver has thousands of rooms and hundreds of users, and
// per-request detail belongs in the request log, which is already structured.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsTotal counts served requests.
	//
	// `outcome` separates a request we answered from one we refused and one
	// the client abandoned. A client that hangs up mid-long-poll is entirely
	// normal for /sync and must not be counted as an error, which is a
	// distinction the status code alone cannot carry.
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_requests_total",
		Help: "Client API requests served, by endpoint, status and outcome.",
	}, []string{"endpoint", "status", "outcome"})

	// RequestDuration measures the whole request.
	//
	// The buckets run to 300s because a long-poll legitimately occupies the
	// upper end: a /sync with timeout=30000 that sees no traffic takes the full
	// 30 seconds and is a success. Sizing these like a normal HTTP histogram
	// would put every healthy idle sync in +Inf.
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gosync_request_duration_seconds",
		Help:    "Client API request duration, including long-poll wait.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"endpoint"})

	// AuthVerdictsTotal counts token validations by outcome.
	//
	// `unavailable` is the one to watch: it means Synapse could not be asked,
	// and the correct response is 502, never 401. Refusing a valid token
	// because whoami was unreachable would log real clients out.
	AuthVerdictsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_auth_verdicts_total",
		Help: "Access token validations, by verdict.",
	}, []string{"verdict"})

	// AuthCacheEntries reports the size of the token cache.
	AuthCacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gosync_auth_cache_entries",
		Help: "Cached access token verdicts.",
	})

	// DBQueries counts queries issued to Synapse's database, by store method.
	//
	// The label is the Go method name, not the SQL: the SQL is long, and two
	// methods that happen to share a query shape are still two different
	// reasons to have gone to the database.
	//
	// This exists because the cost that matters on this worker is round trips,
	// not database seconds, and round trips are invisible from the database
	// side -- pg_stat_statements sees a query shape shared with the other
	// workers on this server and cannot attribute it to us. Roughly sixty
	// label values, which is what "low cardinality" was reserved for.
	DBQueries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_db_queries_total",
		Help: "Queries issued to Synapse's database, by store method.",
	}, []string{"query"})

	// StateCacheEntries reports how full the two state caches are.
	//
	// Worth a panel: a cache pinned at its ceiling is evicting, and on this
	// worker eviction means the 17GB state_groups_state table gets walked
	// again. Sized against the configured maximum, not read alone.
	StateCacheEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gosync_state_cache_entries",
		Help: "Entries held in the immutable state caches.",
	}, []string{"cache"})

	// CacheInvalidationRows counts rows on Synapse's `caches` stream by what
	// we did with them.
	//
	// Watch `purge`: it should be near zero. This stream is busy with rows
	// that concern other workers' caches, and an earlier version treated all
	// of them as destructive, which quietly emptied the state caches every two
	// seconds. A rising `purge` means that has come back.
	CacheInvalidationRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_cache_invalidation_rows_total",
		Help: "Rows seen on Synapse's caches replication stream, by action taken.",
	}, []string{"action"})

	// ReplicationRows counts rows received, by stream and by whether the row
	// named anyone.
	//
	// `scope="global"` is the one to read. A row that names no room and no
	// user wakes EVERY parked client, so a busy stream sitting in that bucket
	// is every long poll on the worker recomputing a sync that will be empty.
	// Four streams were in exactly that state -- sticky_events among them,
	// which is served in /sync -- and nothing here could see it. Some global
	// rows are correct and expected; a large and growing count on one stream
	// is the signal.
	ReplicationRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_replication_rows_total",
		Help: "Replication rows received, by stream and whether the row named a room or user.",
	}, []string{"stream", "scope"})

	// NotifierWakeups counts parked syncs actually woken, by stream.
	//
	// Divided by ReplicationRows for the same stream, this is how many clients
	// each row costs. Near the parked-client count means that stream is waking
	// everybody.
	NotifierWakeups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_notifier_wakeups_total",
		Help: "Parked syncs woken, by the stream that woke them.",
	}, []string{"stream"})

	// DatabaseReadOnly is 1 when the connected role cannot write.
	//
	// A gauge rather than a startup check alone, so that a role changed
	// underneath a running worker is visible.
	DatabaseReadOnly = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gosync_database_read_only",
		Help: "1 if the database role has default_transaction_read_only set.",
	})

	// ReplicationConnected is 1 while the replication subscription is healthy.
	//
	// The single most important gauge here: while it is 0 the worker cannot
	// report typing, and its stream positions are only as fresh as the last
	// database seed.
	ReplicationConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gosync_replication_connected",
		Help: "1 while the replication subscription is healthy.",
	})

	// StreamDiscontinuities counts the two ways a stream position stops being
	// a continuation of the one before it.
	//
	// Both are invisible in every other metric here, and that is the reason
	// this one exists: when the typing writer restarted under a running worker
	// on 2026-09-09, rows kept arriving, wakeups kept firing, the connection
	// stayed healthy, and typing was silently unreportable for a day. A
	// discontinuity is the only evidence such a thing happened.
	//
	//   reason="reset" -- the position went BACKWARDS on a stream whose serial
	//     lives in a writer's memory (see resettableStreams), which is that
	//     writer restarting. Only those streams are counted: on a
	//     database-backed one a lower position is Synapse announcing
	//     max(writer position, persisted-up-to) and is ordinary traffic.
	//   reason="gap" -- a POSITION says the stream moved further than the rows
	//     we saw. We were disconnected, or a row could not be parsed. Whatever
	//     claims completeness below that position has to give up its horizon.
	//
	// Labelled by stream so a restart of one writer is not read as a restart
	// of Synapse.
	StreamDiscontinuities = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_replication_stream_discontinuities_total",
		Help: "Replication positions that were not a continuation of the last, by stream and kind.",
	}, []string{"stream", "reason"})

	// SyncWaiters reports how many long polls are currently parked.
	//
	// On a worker serving real clients this is the workload: an idle /sync is
	// a goroutine and a database-free wait, and the count of them is what
	// says whether clients are connected at all.
	SyncWaiters = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gosync_sync_waiters",
		Help: "Long-polling requests currently waiting.",
	})

	// ToDeviceDeleted counts to-device messages this worker has deleted.
	//
	// The only write it makes, so it is the only metric that can go wrong
	// destructively. A rate far above the rate at which messages arrive for
	// the device would mean something is acknowledging on a client's behalf.
	ToDeviceDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gosync_to_device_deleted_total",
		Help: "Acknowledged to-device messages deleted.",
	})

	// BuildInfo carries the version as a label on a constant 1.
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gosync_build_info",
		Help: "Build information.",
	}, []string{"version"})
)

// Presence relays. This worker's only outbound call to another Synapse worker,
// and the only thing it tells the homeserver rather than asks it.
var (
	// PresenceRelays counts state updates delivered to the presence writer.
	PresenceRelays = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gosync_presence_relays_total",
		Help: "Presence state updates delivered to Synapse's presence writer.",
	})
	// PresenceRelaysSuppressed counts the ones the throttle skipped.
	//
	// Expected to dwarf the delivered count: a client syncs in a loop and the
	// writer's timers are far coarser. If it does NOT dwarf it, the throttle
	// has stopped working and every sync is making an HTTP call.
	PresenceRelaysSuppressed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gosync_presence_relays_suppressed_total",
		Help: "Sync-driven presence updates skipped because the state was unchanged.",
	})
	// PresenceRelayFailures counts calls the writer refused or that never
	// arrived. Sustained non-zero means users this worker serves are drifting
	// offline while looking served.
	//
	// The reason separates the two failures that need different fixes and are
	// otherwise indistinguishable from a flat count:
	//
	//	unreachable  the socket or host did not accept a connection. The
	//	             presence writer moved, or homeserver.yaml points at a
	//	             path this container cannot see.
	//	refused      it answered, with something other than 200. Almost always
	//	             a rotated worker_replication_secret.
	//	timeout      it accepted and did not answer in time. The writer is
	//	             overloaded, not misconfigured.
	//	client_gone  the syncing client hung up mid-relay. Not our failure and
	//	             not the writer's; here so it cannot inflate the others.
	PresenceRelayFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_presence_relay_failures_total",
		Help: "Presence relays that failed, by reason.",
	}, []string{"reason"})

	// PresenceConfigReloads counts re-reads of Synapse's homeserver.yaml
	// prompted by a relay that could not be delivered.
	//
	// The writer moves between workers and the replication secret rotates, and
	// neither event tells this worker anything: the symptom is presence
	// quietly not working until somebody restarts it. A relay failure is the
	// signal to go and look again.
	//
	//	changed    the config had moved on and this worker adopted it. Expect
	//	           these in ones, next to a burst of relay failures that stops.
	//	unchanged  the config still says what it said. The writer is down, or
	//	           the failure was never about configuration.
	//	error      homeserver.yaml could not be read or parsed. The mount is
	//	           gone, or Synapse is mid-write.
	PresenceConfigReloads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_presence_config_reloads_total",
		Help: "Re-reads of Synapse's config after a failed presence relay, by outcome.",
	}, []string{"outcome"})

	// PresenceRelayDuration times one call to the presence writer.
	//
	// This is the only synchronous outbound call on the sync path, so a writer
	// that gets slow makes THIS worker slow -- once per device per relay
	// interval, which is rare but not never. The client's own timeout bounds
	// it; this is how you see it coming before that bound is reached.
	PresenceRelayDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "gosync_presence_relay_duration_seconds",
		Help: "Time for one presence relay to Synapse's presence writer.",
		// A unix socket on the same host: the interesting range is sub-
		// millisecond to a few hundred, with the tail out to the 5s timeout.
		Buckets: []float64{
			.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5,
		},
	})
)

// Presence relay failure reasons.
const (
	PresenceUnreachable = "unreachable"
	PresenceRefused     = "refused"
	PresenceTimeout     = "timeout"
	PresenceClientGone  = "client_gone"
)

// Outcomes of a re-read of Synapse's config.
const (
	PresenceReloadChanged   = "changed"
	PresenceReloadUnchanged = "unchanged"
	PresenceReloadError     = "error"
)

// RegisterPresence exposes the relay throttle's size.
//
// It is a gauge rather than a counter because it is a live population: one
// entry per (user, device) whose presence we have relayed recently. Compare it
// against the number of distinct devices actually syncing -- if it climbs
// without bound, entries are never being dropped and the map is a leak.
func RegisterPresence(tracked func() int) {
	// Create the failure series at zero.
	//
	// A CounterVec exports nothing for a label it has never seen, so without
	// this the failures panel reads "No data" until the first failure -- which
	// is indistinguishable from a broken scrape, on the panel whose whole job
	// is to be zero. An alert on a series that does not exist does not fire.
	for _, reason := range []string{
		PresenceUnreachable, PresenceRefused, PresenceTimeout, PresenceClientGone,
	} {
		PresenceRelayFailures.WithLabelValues(reason)
	}
	for _, outcome := range []string{
		PresenceReloadChanged, PresenceReloadUnchanged, PresenceReloadError,
	} {
		PresenceConfigReloads.WithLabelValues(outcome)
	}

	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gosync_presence_devices_tracked",
		Help: "Devices held in the presence relay throttle.",
	}, func() float64 { return float64(tracked()) }))
}
