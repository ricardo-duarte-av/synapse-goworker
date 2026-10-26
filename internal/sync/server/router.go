package server

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
)

// clientAPIVersions are the path segments Synapse accepts for the client API.
//
// Synapse's own patterns are regexes with alternations
// (`(api/v1|r0|v3|unstable)`), and which alternatives are allowed differs per
// endpoint: `client_patterns(..., v1=True)` adds `api/v1` and is used only by
// the legacy endpoints. Go's ServeMux has no alternation, so each version is
// registered separately and the set is stated per endpoint rather than assumed
// to be uniform.
var (
	// modernVersions is client_patterns() without v1: /sync and friends.
	modernVersions = []string{"r0", "v3", "unstable", "v2_alpha"}
	// legacyVersions is client_patterns(v1=True): /events, /initialSync,
	// /rooms/{id}/initialSync.
	legacyVersions = []string{"api/v1", "r0", "v3", "unstable"}
)

// Routes is the set of handlers the worker serves.
type Routes struct {
	// RoomInitialSync serves /rooms/{roomId}/initialSync.
	RoomInitialSync http.Handler
	// InitialSync serves /initialSync.
	InitialSync http.Handler
	// Events serves /events.
	Events http.Handler
	// Sync serves /sync.
	Sync http.Handler
	// SlidingSync serves MSC4186. Nil leaves the endpoint unregistered, which
	// makes it 404 as M_UNRECOGNIZED -- exactly what a client probing for it
	// should see when it is not configured.
	SlidingSync http.Handler
}

// slidingSyncPaths are the two paths MSC4186 defines.
//
// Both are served. The unstable one is what every client uses today --
// Element X sends 27,465 requests a day to it here -- and the stable one is
// what the MSC settles on, so a client that starts using it works the day it
// does without a change on this side.
//
// Note the asymmetry with the endpoints above: this is not a version-segment
// alternation. `/unstable/org.matrix.simplified_msc3575/sync` and `/v4/sync`
// are two different paths for one endpoint, not one path under two version
// prefixes, so listing them literally is the honest way to write it.
var slidingSyncPaths = []string{
	"/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
	"/_matrix/client/v4/sync",
}

// NewMux assembles the router.
//
// Every unmatched path under /_matrix returns M_UNRECOGNIZED rather than a bare
// 404, matching Synapse: a client probing for an endpoint needs to tell "this
// server does not implement it" from "this resource does not exist".
func NewMux(routes Routes) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	if routes.RoomInitialSync != nil {
		for _, v := range legacyVersions {
			mux.Handle("GET /_matrix/client/"+v+"/rooms/{roomId}/initialSync",
				routes.RoomInitialSync)
		}
	}

	if routes.InitialSync != nil {
		for _, v := range legacyVersions {
			mux.Handle("GET /_matrix/client/"+v+"/initialSync", routes.InitialSync)
		}
	}

	if routes.Events != nil {
		for _, v := range legacyVersions {
			mux.Handle("GET /_matrix/client/"+v+"/events", routes.Events)
		}
	}

	if routes.Sync != nil {
		for _, v := range modernVersions {
			mux.Handle("GET /_matrix/client/"+v+"/sync", routes.Sync)
		}
	}

	if routes.SlidingSync != nil {
		for _, path := range slidingSyncPaths {
			// POST, not GET: the request carries a body describing which rooms
			// and which state the client wants.
			mux.Handle("POST "+path, routes.SlidingSync)
		}
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		matrixerr.WriteCode(w, http.StatusNotFound, matrixerr.CodeUnrecognized,
			"Unrecognized request")
	})
	return mux
}

// ModernVersions exposes the non-legacy client API version segments, for
// handlers registered outside NewMux.
func ModernVersions() []string { return append([]string(nil), modernVersions...) }

// SlidingSyncPaths exposes the sliding sync paths, for deployment
// documentation and for tests that assert both are served.
func SlidingSyncPaths() []string { return append([]string(nil), slidingSyncPaths...) }
