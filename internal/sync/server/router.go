package server

import "net/http"

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
	// RoomState serves /rooms/{roomId}/state and
	// /rooms/{roomId}/state/{eventType}[/{stateKey}], GET only: a PUT is
	// Synapse's.
	RoomState http.Handler
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

// Register adds the sync routes to mux, each wrapped by wrap (the request
// log and CORS).
//
// Unmatched paths are not answered here: the worker's own catch-all sends
// them to Synapse. A preflight is registered alongside every route, so a
// browser's OPTIONS before /sync is answered by the CORS wrapper rather than
// travelling to main.
func Register(mux *http.ServeMux, routes Routes, wrap func(http.Handler) http.Handler) {
	handle := func(method, path string, h http.Handler) {
		w := wrap(h)
		mux.Handle(method+" "+path, w)
		mux.Handle("OPTIONS "+path, w)
	}

	if routes.RoomInitialSync != nil {
		for _, v := range legacyVersions {
			handle("GET", "/_matrix/client/"+v+"/rooms/{roomId}/initialSync", routes.RoomInitialSync)
		}
	}
	if routes.InitialSync != nil {
		for _, v := range legacyVersions {
			handle("GET", "/_matrix/client/"+v+"/initialSync", routes.InitialSync)
		}
	}
	if routes.Events != nil {
		for _, v := range legacyVersions {
			handle("GET", "/_matrix/client/"+v+"/events", routes.Events)
		}
	}
	if routes.Sync != nil {
		for _, v := range modernVersions {
			handle("GET", "/_matrix/client/"+v+"/sync", routes.Sync)
		}
	}
	if routes.RoomState != nil {
		for _, v := range legacyVersions {
			// The handler matches the raw path as Synapse does; a path it
			// does not match it hands back.
			handle("GET", "/_matrix/client/"+v+"/rooms/{roomId}/state", routes.RoomState)
			handle("GET", "/_matrix/client/"+v+"/rooms/{roomId}/state/{rest...}", routes.RoomState)
		}
	}
	if routes.SlidingSync != nil {
		for _, path := range slidingSyncPaths {
			// POST, not GET: the request carries a body describing which rooms
			// and which state the client wants.
			handle("POST", path, routes.SlidingSync)
		}
	}
}

// ModernVersions exposes the non-legacy client API version segments, for
// handlers registered outside Register.
func ModernVersions() []string { return append([]string(nil), modernVersions...) }

// SlidingSyncPaths exposes the sliding sync paths, for deployment
// documentation and for tests that assert both are served.
func SlidingSyncPaths() []string { return append([]string(nil), slidingSyncPaths...) }
