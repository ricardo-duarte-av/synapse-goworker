package httpserve

import (
	"net/http"
	"strings"
)

// Router sends each request to whichever part of the worker serves it, and
// everything else to Synapse.
//
// Federation paths are dispatched on the raw, escaped path before any
// ServeMux sees them. A ServeMux cleans paths -- a room ID carrying %2F%2F
// would be redirected -- and the X-Matrix signature covers the URI exactly
// as the remote server sent it.
type Router struct {
	// Send serves /_matrix/federation/v1/send/. It proxies what it does not
	// handle itself.
	Send http.Handler
	// Reads serves the federation paths ReadsMatch accepts.
	Reads      http.Handler
	ReadsMatch func(escapedPath string) bool
	// Creator serves the client writes CreatorMatch accepts (send, state,
	// redact, membership, join, knock, profile), matched on the escaped
	// path and the method: a GET under the same prefix is a read, and goes
	// on to Client.
	Creator      http.Handler
	CreatorMatch func(method, escapedPath string) bool
	// Client holds the client API routes. A request it has no pattern for is
	// not answered by it.
	Client *http.ServeMux
	// Fallback is Synapse's main process.
	Fallback http.Handler
}

const sendPrefix = "/_matrix/federation/v1/send/"

func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.EscapedPath()
	switch {
	case p == "/health":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	case strings.HasPrefix(p, sendPrefix) && rt.Send != nil:
		rt.Send.ServeHTTP(w, r)
	case rt.Reads != nil && rt.ReadsMatch != nil && rt.ReadsMatch(p):
		rt.Reads.ServeHTTP(w, r)
	case strings.HasPrefix(p, "/_matrix/federation/"), strings.HasPrefix(p, "/_matrix/key/"):
		rt.Fallback.ServeHTTP(w, r)
	case rt.Creator != nil && rt.CreatorMatch != nil && rt.CreatorMatch(r.Method, p):
		rt.Creator.ServeHTTP(w, r)
	default:
		if rt.Client != nil {
			if h, pattern := rt.Client.Handler(r); pattern != "" {
				h.ServeHTTP(w, r)
				return
			}
		}
		rt.Fallback.ServeHTTP(w, r)
	}
}
