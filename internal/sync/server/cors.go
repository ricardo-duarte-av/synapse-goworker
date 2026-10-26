package server

import "net/http"

// SetCORS writes the headers that let a browser-based client talk to this
// worker, byte for byte as Synapse's set_cors_headers does.
//
// They are not optional and nginx does not add them: on this deployment the
// CORS headers a client sees on /_matrix come from Synapse itself, so a worker
// that serves those paths and omits them is one a web client cannot use at all.
// The browser will make the request, receive the answer, and then refuse to
// hand it to the page.
//
// Synapse-Trace-Id is exposed even though we never emit it. The value matches
// upstream exactly, and exposing a header that is never sent costs nothing;
// diverging here would be a difference a client could actually observe.
//
// The one case not mirrored is Synapse's special-casing of the MSC4108
// rendezvous endpoints, which this worker does not serve.
func SetCORS(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, Authorization, Date")
	h.Set("Access-Control-Expose-Headers", "Synapse-Trace-Id, Server")
}

// WithCORS answers preflight requests and adds the CORS headers to everything
// else.
//
// OPTIONS is answered for ANY path, including ones this worker does not serve,
// and that is deliberate: Synapse's OptionsResource selects itself for every
// OPTIONS request before routing happens, so an unknown path gets 204 rather
// than 404. A browser sending a preflight for a path we do not implement must
// still be told which methods are allowed -- answering 404 leaves it unable to
// send the real request even to an endpoint that would have worked.
func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetCORS(w.Header())
		if r.Method == http.MethodOptions {
			// Named, so that `unknown` keeps meaning "a path this worker does
			// not route". Preflights are a real share of traffic -- 3,623 of
			// 60,000 log lines on the live host, all for /sync -- and lumping
			// them in with genuinely unrouted requests both invents a phantom
			// endpoint on the dashboard and gives anything actually unrouted
			// somewhere to hide.
			if ann := Annotate(r.Context()); ann != nil {
				ann.Endpoint = "preflight"
			}
			// 204 with no body, as Synapse answers. Go elides Content-Length
			// on a 204 itself, so it is not set here.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
