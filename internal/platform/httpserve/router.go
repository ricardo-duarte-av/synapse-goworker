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
	// Ping serves the MSC4524 paths PingMatch accepts.
	Ping      http.Handler
	PingMatch func(escapedPath string) bool
	// Creator serves the client writes CreatorMatch accepts (send, state,
	// redact, membership, join, knock, profile), matched on the escaped
	// path and the method: a GET under the same prefix is a read, and goes
	// on to Client.
	Creator      http.Handler
	CreatorMatch func(method, escapedPath string) bool
	// Typing serves PUT .../rooms/{roomId}/typing/{userId}, which
	// TypingMatch accepts.
	Typing      http.Handler
	TypingMatch func(method, escapedPath string) bool
	// Receipts serves POST .../receipt/... and .../read_markers, which
	// ReceiptsMatch accepts.
	Receipts      http.Handler
	ReceiptsMatch func(method, escapedPath string) bool
	// Devices serves the device and key endpoints DevicesMatch accepts.
	Devices      http.Handler
	DevicesMatch func(method, escapedPath string) bool
	// PushRules serves .../pushrules/..., which PushRulesMatch accepts.
	PushRules      http.Handler
	PushRulesMatch func(method, escapedPath string) bool
	// ToDevice serves PUT /sendToDevice/..., which ToDeviceMatch accepts.
	ToDevice      http.Handler
	ToDeviceMatch func(method, escapedPath string) bool
	// Presence serves GET and PUT .../presence/{userId}/status, which
	// PresenceMatch accepts.
	Presence      http.Handler
	PresenceMatch func(method, escapedPath string) bool
	// AccountData serves account data and tags, which AccountDataMatch
	// accepts.
	AccountData      http.Handler
	AccountDataMatch func(method, escapedPath string) bool
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
	case rt.Ping != nil && rt.PingMatch != nil && rt.PingMatch(p):
		rt.Ping.ServeHTTP(w, r)
	case strings.HasPrefix(p, "/_matrix/federation/"), strings.HasPrefix(p, "/_matrix/key/"):
		rt.Fallback.ServeHTTP(w, r)
	case rt.Typing != nil && rt.TypingMatch != nil && rt.TypingMatch(r.Method, p):
		rt.Typing.ServeHTTP(w, r)
	case rt.Receipts != nil && rt.ReceiptsMatch != nil && rt.ReceiptsMatch(r.Method, p):
		rt.Receipts.ServeHTTP(w, r)
	case rt.Devices != nil && rt.DevicesMatch != nil && rt.DevicesMatch(r.Method, p):
		rt.Devices.ServeHTTP(w, r)
	case rt.PushRules != nil && rt.PushRulesMatch != nil && rt.PushRulesMatch(r.Method, p):
		rt.PushRules.ServeHTTP(w, r)
	case rt.ToDevice != nil && rt.ToDeviceMatch != nil && rt.ToDeviceMatch(r.Method, p):
		rt.ToDevice.ServeHTTP(w, r)
	case rt.Presence != nil && rt.PresenceMatch != nil && rt.PresenceMatch(r.Method, p):
		rt.Presence.ServeHTTP(w, r)
	case rt.AccountData != nil && rt.AccountDataMatch != nil && rt.AccountDataMatch(r.Method, p):
		rt.AccountData.ServeHTTP(w, r)
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
