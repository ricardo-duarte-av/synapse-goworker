package httpserve

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func named(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(name)) })
}

func testRouter() *Router {
	client := http.NewServeMux()
	client.Handle("GET /_matrix/client/v3/sync", named("sync"))
	client.Handle("GET /_matrix/client/v3/rooms/{roomId}/state/{rest...}", named("state"))
	return &Router{
		Creator: named("creator"),
		CreatorMatch: func(method, p string) bool {
			return method == "PUT" && strings.Contains(p, "/state/")
		},
		Send:  named("send"),
		Reads: named("reads"),
		ReadsMatch: func(p string) bool {
			return strings.HasPrefix(p, "/_matrix/federation/v1/event/")
		},
		Client:   client,
		Fallback: named("main"),
	}
}

func TestRouting(t *testing.T) {
	rt := testRouter()
	for _, tc := range []struct{ method, target, want string }{
		{"PUT", "/_matrix/federation/v1/send/t1", "send"},
		{"GET", "/_matrix/federation/v1/event/%24abc", "reads"},
		// A path the mux would clean and redirect still reaches us untouched.
		{"GET", "/_matrix/federation/v1/event/%24a%2F%2Fb", "reads"},
		{"PUT", "/_matrix/federation/v2/send_join/!r:x/$e", "main"},
		{"GET", "/_matrix/federation/v1/version", "main"},
		{"GET", "/_matrix/client/v3/sync", "sync"},
		{"POST", "/_matrix/client/v3/sync", "main"},
		{"GET", "/_matrix/client/v3/rooms/!r:x/messages", "main"},
		{"GET", "/_matrix/media/v3/config", "main"},
		// Writes are the creator's, on the escaped path; reads under the
		// same prefix go on to the client routes.
		{"PUT", "/_matrix/client/v3/rooms/!r:x/state/m.room.topic/", "creator"},
		{"PUT", "/_matrix/client/v3/rooms/%21a%2F%2Fb:x/state/t/", "creator"},
		{"GET", "/_matrix/client/v3/rooms/!r:x/state/m.room.topic/", "state"},
		{"GET", "/health", "OK"},
	} {
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))
		if got := rec.Body.String(); got != tc.want {
			t.Errorf("%s %s -> %q, want %q", tc.method, tc.target, got, tc.want)
		}
	}
}
