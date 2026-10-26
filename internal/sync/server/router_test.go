package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Synapse answers an unimplemented endpoint with M_UNRECOGNIZED, not a bare
// 404. Clients probe for optional endpoints and need to tell "this server does
// not implement it" from "this resource does not exist".
func TestUnknownPathReturnsUnrecognized(t *testing.T) {
	mux := NewMux(Routes{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/nonexistent", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if body["errcode"] != "M_UNRECOGNIZED" {
		t.Errorf("errcode = %v, want M_UNRECOGNIZED", body["errcode"])
	}
}

func TestHealth(t *testing.T) {
	mux := NewMux(Routes{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q", body["status"])
	}
}

// Synapse registers the legacy endpoints under api/v1 as well as the modern
// version segments. A client pinned to api/v1 is exactly the kind of caller
// these endpoints still exist for.
func TestRoomInitialSyncRegisteredOnEveryLegacyVersion(t *testing.T) {
	var seen int
	routes := Routes{RoomInitialSync: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen++
		if got := r.PathValue("roomId"); got != "!abc:example.com" {
			t.Errorf("roomId = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	})}
	mux := NewMux(routes)

	for _, v := range legacyVersions {
		path := "/_matrix/client/" + v + "/rooms/!abc:example.com/initialSync"
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", path, rec.Code)
		}
	}
	if seen != len(legacyVersions) {
		t.Errorf("handler ran %d times, want %d", seen, len(legacyVersions))
	}
}

// Nothing is registered unless a handler is supplied, so an endpoint that is
// not implemented yet reports M_UNRECOGNIZED rather than 500ing on a nil
// handler.
func TestUnimplementedEndpointIsNotRegistered(t *testing.T) {
	mux := NewMux(Routes{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/_matrix/client/r0/rooms/!a:b/initialSync", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestPreflightIsAnsweredEverywhere checks the CORS wrapper against what
// Synapse actually returns, which was captured from the live server:
//
//	HTTP/2 204
//	access-control-allow-origin: *
//	access-control-allow-methods: GET, HEAD, POST, PUT, DELETE, OPTIONS
//	access-control-allow-headers: X-Requested-With, Content-Type, Authorization, Date
//	access-control-expose-headers: Synapse-Trace-Id, Server
//
// The unknown-path case is the one that bit a real client: Element sends a
// preflight before every /sync, and our router answered 404 because only GET
// was registered. The browser then refused to send the request at all.
func TestPreflightIsAnsweredEverywhere(t *testing.T) {
	handler := WithCORS(NewMux(Routes{}))

	for _, path := range []string{
		"/_matrix/client/v3/sync",
		"/_matrix/client/r0/events",
		"/_matrix/client/v3/something_we_do_not_serve",
		"/",
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodOptions, path, nil))

		if rec.Code != http.StatusNoContent {
			t.Errorf("OPTIONS %s = %d, want 204", path, rec.Code)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("OPTIONS %s returned a body of %d bytes", path, rec.Body.Len())
		}
		for header, want := range map[string]string{
			"Access-Control-Allow-Origin":   "*",
			"Access-Control-Allow-Methods":  "GET, HEAD, POST, PUT, DELETE, OPTIONS",
			"Access-Control-Allow-Headers":  "X-Requested-With, Content-Type, Authorization, Date",
			"Access-Control-Expose-Headers": "Synapse-Trace-Id, Server",
		} {
			if got := rec.Header().Get(header); got != want {
				t.Errorf("OPTIONS %s: %s = %q, want %q", path, header, got, want)
			}
		}
	}
}

// TestCORSHeadersOnOrdinaryResponses checks the half that is easy to forget:
// a preflight that passes is useless if the response to the real request has
// no Access-Control-Allow-Origin, because the browser discards it.
func TestCORSHeadersOnOrdinaryResponses(t *testing.T) {
	handler := WithCORS(NewMux(Routes{}))

	// An unrouted path, so this exercises the 404 path as well: an error a
	// browser cannot read is an error a client cannot report.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync", nil))

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin on a GET = %q, want *", got)
	}
}
