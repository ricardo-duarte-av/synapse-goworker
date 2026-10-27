package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/limits"
)

func ok() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func TestRequestGateRefusesPastTheBound(t *testing.T) {
	g := limits.NewGate("test_sync_requests", 1)
	h := requestGate(g, ok())
	if !g.TryAcquire() {
		t.Fatal("could not fill the gate")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync?since=s1", nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Errorf("full gate: %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	g.Release()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync?since=s1", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("free gate: %d", rec.Code)
	}
}

// Only a sync without a since token is the expensive kind; an incremental
// sync never waits on the initial-sync gate.
func TestInitialSyncGateOnlyHoldsInitialSyncs(t *testing.T) {
	g := limits.NewGate("test_initial_syncs", 1)
	h := initialSyncGate(g, ok())
	if !g.TryAcquire() {
		t.Fatal("could not fill the gate")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync?since=s1", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("incremental sync held by the initial gate: %d", rec.Code)
	}

	ran := false
	waiting := initialSyncGate(g, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { ran = true }))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	waiting.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync", nil).WithContext(ctx))
	if ran {
		t.Error("an initial sync ran while the gate was full")
	}
	g.Release()
}
