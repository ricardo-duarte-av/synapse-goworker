package limits

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGateBounds(t *testing.T) {
	g := NewGate("test_bounds", 2)
	if !g.TryAcquire() || !g.TryAcquire() {
		t.Fatal("could not take two slots of two")
	}
	if g.TryAcquire() {
		t.Fatal("took a third slot of two")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if g.Acquire(ctx) {
		t.Fatal("Acquire succeeded on a full gate")
	}
	g.Release()
	if !g.Acquire(context.Background()) {
		t.Fatal("a released slot was not reusable")
	}
}

func TestDefaultsAndValidate(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.InitialSyncs == 0 || c.SenderDestinations != 2000 {
		t.Errorf("defaults not applied: %+v", c)
	}
	if (Config{StateStreams: -1}).Validate() == nil {
		t.Error("a negative limit was accepted")
	}
}

func TestBusyShape(t *testing.T) {
	rec := httptest.NewRecorder()
	Busy(rec, http.StatusTooManyRequests, 1500*time.Millisecond)
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "2" {
		t.Errorf("status %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if want := `{"errcode":"M_LIMIT_EXCEEDED","error":"Server is busy","retry_after_ms":1500}`; rec.Body.String() != want {
		t.Errorf("body %s", rec.Body)
	}
}
