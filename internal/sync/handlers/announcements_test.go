package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnnouncementsToMain(t *testing.T) {
	on := false
	d := Deps{Fallback: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(299) })}
	h := AnnouncementsToMain(d, func() bool { return on },
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	for _, c := range []struct {
		on   bool
		want int
	}{{false, 200}, {true, 299}, {false, 200}} {
		on = c.on
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/_matrix/client/v3/sync", nil))
		if rec.Code != c.want {
			t.Errorf("configured=%v: %d, want %d", c.on, rec.Code, c.want)
		}
	}
}
