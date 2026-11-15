package handlers

import (
	"net/http/httptest"
	"testing"
)

// What PaginationConfig.from_request and the servlet accept here, and what
// is Synapse's to answer (its 400s, and values Python reads its own way).
func TestParseMessagesRequest(t *testing.T) {
	for _, c := range []struct {
		query, back string
		limit       int
		backwards   bool
		raw, filter bool
	}{
		{"", "", 10, false, false, false},
		{"dir=b&limit=0", "", 0, true, false, false},
		{"dir=b&limit=5000", "", 1000, true, false, false},
		{"dir=b&limit=007", "", 7, true, false, false},
		{"dir=b&from=END", "", 10, true, false, false},
		{"dir=b&from=", "", 10, true, false, false},
		{"dir=b&from=t2835--26645036_290190456_30106_28218707_1573764_1909_339248_42144964_0_1757_2_7374_5_1811", "", 10, true, false, false},
		{"raw", "", 10, false, true, false},
		{"filter=%7B%7D", "", 10, false, false, false},
		{"filter=null", "", 10, false, false, false},
		{"filter=%7B%22event_format%22%3A%22federation%22%7D", "", 10, false, true, true},
		{"dir=x", "dir", 0, false, false, false},
		{"dir=", "dir", 0, false, false, false},
		{"limit=-1", "limit", 0, false, false, false},
		{"limit=+5", "limit", 0, false, false, false},
		{"limit=", "limit", 0, false, false, false},
		{"from=garbage", "from", 0, false, false, false},
		{"from=m5~1.6", "from", 0, false, false, false},
		{"from=s5_1_1_m3~2.4", "from", 0, false, false, false},
		{"to=s1_x", "to", 0, false, false, false},
		{"filter=notjson", "filter", 0, false, false, false},
		{"filter=", "filter", 0, false, false, false},
		{"filter=%5B1%5D", "filter", 0, false, false, false},
		{"dir=b;limit=2", "query", 0, false, false, false},
	} {
		r := httptest.NewRequest("GET", "/_matrix/client/v3/rooms/!r:x/messages?"+c.query, nil)
		req, back := parseMessagesRequest(r, Deps{})
		if back != c.back {
			t.Errorf("%q: back %q, want %q", c.query, back, c.back)
			continue
		}
		if back != "" {
			continue
		}
		if req.limit != c.limit || req.backwards != c.backwards || req.raw != c.raw || (req.filter != nil) != c.filter {
			t.Errorf("%q: got %+v", c.query, req)
		}
	}
}
