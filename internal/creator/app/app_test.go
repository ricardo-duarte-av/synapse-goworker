package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/creator"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
)

// whoamiOnly answers every request as /whoami would: a verdict the creator
// cannot build a requester from, so every write falls back.
type whoamiOnly struct{}

func (whoamiOnly) Authenticate(context.Context, *http.Request) (clientauth.Verdict, error) {
	return clientauth.Verdict{Valid: true, UserID: "@u:example.org", Source: clientauth.SourceWhoami}, nil
}

// A write the creator leaves to main reaches it unchanged: method, escaped
// URI and body, after the creator has looked at it.
func TestFallbackForwardsUnchanged(t *testing.T) {
	type seen struct{ method, uri, body string }
	got := make(chan seen, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.URL.RequestURI(), string(b)}
		_, _ = w.Write([]byte(`{"event_id":"$x"}`))
	}))
	defer upstream.Close()
	up, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	a := &App{creator: creator.New(creator.Options{Auth: whoamiOnly{}, Log: zerolog.Nop()}), up: up, log: zerolog.Nop()}

	for _, tc := range []struct{ method, uri string }{
		{"PUT", "/_matrix/client/v3/rooms/%21a%2Fb%3Aexample.org/send/m.room.message/t1"},
		{"PUT", "/_matrix/client/v3/rooms/%21r%3Aexample.org/state/m.room.topic/"},
		{"POST", "/_matrix/client/v3/join/%23alias%3Aexample.org"},
		{"PUT", "/_matrix/client/v3/profile/%40u%3Aexample.org/displayname"},
	} {
		uri := tc.uri
		if !Match(tc.method, uri) {
			t.Fatalf("%s %s is not matched", tc.method, uri)
		}
		const body = `{"msgtype":"m.text","body":"hi"}`
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(tc.method, uri, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer tok")
		a.Handler().ServeHTTP(rec, r)
		s := <-got
		if s.method != tc.method || s.uri != uri || s.body != body {
			t.Errorf("upstream saw %+v for %s", s, uri)
		}
		if rec.Code != 200 || rec.Body.String() != `{"event_id":"$x"}` {
			t.Errorf("%s: response %d %q", uri, rec.Code, rec.Body.String())
		}
	}
}

// Reads under the creator's prefixes are not its: they go on to sync's
// routes or to main.
func TestMatchIsWritesOnly(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		{"PUT", "/_matrix/client/v3/rooms/!r:x/send/m.room.message/t", true},
		{"POST", "/_matrix/client/v3/rooms/!r:x/send/m.room.message", true},
		{"PUT", "/_matrix/client/v3/rooms/!r:x/state/m.room.topic/", true},
		{"GET", "/_matrix/client/v3/rooms/!r:x/state/m.room.topic/", false},
		{"GET", "/_matrix/client/v3/rooms/!r:x/state", false},
		{"POST", "/_matrix/client/v3/rooms/!r:x/kick", true},
		{"GET", "/_matrix/client/v3/profile/@u:x/displayname", false},
		{"PUT", "/_matrix/client/v3/profile/@u:x/displayname", true},
		{"GET", "/_matrix/client/v3/rooms/!r:x/messages", false},
		{"GET", "/_matrix/client/v3/sync", false},
	} {
		if got := Match(tc.method, tc.path); got != tc.want {
			t.Errorf("%s %s: %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}
