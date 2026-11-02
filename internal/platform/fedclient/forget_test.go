package fedclient

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"maunium.net/go/mautrix/federation"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func cached(c *ResolveCache, name string) bool {
	r, _ := c.LoadResolution(name)
	return r != nil
}

// 2026-11-02: one .well-known that timed out at start pinned
// continuwuity.org to its unreachable :8448. A connection that fails through
// a cached resolution now drops it, so the next attempt resolves afresh.
func TestForgetOnFailure(t *testing.T) {
	c := NewResolveCache()
	store := func() {
		c.StoreResolution(&federation.ResolvedServerName{ServerName: "example.org", HostHeader: "example.org",
			IPPort: []string{"example.org:8448"}, Expires: time.Now().Add(time.Hour)})
	}
	var fail error
	var answer *http.Response
	tr := &ForgetOnFailure{Cache: c, Next: roundTrip(func(r *http.Request) (*http.Response, error) {
		// As the resolving transport does: rewrite the URL in place.
		r.URL.Scheme, r.URL.Host = "https", "example.org:8448"
		return answer, fail
	})}
	send := func(ctx context.Context) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "matrix-federation://example.org/_matrix/federation/v1/version", nil)
		_, _ = tr.RoundTrip(req)
	}

	store()
	answer, fail = &http.Response{StatusCode: 500, Body: http.NoBody}, nil
	send(context.Background())
	if !cached(c, "example.org") {
		t.Fatal("an answer, even an error status, forgot the resolution")
	}

	answer, fail = nil, errors.New("dial tcp 188.114.96.0:8448: i/o timeout")
	send(context.Background())
	if cached(c, "example.org") {
		t.Fatal("a failed connection kept the resolution")
	}

	store()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	answer, fail = nil, context.Canceled
	send(ctx)
	if !cached(c, "example.org") {
		t.Fatal("a request its caller gave up on forgot the resolution")
	}

	store()
	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	answer, fail = nil, context.DeadlineExceeded
	send(ctx)
	if cached(c, "example.org") {
		t.Fatal("a request that timed out kept the resolution")
	}
}

// Every client NewMautrix builds shares Resolutions and forgets through it.
func TestNewMautrixSharesResolutions(t *testing.T) {
	c := NewMautrix("example.org", nil, clientSettings(time.Second))
	f, ok := c.HTTP.Transport.(*ForgetOnFailure)
	if !ok || f.Cache != Resolutions {
		t.Fatalf("transport is %T", c.HTTP.Transport)
	}
}
