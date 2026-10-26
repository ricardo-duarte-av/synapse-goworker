package fedapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/cache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/fedauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/matrixstate"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/native"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
)

// fakeResolver stands in for the real one so the canary's control flow can be
// tested without a database.
type fakeResolver struct {
	resp  *matrixstate.StateIDsResponse
	err   error
	panic bool
	calls int
	// stateBody is the /state response this resolver produces. Empty means
	// /state is not configured, and State then fails rather than silently
	// returning an empty result a test could mistake for agreement.
	stateBody string
	// eventResp is what Event answers; nil answers with err.
	eventResp *matrixstate.TransactionResponse
	// lookups, if set, runs against the request context the way the store
	// would, so tests can see what the trace carries to the log.
	lookups func(ctx context.Context)
}

func (f *fakeResolver) StateIDs(ctx context.Context, origin, roomID, eventID string) (*matrixstate.StateIDsResponse, error) {
	f.calls++
	if f.panic {
		panic("boom")
	}
	if f.lookups != nil {
		f.lookups(ctx)
	}
	return f.resp, f.err
}

func (f *fakeResolver) Event(ctx context.Context, origin, serverName, eventID string) (*matrixstate.TransactionResponse, error) {
	f.calls++
	if f.eventResp != nil {
		return f.eventResp, nil
	}
	return nil, f.err
}

// canaryFrontend wires a handler with a canary that serves everything, so the
// fallback paths can be exercised deterministically.
func canaryFrontend(t *testing.T, upstreamAddr string, res native.Resolver) string {
	t.Helper()
	p, err := proxy.New(proxy.Config{Addrs: []string{upstreamAddr}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A verifier with no trusted keys rejects everything, which is what makes
	// the auth-rejection fallback observable.
	h := New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(res, fedauth.New("example.com", fedauth.Options{}), 5*time.Second))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// A request we cannot verify must fall back to Synapse rather than be served
// or refused. Synapse verifies independently, so falling back cannot admit
// anything it would reject -- while serving our own rejection would break
// legitimate federation over a bug in our verifier, which has happened.
func TestCanaryFallsBackWhenVerificationFails(t *testing.T) {
	const proxied = `{"pdu_ids":["$from-synapse"],"auth_chain_ids":[]}`
	var upstreamHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(proxied))
	}))
	defer upstream.Close()

	for _, tc := range []struct {
		name string
		res  *fakeResolver
	}{
		// Verification fails for all of these (no trusted keys), so every one
		// must fall back regardless of what the resolver would have done.
		{"resolver would error", &fakeResolver{err: errors.New("db is down")}},
		{"resolver would panic", &fakeResolver{panic: true}},
		{"resolver would succeed", &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$ours"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := upstreamHits
			front := canaryFrontend(t, strings.TrimPrefix(upstream.URL, "http://"), tc.res)
			const uri = "/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt"
			status, body := rawGet(t, front, uri,
				`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)

			if status != http.StatusOK {
				t.Errorf("status = %d, want 200 from the proxy", status)
			}
			if body != proxied {
				t.Errorf("body = %q, want Synapse's answer %q", body, proxied)
			}
			if upstreamHits != before+1 {
				t.Errorf("upstream was called %d times, want exactly 1 fallback", upstreamHits-before)
			}
			// Verification fails first, so the resolver must never be reached.
			// Asserting this keeps the test honest about what it proves: the
			// auth fallback, not the error or panic fallbacks.
			if tc.res.calls != 0 {
				t.Errorf("resolver was called %d times despite verification failing", tc.res.calls)
			}
		})
	}
}

// The error and panic fallbacks cannot be reached through the HTTP path
// without a validly signed request, so they are exercised directly. A panic in
// particular must become an error rather than escaping: unwound through the
// HTTP server it would kill the connection instead of falling back.
func TestNativeAnswerContainsFailures(t *testing.T) {
	h := New("example.com", ratelimit.FederationSettings{}, nil, zerolog.Nop())

	t.Run("panic becomes an error", func(t *testing.T) {
		h.resolver = &fakeResolver{panic: true}
		body, status, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e", nil)
		if err == nil {
			t.Fatal("a panic did not become an error, so it would escape into the HTTP server")
		}
		if body != nil || status != 0 {
			t.Errorf("a failed answer returned body=%q status=%d, want nothing writable", body, status)
		}
	})

	t.Run("resolver error propagates", func(t *testing.T) {
		h.resolver = &fakeResolver{err: errors.New("db is down")}
		if _, _, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e", nil); err == nil {
			t.Error("a resolver error was swallowed, so the caller would serve an empty answer")
		}
	})

	t.Run("a Matrix error is an answer, not a failure", func(t *testing.T) {
		// 403 "Host not in room" is Synapse's answer too, so it must be served
		// rather than treated as a reason to fall back.
		h.resolver = &fakeResolver{err: &matrixstate.MatrixError{
			Status: 403, ErrCode: "M_FORBIDDEN", Message: "Host not in room."}}
		body, status, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e", nil)
		if err != nil {
			t.Fatalf("a Matrix error was treated as a failure: %v", err)
		}
		if status != 403 || !strings.Contains(string(body), "M_FORBIDDEN") {
			t.Errorf("status=%d body=%q, want the 403 served as-is", status, body)
		}
	})
}

// A natively served request must not take a limiter slot twice.
//
// serveNative once acquired the limiter itself, while the caller was already
// holding a slot. With Synapse's default concurrent:3 that deadlocks the
// fourth request permanently -- and every one after it, since no slot is ever
// released. It survived unit tests because a single request with a generous
// concurrency limit works fine; it only appears when the limit actually binds.
func TestNativeServingDoesNotDeadlockOnTheLimiter(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	rc := ratelimit.FederationSettings{
		WindowSize: 1000, SleepLimit: 1000, SleepDelay: 0, RejectLimit: 100, Concurrent: 1,
	}
	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New("example.com", rc, p, zerolog.Nop(), WithNative(&fakeResolver{resp: &matrixstate.StateIDsResponse{}}, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	const n = 8
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			status, _ := rawGet(t, front.Listener.Addr().String(),
				"/_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e",
				`X-Matrix origin="noisy.example",destination="example.com",key="ed25519:a",sig="x"`)
			done <- status
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case status := <-done:
			if status != http.StatusOK {
				t.Errorf("status = %d, want 200", status)
			}
		case <-time.After(8 * time.Second):
			t.Fatalf("only %d of %d requests completed; the limiter slot is not being released", i, n)
		}
	}
}

// A fallback must be accounted for end to end. The client pays our attempt
// *and* Synapse's, and until this was measured there was no way to answer the
// obvious question after a timeout: did Synapse actually answer it?
func TestFallbackIsAccountedForEndToEnd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pdu_ids":[],"auth_chain_ids":[]}`))
	}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A resolver slower than the native budget, so the request times out and
	// falls back -- the expensive case.
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(&slowResolver{delay: 2 * time.Second}, acceptingVerifier{}, 150*time.Millisecond)))
	defer front.Close()

	start := time.Now()
	status, body := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
	elapsed := time.Since(start)

	if status != http.StatusOK {
		t.Errorf("status = %d, want 200 from the fallback", status)
	}
	if !strings.Contains(body, "pdu_ids") {
		t.Errorf("body = %q, want Synapse's answer", body)
	}
	// The whole point of a short native budget: the client waits roughly the
	// budget plus Synapse, not the resolver's full runtime.
	if elapsed > time.Second {
		t.Errorf("client waited %s; the native budget was 150ms, so a fallback "+
			"should not cost the resolver's full 2s", elapsed)
	}
}

// counterValue reads a counter out of the default registry by name, summed
// across labels.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// End to end: a client that disconnects mid-request must be recorded as
// client_gone, never as a timeout.
func TestClientHangupIsNotReportedAsTimeout(t *testing.T) {
	beforeGone := labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone")
	beforeTimeout := labelledCounter(t, "gopro_native_fallback_total", "reason", "timeout")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// Resolver slower than the client's patience but well inside the budget,
	// so only a disconnect can end it.
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(&slowResolver{delay: 3 * time.Second}, acceptingVerifier{}, 30*time.Second)))
	defer front.Close()

	// Connect, send, then hang up before the answer can arrive.
	conn, err := net.DialTimeout("tcp", front.Listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e HTTP/1.1\r\n"+
		"Host: example.com\r\n"+
		`Authorization: X-Matrix origin="n.example",destination="example.com",key="ed25519:a",sig="x"`+"\r\n"+
		"Connection: close\r\n\r\n")
	time.Sleep(200 * time.Millisecond)
	conn.Close()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) &&
		labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone") <= beforeGone {
		time.Sleep(50 * time.Millisecond)
	}

	// The positive assertion is the one that matters: asserting only that
	// "timeout" stayed at zero would pass even if nothing recorded anything.
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone"); got <= beforeGone {
		t.Fatalf("client_gone stayed at %v: a client hangup was not classified as one", beforeGone)
	}
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "timeout"); got > beforeTimeout {
		t.Errorf("a client hangup was also counted as reason=timeout (%v -> %v)", beforeTimeout, got)
	}
}

// labelledCounter reads one labelled counter out of the default registry.
func labelledCounter(t *testing.T, name, label, value string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// safeBuf collects log output written from the handler goroutine while the
// test reads it.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A natively served request must appear in the request log.
//
// It did not: the served path returns before the shared log statement, which
// runs after the proxy forward. While native traffic was a rounding error that
// was merely untidy; at state_ids canary:100 it meant 100% of that endpoint's
// requests produced no log line at all -- and since metrics carry no
// per-origin label by design, "which server asked for this" was recorded
// nowhere.
//
// The absences are asserted as tightly as the presences. upstream_ms and
// backend describe a request to Synapse that never happened, and reporting
// them as zero would be worse than omitting them: a zero upstream_ms reads as
// an instant upstream rather than no upstream.
func TestNativelyServedRequestIsLogged(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be contacted in native mode")
	}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}

	var logs safeBuf
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.New(&logs), WithNative(res, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}

	// The response can reach the client before the handler writes its log
	// line, so this is a race without a settling window.
	var entry map[string]any
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var d map[string]any
			if json.Unmarshal([]byte(l), &d) == nil && d["message"] == "Served federation request" {
				entry = d
			}
		}
		if entry != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if entry == nil {
		t.Fatalf("a natively served request produced no request log line; got:\n%s", logs.String())
	}

	for field, want := range map[string]any{
		"served":   "native",
		"endpoint": "state_ids",
		"status":   "200",
		"origin":   "remote.example",
		"room_id":  "!r:example.com",
	} {
		if got := entry[field]; got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	for _, absent := range []string{"upstream_ms", "backend"} {
		if v, ok := entry[absent]; ok {
			t.Errorf("%s = %v; it describes an upstream request that never happened", absent, v)
		}
	}
	for _, present := range []string{"bytes", "total_ms", "param"} {
		if _, ok := entry[present]; !ok {
			t.Errorf("%s missing from the log line", present)
		}
	}
}

// histogramCount returns how many observations a histogram holds, summed
// across label values.
func histogramCount(t *testing.T, name string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
	}
	return total
}

// A served request must be timed end to end, not just for the answer.
//
// gopro_native_duration_seconds times only h.answer, so it excludes X-Matrix
// verification -- which sits in front of it and needs a network key fetch for
// any origin whose keys are not cached. Reporting that as "native latency"
// would report the number we control rather than the number the federation
// waits for, and it is exactly the shape of mistake §9 of the working notes
// keeps recording: a measurement that answers an adjacent question.
//
// The assertion is that the end-to-end histogram observes at least as much as
// the answer histogram, request for request. Verification is microseconds
// against the fake verifier here, so asserting a strictly larger value would
// be flaky; asserting both were observed is the durable claim.
func TestNativeRequestIsTimedEndToEnd(t *testing.T) {
	beforeE2E := histogramCount(t, "gopro_native_request_seconds")
	beforeAnswer := histogramCount(t, "gopro_native_duration_seconds")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(res, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}

	if got := histogramCount(t, "gopro_native_request_seconds") - beforeE2E; got != 1 {
		t.Errorf("gopro_native_request_seconds observed %d times, want 1: a served "+
			"request must be timed end to end, verification included", got)
	}
	if got := histogramCount(t, "gopro_native_duration_seconds") - beforeAnswer; got != 1 {
		t.Errorf("gopro_native_duration_seconds observed %d times, want 1", got)
	}
}

// Response size must be observed for answers we served, not only proxied ones.
//
// metrics.ResponseBytes was recorded on the proxy path alone, so a promoted
// endpoint reported no payload sizes at all -- state_ids went native and its
// size histogram went empty. That is not a cosmetic gap: payload size is the
// measurement the /state decision turns on, and the case for reopening /state
// rests on knowing how large these responses actually get.
func TestNativelyServedResponseSizeIsObserved(t *testing.T) {
	before := histogramCount(t, "gopro_response_bytes")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(&fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}},
		acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatal("want 200 served natively")
	}

	if got := histogramCount(t, "gopro_response_bytes") - before; got != 1 {
		t.Errorf("gopro_response_bytes observed %d times, want 1", got)
	}
}

// A client that has gone must not cost Synapse an answer.
//
// The fallback exists to give a remote server a response we could not produce.
// When it hung up there is nobody to receive one, and forwarding only asks
// Synapse to compute a body that will be discarded -- on /state_ids that is
// seconds of database time each, and on this deployment the case is common:
// Tuwunel-style servers hang up on /state_ids constantly.
//
// The upstream handler counts its own calls, which is the only assertion that
// can distinguish "we skipped the forward" from "we forwarded and the write
// failed". A test that watched the response would not tell those apart,
// because there is no client left to observe either outcome.
func TestClientGoneDoesNotForwardToSynapse(t *testing.T) {
	before := labelledCounter(t, "gopro_upstream_skipped_total", "endpoint", "state_ids")

	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(&slowResolver{delay: 3 * time.Second}, acceptingVerifier{}, 30*time.Second)))
	defer front.Close()

	conn, err := net.DialTimeout("tcp", front.Listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e HTTP/1.1\r\n"+
		"Host: example.com\r\n"+
		`Authorization: X-Matrix origin="n.example",destination="example.com",key="ed25519:a",sig="x"`+"\r\n"+
		"Connection: close\r\n\r\n")
	time.Sleep(200 * time.Millisecond)
	conn.Close()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) &&
		labelledCounter(t, "gopro_upstream_skipped_total", "endpoint", "state_ids") <= before {
		time.Sleep(50 * time.Millisecond)
	}

	if got := labelledCounter(t, "gopro_upstream_skipped_total", "endpoint", "state_ids"); got <= before {
		t.Fatalf("gopro_upstream_skipped_total stayed at %v: the forward was not skipped", before)
	}
	// Settle, so a forward racing behind the counter would still be seen.
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&upstreamHits); got != 0 {
		t.Errorf("Synapse was asked for %d answer(s) nobody was waiting for", got)
	}
}

// State satisfies native.Resolver. /state is not exercised by these tests; a
// fake that silently returned an empty result would let a test claiming to
// cover it pass without doing anything.
func (f *fakeResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	f.calls++
	if f.panic {
		panic("boom")
	}
	if f.stateBody == "" {
		return matrixstate.StateResult{}, errors.New("fakeResolver: State not configured")
	}
	if _, err := io.WriteString(w, f.stateBody); err != nil {
		return matrixstate.StateResult{}, err
	}
	// Digested the same way the real resolver digests what it writes, so the
	// test exercises the comparison rather than a shortcut around it.
	return matrixstate.DigestStateResponse(strings.NewReader(f.stateBody))
}

func (s *slowResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	select {
	case <-time.After(s.delay):
		return matrixstate.StateResult{}, nil
	case <-ctx.Done():
		return matrixstate.StateResult{}, ctx.Err()
	}
}

// GetMissingEvents satisfies native.Resolver. Not exercised by these tests; a
// fake returning an empty result would let a test claiming to cover the
// endpoint pass without doing anything.
func (f *fakeResolver) GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*matrixstate.MissingEventsResponse, error) {
	return nil, errors.New("fakeResolver: GetMissingEvents not configured")
}

func (s *slowResolver) GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*matrixstate.MissingEventsResponse, error) {
	select {
	case <-time.After(s.delay):
		return &matrixstate.MissingEventsResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The limiter wait histogram must be fed on every acquisition.
//
// It was defined and queried by three dashboard panels but never observed, so
// those panels could not have shown anything -- the same shape as the metrics
// that existed without panels, inverted. Observing only the delayed requests
// would be almost as bad: the zero waits are what stop the quantiles reporting
// half a second as typical.
func TestLimiterWaitIsObservedOnEveryRequest(t *testing.T) {
	before := histogramCount(t, "gopro_rate_limit_queue_wait_seconds")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(res, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	// A single uncontended request: it waits zero, and must still be observed.
	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	if got := histogramCount(t, "gopro_rate_limit_queue_wait_seconds") - before; got != 1 {
		t.Errorf("limiter wait observed %d times, want 1 (an undelayed request still waits zero)", got)
	}
}

// A streamed answer writes nothing until it is sure of one.
//
// Every other endpoint builds its response in memory, so any failure can fall
// back to the proxy. /state cannot buffer 97MB, so the guarantee is narrowed
// rather than dropped: Resolver.State performs every check that can produce a
// Matrix error before writing a byte, and while nothing has been written a
// failure falls back exactly as elsewhere.
func TestStreamedAnswerFallsBackWhenNothingWasWritten(t *testing.T) {
	rec := httptest.NewRecorder()
	out := &lazyResponse{w: rec}

	// Nothing written yet: no status line has been sent.
	if out.written {
		t.Fatal("the status line was written before any body byte")
	}
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Errorf("recorder already touched: code=%d body=%d", rec.Code, rec.Body.Len())
	}

	// The first byte commits.
	if _, err := out.Write([]byte(`{"pdus":[`)); err != nil {
		t.Fatal(err)
	}
	if !out.written {
		t.Error("writing a byte did not mark the response as committed")
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if out.bytes != 9 {
		t.Errorf("bytes = %d, want 9", out.bytes)
	}
}

// Only /state streams. Marking another endpoint would send it down a path
// whose fallback guarantee is weaker, for no reason.
func TestOnlyStateStreams(t *testing.T) {
	for _, e := range []Endpoint{EndpointEvent, EndpointStateIDs, EndpointGetMissingEvents} {
		if e.Streams() {
			t.Errorf("%s claims to stream; only /state should", e)
		}
	}
	if !EndpointState.Streams() {
		t.Error("/state does not claim to stream, so it would buffer a 97MB response")
	}
}

// A client hanging up mid-stream is not a defect and must not be reported as
// one.
//
// /state responses run to a hundred megabytes and take tens of seconds, so a
// remote server giving up part-way through is far more likely here than on any
// other endpoint. The first version counted it as stream_aborted and logged it
// at error -- the same conflation the working notes record for the
// non-streaming path, where a client disconnect was reported as our timeout.
func TestStreamedClientHangupIsNotAnAbort(t *testing.T) {
	beforeGone := labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone")
	beforeAbort := labelledCounter(t, "gopro_native_fallback_total", "reason", "stream_aborted")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A resolver that writes a byte, then fails with the context's error --
	// exactly what happens when the remote server hangs up mid-body.
	res := &hangupResolver{}
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.Nop(), WithNative(res, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	conn, err := net.DialTimeout("tcp", front.Listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /_matrix/federation/v1/state/%21r%3Aex?event_id=%24e HTTP/1.1\r\n"+
		"Host: example.com\r\n"+
		`Authorization: X-Matrix origin="n.example",destination="example.com",key="ed25519:a",sig="x"`+"\r\n"+
		"Connection: close\r\n\r\n")
	time.Sleep(150 * time.Millisecond)
	conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) &&
		labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone") <= beforeGone {
		time.Sleep(50 * time.Millisecond)
	}
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone"); got <= beforeGone {
		t.Errorf("a mid-stream hangup was not classified as client_gone")
	}
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "stream_aborted"); got > beforeAbort {
		t.Errorf("a mid-stream hangup was reported as stream_aborted (%v -> %v)", beforeAbort, got)
	}
}

// hangupResolver writes a byte and then reports the context's error, which is
// what a resolver does when the client goes away mid-body.
type hangupResolver struct{ fakeResolver }

func (h *hangupResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	if _, err := w.Write([]byte(`{"pdus":[`)); err != nil {
		return matrixstate.StateResult{}, err
	}
	<-ctx.Done()
	return matrixstate.StateResult{}, ctx.Err()
}

// requestLogLine waits for the request log line and returns it decoded. The
// response can reach the client before the handler logs, so it polls.
func requestLogLine(t *testing.T, logs *safeBuf) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var d map[string]any
			if json.Unmarshal([]byte(l), &d) == nil && d["message"] == "Served federation request" {
				return d
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no request log line; got:\n%s", logs.String())
	return nil
}

// /event names only an event in its URL, so the room in its log line has to
// come from the answer. Without it, "which room was being fetched" -- the
// first question during the 2026-10-16 fan-out -- is answerable for every
// endpoint except the one carrying 93% of the traffic.
func TestNativeEventLogsRoomFromAnswer(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be contacted in native mode")
	}))
	defer upstream.Close()
	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{eventResp: &matrixstate.TransactionResponse{
		Origin: "example.com",
		PDUs:   []json.RawMessage{json.RawMessage(`{"room_id":"!room:example.com","type":"m.room.message"}`)},
	}}
	var logs safeBuf
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.New(&logs), WithNative(res, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	status, _ := rawGet(t, front.Listener.Addr().String(), "/_matrix/federation/v1/event/%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}
	if got := requestLogLine(t, &logs)["room_id"]; got != "!room:example.com" {
		t.Errorf("room_id = %v, want !room:example.com", got)
	}
}

// A proxied request logs its room decoded, and a proxied /event -- which never
// learns its room -- logs none rather than an empty one.
func TestProxiedRequestLogsRoom(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, want string }{
		{"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt", "!r:example.com"},
		{"/_matrix/federation/v1/event/%24evt", ""},
	} {
		var logs safeBuf
		front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.New(&logs)))
		rawGet(t, front.Listener.Addr().String(), tc.path,
			`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
		entry := requestLogLine(t, &logs)
		front.Close()
		got, present := entry["room_id"]
		switch {
		case tc.want == "" && present:
			t.Errorf("%s: room_id = %v, want the field omitted", tc.path, got)
		case tc.want != "" && got != tc.want:
			t.Errorf("%s: room_id = %v, want %s", tc.path, got, tc.want)
		}
	}
}

// The request log says whether a served answer came from memory. The lookups
// happen deep in the store, so what this checks is the plumbing: the trace
// must reach the resolver through the request context and come back out on
// the log line.
func TestNativeRequestLogsCacheOutcome(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be contacted in native mode")
	}))
	defer upstream.Close()
	p, err := proxy.New(proxy.Config{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{
		resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}},
		lookups: func(ctx context.Context) {
			cache.Record(ctx, "state_groups", true)
			cache.Record(ctx, "auth_chains", false)
		},
	}
	var logs safeBuf
	front := httptest.NewServer(New("example.com", ratelimit.FederationSettings{}, p, zerolog.New(&logs), WithNative(res, acceptingVerifier{}, 5*time.Second)))
	defer front.Close()

	status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}
	entry := requestLogLine(t, &logs)
	if got := entry["cache"]; got != "partial" {
		t.Errorf("cache = %v, want partial", got)
	}
	if got := entry["cache_detail"]; got != "auth_chains=0/1 state_groups=1/1" {
		t.Errorf("cache_detail = %v", got)
	}
}
