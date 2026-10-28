// Package replication speaks Synapse's HTTP replication protocol, as a client.
//
// The inbound path calls fed_send_edu, which is exactly what a Python generic
// worker calls to hand an inbound EDU to the instance that writes its stream
// (FederationHandlerRegistry._send_edu, federation_server.py:1700), and
// fed_send_events. The event creator calls send_events, which is what an
// event creator calls to hand the events it built to the room's events writer
// (EventCreationHandler._persist_events, replication/http/send_events.py),
// and a few others on main and the profile writer. Each part keeps its own
// metrics (inbound.go, creator.go).
//
// The wire format is replication/http/_base.py's: POST
// /_synapse/replication/<NAME>/<path args...>/<txn_id>, a JSON body, and the
// shared secret as a Bearer token. The txn_id is what makes a retry safe: the
// writer keeps a 30-minute ResponseCache keyed on it, so the same txn_id is
// processed once however many times it arrives.
package replication

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Client sends replication requests to named Synapse instances.
type Client struct {
	targets map[string]*target
	secret  string
	// retries is the part's own counter of retried requests.
	retries *prometheus.CounterVec
	// sleep is replaced in tests.
	sleep func(context.Context, time.Duration) error
}

type target struct {
	name string
	base string
	http *http.Client
}

// RequestTimeout bounds one replication request. Synapse's replication client
// uses 60s too; an EDU handler that takes longer is stuck, not busy.
const RequestTimeout = 60 * time.Second

// Part is who a client works for: each keeps its metrics' names from before
// the merge.
type Part int

const (
	Inbound Part = iota
	Creator
)

// NewClient builds a client for every instance in the instance map.
func NewClient(cfg *synapsecfg.Config, part Part) *Client {
	c := &Client{targets: make(map[string]*target), secret: cfg.ReplicationSecret, sleep: sleepCtx, retries: inboundRetries}
	if part == Creator {
		c.retries = creatorRetries
	}
	for name, inst := range cfg.InstanceMap {
		c.targets[name] = newTarget(inst)
	}
	return c
}

func newTarget(inst synapsecfg.Instance) *target {
	t := &target{name: inst.Name}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	if inst.Socket != "" {
		sock := inst.Socket
		var d net.Dialer
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return d.DialContext(ctx, "unix", sock)
		}
		// Synapse ignores the host of a replication request.
		t.base = "http://" + inst.Name
	} else {
		t.base = strings.TrimSuffix(inst.URL, "/")
	}
	t.http = &http.Client{Transport: transport, Timeout: RequestTimeout}
	return t
}

// Has reports whether an instance can be reached.
func (c *Client) Has(instance string) bool {
	_, ok := c.targets[canonical(instance)]
	return ok
}

func canonical(name string) string {
	if name == "main" {
		return synapsecfg.MainProcessInstanceName
	}
	return name
}

// HTTPError is a non-2xx answer from the instance.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("replication: HTTP %d: %s", e.Status, e.Body)
}

// Connection errors are retried this many times, with delays doubling from
// one second: replication/http/_base.py's RETRY_ON_CONNECT_ERROR_ATTEMPTS.
const connectRetries = 5

// call posts body to url with _base.py's retries: a timeout is retried after
// a second, as often as the context allows, and a connection failure up to
// connectRetries times with exponential backoff. Both are safe because every
// attempt carries the same txn_id. Any other failure is returned at once.
func (c *Client) call(ctx context.Context, t *target, name, url string, body []byte) ([]byte, error) {
	connectFailures := 0
	for {
		b, err := c.post(ctx, t, url, body)
		switch {
		case err == nil:
			return b, nil
		case ctx.Err() != nil:
			return nil, fmt.Errorf("replication: %s to %s: %w", name, t.name, ctx.Err())
		case isTimeout(err):
			c.retries.WithLabelValues(t.name, "timeout").Inc()
			_ = c.sleep(ctx, time.Second)
		case isConnectError(err) && connectFailures < connectRetries:
			c.retries.WithLabelValues(t.name, "connect").Inc()
			delay := time.Second << connectFailures
			connectFailures++
			_ = c.sleep(ctx, delay)
		default:
			return nil, err
		}
	}
}

func (c *Client) post(ctx context.Context, t *target, url string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.secret)
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return nil, &HTTPError{Status: resp.StatusCode, Body: string(b)}
	}
	return b, nil
}

// ErrPartialStateConflict is the persister's 409: the room was un-partial
// stated while the event's context was computed, and Synapse recomputes it.
var ErrPartialStateConflict = errors.New("replication: partial state conflict")

// isTimeout matches Synapse's RequestTimedOutError: no answer in time, or a
// 504 from the instance.
func isTimeout(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusGatewayTimeout
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// isConnectError matches Synapse's ConnectError: nothing listening, or a socket
// that is not there -- the shape of an instance mid-restart.
func isConnectError(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quote is Python's urllib.parse.quote(s, safe=""), which _base.py applies to
// every path argument: everything but unreserved characters is escaped.
func quote(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z') || ('0' <= ch && ch <= '9') ||
			ch == '_' || ch == '.' || ch == '-' || ch == '~' {
			b.WriteByte(ch)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", ch)
	}
	return b.String()
}

const txnIDLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// txnID is Synapse's random_string(10).
func txnID() string {
	var raw [10]byte
	_, _ = rand.Read(raw[:])
	out := make([]byte, len(raw))
	for i, r := range raw {
		out[i] = txnIDLetters[int(r)%len(txnIDLetters)]
	}
	return string(out)
}
