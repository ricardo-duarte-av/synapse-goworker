package clientauth

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// WhoamiConfig says how to reach Synapse's /whoami, and how long its answers
// are kept.
type WhoamiConfig struct {
	// Socket is a Synapse client listener's unix socket; URL is used when
	// it is empty.
	Socket string
	URL    string

	PositiveTTL time.Duration
	NegativeTTL time.Duration
	MaxEntries  int
	Timeout     time.Duration
}

// whoami asks Synapse, for what is not reproduced here: a token in neither
// access_tokens nor a registration this worker loaded (a guest's macaroon,
// a typo), or any token at all when native auth is off.
//
// The cache is in memory only: it holds credentials, refills in
// milliseconds, and on disk would be a liability with no upside.
type whoami struct {
	client      *http.Client
	url         string
	positiveTTL time.Duration
	negativeTTL time.Duration
	maxEntries  int

	group singleflight.Group

	mu    sync.Mutex
	items map[string]*list.Element
	order *list.List // front = most recently used
}

type whoamiEntry struct {
	key     string
	verdict Verdict
	expires time.Time
}

func newWhoami(cfg WhoamiConfig) (*whoami, error) {
	transport := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	base := cfg.URL
	if cfg.Socket != "" {
		socket := cfg.Socket
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}
		if base == "" {
			// The host is ignored once the dialer is pinned to a socket, but
			// net/http still needs a syntactically valid URL.
			base = "http://synapse"
		}
	}
	if base == "" {
		return nil, fmt.Errorf("clientauth: a whoami socket or URL must be set")
	}
	return &whoami{
		client:      &http.Client{Transport: transport, Timeout: cfg.Timeout},
		url:         strings.TrimRight(base, "/") + "/_matrix/client/v3/account/whoami",
		positiveTTL: cfg.PositiveTTL,
		negativeTTL: cfg.NegativeTTL,
		maxEntries:  cfg.MaxEntries,
		items:       make(map[string]*list.Element),
		order:       list.New(),
	}, nil
}

// whoamiKey keys the cache without holding raw credentials longer than the
// request that carried them. The masquerade parameters are part of it: one
// appservice token is a different user for every ?user_id=.
func whoamiKey(token string, q url.Values) string {
	h := sha256.New()
	for _, part := range []string{token, first(q, "user_id"), first(q, "device_id")} {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func first(q url.Values, k string) string {
	if v := q[k]; len(v) > 0 {
		return "=" + v[0]
	}
	return ""
}

// authenticate asks Synapse about the request's token. A non-nil error means
// the answer is unknown (Synapse unreachable), never a refusal.
func (w *whoami) authenticate(ctx context.Context, r *http.Request, token string) (Verdict, error) {
	q := r.URL.Query()
	key := whoamiKey(token, q)
	if v, ok := w.lookup(key); ok {
		return v, nil
	}
	// singleflight collapses the stampede when a client reconnects many
	// sessions at once with one token, as sync clients do after a blip.
	res, err, _ := w.group.Do(key, func() (any, error) {
		if v, ok := w.lookup(key); ok {
			return v, nil
		}
		v, ttl, err := w.call(ctx, r, token, q)
		if err != nil {
			return Verdict{}, err
		}
		w.store(key, v, ttl)
		return v, nil
	})
	if err != nil {
		return Verdict{}, err
	}
	return res.(Verdict), nil
}

type whoamiResponse struct {
	UserID string `json:"user_id"`
	// DeviceID is absent for an appservice without a device.
	DeviceID string `json:"device_id"`
	IsGuest  bool   `json:"is_guest"`
}

func (w *whoami) call(ctx context.Context, r *http.Request, token string, q url.Values) (Verdict, time.Duration, error) {
	target := w.url
	// Forwarded so Synapse runs its own appservice checks and answers with
	// the effective user rather than the appservice's.
	fq := url.Values{}
	for _, k := range []string{"user_id", "device_id"} {
		if v, ok := q[k]; ok && len(v) > 0 {
			fq.Set(k, v[0])
		}
	}
	if len(fq) > 0 {
		target += "?" + fq.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return Verdict{}, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	// Synapse records the request (client_ips) from these, and checks an
	// appservice's ip_range_whitelist against the first: without them it
	// would see this worker, not the client.
	for _, h := range []string{"X-Forwarded-For", "X-Forwarded-Proto", "User-Agent"} {
		if v := r.Header.Values(h); len(v) > 0 {
			req.Header[h] = v
		}
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return Verdict{}, 0, fmt.Errorf("whoami request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return Verdict{}, 0, fmt.Errorf("reading whoami response: %w", err)
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		var b whoamiResponse
		if err := json.Unmarshal(body, &b); err != nil {
			return Verdict{}, 0, fmt.Errorf("decoding whoami response: %w", err)
		}
		return Verdict{Valid: true, UserID: b.UserID, DeviceID: b.DeviceID, IsGuest: b.IsGuest, Source: SourceWhoami},
			w.positiveTTL, nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
		// Synapse's own refusal, passed through as sent: it tells an expired
		// token (soft logout, keep local state) from an unknown one.
		var e struct {
			ErrCode string `json:"errcode"`
		}
		if json.Unmarshal(body, &e) != nil || e.ErrCode == "" {
			return Verdict{}, 0, fmt.Errorf("whoami returned %d without a Matrix error", resp.StatusCode)
		}
		return Verdict{Source: SourceWhoami, Rejection: &Rejection{Status: resp.StatusCode, ErrCode: e.ErrCode, Body: body}},
			w.negativeTTL, nil
	default:
		// Anything else (5xx, ratelimit) is an unknown answer, not a refusal.
		// Caching it would turn a Synapse hiccup into a wave of logouts.
		return Verdict{}, 0, fmt.Errorf("whoami returned unexpected status %d", resp.StatusCode)
	}
}

func (w *whoami) lookup(key string) (Verdict, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	elem, ok := w.items[key]
	if !ok {
		return Verdict{}, false
	}
	e := elem.Value.(*whoamiEntry)
	if time.Now().After(e.expires) {
		w.order.Remove(elem)
		delete(w.items, key)
		return Verdict{}, false
	}
	w.order.MoveToFront(elem)
	return e.verdict, true
}

func (w *whoami) store(key string, v Verdict, ttl time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	expires := time.Now().Add(ttl)
	if elem, ok := w.items[key]; ok {
		e := elem.Value.(*whoamiEntry)
		e.verdict, e.expires = v, expires
		w.order.MoveToFront(elem)
		return
	}
	w.items[key] = w.order.PushFront(&whoamiEntry{key: key, verdict: v, expires: expires})
	for w.maxEntries > 0 && w.order.Len() > w.maxEntries {
		oldest := w.order.Back()
		w.order.Remove(oldest)
		delete(w.items, oldest.Value.(*whoamiEntry).key)
	}
}

func (w *whoami) purge() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.items = make(map[string]*list.Element)
	w.order.Init()
}

func (w *whoami) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.order.Len()
}
