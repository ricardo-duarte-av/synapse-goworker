// Package auth validates client access tokens by asking Synapse.
//
// The worker deliberately does not read the access_tokens table, even though
// Synapse's own lookup is a plain `WHERE token = ?` with no hashing and would
// be trivially reproducible with the SELECT privileges we already hold. Three
// kinds of caller would be wrongly rejected by that query:
//
//   - appservice tokens live in registration YAML and never reach the database;
//   - delegated auth (MAS) keeps tokens outside Synapse entirely;
//   - guest tokens are macaroons, verifiable only with macaroon_secret_key,
//     which this worker must never hold. There are 79 guest accounts on this
//     homeserver, so the case is real, not theoretical.
//
// Asking Synapse is authoritative in every deployment, and caching makes it
// cheap. It also yields the device_id, which a per-device endpoint like /sync
// cannot work without: to-device messages and device-list updates are keyed on
// the device, not the user.
//
// The cache is in-memory only and is never persisted: it holds credentials,
// refills in milliseconds, and writing it to disk would be a liability with no
// upside.
package auth

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

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
)

// Verdict is the outcome of validating a caller.
type Verdict struct {
	Valid    bool
	UserID   string
	DeviceID string
	IsGuest  bool

	// Expires is when this verdict leaves the cache.
	Expires time.Time

	// Rejection carries Synapse's own answer when it refuses. Synapse
	// distinguishes an expired token (soft logout, keep local state) from an
	// unknown one (hard logout, wipe), and passing that through is both better
	// parity and a far more useful diagnostic than a generic message.
	Rejection *matrixerr.Error
	Status    int
}

// Config describes how to reach Synapse's whoami endpoint.
type Config struct {
	// WhoamiSocket is a Synapse client-API worker's unix socket.
	WhoamiSocket string
	// WhoamiURL is used when WhoamiSocket is empty.
	WhoamiURL string

	PositiveTTL time.Duration
	NegativeTTL time.Duration
	MaxEntries  int
	Timeout     time.Duration
}

type cacheEntry struct {
	key     string
	verdict Verdict
}

// Authenticator validates access tokens against Synapse's whoami endpoint,
// caching both successes and rejections.
type Authenticator struct {
	client      *http.Client
	whoamiURL   string
	positiveTTL time.Duration
	negativeTTL time.Duration
	maxEntries  int

	group singleflight.Group

	mu    sync.Mutex
	items map[string]*list.Element
	order *list.List // front = most recently used
}

// New builds an Authenticator.
func New(cfg Config) (*Authenticator, error) {
	transport := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	base := cfg.WhoamiURL
	if cfg.WhoamiSocket != "" {
		socket := cfg.WhoamiSocket
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
		return nil, fmt.Errorf("auth: whoami_url or whoami_socket must be set")
	}
	return &Authenticator{
		client:      &http.Client{Transport: transport, Timeout: cfg.Timeout},
		whoamiURL:   strings.TrimRight(base, "/") + "/_matrix/client/v3/account/whoami",
		positiveTTL: cfg.PositiveTTL,
		negativeTTL: cfg.NegativeTTL,
		maxEntries:  cfg.MaxEntries,
		items:       make(map[string]*list.Element),
		order:       list.New(),
	}, nil
}

// Credentials are what the worker presents to Synapse to identify the caller.
//
// UserID and DeviceID matter for appservice tokens: Synapse resolves those
// first, and a `?user_id=` query parameter masquerades as a ghost in the
// appservice's namespace. Everything downstream must use the masqueraded user,
// not the appservice's own.
type Credentials struct {
	Token    string
	UserID   string
	DeviceID string
}

// ExtractCredentials pulls the caller's token and any masquerade parameters
// from the request.
func ExtractCredentials(r *http.Request) Credentials {
	q := r.URL.Query()
	return Credentials{
		Token:    ExtractToken(r),
		UserID:   q.Get("user_id"),
		DeviceID: q.Get("device_id"),
	}
}

// ExtractToken pulls the access token from the request, accepting both the
// Authorization header and the legacy query parameter.
//
// An Authorization header that is present but not Bearer yields no token
// rather than falling through to the query parameter: a caller who sent
// credentials one way did not mean to also be read the other way.
func ExtractToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		if token, ok := strings.CutPrefix(auth, "Bearer "); ok {
			return strings.TrimSpace(token)
		}
		return ""
	}
	return r.URL.Query().Get("access_token")
}

// cacheKey keys the cache without holding raw credentials in memory longer than
// the request that carried them.
//
// The masquerade parameters are part of the key. Keying on the token alone
// would let one appservice ghost's verdict be served for another, since a
// single appservice token resolves to a different user for every `?user_id=`.
func (c Credentials) cacheKey() string {
	h := sha256.New()
	for _, part := range []string{c.Token, c.UserID, c.DeviceID} {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

type whoamiResponse struct {
	UserID string `json:"user_id"`
	// DeviceID is absent for appservice and similar accounts, which have no
	// device. Synapse omits the field rather than sending an empty one.
	DeviceID string `json:"device_id"`
	IsGuest  bool   `json:"is_guest"`
}

// Authenticate validates a bare token.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (Verdict, error) {
	return a.AuthenticateAs(ctx, Credentials{Token: token})
}

// AuthenticateAs validates a caller, resolving appservice masquerading by
// asking Synapse rather than trusting the request.
//
// A non-nil error means the answer is unknown (Synapse unreachable), which
// callers must surface as 502/503 rather than 401: refusing a valid token
// because we could not reach Synapse would log real clients out.
func (a *Authenticator) AuthenticateAs(ctx context.Context, creds Credentials) (Verdict, error) {
	if creds.Token == "" {
		return Verdict{Valid: false}, nil
	}
	key := creds.cacheKey()
	if v, ok := a.lookup(key); ok {
		return v, nil
	}

	// singleflight collapses the stampede when a client reconnects many
	// sessions at once with the same token. Sync clients do exactly this after
	// a network blip.
	res, err, _ := a.group.Do(key, func() (any, error) {
		if v, ok := a.lookup(key); ok {
			return v, nil
		}
		v, err := a.callWhoami(ctx, creds)
		if err != nil {
			return Verdict{}, err
		}
		a.store(key, v)
		return v, nil
	})
	if err != nil {
		return Verdict{}, err
	}
	return res.(Verdict), nil
}

func (a *Authenticator) callWhoami(ctx context.Context, creds Credentials) (Verdict, error) {
	target := a.whoamiURL
	// Forwarding these makes Synapse run its appservice namespace checks and
	// return the effective user, rather than the appservice's own.
	if creds.UserID != "" || creds.DeviceID != "" {
		q := url.Values{}
		if creds.UserID != "" {
			q.Set("user_id", creds.UserID)
		}
		if creds.DeviceID != "" {
			q.Set("device_id", creds.DeviceID)
		}
		target += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return Verdict{}, err
	}
	req.Header.Set("Authorization", "Bearer "+creds.Token)
	resp, err := a.client.Do(req)
	if err != nil {
		return Verdict{}, fmt.Errorf("whoami request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		var body whoamiResponse
		if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&body); err != nil {
			return Verdict{}, fmt.Errorf("decoding whoami response: %w", err)
		}
		return Verdict{
			Valid:    true,
			UserID:   body.UserID,
			DeviceID: body.DeviceID,
			IsGuest:  body.IsGuest,
			Expires:  time.Now().Add(a.positiveTTL),
		}, nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		v := Verdict{Valid: false, Expires: time.Now().Add(a.negativeTTL), Status: resp.StatusCode}
		var body matrixerr.Error
		if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&body); err == nil &&
			body.ErrCode != "" {
			v.Rejection = &body
		}
		return v, nil
	default:
		// Anything else (5xx, ratelimit) is an unknown answer, not a rejection.
		// Caching it would turn a Synapse hiccup into a wave of logouts.
		return Verdict{}, fmt.Errorf("whoami returned unexpected status %d", resp.StatusCode)
	}
}

func (a *Authenticator) lookup(key string) (Verdict, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	elem, ok := a.items[key]
	if !ok {
		return Verdict{}, false
	}
	entry := elem.Value.(*cacheEntry)
	if time.Now().After(entry.verdict.Expires) {
		a.order.Remove(elem)
		delete(a.items, key)
		return Verdict{}, false
	}
	a.order.MoveToFront(elem)
	return entry.verdict, true
}

func (a *Authenticator) store(key string, v Verdict) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if elem, ok := a.items[key]; ok {
		elem.Value.(*cacheEntry).verdict = v
		a.order.MoveToFront(elem)
		return
	}
	elem := a.order.PushFront(&cacheEntry{key: key, verdict: v})
	a.items[key] = elem
	for a.maxEntries > 0 && a.order.Len() > a.maxEntries {
		oldest := a.order.Back()
		if oldest == nil {
			break
		}
		a.order.Remove(oldest)
		delete(a.items, oldest.Value.(*cacheEntry).key)
	}
}

// Len reports the number of cached verdicts, for metrics.
func (a *Authenticator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.order.Len()
}
