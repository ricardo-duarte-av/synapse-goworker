// Package presence tells Synapse's presence writer that a user is syncing.
//
// This worker reads. Presence is the one thing a /sync cannot answer without
// also SAYING something: Synapse's own sync handler calls user_syncing(), which
// marks the account online and refreshes the timers that keep it there. A
// worker that serves /sync and stays quiet leaves every account it serves
// looking permanently offline -- to other users and over federation -- and no
// amount of correctness in the response fixes that, because the fact is
// recorded elsewhere.
//
// It is NOT a database write. Presence has a single writer instance
// (`stream_writers.presence`, `av-edu-worker` on this deployment) and every
// other process reaches it over Synapse's HTTP replication API. So this package
// is an HTTP client and nothing else: internal/store stays 100% SELECT, and the
// only new privilege is the shared replication secret.
//
// Three properties of the writer make this safe to speak to, all verified
// against v1.159.0 and against the live deployment:
//
//   - It authorises on the bearer secret alone. There is no registration step,
//     no instance_map entry, and nothing ever calls back, so a pure client with
//     no replication listener is a supported shape.
//   - Sync-driven updates are idempotent and self-limiting. The writer's timers
//     are coarser than a sync loop, so relaying one update per interval per
//     device is enough; more is wasted work, not wrong data.
//   - A process that goes quiet expires. EXTERNAL_PROCESS_EXPIRY is five
//     minutes, after which the writer drops what we told it and times the users
//     out. If this worker dies, its users go offline on their own rather than
//     being pinned online forever.
package presence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
)

// State is a presence value the writer will accept.
//
// Synapse's VALID_PRESENCE, minus `busy`: that one is gated on MSC3026, and
// sending a state the writer rejects turns a sync into a 500.
const (
	StateOnline      = "online"
	StateUnavailable = "unavailable"
	StateOffline     = "offline"
)

// ValidState reports whether the writer will accept this state.
func ValidState(s string) bool {
	switch s {
	case StateOnline, StateUnavailable, StateOffline:
		return true
	}
	return false
}

// DefaultRelayInterval is how often an UNCHANGED sync-driven state is relayed.
//
// Synapse derives it as min(presence.sync_online_timeout,
// presence.last_active_granularity) * 5/6, which at the defaults of 30s and 60s
// is 25s. The 5/6 is what stops a user flapping offline between relays: the
// refresh has to arrive comfortably before the timer it is refreshing. Deriving
// it the same way rather than picking a round number keeps that margin if the
// homeserver's timers are ever retuned.
const DefaultRelayInterval = 25 * time.Second

// DeriveRelayInterval reproduces Synapse's calculation.
func DeriveRelayInterval(syncOnlineTimeout, lastActiveGranularity time.Duration) time.Duration {
	shorter := syncOnlineTimeout
	if lastActiveGranularity < shorter {
		shorter = lastActiveGranularity
	}
	if shorter <= 0 {
		return DefaultRelayInterval
	}
	return shorter * 5 / 6
}

// Config describes how to reach the presence writer.
type Config struct {
	// Socket is the writer's unix socket, from Synapse's instance_map entry
	// for whichever instance holds stream_writers.presence.
	Socket string
	// URL is the alternative to Socket, for a writer reached over TCP.
	URL string
	// Secret is Synapse's worker_replication_secret. Without it the writer
	// answers 500 and every sync that tries to relay presence fails.
	Secret string
	// RelayInterval throttles unchanged sync-driven updates. Zero means
	// DefaultRelayInterval.
	RelayInterval time.Duration
	// Timeout bounds one call to the writer. Zero means five seconds.
	Timeout time.Duration
	// Resolve re-reads Socket, URL and Secret from Synapse's own config.
	//
	// Optional, and the reason it exists is that all three change without
	// anybody thinking about this worker: the presence writer moves between
	// instances, its socket path moves with it, and the replication secret
	// rotates. synapsecfg reads them at every start, but "every start" is not
	// often enough -- the failure is silent, and presence simply stops working
	// until somebody notices and restarts us.
	//
	// Called only when a relay fails in a way configuration could explain,
	// never on the sync path's happy case, and no more than once per
	// ResolveCooldown. RelayInterval in what it returns is ignored: the
	// throttle is ours, not the writer's.
	Resolve func() (Config, error)
	// ResolveCooldown bounds how often Resolve runs. Zero means 30s.
	ResolveCooldown time.Duration
}

// errWriterRefused marks a writer that answered with a non-200.
var errWriterRefused = errors.New("presence: writer refused")

type key struct {
	userID   string
	deviceID string
}

type sent struct {
	state string
	when  time.Time
}

// target is where the writer is and how to authenticate to it.
//
// Held behind a pointer swap rather than a lock because every relay reads it
// and only a failed relay ever replaces it: readers on the sync path must not
// queue behind a re-read of a file.
type target struct {
	http    *http.Client
	baseURL string
	secret  string
	// addr is what the config named, for logging and for deciding whether a
	// re-read actually changed anything.
	addr string
}

// Client relays presence to the writer.
type Client struct {
	current atomic.Pointer[target]
	timeout time.Duration
	relay   time.Duration
	log     zerolog.Logger

	mu   sync.Mutex
	last map[key]sent

	// resolve re-reads the writer's address and secret. See Config.Resolve.
	resolve         func() (Config, error)
	resolveCooldown time.Duration
	resolveMu       sync.Mutex
	lastResolve     time.Time

	// now is overridable for tests.
	now func() time.Time
}

// New builds a client, or returns an error the caller should refuse to start on.
func New(cfg Config, log zerolog.Logger) (*Client, error) {
	if cfg.Socket == "" && cfg.URL == "" {
		return nil, fmt.Errorf("presence: needs the writer's socket or url")
	}
	if cfg.Socket != "" && cfg.URL != "" {
		return nil, fmt.Errorf("presence: set exactly one of socket and url")
	}
	// A missing secret is refused rather than tried. The writer's answer to an
	// unauthenticated call is a 500, which would surface as this worker failing
	// syncs rather than as a configuration mistake.
	if cfg.Secret == "" {
		return nil, fmt.Errorf("presence: needs Synapse's worker_replication_secret")
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	relay := cfg.RelayInterval
	if relay <= 0 {
		relay = DefaultRelayInterval
	}

	cooldown := cfg.ResolveCooldown
	if cooldown <= 0 {
		cooldown = DefaultResolveCooldown
	}

	c := &Client{
		timeout:         timeout,
		relay:           relay,
		log:             log,
		last:            map[key]sent{},
		resolve:         cfg.Resolve,
		resolveCooldown: cooldown,
		now:             time.Now,
	}
	c.current.Store(newTarget(cfg.Socket, cfg.URL, cfg.Secret, timeout))
	return c, nil
}

// DefaultResolveCooldown bounds how often a failing relay re-reads Synapse's
// config.
//
// Short enough that a writer that moved is picked up within a sync loop or two,
// long enough that a writer which is simply down does not turn every relay into
// a file read. A relay is already rare -- one per device per 25 seconds.
const DefaultResolveCooldown = 30 * time.Second

// newTarget builds the HTTP client for one writer address.
func newTarget(socket, url, secret string, timeout time.Duration) *target {
	t := &target{secret: secret}
	transport := &http.Transport{}
	if socket != "" {
		t.addr = socket
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}
		// The host is ignored for a unix socket but has to be syntactically
		// present for net/http to build a request at all.
		t.baseURL = "http://synapse-replication"
	} else {
		t.addr = url
		t.baseURL = strings.TrimSuffix(url, "/")
	}
	t.http = &http.Client{Transport: transport, Timeout: timeout}
	return t
}

// SetState relays a presence state for one device.
//
// isSync marks the update as sync-driven, which is what makes the writer
// refresh last_user_sync_ts and leave a BUSY state alone. Sync-driven updates
// are throttled per device while the state is unchanged; an explicit change
// always goes through immediately, and clears the throttle so the next
// sync-driven one does too. That mirrors WorkerPresenceHandler.set_state.
func (c *Client) SetState(ctx context.Context, userID, deviceID, state string, isSync bool) error {
	if c == nil {
		return nil
	}
	if !ValidState(state) {
		return fmt.Errorf("presence: invalid state %q", state)
	}

	k := key{userID, deviceID}
	now := c.now()

	c.mu.Lock()
	if isSync {
		if prev, ok := c.last[k]; ok &&
			prev.state == state && now.Sub(prev.when) < c.relay {
			c.mu.Unlock()
			metrics.PresenceRelaysSuppressed.Inc()
			return nil
		}
		c.last[k] = sent{state: state, when: now}
	} else {
		// An explicit update does not refresh the writer's last_user_sync_ts,
		// so it must not count as a recent relay.
		delete(c.last, k)
	}
	c.mu.Unlock()

	start := c.now()
	err := c.post(ctx, userID, deviceID, state, isSync)
	metrics.PresenceRelayDuration.Observe(c.now().Sub(start).Seconds())

	// A relay that failed for a reason configuration could explain is the
	// signal to go and re-read Synapse's config: the writer moves between
	// instances and the secret rotates, and neither event announces itself.
	// If it had in fact moved, the retry lands on the new writer and the
	// caller never sees a failure at all.
	if err != nil && c.reresolve(failureReason(err)) {
		start = c.now()
		err = c.post(ctx, userID, deviceID, state, isSync)
		metrics.PresenceRelayDuration.Observe(c.now().Sub(start).Seconds())
	}

	if err != nil {
		// A failed relay must not be remembered as sent, or the throttle would
		// suppress retries for a whole interval and the user would look offline
		// until it expired.
		c.mu.Lock()
		delete(c.last, k)
		c.mu.Unlock()
		metrics.PresenceRelayFailures.WithLabelValues(failureReason(err)).Inc()
		return err
	}
	metrics.PresenceRelays.Inc()
	return nil
}

// Forget drops a device's throttle entry.
func (c *Client) Forget(userID, deviceID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.last, key{userID, deviceID})
	c.mu.Unlock()
}

// Tracked reports how many devices the throttle is holding, for metrics.
func (c *Client) Tracked() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.last)
}

type setStateBody struct {
	DeviceID    *string         `json:"device_id"`
	State       json.RawMessage `json:"state"`
	ForceNotify bool            `json:"force_notify"`
	IsSync      bool            `json:"is_sync"`
}

func (c *Client) post(ctx context.Context, userID, deviceID, state string, isSync bool) error {
	t := c.current.Load()
	var dev *string
	if deviceID != "" {
		dev = &deviceID
	}
	body, err := json.Marshal(setStateBody{
		DeviceID:    dev,
		State:       json.RawMessage(`{"presence":` + quote(state) + `}`),
		ForceNotify: false,
		IsSync:      isSync,
	})
	if err != nil {
		return fmt.Errorf("presence: encode: %w", err)
	}

	// The user ID goes in the path and contains a colon and an @, both legal
	// in a path segment; Synapse's own client does not escape them and its
	// route regex expects them raw.
	url := t.baseURL + "/_synapse/replication/presence_set_state/" + userID

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("presence: request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+t.secret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.http.Do(req)
	if err != nil {
		return fmt.Errorf("presence: set state: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Distinguished from a transport failure: the writer is reachable and
		// said no, which is almost always a rotated replication secret.
		return fmt.Errorf("%w: answered %d", errWriterRefused, resp.StatusCode)
	}
	return nil
}

// reresolve re-reads Synapse's config after a failed relay and adopts a writer
// that has moved or a secret that has rotated. It reports whether anything
// changed, which is the caller's cue to retry.
//
// Only two failures can be explained by configuration:
//
//	unreachable  nothing accepted a connection. The writer moved off this
//	             socket, or moved to another instance entirely.
//	refused      it answered with something other than 200, which for this
//	             endpoint is almost always a rotated replication secret.
//
// A timeout means the writer is there and slow, and a cancelled request means
// the syncing client hung up; re-reading a file would help neither, and doing
// it on every timeout would turn an overloaded writer into a busy loop over
// homeserver.yaml.
func (c *Client) reresolve(reason string) bool {
	if c.resolve == nil {
		return false
	}
	if reason != metrics.PresenceUnreachable && reason != metrics.PresenceRefused {
		return false
	}

	c.resolveMu.Lock()
	defer c.resolveMu.Unlock()
	now := c.now()
	if !c.lastResolve.IsZero() && now.Sub(c.lastResolve) < c.resolveCooldown {
		return false
	}
	c.lastResolve = now

	cfg, err := c.resolve()
	if err != nil {
		// Not fatal, and deliberately not loud enough to drown the relay
		// failure that prompted it: the config being unreadable is a second
		// problem, not the one the user is having.
		metrics.PresenceConfigReloads.WithLabelValues(metrics.PresenceReloadError).Inc()
		c.log.Warn().Err(err).Msg("presence relay failed and Synapse's config could not be re-read")
		return false
	}

	current := c.current.Load()
	addr := cfg.Socket
	if addr == "" {
		addr = cfg.URL
	}
	if addr == current.addr && cfg.Secret == current.secret {
		metrics.PresenceConfigReloads.WithLabelValues(metrics.PresenceReloadUnchanged).Inc()
		return false
	}
	if addr == "" || cfg.Secret == "" {
		// Refusing an empty answer rather than adopting it: half a config is
		// how a running worker loses a working writer to a file that was being
		// rewritten as we read it.
		metrics.PresenceConfigReloads.WithLabelValues(metrics.PresenceReloadError).Inc()
		c.log.Warn().Msg("presence: Synapse's config named no writer or no secret; keeping the current one")
		return false
	}

	c.current.Store(newTarget(cfg.Socket, cfg.URL, cfg.Secret, c.timeout))
	metrics.PresenceConfigReloads.WithLabelValues(metrics.PresenceReloadChanged).Inc()
	c.log.Info().
		Str("from", current.addr).Str("to", addr).
		Bool("secret_changed", cfg.Secret != current.secret).
		Msg("presence writer moved; adopted from Synapse's config")
	return true
}

// failureReason classifies a relay failure into something actionable.
//
// The three real ones need different fixes and look identical in a flat count:
// a moved writer, a rotated secret, and an overloaded writer.
func failureReason(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		// The syncing client hung up while we were relaying. Neither our fault
		// nor the writer's, and kept out of the counts that are.
		return metrics.PresenceClientGone
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return metrics.PresenceTimeout
	case errors.Is(err, errWriterRefused):
		return metrics.PresenceRefused
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return metrics.PresenceTimeout
	}
	// Anything left reached neither a listener nor an answer: a socket path
	// that does not exist, a container that cannot see it, a refused dial.
	return metrics.PresenceUnreachable
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
