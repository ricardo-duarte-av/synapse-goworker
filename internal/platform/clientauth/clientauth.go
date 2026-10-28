// Package clientauth authenticates client-API requests as Synapse's
// InternalAuth does (api/auth/internal.py, api/auth/base.py), reading
// access_tokens and the appservice registrations itself rather than asking
// main's /whoami.
//
// What it reproduces, in Synapse's order:
//
//   - the token, from one "Authorization: Bearer" header or ?access_token=,
//     never both (get_access_token_from_request);
//   - an appservice's as_token, its ip_range_whitelist, ?user_id= within its
//     namespaces and registered, ?device_id= an existing device
//     (get_appservice_user);
//   - otherwise access_tokens joined to users, puppets included; an expired
//     token is a soft logout; the token is marked used, which is what lets a
//     refresh token be retired; then a locked user is refused
//     (get_user_by_access_token, _wrapped_get_user_by_req);
//   - the request recorded for client_ips and MAU: USER_IP on the bus, as a
//     worker does (_record_request, client_ips.py).
//
// What it does not reproduce goes to /whoami: a token in neither
// access_tokens nor a loaded registration -- a guest's macaroon, which needs
// macaroon_secret_key this worker does not hold (operator, 2026-10-27), or a
// token that is simply wrong -- and an appservice whose namespace regex RE2
// cannot compile. Everything goes there when homeserver.yaml configures what
// InternalAuth defers to: delegated auth (MAS), account_validity, or modules
// (any of which may register is_user_expired).
//
// Synapse caches each lookup until the caches stream invalidates it; so does
// this, over the worker's one subscription, with a TTL behind it for an
// invalidation missed while the bus was down.
package clientauth

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cache"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Where a verdict came from.
const (
	SourceNative = "native"
	SourceWhoami = "whoami"
)

// Verdict is who a request is from, or why it was refused.
type Verdict struct {
	Valid    bool
	UserID   string
	DeviceID string
	IsGuest  bool

	// AppServiceID is set for an appservice's request.
	AppServiceID string
	// TokenID is access_tokens.id; zero for an appservice or a guest's
	// macaroon, as Synapse's requester has None.
	TokenID int64

	// Rejection is Synapse's refusal, when Valid is false.
	Rejection *Rejection

	Source string
}

// Rejection is a refusal, with the body Synapse sends for it.
type Rejection struct {
	Status  int
	ErrCode string
	Body    []byte
}

func reject(status int, errcode, msg string, extra map[string]any) *Rejection {
	m := map[string]any{"errcode": errcode, "error": msg}
	for k, v := range extra {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	return &Rejection{Status: status, ErrCode: errcode, Body: b}
}

// Synapse's refusals, word for word (api/errors.py, api/auth/).
func missingToken(msg string) *Rejection {
	return reject(http.StatusUnauthorized, "M_MISSING_TOKEN", msg, nil)
}

var (
	errExpired = reject(http.StatusUnauthorized, "M_UNKNOWN_TOKEN", "Access token has expired",
		map[string]any{"soft_logout": true})
	errLocked = reject(http.StatusUnauthorized, "M_USER_LOCKED", "User account has been locked",
		map[string]any{"soft_logout": true})
)

// Config is what an Authenticator needs.
type Config struct {
	ServerName      string
	Synapse         synapsecfg.ClientAuth
	AppServiceFiles []string

	// DSN and MaxConns open the pool for the lookups and for marking a
	// token used: its own, so authentication never queues behind an
	// initial sync.
	DSN            string
	MaxConns       int32
	ConnectTimeout time.Duration

	Whoami WhoamiConfig

	// TTL bounds how long a lookup is kept without an invalidation.
	TTL time.Duration
	// CacheBytes bounds each lookup cache.
	CacheBytes int64

	// Replication is the worker's subscription, for invalidations. Nil
	// leaves the TTL alone to expire lookups.
	Replication *bus.Consumer
	// Publisher sends USER_IP. Nil records nothing.
	Publisher Publisher

	Log zerolog.Logger
}

// Authenticator authenticates requests.
type Authenticator struct {
	native  bool
	reason  string // why not native
	cfg     synapsecfg.ClientAuth
	as      *appServices
	pool    *pgxpool.Pool
	whoami  *whoami
	ttl     time.Duration
	tokens  *cache.LRU[[sha256.Size]byte, timed[*tokenRow]]
	users   *cache.LRU[string, timed[userRow]]
	devices *cache.LRU[[2]string, timed[bool]]
	ids     *cache.LRU[[sha256.Size]byte, timed[int64]]
	ips     *userIPs
	repl    *bus.Consumer
	log     zerolog.Logger
}

type timed[V any] struct {
	v  V
	at time.Time
}

// tokenRow is _query_for_auth's row; nil cached means no such token.
type tokenRow struct {
	userID       string
	isGuest      bool
	tokenID      int64
	deviceID     string
	validUntilMS *int64
	tokenOwner   string
	used         bool
}

// userRow is what a users row answers: whether it exists (get_user_by_id)
// and whether it is locked (get_user_locked_status).
type userRow struct {
	exists bool
	locked bool
}

// New builds an Authenticator. Native authentication needs the pool and the
// registrations; without either it says why and asks /whoami for everything,
// as before.
func New(ctx context.Context, cfg Config) (*Authenticator, error) {
	w, err := newWhoami(cfg.Whoami)
	if err != nil {
		return nil, err
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	bytes := cfg.CacheBytes
	if bytes <= 0 {
		bytes = 16 << 20
	}
	a := &Authenticator{
		cfg:    cfg.Synapse,
		whoami: w,
		ttl:    ttl,
		tokens: cache.NewLRU[[sha256.Size]byte, timed[*tokenRow]]("client_auth_tokens", bytes,
			func(t timed[*tokenRow]) int64 {
				if t.v == nil {
					return 96
				}
				return 160 + int64(len(t.v.userID)+len(t.v.deviceID)+len(t.v.tokenOwner))
			}),
		users: cache.NewLRU[string, timed[userRow]]("client_auth_users", bytes/4,
			func(timed[userRow]) int64 { return 128 }),
		devices: cache.NewLRU[[2]string, timed[bool]]("client_auth_devices", bytes/4,
			func(timed[bool]) int64 { return 160 }),
		ids: cache.NewLRU[[sha256.Size]byte, timed[int64]]("client_auth_token_ids", bytes/4,
			func(timed[int64]) int64 { return 96 }),
		repl: cfg.Replication,
		log:  cfg.Log,
	}
	if cfg.Publisher != nil {
		a.ips = newUserIPs(cfg.Publisher, cfg.ServerName, cfg.Log)
	}

	switch {
	case cfg.Synapse.Delegated:
		a.reason = "delegated auth (MAS) is enabled"
	case cfg.Synapse.AccountValidity:
		a.reason = "account_validity is enabled"
	case cfg.Synapse.Modules > 0:
		a.reason = "modules are configured, and may decide account expiry"
	}
	if a.reason == "" {
		if a.as, err = loadAppServices(cfg.ServerName, cfg.AppServiceFiles); err != nil {
			a.reason = err.Error()
		}
	}
	// Under MAS there are no access_tokens rows. Otherwise the pool is
	// wanted even when /whoami answers: it gives /whoami's yes its token's
	// ID, which decides whose transaction_id an event shows.
	if !cfg.Synapse.Delegated {
		if a.pool, err = openPool(ctx, cfg); err != nil {
			if a.reason == "" {
				a.reason = err.Error()
			} else {
				cfg.Log.Warn().Err(err).Msg("client auth: no pool; events will not show their sender's transaction_id")
			}
		}
	}
	a.native = a.reason == ""
	if a.native {
		for _, why := range a.as.unsupported {
			cfg.Log.Warn().Str("why", why).Msg("client auth: an appservice's requests go to /whoami")
		}
		cfg.Log.Info().Int("appservices", len(a.as.byToken)).Msg("client auth: native")
		nativeGauge.Set(1)
	} else {
		cfg.Log.Warn().Str("why", a.reason).Msg("client auth: every token goes to /whoami")
		nativeGauge.Set(0)
	}
	return a, nil
}

func openPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("clientauth: parse dsn: %w", err)
	}
	pcfg.MaxConns = 4
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}
	// Behind pgcat's transaction pooling a described statement is gone by
	// the time it is executed.
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("clientauth: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("clientauth: ping: %w", err)
	}
	return pool, nil
}

// Close releases the pool and the publisher.
func (a *Authenticator) Close() {
	if a.pool != nil {
		a.pool.Close()
	}
	if a.ips != nil {
		a.ips.close()
	}
}

// Native reports whether tokens are checked here, and if not, why.
func (a *Authenticator) Native() (bool, string) { return a.native, a.reason }

// Len is how many verdicts and lookups are cached, for metrics.
func (a *Authenticator) Len() int {
	return a.whoami.len() + a.tokens.Stats().Entries
}

// Authenticate is get_user_by_req. A non-nil error means the answer is
// unknown -- the database or Synapse unreachable -- which a caller must
// answer with 5xx, never 401: refusing a valid token for want of an answer
// logs real clients out.
func (a *Authenticator) Authenticate(ctx context.Context, r *http.Request) (Verdict, error) {
	v, err := a.authenticate(ctx, r)
	switch {
	case err != nil:
		verdicts.WithLabelValues("unknown", "").Inc()
	case v.Valid:
		verdicts.WithLabelValues("accepted", v.Source).Inc()
	default:
		verdicts.WithLabelValues("rejected", v.Source).Inc()
	}
	return v, err
}

func (a *Authenticator) authenticate(ctx context.Context, r *http.Request) (Verdict, error) {
	token, rej := TokenFromRequest(r)
	if rej != nil {
		return Verdict{Rejection: rej, Source: SourceNative}, nil
	}
	if !a.native {
		v, err := a.whoami.authenticate(ctx, r, token)
		if err == nil && v.Valid && a.pool != nil {
			// Synapse's requester carries the token's ID; /whoami does not
			// say it.
			if v.TokenID, err = a.tokenID(ctx, token); err != nil {
				return Verdict{}, err
			}
		}
		return v, err
	}
	q := r.URL.Query()
	ip := ClientIP(r)

	if as, ok := a.as.byToken[token]; ok && as.allowsIP(ip) {
		v, err := a.appServiceUser(ctx, as, q)
		if err != nil || !v.Valid {
			return v, err
		}
		a.record(r, token, ip, v, v.UserID)
		return v, nil
	}
	if _, ok := a.as.unsupported[token]; ok {
		return a.whoami.authenticate(ctx, r, token)
	}

	row, err := a.tokenRow(ctx, token)
	if err != nil {
		return Verdict{}, err
	}
	if row == nil {
		// Not an opaque token: perhaps a guest's macaroon, more likely
		// wrong. Synapse decides either way.
		return a.whoami.authenticate(ctx, r, token)
	}
	if row.validUntilMS != nil && *row.validUntilMS < time.Now().UnixMilli() {
		return Verdict{Rejection: errExpired, Source: SourceNative}, nil
	}
	if !row.used {
		if err := a.markUsed(ctx, token, row); err != nil {
			return Verdict{}, err
		}
	}
	u, err := a.userRow(ctx, row.userID)
	if err != nil {
		return Verdict{}, err
	}
	if u.locked {
		return Verdict{Rejection: errLocked, Source: SourceNative}, nil
	}
	v := Verdict{
		Valid:    true,
		UserID:   row.userID,
		DeviceID: row.deviceID,
		IsGuest:  row.isGuest,
		TokenID:  row.tokenID,
		Source:   SourceNative,
	}
	a.record(r, token, ip, v, row.tokenOwner)
	return v, nil
}

// TokenFromRequest is get_access_token_from_request: one Authorization
// header, "Bearer" and the token separated by exactly one space, or else
// the first ?access_token=, and never both.
func TokenFromRequest(r *http.Request) (string, *Rejection) {
	headers := r.Header.Values("Authorization")
	query, inQuery := r.URL.Query()["access_token"]
	if len(headers) > 0 {
		if inQuery {
			return "", missingToken("Mixing Authorization headers and access_token query parameters.")
		}
		if len(headers) > 1 {
			return "", missingToken("Too many Authorization headers.")
		}
		parts := strings.Split(headers[0], " ")
		if parts[0] == "Bearer" && len(parts) == 2 {
			return parts[1], nil
		}
		return "", missingToken("Invalid Authorization header.")
	}
	if len(query) == 0 {
		return "", missingToken("Missing access token")
	}
	return query[0], nil
}

// ClientIP is the address Synapse records behind a proxy (x_forwarded,
// http/site.py): the first X-Forwarded-For entry; else, on a unix socket,
// the socket's path (get_client_ip_if_available); else the peer.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
		ip, _, _ := strings.Cut(xff[0], ",")
		return strings.TrimSpace(ip)
	}
	if la, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok && la.Network() == "unix" {
		return la.String()
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// appServiceUser is the rest of get_appservice_user, once the token is
// known to be this service's and its address allowed.
func (a *Authenticator) appServiceUser(ctx context.Context, as *appService, q url.Values) (Verdict, error) {
	userID := as.sender
	if v, ok := q["user_id"]; ok {
		userID = v[0]
		// validate_appservice_can_control_user_id.
		if userID != as.sender {
			if !as.interestedInUser(userID) {
				return Verdict{Source: SourceNative, Rejection: reject(http.StatusForbidden, "M_FORBIDDEN",
					fmt.Sprintf("Application service cannot masquerade as this user (%s).", userID), nil)}, nil
			}
			u, err := a.userRow(ctx, userID)
			if err != nil {
				return Verdict{}, err
			}
			if !u.exists {
				return Verdict{Source: SourceNative, Rejection: reject(http.StatusForbidden, "M_FORBIDDEN",
					fmt.Sprintf("Application service has not registered this user (%s)", userID), nil)}, nil
			}
		}
	}
	deviceID := ""
	if v := q["device_id"]; len(v) > 0 {
		deviceID = v[0]
		ok, err := a.device(ctx, userID, deviceID)
		if err != nil {
			return Verdict{}, err
		}
		if !ok {
			return Verdict{Source: SourceNative, Rejection: reject(http.StatusBadRequest, "M_UNKNOWN_DEVICE",
				fmt.Sprintf("Application service trying to use a device that doesn't exist ('%s' for %s)",
					deviceID, userID), nil)}, nil
		}
	}
	return Verdict{Valid: true, UserID: userID, DeviceID: deviceID, AppServiceID: as.id, Source: SourceNative}, nil
}

// record is _record_request: USER_IP for the entity that authenticated, and
// for the puppeted user too when track_puppeted_user_ips is on.
func (a *Authenticator) record(r *http.Request, token, ip string, v Verdict, entity string) {
	if a.ips == nil || ip == "" {
		return
	}
	if v.AppServiceID != "" && !a.cfg.TrackAppserviceUserIPs {
		return
	}
	ua := userAgent(r)
	device := v.DeviceID
	if device == "" && v.AppServiceID != "" {
		device = "dummy-device"
	}
	a.ips.insert(entity, token, ip, ua, device)
	if v.UserID != entity && a.cfg.TrackPuppetedUserIPs {
		a.ips.insert(v.UserID, token, ip, ua, v.DeviceID)
	}
}

// userAgent is get_request_user_agent: the last User-Agent header, decoded
// as ASCII with anything else replaced.
func userAgent(r *http.Request) string {
	h := r.Header.Values("User-Agent")
	if len(h) == 0 {
		return ""
	}
	s := h[len(h)-1]
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] < 0x80 {
			b.WriteByte(s[i])
		} else {
			b.WriteRune('�')
		}
	}
	return b.String()
}

func (a *Authenticator) fresh(at time.Time) bool { return time.Since(at) < a.ttl }

// tokenRow is get_user_by_access_token, cached until invalidated.
func (a *Authenticator) tokenRow(ctx context.Context, token string) (*tokenRow, error) {
	key := sha256.Sum256([]byte(token))
	if t, ok := a.tokens.Get(key); ok && a.fresh(t.at) {
		return t.v, nil
	}
	// _query_for_auth, verbatim.
	const q = `
		SELECT users.name, users.is_guest, access_tokens.id, access_tokens.device_id,
			access_tokens.valid_until_ms, access_tokens.user_id, access_tokens.used
		FROM users
		INNER JOIN access_tokens ON users.name = COALESCE(puppets_user_id, access_tokens.user_id)
		WHERE token = $1`
	var (
		row      tokenRow
		isGuest  *int16
		deviceID *string
		used     *bool
	)
	start := time.Now()
	err := a.pool.QueryRow(ctx, q, token).Scan(&row.userID, &isGuest, &row.tokenID, &deviceID,
		&row.validUntilMS, &row.tokenOwner, &used)
	queries.WithLabelValues("token").Observe(time.Since(start).Seconds())
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		a.tokens.Add(key, timed[*tokenRow]{nil, time.Now()})
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("clientauth: token lookup: %w", err)
	}
	row.isGuest = isGuest != nil && *isGuest != 0
	row.used = used != nil && *used
	if deviceID != nil {
		row.deviceID = *deviceID
	}
	a.tokens.Add(key, timed[*tokenRow]{&row, time.Now()})
	return &row, nil
}

// tokenID is access_tokens.id for a token /whoami accepted; zero for one
// with no row (an appservice's, a guest's macaroon), as Synapse's
// requester has None.
func (a *Authenticator) tokenID(ctx context.Context, token string) (int64, error) {
	key := sha256.Sum256([]byte(token))
	if t, ok := a.ids.Get(key); ok && a.fresh(t.at) {
		return t.v, nil
	}
	var id int64
	start := time.Now()
	err := a.pool.QueryRow(ctx, `SELECT id FROM access_tokens WHERE token = $1`, token).Scan(&id)
	queries.WithLabelValues("token_id").Observe(time.Since(start).Seconds())
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("clientauth: token ID lookup: %w", err)
	}
	a.ids.Add(key, timed[int64]{id, time.Now()})
	return id, nil
}

// markUsed is mark_access_token_as_used: the first use of a token retires
// the refresh token that produced it (lookup_refresh_token). It only ever
// goes from false to true, so it is written once.
func (a *Authenticator) markUsed(ctx context.Context, token string, row *tokenRow) error {
	start := time.Now()
	_, err := a.pool.Exec(ctx, `UPDATE access_tokens SET used = TRUE WHERE id = $1`, row.tokenID)
	queries.WithLabelValues("mark_used").Observe(time.Since(start).Seconds())
	if err != nil {
		return fmt.Errorf("clientauth: marking a token used: %w", err)
	}
	marked := *row
	marked.used = true
	a.tokens.Add(sha256.Sum256([]byte(token)), timed[*tokenRow]{&marked, time.Now()})
	return nil
}

// userRow is get_user_by_id's existence and get_user_locked_status.
func (a *Authenticator) userRow(ctx context.Context, userID string) (userRow, error) {
	if t, ok := a.users.Get(userID); ok && a.fresh(t.at) {
		return t.v, nil
	}
	var locked *bool
	start := time.Now()
	err := a.pool.QueryRow(ctx, `SELECT locked FROM users WHERE name = $1`, userID).Scan(&locked)
	queries.WithLabelValues("user").Observe(time.Since(start).Seconds())
	var u userRow
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return userRow{}, fmt.Errorf("clientauth: user lookup: %w", err)
	default:
		u = userRow{exists: true, locked: locked != nil && *locked}
	}
	a.users.Add(userID, timed[userRow]{u, time.Now()})
	return u, nil
}

// device is get_device: a device that exists and is not hidden.
func (a *Authenticator) device(ctx context.Context, userID, deviceID string) (bool, error) {
	key := [2]string{userID, deviceID}
	if t, ok := a.devices.Get(key); ok && a.fresh(t.at) {
		return t.v, nil
	}
	var one int
	start := time.Now()
	err := a.pool.QueryRow(ctx,
		`SELECT 1 FROM devices WHERE user_id = $1 AND device_id = $2 AND hidden = FALSE`,
		userID, deviceID).Scan(&one)
	queries.WithLabelValues("device").Observe(time.Since(start).Seconds())
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		a.devices.Add(key, timed[bool]{false, time.Now()})
		return false, nil
	case err != nil:
		return false, fmt.Errorf("clientauth: device lookup: %w", err)
	}
	a.devices.Add(key, timed[bool]{true, time.Now()})
	return true, nil
}
