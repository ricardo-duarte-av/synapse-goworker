// Package config loads the gosync-worker configuration.
//
// The shape follows synapse-gopro-worker's config deliberately: a strict YAML
// decode (unknown fields are an error, so a typo in a socket path fails at
// startup rather than being silently ignored), defaults applied before decoding,
// and a validate() that cross-checks fields.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level worker configuration.
type Config struct {
	// ServerName is this homeserver's name, e.g. "example.com".
	//
	// It is also the default Redis pub/sub channel name: Synapse's replication
	// traffic is published on a channel named after the homeserver.
	ServerName string `yaml:"server_name"`

	// Caches mirrors the parts of Synapse's `caches` block that change what a
	// response contains.
	Caches Caches `yaml:"caches"`

	// FilterTimelineLimit caps `room.timeline.limit` in an INLINE filter, and
	// only an inline one: Synapse applies set_timeline_upper_limit when the
	// filter arrives as JSON in the query string, and not when it is looked up
	// by ID. Synapse's default is 100; -1 disables the cap.
	FilterTimelineLimit *int `yaml:"filter_timeline_limit"`

	// SynapseConfig is the path to Synapse's own homeserver.yaml, read at
	// every start.
	//
	// Presence needs three facts that live there and change without anybody
	// thinking about this worker: worker_replication_secret, which instance
	// holds stream_writers.presence, and that instance's address in
	// instance_map. Copying them here would create a duplicate that fails
	// silently when it drifts. Empty disables everything that depends on it.
	SynapseConfig string `yaml:"synapse_config"`

	Listen       Listen       `yaml:"listen"`
	Database     Database     `yaml:"database"`
	ToDevice     ToDevice     `yaml:"to_device"`
	SlidingSync  SlidingSync  `yaml:"sliding_sync"`
	Replication  Replication  `yaml:"replication"`
	Auth         Auth         `yaml:"auth"`
	Reference    Reference    `yaml:"reference"`
	Experimental Experimental `yaml:"experimental"`
	Testing      Testing      `yaml:"testing"`
	Metrics      Metrics      `yaml:"metrics"`
	Log          Log          `yaml:"log"`
}

// Listen describes where this worker serves the client API.
//
// nginx routes to it on its own hostname only (CLAUDE.md §2).
type Listen struct {
	// Socket is a unix socket path, matching the Synapse worker convention.
	Socket string `yaml:"socket"`
	// Addr is a TCP address such as ":8090". Exactly one of Socket or Addr.
	Addr string `yaml:"addr"`
	// SocketMode is the permission bits applied to Socket, as an octal string.
	// Defaults to "0660", which suffices for cmd/syncdiff alone. Behind nginx it
	// must be "0666": nginx runs in a separate container as a different uid.
	SocketMode string `yaml:"socket_mode"`
}

// Database is read-only access to Synapse's PostgreSQL.
//
// The DSN is libpq keyword form rather than a postgres:// URI, matching
// gopro-worker and deploy/readonly-role.sql. A unix socket needs no password.
type Database struct {
	DSN string `yaml:"dsn"`
	// MaxConns is bounded well below Synapse's own pool so this worker cannot
	// starve it. Zero means 16.
	MaxConns int `yaml:"max_conns"`
	// ConnectTimeoutSeconds bounds pool acquisition at startup. Zero means 10.
	ConnectTimeoutSeconds int `yaml:"connect_timeout_seconds"`
}

// ToDevice governs the to_device section of /sync, and with it the one write
// this worker makes.
//
// Serving to_device and deleting it are a single decision. Synapse's /sync
// deletes the messages a device has acknowledged, bounded by its `since`; a
// worker that served the section without deleting would hand a client the same
// room keys on every sync for ever. So there is no "serve but do not delete"
// setting: leave this disabled and the section is omitted entirely, which is
// correct as long as a real Synapse worker is also syncing that device.
//
// The DSN must name a role granted SELECT and DELETE on device_inbox and
// nothing more (deploy/device-inbox-role.sql). It is deliberately a second
// connection: the main pool keeps its read-only role and its startup check, so
// the read-only guarantee is weakened in exactly one named place.
type ToDevice struct {
	Enabled bool `yaml:"enabled"`
	// DSN is a libpq connection string for the deleting role.
	DSN string `yaml:"dsn"`
	// MaxConns bounds the deleting pool. Zero means 4.
	MaxConns int `yaml:"max_conns"`
	// ConnectTimeoutSeconds bounds the initial connection. Zero means 10.
	ConnectTimeoutSeconds int `yaml:"connect_timeout_seconds"`
}

// SlidingSync configures MSC4186, which is the project's second write and the
// only stateful endpoint it serves.
//
// Disabled by default. With it off the two endpoints are not registered at all
// and probing either returns M_UNRECOGNIZED, which is what a client should see
// from a server that does not implement them.
type SlidingSync struct {
	Enabled bool `yaml:"enabled"`
	// DSN is a libpq connection string for the role from
	// deploy/sliding-sync-role.sql: owner of the `gosync` schema, with nothing
	// in `public`. The worker checks that narrowness at startup and warns about
	// a role that can read Synapse's tables; it refuses only a role that cannot
	// write `gosync`.
	DSN string `yaml:"dsn"`
	// MaxConns bounds the writing pool. Zero means 8.
	MaxConns int `yaml:"max_conns"`
	// ConnectTimeoutSeconds bounds the initial connection. Zero means 10.
	ConnectTimeoutSeconds int `yaml:"connect_timeout_seconds"`
	// ReapIntervalMinutes is how often stale connections are collected. Zero
	// means 60, matching Synapse's CONNECTION_EXPIRY_FREQUENCY.
	//
	// Not optional in effect: reading a position prunes only that connection's
	// own forks, and nothing else prunes a connection whose client stopped
	// coming back. Without the reaper the six tables grow without bound.
	ReapIntervalMinutes int `yaml:"reap_interval_minutes"`
}

// Replication consumes Synapse's replication stream over Redis (KeyDB here).
//
// A sync worker needs far more of this stream than gopro-worker did: that one
// acted only on the `caches` stream, while every long-poll wakeup here comes
// from `events`, `typing`, `receipts`, `to_device`, `presence`, `account_data`,
// `device_lists` and `push_rules`. It is read-only (SUBSCRIBE only).
type Replication struct {
	Enabled bool `yaml:"enabled"`
	// Address is a unix socket path, or host:port.
	Address string `yaml:"address"`
	// Channel MUST equal Synapse's server_name: the channel is named after it,
	// and one Redis can carry several homeservers' streams. Subscribing to the
	// wrong channel looks perfectly healthy and delivers nothing, so a sync
	// worker on the wrong channel simply never wakes up. Defaults to ServerName.
	Channel  string `yaml:"channel"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// Auth resolves an access token to a user and device.
//
// Deliberately NOT a lookup against the access_tokens table, even though
// Synapse's own lookup is a plain `WHERE token = ?` with no hashing. Three
// kinds of caller would be wrongly rejected by that query: appservice tokens
// live in registration files and never reach the database; delegated auth (MAS)
// keeps tokens outside Synapse entirely; and guest tokens are macaroons,
// verifiable only with macaroon_secret_key, which this worker must never hold.
// Asking Synapse via /account/whoami sidesteps all three and yields the
// device_id that a per-device endpoint like /sync cannot work without.
type Auth struct {
	// WhoamiSocket is a Synapse client-API worker's unix socket.
	WhoamiSocket string `yaml:"whoami_socket"`
	// WhoamiURL is used when WhoamiSocket is empty.
	WhoamiURL string `yaml:"whoami_url"`
	// PositiveTTL caches a successful lookup. Zero means 5m.
	PositiveTTL time.Duration `yaml:"positive_ttl"`
	// NegativeTTL caches a rejection, so a bad token cannot become a stream of
	// requests to Synapse. Zero means 30s.
	NegativeTTL time.Duration `yaml:"negative_ttl"`
	// MaxEntries bounds the token cache. Zero means 20000.
	MaxEntries int `yaml:"max_entries"`
	// TimeoutSeconds bounds one whoami call. Zero means 10.
	TimeoutSeconds int `yaml:"timeout_seconds"`
}

// Reference is the dedicated Synapse sync worker that cmd/syncdiff compares
// against. Nothing is routed to it either.
//
// It MUST be a worker with `sync_response_cache_duration: 0`. Synapse caches a
// sync response keyed by (user, timeout, since, filter, full_state, device_id,
// ...) for two minutes by default, so a comparator that replays a request would
// otherwise be handed a frozen earlier answer and told it matched.
type Reference struct {
	Socket string `yaml:"socket"`
	URL    string `yaml:"url"`
}

// Experimental mirrors the parts of Synapse's `experimental_features` block
// that change what a response contains, using Synapse's own field names so the
// settings paste straight across from homeserver.yaml.
//
// These are read from our config rather than from homeserver.yaml, which is
// never mounted: it carries macaroon_secret_key, registration_shared_secret and
// the database password inline.
type Experimental struct {
	// MSC4354Enabled adds `unsigned.msc4354_sticky_duration_ttl_ms` to sticky
	// events. Synapse defaults this to false; this deployment sets it true.
	MSC4354Enabled bool `yaml:"msc4354_enabled"`
	// MSC3391Enabled treats account data with empty content as deleted, and
	// omits it. Synapse defaults this to false; this deployment sets it true.
	MSC3391Enabled bool `yaml:"msc3391_enabled"`
	// MSC3575Enabled is the homeserver-wide default for sliding sync, which a
	// per-user row in `per_user_experimental_features` overrides in either
	// direction. Synapse defaults it to TRUE, unlike the flags around it.
	MSC3575Enabled bool `yaml:"msc3575_enabled"`
	// MSC4308Enabled gates the thread-subscriptions sliding sync extension.
	// Synapse defaults it to false; this deployment sets it true.
	MSC4308Enabled bool `yaml:"msc4308_enabled"`

	// The following gate individual base push rules. Each one adds or removes
	// a rule from every user's reported ruleset, so a wrong value here is
	// wrong for everybody. Synapse defaults them all to false.
	MSC1767Enabled             bool `yaml:"msc1767_enabled"`
	MSC3381PollsEnabled        bool `yaml:"msc3381_polls_enabled"`
	MSC3664Enabled             bool `yaml:"msc3664_enabled"`
	MSC4028PushEncryptedEvents bool `yaml:"msc4028_push_encrypted_events"`
	// MSC4210 REMOVES the legacy mention rules when enabled, the opposite
	// sense to the others.
	MSC4210Enabled bool `yaml:"msc4210_enabled"`
	MSC4306Enabled bool `yaml:"msc4306_enabled"`
	// MSC4222Enabled lets a client ask for `state_after` instead of `state`.
	// Without it the query parameter is ignored, as Synapse ignores it.
	MSC4222Enabled bool `yaml:"msc4222_enabled"`
	// MSC3773Enabled makes a filter's
	// `org.matrix.msc3773.unread_thread_notifications` an alias for the stable
	// field. It changes only how a filter is read, not what is returned.
	MSC3773Enabled bool `yaml:"msc3773_enabled"`
	// MSC3874Enabled enables a filter's `rel_types` fields. They filter
	// /messages rather than /sync, so this is here for completeness: a filter
	// carrying them must be read the same way by both.
	MSC3874Enabled bool `yaml:"msc3874_enabled"`
}

// Caches configures the worker's caches.
//
// LazyLoadMembersCacheSize is the one whose size is visible in responses -- it
// changes how many member events a lazy-loading sync repeats, so it must match
// the reference server. The state caches below are invisible to a client:
// everything in them is immutable, so their size trades memory for round trips
// and nothing else.
type Caches struct {
	// LazyLoadMembersCacheSize is how many member events one device's
	// lazy-loading cache remembers.
	//
	// Synapse asks for 100 and multiplies by `caches.global_factor`, whose
	// default is 0.5 -- so a default deployment remembers 50. Set this to
	// match the reference server, or lazy-loading syncs will differ in how
	// many member events they repeat.
	LazyLoadMembersCacheSize int `yaml:"lazy_load_members_cache_size"`
	// LazyLoadMembersCacheTTL is how long a device's cache lives, measured
	// from creation rather than last use, as Synapse's ExpiringCache does.
	LazyLoadMembersCacheTTL time.Duration `yaml:"lazy_load_members_cache_ttl"`

	// StateGroupCacheSize and FilteredStateCacheSize bound the two state
	// caches, in entries. Zero takes the store's default; negative disables
	// the cache, which is what makes "is the cache hiding this bug?" a
	// question that can be answered rather than argued about.
	StateGroupCacheSize    int `yaml:"state_group_cache_size"`
	FilteredStateCacheSize int `yaml:"filtered_state_cache_size"`

	// Enabled is the master switch for the derived caches -- the ones holding
	// data that can change, invalidated over replication.
	//
	// It exists to be turned OFF. Two builds of one commit, one with caches
	// and one without, compared against each other with cmd/syncdiff, is the
	// only way to test a cache: both sides are this worker, so every
	// legitimate reason two /sync answers differ falls away and what is left
	// is a cache bug. Defaults to true; set false for that comparison, and to
	// answer "is the cache hiding this?" about anything else.
	Enabled bool `yaml:"enabled"`

	// Per-cache sizes in entries. Zero takes a default; negative disables one
	// cache without disabling the rest.
	UserFilterCacheSize        int `yaml:"user_filter_cache_size"`
	RoomInfoCacheSize          int `yaml:"room_info_cache_size"`
	AccessTokenCacheSize       int `yaml:"access_token_cache_size"`
	RoomSummaryCacheSize       int `yaml:"room_summary_cache_size"`
	HistoryVisibilityCacheSize int `yaml:"history_visibility_cache_size"`
	IgnoredUsersCacheSize      int `yaml:"ignored_users_cache_size"`
	RoomsForUserCacheSize      int `yaml:"rooms_for_user_cache_size"`

	// Stream-change cache sizes, in distinct stream POSITIONS rather than
	// entities -- one position can name many rooms, so these do not scale the
	// way the entry-counted caches above do.
	//
	// They are the wrong knob to reach for when tuning memory and the right one
	// when a gate stops working: a cache too small for a busy stream has its
	// horizon climb above the `since` values clients actually send, every
	// question falls below it, and the queries quietly come back. Watch
	// gosync_stream_cache_earliest_position rather than guessing.
	EventsStreamCacheSize      int `yaml:"events_stream_cache_size"`
	MembershipStreamCacheSize  int `yaml:"membership_stream_cache_size"`
	ReceiptsStreamCacheSize    int `yaml:"receipts_stream_cache_size"`
	AccountDataStreamCacheSize int `yaml:"account_data_stream_cache_size"`
	ToDeviceStreamCacheSize    int `yaml:"to_device_stream_cache_size"`
	PresenceStreamCacheSize    int `yaml:"presence_stream_cache_size"`
}

// Testing gates behaviour that exists only to make the comparator possible.
type Testing struct {
	// AllowPinNow enables the ?_gosync_now=<token> query parameter, which
	// forces the now_token this worker computes against instead of reading the
	// current stream position.
	//
	// This is the hinge the whole verification story turns on. Two sync
	// implementations asked the same question at different instants give
	// different answers for entirely legitimate reasons, because each snapshots
	// the current stream position at its own start. Pinning both to one token
	// makes the comparison meaningful.
	//
	// It also lets a caller ask for a window that has not happened yet, so it
	// MUST be false in production.
	AllowPinNow bool `yaml:"allow_pin_now"`
}

// Metrics configures the Prometheus listener.
type Metrics struct {
	Addr string `yaml:"addr"`
}

// Log configures logging.
type Log struct {
	// Level is one of trace, debug, info, warn, error.
	Level string `yaml:"level"`
	// Pretty enables human-readable console output instead of JSON. For eyes
	// only: leave it off if anything downstream parses the log.
	Pretty bool `yaml:"pretty"`
}

// Load reads and validates the configuration at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse decodes and validates a configuration document.
func Parse(data []byte) (*Config, error) {
	cfg := &Config{
		Listen:  Listen{SocketMode: "0660"},
		Metrics: Metrics{Addr: ":9201"},
		Log:     Log{Level: "info"},
		// Caches on unless the document says otherwise. Set before decoding
		// rather than after, so an explicit `enabled: false` still wins: the
		// decoder only writes fields the document actually contains.
		Caches: Caches{Enabled: true},
		// Synapse's msc3575_enabled defaults to TRUE, unlike the experimental
		// flags around it. Set here for the same reason as Caches: an explicit
		// `false` in the document must still win.
		Experimental: Experimental{MSC3575Enabled: true},
	}

	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// TimelineLimitCap is the configured cap on an inline filter's timeline limit,
// defaulting to Synapse's 100.
//
// A pointer in the struct, because 0 is a meaningful value here -- it means
// "return no timeline events at all" -- and so is -1, which disables the cap.
func (c *Config) TimelineLimitCap() int {
	if c.FilterTimelineLimit == nil {
		return 100
	}
	return *c.FilterTimelineLimit
}

func (c *Config) validate() error {
	if c.ServerName == "" {
		return fmt.Errorf("server_name is required")
	}
	if (c.Listen.Socket == "") == (c.Listen.Addr == "") {
		return fmt.Errorf("listen: exactly one of socket or addr must be set")
	}
	if c.Listen.SocketMode == "" {
		c.Listen.SocketMode = "0660"
	}
	if _, err := c.Listen.ParsedSocketMode(); err != nil {
		return err
	}
	if c.Database.DSN == "" {
		return fmt.Errorf("database: dsn is required")
	}
	if c.Database.MaxConns < 0 || c.Database.ConnectTimeoutSeconds < 0 {
		return fmt.Errorf("database: values must not be negative")
	}
	if c.ToDevice.Enabled {
		if c.ToDevice.DSN == "" {
			return fmt.Errorf("to_device: dsn is required when enabled")
		}
	}
	if c.ToDevice.MaxConns < 0 || c.ToDevice.ConnectTimeoutSeconds < 0 {
		return fmt.Errorf("to_device: values must not be negative")
	}
	if c.Replication.Enabled && c.Replication.Address == "" {
		return fmt.Errorf("replication: address is required when enabled")
	}
	if c.Replication.Channel == "" {
		c.Replication.Channel = c.ServerName
	}
	if (c.Auth.WhoamiSocket == "") == (c.Auth.WhoamiURL == "") {
		return fmt.Errorf("auth: exactly one of whoami_socket or whoami_url must be set")
	}
	if c.Auth.PositiveTTL < 0 || c.Auth.NegativeTTL < 0 || c.Auth.MaxEntries < 0 || c.Auth.TimeoutSeconds < 0 {
		return fmt.Errorf("auth: values must not be negative")
	}
	if c.Reference.Socket != "" && c.Reference.URL != "" {
		return fmt.Errorf("reference: set at most one of socket or url")
	}
	if c.SlidingSync.Enabled {
		if c.SlidingSync.DSN == "" {
			return fmt.Errorf("sliding_sync.enabled is set but sliding_sync.dsn is empty; " +
				"see deploy/sliding-sync-role.sql")
		}
		// Deliberately no check against database.dsn: a deployment with a
		// single database user shares it. slidingstore.Open still refuses a
		// read-only role at startup, and warns about one that can read public.
	}
	return nil
}

// ParsedSocketMode returns the listen socket permission bits.
func (l Listen) ParsedSocketMode() (os.FileMode, error) {
	mode := l.SocketMode
	if mode == "" {
		mode = "0660"
	}
	n, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("listen: invalid socket_mode %q: %w", l.SocketMode, err)
	}
	return os.FileMode(n), nil
}

// Defaulted accessors. Zero in the file means "take the default", so that an
// explicit 0 is never mistaken for an unset field.

func (d Database) Conns() int32 {
	if d.MaxConns == 0 {
		return 16
	}
	return int32(d.MaxConns)
}

func (d Database) ConnectTimeout() time.Duration {
	if d.ConnectTimeoutSeconds == 0 {
		return 10 * time.Second
	}
	return time.Duration(d.ConnectTimeoutSeconds) * time.Second
}

func (t ToDevice) Conns() int32 {
	if t.MaxConns == 0 {
		return 4
	}
	return int32(t.MaxConns)
}

func (t ToDevice) ConnectTimeout() time.Duration {
	if t.ConnectTimeoutSeconds == 0 {
		return 10 * time.Second
	}
	return time.Duration(t.ConnectTimeoutSeconds) * time.Second
}

func (a Auth) Positive() time.Duration {
	if a.PositiveTTL == 0 {
		return 5 * time.Minute
	}
	return a.PositiveTTL
}

func (a Auth) Negative() time.Duration {
	if a.NegativeTTL == 0 {
		return 30 * time.Second
	}
	return a.NegativeTTL
}

func (a Auth) Entries() int {
	if a.MaxEntries == 0 {
		return 20000
	}
	return a.MaxEntries
}

func (a Auth) Timeout() time.Duration {
	if a.TimeoutSeconds == 0 {
		return 10 * time.Second
	}
	return time.Duration(a.TimeoutSeconds) * time.Second
}
