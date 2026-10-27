// Package config is the sync section of goworker.yaml.
//
// Only what is ours lives here. The database, the replication bus, the
// listener and the experimental flags are shared with the rest of the worker
// or read from homeserver.yaml.
package config

import (
	"fmt"
	"time"
)

// Config is the `sync:` block.
type Config struct {
	// FilterTimelineLimit caps an inline filter's timeline limit. Nil means
	// Synapse's 100; 0 means no timeline events at all; -1 means no cap.
	FilterTimelineLimit *int `yaml:"filter_timeline_limit"`
	// ToDevice turns on deleting acknowledged to-device messages: the one
	// write the classic sync path makes, through its own narrow role.
	ToDevice bool `yaml:"to_device"`
	// SlidingSync serves MSC4186. Off, its two paths are not registered.
	SlidingSync SlidingSync `yaml:"sliding_sync"`
	Auth        Auth        `yaml:"auth"`
	Caches      Caches      `yaml:"caches"`
}

// SlidingSync configures MSC4186.
type SlidingSync struct {
	Enabled bool `yaml:"enabled"`
	// ReapIntervalMinutes is how often connections unused for a week are
	// deleted. Zero means 60, Synapse's CONNECTION_EXPIRY_FREQUENCY.
	ReapIntervalMinutes int `yaml:"reap_interval_minutes"`
}

// Auth bounds the access-token caches (platform/clientauth): how long a
// lookup is kept without an invalidation, and how long /whoami's answers
// are, for what is not checked natively.
type Auth struct {
	PositiveTTL    time.Duration `yaml:"positive_ttl"`
	NegativeTTL    time.Duration `yaml:"negative_ttl"`
	MaxEntries     int           `yaml:"max_entries"`
	TimeoutSeconds int           `yaml:"timeout_seconds"`
}

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

	// FilteredStateCacheSize bounds the filtered state cache, in entries.
	// Zero takes the store's default; negative disables it, which is what
	// makes "is the cache hiding this bug?" a question that can be answered
	// rather than argued about. Events' state groups are in the shared state
	// cache (memory.event_group_cache_mb).
	FilteredStateCacheSize int `yaml:"filtered_state_cache_size"`

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

// TimelineLimitCap is the cap on an inline filter's timeline limit,
// defaulting to Synapse's 100.
func (c Config) TimelineLimitCap() int {
	if c.FilterTimelineLimit == nil {
		return 100
	}
	return *c.FilterTimelineLimit
}

// Validate refuses values the sync path cannot run with.
func (c Config) Validate() error {
	if c.Auth.PositiveTTL < 0 || c.Auth.NegativeTTL < 0 || c.Auth.MaxEntries < 0 || c.Auth.TimeoutSeconds < 0 {
		return fmt.Errorf("sync.auth: values must not be negative")
	}
	if c.SlidingSync.ReapIntervalMinutes < 0 {
		return fmt.Errorf("sync.sliding_sync.reap_interval_minutes must not be negative")
	}
	return nil
}

// Defaulted accessors. Zero means "take the default", so an explicit 0 is
// never mistaken for an unset field.

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
