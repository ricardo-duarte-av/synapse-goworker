// Package config loads goworker.yaml, the worker's one configuration file.
//
// It says how to reach things and how big to make them, and nothing about
// what the worker does: nginx decides which requests reach it, and
// homeserver.yaml decides whether it sends federation. Everything Synapse
// also needs to agree on -- server_name, redis, the stream writers, the
// signing key, the experimental flags -- is read from homeserver.yaml, so the
// two cannot drift apart.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	fedread "github.com/ricardo-duarte-av/synapse-goworker/internal/fedread/app"
	inbound "github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/limits"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/proxy"
	sender "github.com/ricardo-duarte-av/synapse-goworker/internal/sender/config"
	syncc "github.com/ricardo-duarte-av/synapse-goworker/internal/sync/config"
)

// Config is goworker.yaml.
type Config struct {
	// HomeserverYAML is Synapse's own config, mounted read-only.
	HomeserverYAML string `yaml:"homeserver_yaml"`
	// InstanceName is this process's name: what federation_sender_instances
	// must list for it to send, its name in Synapse's lock tables and on the
	// caches stream, and what its sender cursors are keyed by.
	InstanceName string `yaml:"instance_name"`

	Listen   Listen        `yaml:"listen"`
	Upstream proxy.Config  `yaml:"upstream"`
	Database Database      `yaml:"database"`
	Memory   Memory        `yaml:"memory"`
	Limits   limits.Config `yaml:"limits"`
	Log      Log           `yaml:"log"`

	Sync            syncc.Config   `yaml:"sync"`
	FederationReads fedread.Config `yaml:"federation_reads"`
	Inbound         inbound.Config `yaml:"inbound"`
	Sender          sender.Config  `yaml:"sender"`
}

// Listen is where requests and scrapes arrive.
type Listen struct {
	// Exactly one of Socket or Addr. Client and federation paths share it:
	// they never overlap, and nginx decides which of them reach us.
	Socket string `yaml:"socket"`
	Addr   string `yaml:"addr"`
	// SocketMode is octal. nginx usually runs as another uid, hence 0666.
	SocketMode string `yaml:"socket_mode"`
	// Metrics serves /metrics, /health and, with PProf, /debug/pprof. It is
	// unauthenticated: keep it off the reverse proxy.
	Metrics Metrics `yaml:"metrics"`
}

// Metrics is the scrape listener.
type Metrics struct {
	Addr  string `yaml:"addr"`
	PProf bool   `yaml:"pprof"`
}

// Database is how the worker reaches Synapse's PostgreSQL: by default exactly
// as Synapse does, with homeserver.yaml's database block -- through pgcat
// here. Every pool runs in pgx's Exec mode, the one that survives a
// transaction pooler, and no query relies on session state.
type Database struct {
	// DSN overrides homeserver.yaml's connection for every pool. Leave it
	// empty to go through Synapse's own pooler with Synapse's credentials.
	DSN                   string `yaml:"dsn"`
	ConnectTimeoutSeconds int    `yaml:"connect_timeout_seconds"`
	// Pools are the client-side pool sizes. Through pgcat these are client
	// connections; the server connections are pgcat's pool_size, shared with
	// Synapse.
	Pools Pools `yaml:"pools"`
}

// Pools sizes each connection pool.
type Pools struct {
	// Read is each of sync's, the federation reads' and the sender's
	// read pools.
	Read int32 `yaml:"read"`
	// ToDevice deletes acknowledged to-device messages.
	ToDevice int32 `yaml:"to_device"`
	// SlidingSync reads and writes the gosync schema.
	SlidingSync int32 `yaml:"sliding_sync"`
	// SenderWrite does a federation sender's bookkeeping; SenderState
	// reads and writes the gofederation schema.
	SenderWrite int32 `yaml:"sender_write"`
	SenderState int32 `yaml:"sender_state"`
	// InboundRead, InboundPDU and InboundWrite are the /send path's.
	InboundRead  int32 `yaml:"inbound_read"`
	InboundPDU   int32 `yaml:"inbound_pdu"`
	InboundWrite int32 `yaml:"inbound_write"`
}

// Memory bounds the process.
type Memory struct {
	// LimitMB is the Go runtime's soft memory limit (GOMEMLIMIT). The
	// collector works harder as the heap approaches it. 0 leaves it unset.
	LimitMB int `yaml:"limit_mb"`
	// StateCacheMB bounds the one cache of state groups' full state, shared
	// by the federation reads and inbound write mode.
	StateCacheMB int `yaml:"state_cache_mb"`
	// EventGroupCacheMB bounds the shared cache of which state group each
	// event is at, read by sync and the federation reads.
	EventGroupCacheMB int `yaml:"event_group_cache_mb"`
}

// Log is the log output.
type Log struct {
	Level  string `yaml:"level"`
	Pretty bool   `yaml:"pretty"`
}

// Load reads and validates goworker.yaml. Unknown keys are an error, so a
// misspelt setting is refused rather than silently defaulted.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return Parse(data)
}

// Parse is Load on bytes.
func Parse(data []byte) (*Config, error) {
	cfg := &Config{
		HomeserverYAML: "/etc/synapse/homeserver.yaml",
		Listen: Listen{
			SocketMode: "0660",
			Metrics:    Metrics{Addr: ":9200", PProf: true},
		},
		Upstream: proxy.Config{TimeoutSeconds: 180},
		Database: Database{
			ConnectTimeoutSeconds: 10,
			Pools: Pools{
				Read: 16, ToDevice: 4, SlidingSync: 8, SenderWrite: 8, SenderState: 4,
				InboundRead: 4, InboundPDU: 4, InboundWrite: 4,
			},
		},
		Memory:  Memory{StateCacheMB: 768, EventGroupCacheMB: 32},
		Log:     Log{Level: "info"},
		Inbound: inbound.Defaults(),
		Sender:  sender.Defaults(),
	}
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.HomeserverYAML == "" {
		return fmt.Errorf("config: homeserver_yaml is required")
	}
	if c.InstanceName == "" {
		return fmt.Errorf("config: instance_name is required; it is this process's name " +
			"in federation_sender_instances, Synapse's lock tables and the caches stream")
	}
	if (c.Listen.Socket == "") == (c.Listen.Addr == "") {
		return fmt.Errorf("config: listen: exactly one of socket or addr must be set")
	}
	if _, err := c.Listen.ParsedSocketMode(); err != nil {
		return err
	}
	if len(c.Upstream.Sockets) == 0 && len(c.Upstream.Addrs) == 0 {
		return fmt.Errorf("config: upstream: at least one of sockets or addrs must be set; " +
			"it is Synapse's main process, the fallback for everything not answered here")
	}
	if c.Memory.LimitMB < 0 || c.Memory.StateCacheMB < 0 || c.Memory.EventGroupCacheMB < 0 || c.Database.ConnectTimeoutSeconds < 0 {
		return fmt.Errorf("config: memory.limit_mb, memory.state_cache_mb and database.connect_timeout_seconds must not be negative")
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	c.Limits = c.Limits.WithDefaults()
	for _, check := range []func() error{
		c.Sync.Validate, c.FederationReads.Validate, c.Inbound.Validate, c.Sender.Validate,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	if fixed, limit := c.FixedCacheMB(), c.Memory.LimitMB; limit > 0 && fixed > limit*cacheShare/100 {
		return fmt.Errorf("config: the bounded caches take about %dMB, more than %d%% of memory.limit_mb (%dMB); "+
			"shrink memory.state_cache_mb or federation_reads.caches, or raise the limit. The rest is "+
			"for requests in flight: one initial sync of a large account is 1.5GB", fixed, cacheShare, limit)
	}
	return nil
}

// cacheShare is the most of memory.limit_mb the bounded caches may take.
const cacheShare = 70

// FixedCacheMB is what the byte-bounded caches hold when full: the shared
// state cache and the federation reads' others. The sync caches are bounded
// by entry count and are small beside these.
func (c *Config) FixedCacheMB() int {
	mb := c.Memory.StateCacheMB + c.Memory.EventGroupCacheMB
	s := c.FederationReads.Caches.WithDefaults()
	for _, v := range []int{s.EventsMB, s.AuthChainsMB, s.FilteredStateMB} {
		if v > 0 {
			mb += v
		}
	}
	return mb
}

// ParsedSocketMode returns the listen socket's permission bits.
func (l Listen) ParsedSocketMode() (os.FileMode, error) {
	n, err := strconv.ParseUint(l.SocketMode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("config: listen.socket_mode %q is not an octal mode: %w", l.SocketMode, err)
	}
	return os.FileMode(n), nil
}

// ConnectTimeout is ConnectTimeoutSeconds as a duration.
func (d Database) ConnectTimeout() time.Duration {
	return time.Duration(d.ConnectTimeoutSeconds) * time.Second
}

// DSNOr is the connection every pool uses: database.dsn, or homeserver.yaml's
// (synapse) when that is unset.
func (d Database) DSNOr(synapse string) string {
	if d.DSN != "" {
		return d.DSN
	}
	return synapse
}
