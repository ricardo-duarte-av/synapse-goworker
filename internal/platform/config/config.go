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

// Pool is one database connection pool.
type Pool struct {
	// DSN, when set, is used as is. Otherwise the pool connects with
	// database.dsn as User, or with homeserver.yaml's own credentials when
	// neither is set.
	DSN      string `yaml:"dsn"`
	User     string `yaml:"user"`
	MaxConns int32  `yaml:"max_conns"`
}

// Database is how the worker reaches Synapse's PostgreSQL.
type Database struct {
	// DSN is a libpq connection string without a user, reaching PostgreSQL
	// directly: a transaction pooler such as pgcat only accepts the users in
	// its own config. Empty takes homeserver.yaml's.
	DSN                   string `yaml:"dsn"`
	ConnectTimeoutSeconds int    `yaml:"connect_timeout_seconds"`
	// ManageRoles creates or repairs the inbound roles at every start, with
	// homeserver.yaml's credentials. Defaults to true.
	ManageRoles *bool `yaml:"manage_roles"`

	// Read is the SELECT-only role sync, the federation reads and the sender
	// read through; each opens its own pool of this size.
	Read Pool `yaml:"read"`
	// ToDevice deletes acknowledged to-device messages (SELECT, DELETE on
	// device_inbox alone).
	ToDevice Pool `yaml:"to_device"`
	// SlidingSync owns the gosync schema and has nothing in public.
	SlidingSync Pool `yaml:"sliding_sync"`
	// SenderWrite does a federation sender's bookkeeping in Synapse's tables;
	// SenderState owns the gofederation schema.
	SenderWrite Pool `yaml:"sender_write"`
	SenderState Pool `yaml:"sender_state"`
	// Inbound names the roles the /send path connects as. They are created
	// with derived passwords, so only names and sizes are configured.
	Inbound InboundRoles `yaml:"inbound"`
}

// InboundRoles are the /send path's roles.
type InboundRoles struct {
	ReadRole     string `yaml:"read_role"`
	WriteRole    string `yaml:"write_role"`
	ReadMaxConns int32  `yaml:"read_max_conns"`
	PDUMaxConns  int32  `yaml:"pdu_max_conns"`
}

// Memory bounds the process.
type Memory struct {
	// LimitMB is the Go runtime's soft memory limit (GOMEMLIMIT). The
	// collector works harder as the heap approaches it. 0 leaves it unset.
	LimitMB int `yaml:"limit_mb"`
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
			Read:                  Pool{MaxConns: 16},
			ToDevice:              Pool{MaxConns: 4},
			SlidingSync:           Pool{MaxConns: 8},
			SenderWrite:           Pool{MaxConns: 8},
			SenderState:           Pool{MaxConns: 4},
			Inbound: InboundRoles{
				ReadRole: "goinbound_ro", WriteRole: "goinbound_rw",
				ReadMaxConns: 4, PDUMaxConns: 4,
			},
		},
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
	if c.Memory.LimitMB < 0 || c.Database.ConnectTimeoutSeconds < 0 {
		return fmt.Errorf("config: memory.limit_mb and database.connect_timeout_seconds must not be negative")
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
			"shrink federation_reads.caches or inbound.state_cache_entries, or raise the limit. The rest is "+
			"for requests in flight: one initial sync of a large account is 1.5GB", fixed, cacheShare, limit)
	}
	return nil
}

// cacheShare is the most of memory.limit_mb the bounded caches may take.
const cacheShare = 70

// inboundEntryBytes is what one state-resolution cache entry costs: 400k
// entries were measured at about 85MB.
const inboundEntryBytes = 220

// FixedCacheMB estimates what the size-bounded caches hold when full: the
// federation reads' byte budgets and the inbound state cache. The sync
// caches are bounded by entry count and are small beside these.
func (c *Config) FixedCacheMB() int {
	mb := 0
	s := c.FederationReads.Caches.WithDefaults()
	for _, v := range []int{s.StateGroupsMB, s.EventsMB, s.EventStateGroupsMB, s.AuthChainsMB, s.FilteredStateMB} {
		if v > 0 {
			mb += v
		}
	}
	return mb + c.Inbound.StateCacheEntries*inboundEntryBytes>>20
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

// ManagesRoles is ManageRoles with its default.
func (d Database) ManagesRoles() bool { return d.ManageRoles == nil || *d.ManageRoles }

// DSNFor resolves a pool's connection string: its own DSN, else base as its
// user, else synapse (homeserver.yaml's own credentials). base is
// database.dsn, or homeserver.yaml's when that is empty.
func DSNFor(p Pool, base, synapse string) string {
	switch {
	case p.DSN != "":
		return p.DSN
	case p.User != "" && base != "":
		return base + " user=" + p.User
	case base != "":
		return base
	default:
		return synapse
	}
}
