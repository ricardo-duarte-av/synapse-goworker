// Package synapsecfg reads the facts we need out of Synapse's own
// homeserver.yaml, at every start.
//
// The alternative is copying them into gosync-worker.yaml, and that is a trap
// rather than a shortcut. Three of these change without anybody thinking about
// this worker: `worker_replication_secret` on a rotation, the instance holding
// `stream_writers.presence` when workers are rebalanced, and that instance's
// socket path in `instance_map`. A duplicated copy does not fail loudly when it
// drifts -- it keeps pointing at a socket that is gone, or presents a secret
// that is no longer accepted, and the symptom is presence quietly not working.
//
// So Synapse's config is the single source of truth and we re-read it on every
// start. Nothing here is cached to disk, and nothing is written back.
//
// The resolution rules are Synapse's, from synapse/config/workers.py:
//
//   - `stream_writers.presence` is a string or a list and defaults to ["main"].
//     Synapse requires exactly one entry and so do we.
//   - `instance_map[<name>]` is either {path} for a unix socket or
//     {host, port, tls} for TCP.
//   - durations are milliseconds as an integer, or a string with an s/m/h/d/w/y
//     suffix (synapse/config/_base.py, parse_duration).
package synapsecfg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// MainProcessInstanceName is Synapse's name for the main process, and the
// default writer for every stream.
const MainProcessInstanceName = "main"

// Synapse's presence timer defaults, from synapse/config/server.py.
const (
	DefaultSyncOnlineTimeout     = 30 * time.Second
	DefaultLastActiveGranularity = 60 * time.Second
)

// Instance is where one Synapse instance can be reached for HTTP replication.
type Instance struct {
	Name string
	// Socket is set for a unix-socket instance, URL for a TCP one. Exactly
	// one is ever non-empty.
	Socket string
	URL    string
}

// Config is the subset of homeserver.yaml this worker needs.
type Config struct {
	ServerName string
	// ReplicationSecret is worker_replication_secret. Empty means Synapse
	// accepts unauthenticated replication calls, which is a valid (if
	// unwise) configuration.
	ReplicationSecret string
	// PresenceEnabled is presence.enabled, which defaults to true.
	PresenceEnabled bool
	// PresenceWriter is the instance holding stream_writers.presence.
	PresenceWriter Instance
	// SyncOnlineTimeout and LastActiveGranularity feed the relay interval.
	SyncOnlineTimeout     time.Duration
	LastActiveGranularity time.Duration
}

// raw mirrors only the keys we read. Everything else in the file is ignored,
// so a Synapse upgrade that adds options cannot stop this parsing.
type raw struct {
	ServerName        string `yaml:"server_name"`
	ReplicationSecret string `yaml:"worker_replication_secret"`

	Presence struct {
		Enabled               *bool  `yaml:"enabled"`
		SyncOnlineTimeout     any    `yaml:"sync_online_timeout"`
		LastActiveGranularity any    `yaml:"last_active_granularity"`
		IdleTimeout           any    `yaml:"idle_timeout"`
		_                     string `yaml:"-"`
	} `yaml:"presence"`

	InstanceMap map[string]struct {
		Path string `yaml:"path"`
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		TLS  bool   `yaml:"tls"`
	} `yaml:"instance_map"`

	StreamWriters struct {
		// A string or a list of strings, per _instance_to_list_converter.
		Presence any `yaml:"presence"`
	} `yaml:"stream_writers"`
}

// Load reads and resolves homeserver.yaml.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("synapsecfg: %w", err)
	}
	var r raw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("synapsecfg: parsing %s: %w", path, err)
	}

	cfg := &Config{
		ServerName:            r.ServerName,
		ReplicationSecret:     r.ReplicationSecret,
		PresenceEnabled:       true,
		SyncOnlineTimeout:     DefaultSyncOnlineTimeout,
		LastActiveGranularity: DefaultLastActiveGranularity,
	}
	if r.Presence.Enabled != nil {
		cfg.PresenceEnabled = *r.Presence.Enabled
	}
	if d, ok, err := parseDuration(r.Presence.SyncOnlineTimeout); err != nil {
		return nil, fmt.Errorf("synapsecfg: presence.sync_online_timeout: %w", err)
	} else if ok {
		cfg.SyncOnlineTimeout = d
	}
	if d, ok, err := parseDuration(r.Presence.LastActiveGranularity); err != nil {
		return nil, fmt.Errorf("synapsecfg: presence.last_active_granularity: %w", err)
	} else if ok {
		cfg.LastActiveGranularity = d
	}

	writers, err := instanceList(r.StreamWriters.Presence)
	if err != nil {
		return nil, fmt.Errorf("synapsecfg: stream_writers.presence: %w", err)
	}
	if len(writers) == 0 {
		writers = []string{MainProcessInstanceName}
	}
	if len(writers) != 1 {
		// Synapse refuses to start on this, so a config that has it is broken
		// for Synapse too and we should say so rather than pick one.
		return nil, fmt.Errorf(
			"synapsecfg: stream_writers.presence names %d instances, Synapse allows exactly one",
			len(writers))
	}
	name := writers[0]

	loc, ok := r.InstanceMap[name]
	if !ok {
		return nil, fmt.Errorf(
			"synapsecfg: the presence writer %q is not in instance_map, so it has no address", name)
	}
	inst := Instance{Name: name}
	switch {
	case loc.Path != "":
		inst.Socket = loc.Path
	case loc.Host != "" && loc.Port != 0:
		scheme := "http"
		if loc.TLS {
			scheme = "https"
		}
		inst.URL = fmt.Sprintf("%s://%s:%d", scheme, loc.Host, loc.Port)
	default:
		return nil, fmt.Errorf(
			"synapsecfg: instance_map entry for %q has neither a path nor a host and port", name)
	}
	cfg.PresenceWriter = inst

	return cfg, nil
}

// instanceList accepts Synapse's string-or-list, per _instance_to_list_converter.
func instanceList(v any) ([]string, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return []string{t}, nil
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("expected instance names, got %T", e)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected a string or a list, got %T", v)
	}
}

var durationUnits = map[byte]time.Duration{
	's': time.Second,
	'm': time.Minute,
	'h': time.Hour,
	'd': 24 * time.Hour,
	'w': 7 * 24 * time.Hour,
	'y': 365 * 24 * time.Hour,
}

// parseDuration is Synapse's parse_duration: a bare number is MILLISECONDS,
// and a string may carry an s/m/h/d/w/y suffix.
//
// The millisecond default is the trap. `sync_online_timeout: 30` means thirty
// milliseconds to Synapse, not thirty seconds, and reading it as seconds would
// give a relay interval a thousand times too long -- which looks like presence
// simply not working.
func parseDuration(v any) (time.Duration, bool, error) {
	switch t := v.(type) {
	case nil:
		return 0, false, nil
	case int:
		return time.Duration(t) * time.Millisecond, true, nil
	case float64:
		return time.Duration(t) * time.Millisecond, true, nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false, nil
		}
		unit := time.Millisecond
		if u, ok := durationUnits[s[len(s)-1]]; ok {
			unit = u
			s = s[:len(s)-1]
		}
		n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return 0, false, fmt.Errorf("%q is not a duration", t)
		}
		return time.Duration(n * float64(unit)), true, nil
	default:
		return 0, false, fmt.Errorf("expected a number or a string, got %T", v)
	}
}
