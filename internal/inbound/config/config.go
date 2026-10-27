// Package config is the inbound section of goworker.yaml.
//
// Only what is ours lives here. Everything Synapse already knows -- stream
// writers, the replication secret, trusted key servers, the whitelist, the
// retry schedule -- is read from homeserver.yaml.
package config

import (
	"fmt"
	"time"
)

// What write mode does with an event it cannot handle.
const (
	// FallbackSynapse leaves it staged and nudges main into sweeping it.
	FallbackSynapse = "synapse"
	// FallbackNone retries, then drops it, as Synapse drops one it fails.
	FallbackNone = "none"
)

// Config is the `inbound:` block.
type Config struct {
	// MaxBodyMB is the largest /send body buffered; larger ones are streamed
	// to Synapse untouched. Defaults to 16.
	MaxBodyMB int `yaml:"max_body_mb"`
	// EDUForwardTimeoutSeconds bounds forwarding a transaction's EDUs to
	// their stream writers. Defaults to 60.
	EDUForwardTimeoutSeconds int `yaml:"edu_forward_timeout_seconds"`
	// MissingEvents is "fetch" (pull missing prev events from the origin, as
	// Synapse does) or "defer" (leave such events to Synapse).
	MissingEvents string `yaml:"missing_events"`
	// MarkOriginsUp resets an origin in backoff when it sends us a signed
	// request, as Synapse does, publishing REMOTE_SERVER_UP. Without it such
	// transactions go to Synapse. Defaults to true.
	MarkOriginsUp *bool `yaml:"mark_origins_up"`
	// Fallback is FallbackSynapse or FallbackNone.
	Fallback string `yaml:"fallback"`
	// KeyRefetchMinutes and KeyTimeoutSeconds tune signing key lookups.
	KeyRefetchMinutes int `yaml:"key_refetch_minutes"`
	KeyTimeoutSeconds int `yaml:"key_timeout_seconds"`
}

// Defaults returns the block as production runs it.
func Defaults() Config {
	return Config{
		MaxBodyMB:                16,
		EDUForwardTimeoutSeconds: 60,
		MissingEvents:            "fetch",
		Fallback:                 FallbackSynapse,
		KeyRefetchMinutes:        60,
		KeyTimeoutSeconds:        30,
	}
}

// Validate refuses values the worker cannot run with.
func (c Config) Validate() error {
	for _, n := range []struct {
		name string
		v    int
	}{
		{"inbound.max_body_mb", c.MaxBodyMB},
		{"inbound.edu_forward_timeout_seconds", c.EDUForwardTimeoutSeconds},
		{"inbound.key_refetch_minutes", c.KeyRefetchMinutes},
		{"inbound.key_timeout_seconds", c.KeyTimeoutSeconds},
	} {
		if n.v < 0 {
			return fmt.Errorf("%s must not be negative", n.name)
		}
	}
	switch c.MissingEvents {
	case "fetch", "defer":
	default:
		return fmt.Errorf("inbound.missing_events must be fetch or defer, got %q", c.MissingEvents)
	}
	switch c.Fallback {
	case FallbackSynapse, FallbackNone:
	default:
		return fmt.Errorf("inbound.fallback must be %s or %s, got %q", FallbackSynapse, FallbackNone, c.Fallback)
	}
	return nil
}

// MaxBodyBytes is MaxBodyMB in bytes.
func (c Config) MaxBodyBytes() int64 { return int64(c.MaxBodyMB) << 20 }

// EDUForwardTimeout is EDUForwardTimeoutSeconds as a duration.
func (c Config) EDUForwardTimeout() time.Duration {
	return time.Duration(c.EDUForwardTimeoutSeconds) * time.Second
}

// FetchesMissingEvents says write mode pulls missing prev events itself.
func (c Config) FetchesMissingEvents() bool { return c.MissingEvents == "fetch" }

// MarksOriginsUp is MarkOriginsUp with its default.
func (c Config) MarksOriginsUp() bool { return c.MarkOriginsUp == nil || *c.MarkOriginsUp }
