// Package config is the sender's section of goworker.yaml.
//
// Only the sender's own tuning lives here. Everything Synapse also needs to
// agree on -- server_name, federation_sender_instances, the signing key, the
// retry schedule, Redis -- is read from homeserver.yaml, so the two cannot
// drift apart.
package config

import (
	"fmt"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/destinations"
)

// Synapse's own transaction limits (federation/sender/per_destination_queue.py).
const (
	SynapseMaxPDUsPerTransaction = 50
	SynapseMaxEDUsPerTransaction = 100
	SynapseEventBatchLimit       = 100
)

// Config is the `sender:` block.
type Config struct {
	// MaxPDUsPerTransaction and MaxEDUsPerTransaction are Synapse's 50 and
	// 100. Raising them sends transactions remote servers may refuse.
	MaxPDUsPerTransaction int `yaml:"max_pdus_per_transaction"`
	MaxEDUsPerTransaction int `yaml:"max_edus_per_transaction"`
	// EventBatchLimit is the pickup query's limit, Synapse's 100.
	EventBatchLimit int `yaml:"event_batch_limit"`
	// PresenceBatchStates and PresenceBatchMaxWaitSeconds hold presence for a
	// destination until this many states or this long. 0 sends at once.
	PresenceBatchStates         int `yaml:"presence_batch_states"`
	PresenceBatchMaxWaitSeconds int `yaml:"presence_batch_max_wait_seconds"`
	// HostCacheEntries bounds the joined-hosts-per-state-group cache.
	HostCacheEntries int `yaml:"host_cache_entries"`
}

// Defaults returns the block with every unset value Synapse's.
func Defaults() Config {
	return Config{
		MaxPDUsPerTransaction:       SynapseMaxPDUsPerTransaction,
		MaxEDUsPerTransaction:       SynapseMaxEDUsPerTransaction,
		EventBatchLimit:             SynapseEventBatchLimit,
		PresenceBatchStates:         SynapseMaxEDUsPerTransaction / 2,
		PresenceBatchMaxWaitSeconds: 30,
		HostCacheEntries:            destinations.DefaultHostCacheEntries,
	}
}

// Validate refuses values the sender cannot run with.
func (c Config) Validate() error {
	for _, f := range []struct {
		name  string
		value int
	}{
		{"sender.max_pdus_per_transaction", c.MaxPDUsPerTransaction},
		{"sender.max_edus_per_transaction", c.MaxEDUsPerTransaction},
		{"sender.event_batch_limit", c.EventBatchLimit},
		{"sender.host_cache_entries", c.HostCacheEntries},
	} {
		if f.value < 1 {
			return fmt.Errorf("config: %s must be at least 1, got %d", f.name, f.value)
		}
	}
	if c.PresenceBatchStates < 0 || c.PresenceBatchMaxWaitSeconds < 0 {
		return fmt.Errorf("config: sender.presence_batch_states and "+
			"sender.presence_batch_max_wait_seconds cannot be negative, got %d and %d; "+
			"set either to 0 to send presence as soon as it arrives",
			c.PresenceBatchStates, c.PresenceBatchMaxWaitSeconds)
	}
	return nil
}

// PresenceBatchMaxWait is PresenceBatchMaxWaitSeconds as a duration.
func (c Config) PresenceBatchMaxWait() time.Duration {
	return time.Duration(c.PresenceBatchMaxWaitSeconds) * time.Second
}
