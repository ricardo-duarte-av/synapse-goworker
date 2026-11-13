package synapsecfg

import (
	"fmt"
	"strconv"
	"strings"
)

// EventCache is what CacheConfig says of Synapse's event cache
// (*getEvent*): event_cache_size entries, no cache factor applied, and,
// while caches.expire_caches (default true), an entry not read for
// caches.cache_entry_ttl (default 30m) dropped.
type EventCache struct {
	Size     int64
	ExpiryMS int64 // 0: entries do not expire
}

type rawCaches struct {
	ExpireCaches  any `yaml:"expire_caches"`
	CacheEntryTTL any `yaml:"cache_entry_ttl"`
}

func eventCache(size any, c rawCaches) (EventCache, error) {
	var ec EventCache
	if size == nil {
		size = "10K"
	}
	n, err := ParseSize(size)
	if err != nil {
		return ec, fmt.Errorf("synapsecfg: event_cache_size: %w", err)
	}
	ec.Size = n
	if c.ExpireCaches == nil || truthy(c.ExpireCaches) {
		ttl := c.CacheEntryTTL
		if ttl == nil {
			ttl = "30m"
		}
		if ec.ExpiryMS, err = ParseDuration(ttl); err != nil {
			return ec, fmt.Errorf("synapsecfg: caches.cache_entry_ttl: %w", err)
		}
	}
	return ec, nil
}

// ParseSize is Config.parse_size: an int is bytes (here, a count); a string
// is digits with an optional K, M, G or T suffix, in powers of 1024.
func ParseSize(v any) (int64, error) {
	switch s := v.(type) {
	case int:
		return int64(s), nil
	case string:
		if s == "" {
			return 0, fmt.Errorf("empty size")
		}
		size := int64(1)
		switch s[len(s)-1] {
		case 'K':
			size, s = 1<<10, s[:len(s)-1]
		case 'M':
			size, s = 1<<20, s[:len(s)-1]
		case 'G':
			size, s = 1<<30, s[:len(s)-1]
		case 'T':
			size, s = 1<<40, s[:len(s)-1]
		}
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return 0, err
		}
		return n * size, nil
	}
	return 0, fmt.Errorf("bad size type %v", v)
}
