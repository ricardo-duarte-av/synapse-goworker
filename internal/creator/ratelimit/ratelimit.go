// Package ratelimit is Synapse's Ratelimiter (api/ratelimiting.py, v1.163.0):
// a leaky bucket per key, held in the process's memory as Synapse holds it in
// each worker's. The ratelimit_override exemption is the caller's to apply.
package ratelimit

import (
	"math"
	"sync"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Limiter is one Ratelimiter.
type Limiter struct {
	cfg synapsecfg.Ratelimit
	now func() time.Time

	mu      sync.Mutex
	actions map[string]bucket
}

// bucket is Synapse's (action_count, time_start, rate_hz).
type bucket struct {
	count float64
	start float64
	rate  float64
}

// New builds a limiter. now is time.Now when nil.
func New(cfg synapsecfg.Ratelimit, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{cfg: cfg, now: now, actions: map[string]bucket{}}
}

// Name is the limiter's config key.
func (l *Limiter) Name() string { return l.cfg.Key }

func (l *Limiter) seconds() float64 {
	return float64(l.now().UnixNano()) / 1e9
}

// CanDo is can_do_action for one action: whether it may happen now, and the
// retry_after_ms ratelimit would refuse it with. With update, an allowed
// action is counted.
func (l *Limiter) CanDo(key string, update bool) (bool, int64) {
	return l.CanDoRate(key, l.cfg.PerSecond, l.cfg.Burst, update)
}

// CanDoRate is CanDo with rate_hz and burst_count given for the call, as the
// request ratelimiter gives a user's override.
func (l *Limiter) CanDoRate(key string, perSecond float64, burstCount int, update bool) (bool, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.seconds()
	l.prune(now)
	rate, burst := perSecond, float64(burstCount)

	b, ok := l.actions[key]
	if !ok {
		b = bucket{start: now, rate: rate}
	}
	count, start := b.count, b.start
	performed := count - (now-start)*rate
	if performed < 0 {
		performed, count, start = 0, 0, now
	}
	allowed := performed <= burst-1
	if allowed {
		count++
		if update {
			l.record(key, count, start, rate, now)
		}
	}
	timeAllowed := -1.0
	if rate > 0 {
		timeAllowed = start + (count-burst+1)/rate
		if timeAllowed < now {
			timeAllowed = now
		}
	}
	return allowed, int64(1000 * (timeAllowed - now))
}

// Record is record_action: an action happened, allowed or not.
func (l *Limiter) Record(key string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := float64(at.UnixNano()) / 1e9
	b, ok := l.actions[key]
	if !ok {
		b = bucket{start: now, rate: l.cfg.PerSecond}
	}
	l.record(key, b.count+1, b.start, b.rate, now)
}

// record is _record_action_inner.
func (l *Limiter) record(key string, count, start, rate, now float64) {
	if rate <= 0 || start+count/rate <= now {
		delete(l.actions, key)
		return
	}
	l.actions[key] = bucket{count: count, start: start, rate: rate}
}

// prune is _prune_message_counts, run on use rather than every 15 seconds:
// buckets that have leaked empty are forgotten.
func (l *Limiter) prune(now float64) {
	if len(l.actions) < 1024 {
		return
	}
	for k, b := range l.actions {
		if b.count-(now-b.start)*b.rate <= 0 {
			delete(l.actions, k)
		}
	}
}

// RetryAfterHeader is the Retry-After header Synapse sends with it.
func RetryAfterHeader(ms int64) int64 {
	return int64(math.Ceil(float64(ms) / 1000))
}
