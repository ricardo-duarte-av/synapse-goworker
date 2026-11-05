package replserver

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
)

// WaitTimeout is how long a request waits for the caller's streams
// (_WAIT_FOR_REPLICATION_TIMEOUT_SECONDS, replication/tcp/client.py:74),
// after which it goes on, as Synapse's does, with a warning.
const WaitTimeout = 5 * time.Second

var streamWaits = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goworker_replication_server_stream_waits_total",
	Help: "Waits for a caller's stream position, by outcome: already (seen), reached (seen while waiting), timeout, unknown (nothing ever seen from that writer on that stream).",
}, []string{"outcome"})

// Tracker is a Waiter fed from the bus: how far this process has seen each
// writer's stream, from RDATA tokens and POSITION new_tokens.
//
// It knows only that a line ARRIVED, not that every part of this process
// that reads it has handled it, which is what Synapse's
// wait_for_stream_position means (its one handler has processed the rows).
// An endpoint whose answer depends on a particular part having processed a
// row must wait on that part instead.
//
// A writer seen on no line since this process started is not waited for.
// Synapse's followers know such a writer's position from the database
// (a MultiWriterIdGenerator's persisted position); this process does not,
// and waiting the whole timeout on every request from a quiet writer would
// make each one 5 s slow. Such waits are counted as unknown.
type Tracker struct {
	mu      sync.Mutex
	seen    map[trackKey]int64
	waiters map[trackKey][]*trackWaiter
	timeout time.Duration
}

type trackKey struct{ stream, instance string }

type trackWaiter struct {
	position int64
	done     chan struct{}
}

// NewTracker builds an empty Tracker; Run feeds it.
func NewTracker() *Tracker {
	return &Tracker{seen: map[trackKey]int64{}, waiters: map[trackKey][]*trackWaiter{}, timeout: WaitTimeout}
}

// Run reads the bus until ctx ends.
func (t *Tracker) Run(ctx context.Context, c *bus.Consumer) {
	for {
		ev, ok := c.Next(ctx)
		if !ok {
			return
		}
		if ev.Kind == bus.Message {
			t.Observe(ev.Line)
		}
	}
}

// Observe reads one command line.
func (t *Tracker) Observe(line string) {
	var stream, instance, token string
	switch {
	case strings.HasPrefix(line, "RDATA "):
		// RDATA <stream> <instance> <token|batch> <row>
		f := strings.SplitN(line[len("RDATA "):], " ", 4)
		if len(f) < 3 {
			return
		}
		stream, instance, token = f[0], f[1], f[2]
	case strings.HasPrefix(line, "POSITION "):
		// POSITION <stream> <instance> <prev> <new>
		f := strings.Fields(line[len("POSITION "):])
		if len(f) != 4 {
			return
		}
		stream, instance, token = f[0], f[1], f[3]
	default:
		return
	}
	pos, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return // "batch": its last line carries the token
	}
	k := trackKey{stream, instance}
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev, ok := t.seen[k]; ok && prev >= pos {
		return
	}
	t.seen[k] = pos
	kept := t.waiters[k][:0]
	for _, w := range t.waiters[k] {
		if w.position <= pos {
			close(w.done)
		} else {
			kept = append(kept, w)
		}
	}
	if len(kept) == 0 {
		delete(t.waiters, k)
	} else {
		t.waiters[k] = kept
	}
}

// WaitForStreamPosition waits until the stream has been seen at position
// from instance, at most WaitTimeout.
func (t *Tracker) WaitForStreamPosition(ctx context.Context, instance, stream string, position int64) {
	k := trackKey{stream, instance}
	t.mu.Lock()
	seen, ok := t.seen[k]
	switch {
	case !ok:
		t.mu.Unlock()
		streamWaits.WithLabelValues("unknown").Inc()
		return
	case position <= seen:
		t.mu.Unlock()
		streamWaits.WithLabelValues("already").Inc()
		return
	}
	w := &trackWaiter{position: position, done: make(chan struct{})}
	t.waiters[k] = append(t.waiters[k], w)
	t.mu.Unlock()

	timer := time.NewTimer(t.timeout)
	defer timer.Stop()
	select {
	case <-w.done:
		streamWaits.WithLabelValues("reached").Inc()
	case <-timer.C:
		streamWaits.WithLabelValues("timeout").Inc()
		t.forget(k, w)
	case <-ctx.Done():
		t.forget(k, w)
	}
}

func (t *Tracker) forget(k trackKey, w *trackWaiter) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ws := t.waiters[k]
	for i, x := range ws {
		if x == w {
			t.waiters[k] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(t.waiters[k]) == 0 {
		delete(t.waiters, k)
	}
}

// Seen is how far this process has seen instance's stream on the bus, and
// whether it has seen it at all since start.
func (t *Tracker) Seen(stream, instance string) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, ok := t.seen[trackKey{stream, instance}]
	return p, ok
}
