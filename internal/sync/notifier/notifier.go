// Package notifier wakes long-polling syncs when something they care about
// happens.
//
// A port of the shape of synapse/notifier.py, not its internals. The essential
// property is the one its comments dwell on: a waiter registers its interest
// BEFORE it computes an answer, so an event arriving during the computation
// still wakes it. Registering afterwards loses every event that lands in the
// gap, and the client hangs until its timeout for news that already arrived.
package notifier

import (
	"context"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
	"sync"
	"time"
)

// Notifier tracks who is waiting for what.
type Notifier struct {
	mu      sync.Mutex
	nextID  uint64
	waiters map[uint64]*waiter
}

type waiter struct {
	rooms map[string]bool
	users map[string]bool
	// wake is closed once, when something relevant arrives.
	wake chan struct{}
	done bool
}

// New builds a Notifier.
func New() *Notifier {
	return &Notifier{waiters: map[uint64]*waiter{}}
}

// Wait registers interest and returns a handle.
//
// Call Register before computing a response, then Wait on the handle if the
// response turned out empty. The gap between the two is exactly what the
// early registration closes.
type Handle struct {
	n  *Notifier
	id uint64
	w  *waiter
}

// Register declares interest in a set of rooms and users.
//
// An empty room set means "any room", which is what a caller wants when it does
// not yet know which rooms it is in.
func (n *Notifier) Register(rooms, users []string) *Handle {
	w := &waiter{
		rooms: toSet(rooms),
		users: toSet(users),
		wake:  make(chan struct{}),
	}
	n.mu.Lock()
	n.nextID++
	id := n.nextID
	n.waiters[id] = w
	n.mu.Unlock()
	return &Handle{n: n, id: id, w: w}
}

// Wait blocks until something relevant arrives, the timeout expires, or the
// context is cancelled. It reports whether it was woken.
func (h *Handle) Wait(ctx context.Context, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-h.w.wake:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// Close releases the registration. Always call it, including when the response
// was not empty and Wait was never reached.
func (h *Handle) Close() {
	h.n.mu.Lock()
	defer h.n.mu.Unlock()
	delete(h.n.waiters, h.id)
}

// OnStreamAdvance wakes every waiter that cares. It satisfies
// replication.Listener.
//
// A row that names no room and no user wakes everyone: the conservative reading
// is that we do not know who it concerns, and a spurious wakeup costs one
// recomputation while a missed one costs a client its timeout.
func (n *Notifier) OnStreamAdvance(stream string, _ int64, roomIDs, userIDs []string) {
	n.mu.Lock()
	woken := 0
	global := len(roomIDs) == 0 && len(userIDs) == 0
	for _, w := range n.waiters {
		if w.done {
			continue
		}
		if global || w.interested(roomIDs, userIDs) {
			w.done = true
			woken++
			close(w.wake)
		}
	}
	n.mu.Unlock()

	// Counted outside the lock, and only when something was actually woken:
	// this runs for every replication row, and most rows wake nobody.
	if woken > 0 {
		metrics.NotifierWakeups.WithLabelValues(stream).Add(float64(woken))
	}
}

func (w *waiter) interested(roomIDs, userIDs []string) bool {
	// No declared rooms means every room is interesting.
	if len(w.rooms) == 0 && len(w.users) == 0 {
		return true
	}
	for _, r := range roomIDs {
		if w.rooms[r] {
			return true
		}
	}
	for _, u := range userIDs {
		if w.users[u] {
			return true
		}
	}
	return false
}

// Waiters reports how many syncs are currently blocked, for metrics.
func (n *Notifier) Waiters() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.waiters)
}

func toSet(xs []string) map[string]bool {
	if len(xs) == 0 {
		return nil
	}
	out := make(map[string]bool, len(xs))
	for _, x := range xs {
		out[x] = true
	}
	return out
}
