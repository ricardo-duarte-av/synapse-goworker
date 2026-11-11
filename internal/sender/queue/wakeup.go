package queue

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// maxTimeInWakeupQueue is _DestinationWakeupQueue._MAX_TIME_IN_QUEUE: every
// destination queued is woken within 30 s.
const maxTimeInWakeupQueue = 30 * time.Second

// wakeups is Synapse's _DestinationWakeupQueue (federation/sender/
// __init__.py, v1.165.0): destinations to be woken one after another rather
// than all at once, "to ensure that we don't attempt to start TLS
// connections with many hosts all at once, leading to pinned CPU". Synapse
// stages presence, device list updates and receipts for old events in big
// rooms through it. Before it, a presence ping for a user in big rooms woke
// some 5,000 destinations here in the same instant: 5,500 goroutines and up
// to MaxConcurrent handshakes at once (av, 2026-11-11).
type wakeups struct {
	mu sync.Mutex
	// order and at are the OrderedDict: a destination queued again keeps its
	// place.
	order      *list.List
	at         map[string]*list.Element
	processing bool
}

// WakeLater queues a destination to be woken by the staggered drain
// (add_to_queue): the first at once, the rest at most maxDelay apart, all
// within 30 s.
func (m *Manager) WakeLater(d *Destination) {
	w := &m.wakeups
	w.mu.Lock()
	if _, ok := w.at[d.name]; !ok {
		w.at[d.name] = w.order.PushBack(d)
	}
	if w.processing {
		w.mu.Unlock()
		return
	}
	w.processing = true
	w.mu.Unlock()
	m.mu.RLock()
	ctx := m.base
	m.mu.RUnlock()
	go m.drainWakeups(ctx)
}

// pop takes the first destination queued; with none left the drain ends.
func (w *wakeups) pop() *Destination {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := w.order.Front()
	if e == nil {
		w.processing = false
		return nil
	}
	d := w.order.Remove(e).(*Destination)
	delete(w.at, d.name)
	return d
}

// drainWakeups is _DestinationWakeupQueue._handle.
func (m *Manager) drainWakeups(ctx context.Context) {
	w := &m.wakeups
	w.mu.Lock()
	n := w.order.Len()
	if n == 0 {
		// Checked and cleared under one lock, or a destination queued in
		// between would find processing set and never be woken.
		w.processing = false
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	pause := min(m.wakeupMaxDelay, maxTimeInWakeupQueue/time.Duration(n))
	for {
		d := w.pop()
		if d == nil {
			return
		}
		// A destination whose loop already took what was queued has been
		// woken by other means; it costs no pause.
		if !d.wantsWake() {
			continue
		}
		m.Wake(d)
		if !m.sleep(ctx, pause) {
			w.mu.Lock()
			w.processing = false
			w.mu.Unlock()
			return
		}
		w.mu.Lock()
		l := w.order.Len()
		if l == 0 {
			w.processing = false
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		// More may have been queued: keep within the 30 s.
		pause = min(pause, maxTimeInWakeupQueue/time.Duration(l))
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// wantsWake is _new_data_to_send: something was queued that no transaction
// has started on since.
func (d *Destination) wantsWake() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.newData
}

// Staggered lists the destinations waiting in the wake-up queue, in order.
func (m *Manager) Staggered() []string {
	w := &m.wakeups
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, w.order.Len())
	for e := w.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*Destination).name)
	}
	return out
}
