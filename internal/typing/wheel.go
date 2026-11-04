package typing

import "math"

// wheel is Synapse's WheelTimer (util/wheel_timer.py) with its default
// 5-second buckets: objects come back from fetch once the bucket after the
// one their time falls in has passed. Typing timeouts are only as exact as
// that, and the order in which they fire follows from it, so it is ported
// rather than replaced by per-member timers.
type wheel struct {
	bucketMs float64
	entries  []wheelEntry
}

type wheelEntry struct {
	endKey   int64
	elements map[member]struct{}
}

func newWheel() *wheel { return &wheel{bucketMs: 5000} }

// key is int(t / bucket_size): Python's int() truncates towards zero.
func (w *wheel) key(t float64) int64 { return int64(math.Trunc(t / w.bucketMs)) }

// insert files obj to come back strictly after then (ms).
func (w *wheel) insert(now float64, obj member, then float64) {
	thenKey := w.key(then) + 1
	nowKey := w.key(now)
	if len(w.entries) > 0 {
		minKey := w.entries[0].endKey
		maxKey := w.entries[len(w.entries)-1].endKey
		if thenKey <= maxKey {
			// The max protects against inserts for times in the past.
			w.entries[max(minKey, thenKey)-minKey].elements[obj] = struct{}{}
			return
		}
	}
	lastKey := nowKey + 1
	if len(w.entries) > 0 {
		lastKey = w.entries[len(w.entries)-1].endKey + 1
	}
	thenKey = max(lastKey, thenKey)
	for k := lastKey; k <= thenKey; k++ {
		w.entries = append(w.entries, wheelEntry{endKey: k, elements: map[member]struct{}{}})
	}
	w.entries[len(w.entries)-1].elements[obj] = struct{}{}
}

// fetch returns every object whose bucket has passed.
func (w *wheel) fetch(now float64) []member {
	nowKey := w.key(now)
	var out []member
	for len(w.entries) > 0 && w.entries[0].endKey <= nowKey {
		for m := range w.entries[0].elements {
			out = append(out, m)
		}
		w.entries = w.entries[1:]
	}
	return out
}

func (w *wheel) len() int {
	n := 0
	for _, e := range w.entries {
		n += len(e.elements)
	}
	return n
}
