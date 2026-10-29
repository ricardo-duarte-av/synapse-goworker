package queue

import (
	"context"
	"sync"
	"testing"

	"github.com/tidwall/gjson"
)

// fakeSticky hands out queued backlog batches, recording completions.
type fakeSticky struct {
	mu        sync.Mutex
	batches   [][]PDU
	completed int
}

func (f *fakeSticky) Next(context.Context, string) ([]PDU, func(context.Context) error, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batches) == 0 {
		return nil, nil, false, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, func(context.Context) error {
		f.mu.Lock()
		f.completed++
		f.mu.Unlock()
		return nil
	}, true, nil
}

func (f *fakeSticky) done() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed
}

func eventIDs(t *testing.T, s *recordingSink) [][]string {
	t.Helper()
	var out [][]string
	for _, req := range s.sent() {
		var ids []string
		for _, p := range gjson.GetBytes(req.Body, "pdus").Array() {
			ids = append(ids, p.Get("event_id").Str)
		}
		out = append(out, ids)
	}
	return out
}

// The backlog and the live queue take turns (_prepare_transaction's round
// robin), a backlog transaction leaves live units queued and does not move
// the events cursor, an empty (all filtered) backlog batch still completes,
// and once the backlog says it is empty it is not asked again until notified.
func TestStickyBacklogTakesTurns(t *testing.T) {
	s := &recordingSink{}
	st := &fakeSticky{batches: [][]PDU{{pdu("$s1", 0)}, {}, {pdu("$s2", 0)}}}
	d := testDestination(t, s, Limits{})
	d.sticky, d.stickyBacklogged = st, true
	d.EnqueuePDU(pdu("$a", 5))
	d.TryStart()
	d.Run(context.Background())

	// Live first (a main turn), then the backlog; the empty batch is
	// completed without a transaction; then the backlog again, with nothing
	// live left; then it reports empty.
	got := eventIDs(t, s)
	want := [][]string{{"$a"}, {"$s1"}, {"$s2"}}
	if len(got) != len(want) {
		t.Fatalf("transactions %v, want %v", got, want)
	}
	for i := range want {
		if len(got[i]) != 1 || got[i][0] != want[i][0] {
			t.Fatalf("transactions %v, want %v", got, want)
		}
	}
	if st.done() != 3 {
		t.Errorf("%d backlog batches completed, want 3", st.done())
	}
	if d.LastSuccessfulStreamOrdering() != 5 {
		t.Errorf("events cursor %d, want 5: backlog sends do not move it", d.LastSuccessfulStreamOrdering())
	}
	if d.StickyBacklogged() {
		t.Error("still backlogged after the source said it is empty")
	}

	// A notification re-arms it.
	st.batches = [][]PDU{{pdu("$s3", 0)}}
	d.NotifyStickyBacklog()
	d.TryStart()
	d.Run(context.Background())
	if got := eventIDs(t, s); len(got) != 4 || got[3][0] != "$s3" {
		t.Errorf("after a notification: %v", got)
	}
}

// Without a sticky source nothing changes.
func TestNoStickySource(t *testing.T) {
	s := &recordingSink{}
	d := testDestination(t, s, Limits{})
	d.NotifyStickyBacklog()
	if d.StickyBacklogged() {
		t.Error("backlogged without a source")
	}
}
