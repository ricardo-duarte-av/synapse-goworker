package replication

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// posListener records the position each call carried, which recordingListener
// deliberately discards. The position is the whole point here.
type posListener struct {
	name  string
	calls []posCall
}

type posCall struct {
	stream string
	pos    int64
	rooms  []string
	users  []string
}

func (p *posListener) OnStreamAdvance(stream string, pos int64, rooms, users []string) {
	p.calls = append(p.calls, posCall{stream, pos, rooms, users})
}

func eventRow(roomID string) string {
	return `["ev",["$x","` + roomID + `","m.room.message",null,null,null,null,false,false]]`
}

// A batched row must not reach anybody until the row that names the batch's
// position arrives. Delivering it early with the position we happen to hold
// files the change below where it happened.
func TestBatchRowsAreHeldUntilThePositionArrives(t *testing.T) {
	s := newTestSub()
	rec := &posListener{}
	s.listener = rec
	s.advance(StreamEvents, "av-event-persister-1", 500)

	s.handle(`RDATA events p1 batch ` + eventRow("!a:e"))
	s.handle(`RDATA events p1 batch ` + eventRow("!b:e"))

	if len(rec.calls) != 0 {
		t.Fatalf("a held batch reached the listener %d times, want 0", len(rec.calls))
	}
	if got := s.Position(StreamEvents); got != 500 {
		t.Fatalf("position moved to %d during a batch, want 500", got)
	}
}

// Every row of a batch belongs at the position the LAST row names. This is the
// property the stream-change caches depend on: a change filed at 500 when it
// happened at 600 makes "anything since 500?" answer no.
func TestBatchRowsAreAppliedAtTheFinalPosition(t *testing.T) {
	s := newTestSub()
	rec := &posListener{}
	s.listener = rec
	s.advance(StreamEvents, "av-event-persister-1", 500)

	s.handle(`RDATA events p1 batch ` + eventRow("!a:e"))
	s.handle(`RDATA events p1 batch ` + eventRow("!b:e"))
	s.handle(`RDATA events p1 600 ` + eventRow("!c:e"))

	if len(rec.calls) != 3 {
		t.Fatalf("listener saw %d rows, want all 3 of the batch", len(rec.calls))
	}
	for i, c := range rec.calls {
		if c.pos != 600 {
			t.Errorf("row %d delivered at position %d, want 600", i, c.pos)
		}
	}
	// Order must be preserved, and each row must keep its own subject rather
	// than inheriting the terminating row's.
	want := []string{"!a:e", "!b:e", "!c:e"}
	for i, c := range rec.calls {
		if len(c.rooms) != 1 || c.rooms[0] != want[i] {
			t.Errorf("row %d named %v, want [%s]", i, c.rooms, want[i])
		}
	}
	if got := s.Position(StreamEvents); got != 600 {
		t.Fatalf("position = %d, want 600", got)
	}
}

// A batch interrupted by a lost connection must be dropped, not carried across
// the reconnect: its rows would be applied against whatever position the next
// connection happens to supply.
func TestALostConnectionDiscardsAHalfBatch(t *testing.T) {
	s := newTestSub()
	rec := &posListener{}
	s.listener = rec
	s.setLive(true)

	s.handle(`RDATA events p1 batch ` + eventRow("!a:e"))
	s.setLive(false)
	s.setLive(true)
	s.handle(`RDATA events p1 700 ` + eventRow("!c:e"))

	if len(rec.calls) != 1 {
		t.Fatalf("listener saw %d rows, want only the one after the reconnect", len(rec.calls))
	}
	if rec.calls[0].rooms[0] != "!c:e" {
		t.Fatalf("the surviving row named %v, want the post-reconnect one", rec.calls[0].rooms)
	}
}

// A batch that never ends is a broken connection, not a large one. It must not
// grow without bound.
func TestAnUnterminatedBatchIsTreatedAsALostConnection(t *testing.T) {
	s := New(Config{}, zerolog.Nop(), nil)
	s.setLive(true)
	dropped := false
	s.SetOnDrop(func() { dropped = true })

	row := `RDATA events p1 batch ` + eventRow("!a:e")
	for i := 0; i < maxPendingBatch+2; i++ {
		s.handle(row)
	}

	if !dropped {
		t.Fatal("an unterminated batch did not trip the drop path")
	}
	s.mu.RLock()
	held := len(s.pending[StreamEvents])
	s.mu.RUnlock()
	if held > maxPendingBatch {
		t.Fatalf("pending holds %d rows, want the buffer released", held)
	}
}

// The stream-change caches ride alongside the notifier rather than inside it.
// Both must see the same rows.
func TestAddListenerFeedsEveryListener(t *testing.T) {
	s := newTestSub()
	first, second := &posListener{name: "notifier"}, &posListener{name: "caches"}
	s.listener = first
	s.AddListener(second)

	s.handle(`RDATA events p1 42 ` + eventRow("!a:e"))

	for _, l := range []*posListener{first, second} {
		if len(l.calls) != 1 {
			t.Fatalf("%s saw %d rows, want 1", l.name, len(l.calls))
		}
		if l.calls[0].pos != 42 {
			t.Errorf("%s saw position %d, want 42", l.name, l.calls[0].pos)
		}
	}
}

// A POSITION jump names nobody: rows happened that we did not see. Every
// listener has to hear about it, because "we do not know what changed" is the
// one thing a cache must not miss.
func TestPositionCommandReachesEveryListener(t *testing.T) {
	s := newTestSub()
	first, second := &posListener{name: "notifier"}, &posListener{name: "caches"}
	s.listener = first
	s.AddListener(second)

	s.handle(`POSITION events p1 100 900`)

	for _, l := range []*posListener{first, second} {
		if len(l.calls) != 1 {
			t.Fatalf("%s saw %d POSITION calls, want 1", l.name, len(l.calls))
		}
		c := l.calls[0]
		if c.pos != 900 || len(c.rooms) != 0 || len(c.users) != 0 {
			t.Errorf("%s saw pos=%d rooms=%v users=%v, want 900 and no subjects",
				l.name, c.pos, c.rooms, c.users)
		}
	}
}

// A silent stream stays silent even when batched, and the buffer must not leak
// rows for it either.
func TestSilentStreamsStaySilentWhenBatched(t *testing.T) {
	s := newTestSub()
	rec := &posListener{}
	s.listener = rec

	s.handle(`RDATA presence_federation p1 batch ["matrix.org","@u:e"]`)
	s.handle(`RDATA presence_federation p1 9 ["matrix.org","@v:e"]`)

	if len(rec.calls) != 0 {
		t.Fatalf("a silent stream woke the listener %d times, want 0", len(rec.calls))
	}
	s.mu.RLock()
	held := len(s.pending)
	s.mu.RUnlock()
	if held != 0 {
		t.Fatalf("pending still holds %d streams after the batch closed", held)
	}
}

func TestBatchesAreKeptPerStream(t *testing.T) {
	s := newTestSub()
	rec := &posListener{}
	s.listener = rec

	s.handle(`RDATA events p1 batch ` + eventRow("!a:e"))
	s.handle(`RDATA receipts p1 batch ["m.read","!r:e","@u:e",null,{}]`)
	s.handle(`RDATA events p1 600 ` + eventRow("!b:e"))

	// Closing the events batch must not flush the receipts one.
	streams := map[string]int{}
	for _, c := range rec.calls {
		streams[c.stream]++
	}
	if streams[StreamEvents] != 2 {
		t.Errorf("events delivered %d rows, want 2", streams[StreamEvents])
	}
	if streams[StreamReceipts] != 0 {
		t.Errorf("receipts delivered %d rows while still batching, want 0", streams[StreamReceipts])
	}
	if !strings.Contains(rec.calls[0].stream, "events") {
		t.Errorf("first delivered row was %s", rec.calls[0].stream)
	}
}

// The events stream merges three sources, each with its own row shape. Only
// "ev" was parsed, so a current-state delta -- emitted alongside every "ev" row
// for a state change -- named no room and woke every parked client.
func TestEveryEventsRowShapeNamesItsRoom(t *testing.T) {
	cases := []struct {
		name         string
		row          string
		wantRoom     string
		wantType     string
		wantStateKey string
	}{
		{
			name:     "ev",
			row:      `["ev",["$x","!a:e","m.room.message",null,null,null,null,false,false]]`,
			wantRoom: "!a:e", wantType: "m.room.message",
		},
		{
			// A state reset moves a membership with no event of its own, so
			// the type and state key here are load-bearing rather than noise.
			name:     "state",
			row:      `["state",["!b:e","m.room.member","@u:e","$y"]]`,
			wantRoom: "!b:e", wantType: "m.room.member", wantStateKey: "@u:e",
		},
		{
			name:     "state-all",
			row:      `["state-all",["!c:e"]]`,
			wantRoom: "!c:e",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rowDetails(StreamEvents, tc.row)
			if len(d.RoomIDs) != 1 || d.RoomIDs[0] != tc.wantRoom {
				t.Fatalf("rooms = %v, want [%s] -- an unnamed room wakes everybody",
					d.RoomIDs, tc.wantRoom)
			}
			if d.Type != tc.wantType {
				t.Errorf("type = %q, want %q", d.Type, tc.wantType)
			}
			if d.StateKey != tc.wantStateKey {
				t.Errorf("state key = %q, want %q", d.StateKey, tc.wantStateKey)
			}
		})
	}

	// And the classification that follows from it: none of the three may be
	// global.
	for _, tc := range cases {
		d := rowDetails(StreamEvents, tc.row)
		if len(d.RoomIDs) == 0 && len(d.UserIDs) == 0 {
			t.Errorf("%s row classified global", tc.name)
		}
	}
}

// A row shape we do not recognise must still name nobody rather than name the
// wrong thing: waking everyone is the safe failure.
func TestUnknownEventsRowShapeNamesNobody(t *testing.T) {
	d := rowDetails(StreamEvents, `["some-future-shape",["!a:e"]]`)
	if len(d.RoomIDs) != 0 || len(d.UserIDs) != 0 {
		t.Fatalf("an unknown row shape named %v/%v; it must name nobody", d.RoomIDs, d.UserIDs)
	}
}

// An unterminated batch must end the SESSION, not just the liveness flag.
//
// This is the shape of the bug it was written for. The overflow path used to
// call setLive(false) and carry on reading, but setLive(true) only ever runs
// when a new session subscribes -- so the subscriber sat in its read loop
// reporting "not connected" for as long as the process lived, and a restart was
// the only cure. On 2026-09-03 the deployed worker did exactly that: up 48
// minutes, gosync_replication_connected stuck at 0, KeyDB and Synapse both
// healthy, and a freshly started worker connecting on the first try.
//
// A Synapse restart is what produces the unterminated batch, so this is not a
// rare path -- it is the one that follows every restart of the thing we follow.
func TestAnUnterminatedBatchEndsTheSession(t *testing.T) {
	s := newTestSub()

	aborted := false
	s.abortSession = func() { aborted = true }

	// One row past the cap. Each carries the "batch" token, so none of them
	// ever names a position.
	for i := 0; i <= maxPendingBatch; i++ {
		s.handle(`RDATA events p1 batch ` + eventRow("!r:e.com"))
	}

	if !aborted {
		t.Error("the session was not ended; the subscriber would report " +
			"itself disconnected forever while still reading the stream")
	}
	if got := len(s.pending[StreamEvents]); got != 0 {
		t.Errorf("%d rows still buffered; a half-batch must not survive the drop", got)
	}
}

// The counterpart: a batch that stays under the cap must NOT end the session.
// Reconnecting on ordinary traffic would be its own outage.
func TestANormalBatchDoesNotEndTheSession(t *testing.T) {
	s := newTestSub()

	aborted := false
	s.abortSession = func() { aborted = true }

	for i := 0; i < maxPendingBatch-1; i++ {
		s.handle(`RDATA events p1 batch ` + eventRow("!r:e.com"))
	}
	if aborted {
		t.Fatal("ended the session on a batch that was still within the limit")
	}
	s.handle(`RDATA events p1 900 ` + eventRow("!r:e.com"))
	if aborted {
		t.Error("ended the session on a batch that terminated normally")
	}
	if got := len(s.pending[StreamEvents]); got != 0 {
		t.Errorf("%d rows left buffered after the batch terminated", got)
	}
}

// orderListener records the subscriber's position at the moment it is called.
type orderListener struct {
	s    *Subscriber
	seen []int64
}

func (o *orderListener) OnStreamAdvance(stream string, _ int64, _, _ []string) {
	o.seen = append(o.seen, o.s.Position(stream))
}

// The stream-change caches must hear of a row BEFORE the position moves, and
// the notifier after. A now token is built from the position, so in the other
// order a sync in between would cover the row with a cache that answers
// "nothing changed here", and the client's next since would be past it.
func TestCachesRecordBeforeThePositionMovesAndClientsWakeAfter(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"RDATA", `RDATA events p1 600 ` + eventRow("!a:e")},
		{"POSITION", `POSITION events p1 500 600`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSub()
			cache := &orderListener{s: s}
			waker := &orderListener{s: s}
			s.listener = waker
			s.AddListener(cache)
			s.advance(StreamEvents, "p1", 500)

			s.handle(tc.payload)

			if len(cache.seen) != 1 || cache.seen[0] != 500 {
				t.Errorf("cache listener saw position %v, want [500]: it must record before the advance", cache.seen)
			}
			if len(waker.seen) != 1 || waker.seen[0] != 600 {
				t.Errorf("notifier saw position %v, want [600]: it must wake after the advance", waker.seen)
			}
		})
	}
}
