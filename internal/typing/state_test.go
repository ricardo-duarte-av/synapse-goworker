package typing

import (
	"encoding/json"
	"fmt"
	"testing"
)

type recorder struct{ rows []Row }

func (r *recorder) on(row Row) { r.rows = append(r.rows, row) }

func newTestState() (*State, *recorder, *float64) {
	rec := &recorder{}
	s := NewState(rec.on)
	clock := 1_000_000.0
	s.now = func() float64 { return clock }
	return s, rec, &clock
}

func TestStartStop(t *testing.T) {
	s, rec, _ := newTestState()
	s.Start("!r:x", "@a:x", 30000)
	s.Start("!r:x", "@b:x", 30000)
	// Already typing: the timer moves, no serial.
	s.Start("!r:x", "@a:x", 30000)
	s.Stop("!r:x", "@a:x")
	// Not typing: nothing.
	s.Stop("!r:x", "@a:x")
	s.Stop("!other:x", "@a:x")

	want := []Row{
		{1, "!r:x", []string{"@a:x"}},
		{2, "!r:x", []string{"@a:x", "@b:x"}},
		{3, "!r:x", []string{"@b:x"}},
	}
	if fmt.Sprint(rec.rows) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", rec.rows, want)
	}
	if s.CurrentToken() != 3 {
		t.Fatalf("serial %d", s.CurrentToken())
	}
}

func TestRemoteTakesASerialEveryTime(t *testing.T) {
	s, rec, _ := newTestState()
	s.Remote("!r:x", "@z:y", true)
	s.Remote("!r:x", "@z:y", true)
	s.Remote("!r:x", "@z:y", false)
	s.Remote("!r:x", "@z:y", false) // absent already: still a row
	if len(rec.rows) != 4 || len(rec.rows[3].Users) != 0 || rec.rows[3].Token != 4 {
		t.Fatalf("got %v", rec.rows)
	}
}

func TestTimeouts(t *testing.T) {
	s, rec, clock := newTestState()
	s.Start("!r:x", "@a:x", 30000)
	s.Remote("!r:x", "@z:y", true)

	// Nothing before the bucket after the timeout has passed.
	*clock += 30000
	if n := s.HandleTimeouts(); n != 0 {
		t.Fatalf("timed out %d at 30s", n)
	}
	*clock += 5000
	if n := s.HandleTimeouts(); n != 1 {
		t.Fatalf("timed out %d at 35s, want the local user", n)
	}
	if last := rec.rows[len(rec.rows)-1]; fmt.Sprint(last.Users) != "[@z:y]" {
		t.Fatalf("after the local timeout: %v", last)
	}
	// The remote one lasts a minute.
	*clock += 30000
	if n := s.HandleTimeouts(); n != 1 {
		t.Fatalf("remote: timed out %d at 65s", n)
	}
	if _, users, timers := s.Size(); users != 0 || timers != 0 {
		t.Fatalf("left: users=%d timers=%d", users, timers)
	}
}

func TestATimeoutExtendedIsNotFired(t *testing.T) {
	s, rec, clock := newTestState()
	s.Start("!r:x", "@a:x", 10000)
	*clock += 8000
	s.Start("!r:x", "@a:x", 30000) // still typing
	*clock += 10000
	if n := s.HandleTimeouts(); n != 0 {
		t.Fatalf("an extended notice timed out early (%d)", n)
	}
	*clock += 30000
	if n := s.HandleTimeouts(); n != 1 || len(rec.rows) != 2 {
		t.Fatalf("not timed out at the new time: %d %v", n, rec.rows)
	}
}

func TestPrune(t *testing.T) {
	s, _, _ := newTestState()
	s.Start("!old:x", "@a:x", 120000)
	s.Prune() // updated since the last prune: kept
	if rooms, _, _ := s.Size(); rooms != 1 {
		t.Fatalf("rooms %d", rooms)
	}
	s.Start("!new:x", "@b:x", 120000)
	s.Prune() // !old:x had no change since: forgotten, its typist too
	if rooms, users, _ := s.Size(); rooms != 1 || users != 1 {
		t.Fatalf("rooms=%d users=%d", rooms, users)
	}
}

func TestUpdatesSince(t *testing.T) {
	s, _, _ := newTestState()
	for i := range 5 {
		s.Start(fmt.Sprintf("!r%d:x", i), "@a:x", 30000) // serials 1..5
	}
	s.Start("!r0:x", "@b:x", 30000) // !r0 moves to 6

	rows, upto, limited := s.UpdatesSince(1, 6, 100)
	var got []string
	for _, r := range rows {
		got = append(got, fmt.Sprintf("%d %s", r.Token, r.Row))
	}
	want := []string{
		`2 ["!r1:x",["@a:x"]]`, `3 ["!r2:x",["@a:x"]]`, `4 ["!r3:x",["@a:x"]]`,
		`5 ["!r4:x",["@a:x"]]`, `6 ["!r0:x",["@a:x","@b:x"]]`,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) || upto != 6 || limited {
		t.Fatalf("got %v %d %v", got, upto, limited)
	}
	// A room is its latest serial only: !r0 at 1 is gone.
	rows, _, _ = s.UpdatesSince(0, 1, 100)
	if len(rows) != 0 {
		t.Fatalf("a room's old serial: %v", rows)
	}
	// The strict limit moves upto back.
	rows, upto, limited = s.UpdatesSince(0, 6, 2)
	if len(rows) != 2 || upto != 3 || !limited {
		t.Fatalf("limited: %d rows, upto %d, %v", len(rows), upto, limited)
	}
	// Equal tokens: nothing.
	if rows, upto, _ := s.UpdatesSince(4, 4, 100); rows != nil || upto != 4 {
		t.Fatalf("equal: %v %d", rows, upto)
	}
	var row []any
	if err := json.Unmarshal(rows0(s), &row); err != nil || len(row) != 2 {
		t.Fatalf("row shape: %v", err)
	}
}

func rows0(s *State) json.RawMessage {
	rows, _, _ := s.UpdatesSince(0, s.CurrentToken(), 100)
	return rows[0].Row
}

// The wheel files and fetches as Synapse's does.
func TestWheel(t *testing.T) {
	w := newWheel()
	a, b := member{"r", "a"}, member{"r", "b"}
	w.insert(10_000, a, 12_000) // key 2+1=3
	w.insert(10_000, b, 31_000) // key 6+1=7
	w.insert(10_000, a, 1_000)  // the past: into the first bucket
	if got := w.fetch(14_999); len(got) != 0 {
		t.Fatalf("early: %v", got)
	}
	if got := w.fetch(15_000); len(got) != 1 || got[0] != a {
		t.Fatalf("at 15s: %v", got)
	}
	if got := w.fetch(34_999); len(got) != 0 {
		t.Fatalf("early b: %v", got)
	}
	if got := w.fetch(35_000); len(got) != 1 || got[0] != b || w.len() != 0 {
		t.Fatalf("at 35s: %v, %d left", got, w.len())
	}
}
