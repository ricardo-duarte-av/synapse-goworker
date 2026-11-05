package streamwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
)

type fakePub struct {
	mu    sync.Mutex
	lines []string
	fail  bool
}

func (p *fakePub) Publish(_ context.Context, line string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("down")
	}
	p.lines = append(p.lines, line)
	return nil
}

func (p *fakePub) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.lines
	p.lines = nil
	return out
}

// memStream holds updates; current is the last token unless set.
type memStream struct {
	mu      sync.Mutex
	name    string
	updates []Update
	current int64
}

func (s *memStream) Name() string { return s.name }
func (s *memStream) CurrentToken() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}
func (s *memStream) add(token int64, row string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, Update{Token: token, Row: json.RawMessage(row)})
	s.current = token
}
func (s *memStream) UpdatesSince(_ context.Context, from, upto int64, limit int) ([]Update, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Update
	for _, u := range s.updates {
		if u.Token > from && u.Token <= upto {
			out = append(out, u)
		}
	}
	if len(out) > limit {
		out = out[:limit]
		return out, out[len(out)-1].Token, true, nil
	}
	return out, upto, false, nil
}

func newWriter(p *fakePub) *Writer {
	return New(Options{Instance: "gw", Publisher: p, Log: zerolog.Nop(), Attempts: 1, RetryDelay: 1})
}

func TestPublishes(t *testing.T) {
	p := &fakePub{}
	w := newWriter(p)
	s := &memStream{name: "typing"}
	w.Add(s)
	ctx := context.Background()

	// Adding announces where the stream starts.
	w.loop(ctx)
	if got := p.take(); fmt.Sprint(got) != "[POSITION typing gw 0 0]" {
		t.Fatalf("start: %q", got)
	}

	s.add(1, `["!a:x",["@u:x"]]`)
	s.add(2, `["!b:x",[]]`)
	w.loop(ctx)
	want := []string{`RDATA typing gw 1 ["!a:x",["@u:x"]]`, `RDATA typing gw 2 ["!b:x",[]]`}
	if got := p.take(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %q, want %q", got, want)
	}

	// Nothing new: nothing said.
	w.loop(ctx)
	if got := p.take(); len(got) != 0 {
		t.Fatalf("idle: %q", got)
	}

	// The stream moved without a row it can return: POSITION.
	s.mu.Lock()
	s.current = 5
	s.mu.Unlock()
	w.loop(ctx)
	if got := p.take(); fmt.Sprint(got) != "[POSITION typing gw 2 5]" {
		t.Fatalf("no rows: %q", got)
	}

	// REPLICATE: the position published so far.
	w.Announce("replicate")
	w.loop(ctx)
	if got := p.take(); fmt.Sprint(got) != "[POSITION typing gw 5 5]" {
		t.Fatalf("announce: %q", got)
	}
	if pos := w.Positions(); pos["typing"] != 5 {
		t.Fatalf("positions: %v", pos)
	}
}

func TestBatchesAndLimits(t *testing.T) {
	p := &fakePub{}
	w := newWriter(p)
	s := &memStream{name: "receipts"}
	w.Add(s)
	w.loop(context.Background())
	p.take()

	// Two rows under one token: the first says batch.
	s.add(1, `["a"]`)
	s.add(1, `["b"]`)
	for i := int64(2); i <= TargetRowCount+5; i++ {
		s.add(i, `["c"]`)
	}
	w.loop(context.Background())
	got := p.take()
	if got[0] != `RDATA receipts gw batch ["a"]` || got[1] != `RDATA receipts gw 1 ["b"]` {
		t.Fatalf("batch: %q", got[:2])
	}
	// Past the row limit the loop goes round again for the rest.
	if n := len(got); n != TargetRowCount+6 {
		t.Fatalf("published %d lines", n)
	}
	if last := got[len(got)-1]; last != fmt.Sprintf(`RDATA receipts gw %d ["c"]`, TargetRowCount+5) {
		t.Fatalf("last: %q", last)
	}
}

func TestFailureAnnouncesAgain(t *testing.T) {
	p := &fakePub{}
	w := newWriter(p)
	s := &memStream{name: "typing"}
	w.Add(s)
	w.loop(context.Background())
	p.take()

	p.fail = true
	s.add(1, `["!a:x",["@u:x"]]`)
	w.loop(context.Background())
	p.fail = false
	// The row is gone; the next loop says where the stream is, which makes
	// a follower that missed it fetch it.
	w.loop(context.Background())
	if got := p.take(); fmt.Sprint(got) != "[POSITION typing gw 1 1]" {
		t.Fatalf("after a failure: %q", got)
	}
}

func TestEndpoint(t *testing.T) {
	w := newWriter(&fakePub{})
	s := &memStream{name: "typing"}
	w.Add(s)
	for i := int64(1); i <= 3; i++ {
		s.add(i, fmt.Sprintf(`["!%d:x",[]]`, i))
	}
	e := w.Endpoint()
	call := func(stream string, q string) (map[string]any, error) {
		v, _ := url.ParseQuery(q)
		_, body, err := e.Handle(context.Background(), replserver.Request{Args: map[string]string{"stream_name": stream}, Query: v})
		return body, err
	}
	body, err := call("typing", "from_token=1&upto_token=3")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(body)
	if string(b) != `{"limited":false,"updates":[[2,["!2:x",[]]],[3,["!3:x",[]]]],"upto_token":3}` {
		t.Fatalf("got %s", b)
	}
	// Equal tokens: nothing, without asking the stream.
	body, _ = call("typing", "from_token=2&upto_token=2")
	if b, _ := json.Marshal(body); string(b) != `{"limited":false,"updates":[],"upto_token":2}` {
		t.Fatalf("equal tokens: %s", b)
	}
	for q, want := range map[string]string{
		"upto_token=3":                   "Missing required integer query parameter from_token",
		"from_token=x&upto_token=3":      "Query parameter from_token must be an integer",
		"from_token=-1&upto_token=3":     "Query parameter from_token must be a positive integer.",
		"from_token= +1_0 &upto_token=3": "",
	} {
		_, err := call("typing", q)
		var serr *replserver.Error
		switch {
		case want == "" && err != nil:
			t.Errorf("%s: %v", q, err)
		case want != "" && (!errors.As(err, &serr) || serr.Msg != want || serr.Code != 400):
			t.Errorf("%s: got %v, want %q", q, err, want)
		}
	}
	if _, err := call("events", "from_token=1&upto_token=2"); err == nil || err.(*replserver.Error).Msg != "Unknown stream" {
		t.Errorf("a stream we do not write: %v", err)
	}
}
