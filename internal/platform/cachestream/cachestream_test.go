package cachestream

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
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
		return errors.New("bus down")
	}
	p.lines = append(p.lines, line)
	return nil
}

func newWriter(p *fakePub) *Writer {
	w := New(Options{Publisher: p, Instance: "gw", Log: zerolog.Nop()})
	w.RetryDelay = time.Millisecond
	return w
}

// Two parts writing at once: whatever order they take IDs in, each takes
// them inside Do, so the RDATA goes out in ID order.
func TestOrderAcrossWriters(t *testing.T) {
	p := &fakePub{}
	w := newWriter(p)
	var (
		mu   sync.Mutex
		next int64
		wg   sync.WaitGroup
	)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := w.Do(func() ([]Row, error) {
				mu.Lock()
				next += 2
				a, b := next-1, next
				mu.Unlock()
				return []Row{{StreamID: a, Instance: "gw", CacheFunc: "f", Keys: []string{"k"}},
					{StreamID: b, Instance: "gw", CacheFunc: "f", Keys: []string{"k"}}}, nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var last int64
	for _, l := range p.lines {
		id, _ := strconv.ParseInt(strings.Fields(l)[3], 10, 64)
		if id <= last {
			t.Fatalf("%d after %d", id, last)
		}
		last = id
	}
	if len(p.lines) != 100 {
		t.Errorf("%d lines", len(p.lines))
	}
}

func TestPublishFailureAnnouncesNextTime(t *testing.T) {
	p := &fakePub{}
	w := newWriter(p)
	row := func(id int64) func() ([]Row, error) {
		return func() ([]Row, error) {
			return []Row{{StreamID: id, Instance: "gw", CacheFunc: "ignored_users", Keys: []string{"@a:x"}, InvalidationTS: 5}}, nil
		}
	}
	p.fail = true
	var perr *PublishError
	if err := w.Do(row(7)); !errors.As(err, &perr) {
		t.Fatalf("%v", err)
	}
	// The bus is still down: the next write does not run.
	ran := false
	if err := w.Do(func() ([]Row, error) { ran = true; return nil, nil }); !errors.As(err, &perr) || ran {
		t.Fatalf("%v %v", err, ran)
	}
	p.fail = false
	if err := w.Do(row(9)); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"POSITION caches gw 7 7",
		`RDATA caches gw 9 ["ignored_users",["@a:x"],5]`,
	}
	if strings.Join(p.lines, "\n") != strings.Join(want, "\n") {
		t.Errorf("%q", p.lines)
	}
	// A write's own error is returned as it is, and publishes nothing.
	boom := errors.New("boom")
	if err := w.Do(func() ([]Row, error) { return nil, boom }); err != boom {
		t.Errorf("%v", err)
	}
	w.OnReplicate()
	if got := p.lines[len(p.lines)-1]; got != "POSITION caches gw 9 9" {
		t.Errorf("%q", got)
	}
}

func TestAnnouncedBeforeFirstRow(t *testing.T) {
	p := &fakePub{}
	newWriter(p).OnReplicate()
	if len(p.lines) != 0 {
		t.Errorf("no start position, yet %q", p.lines)
	}
	w := New(Options{Publisher: p, Instance: "gw", Start: 28, Log: zerolog.Nop()})
	w.OnReplicate()
	if strings.Join(p.lines, "|") != "POSITION caches gw 28 28" {
		t.Errorf("%q", p.lines)
	}
}
