package bus

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func next(t *testing.T, c *Consumer) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ev, ok := c.Next(ctx)
	if !ok {
		t.Fatal("no event")
	}
	return ev
}

func TestEveryConsumerGetsEveryLineInOrder(t *testing.T) {
	b := New(Config{}, zerolog.Nop())
	a, c := b.Consumer("a", 0), b.Consumer("c", 0)
	b.broadcast(Event{Kind: Connected})
	b.broadcast(Event{Kind: Message, Line: "RDATA 1"})
	b.broadcast(Event{Kind: Message, Line: "RDATA 2"})
	b.broadcast(Event{Kind: Dropped})
	for _, cons := range []*Consumer{a, c} {
		for _, want := range []Event{{Kind: Connected}, {Message, "RDATA 1"}, {Message, "RDATA 2"}, {Kind: Dropped}} {
			if got := next(t, cons); got != want {
				t.Errorf("%s: got %+v, want %+v", cons.name, got, want)
			}
		}
	}
}

// A slow consumer is resynced, not allowed to hold up the rest or grow
// without bound.
func TestASlowConsumerIsResyncedNotBlocking(t *testing.T) {
	b := New(Config{}, zerolog.Nop())
	slow, fast := b.Consumer("slow", 2), b.Consumer("fast", 0)
	b.broadcast(Event{Kind: Connected})
	for i := 0; i < 5; i++ {
		b.broadcast(Event{Kind: Message, Line: "x"})
	}
	if got := next(t, slow); got.Kind != Dropped {
		t.Fatalf("slow consumer: %+v, want Dropped", got)
	}
	if got := next(t, slow); got.Kind != Connected {
		t.Fatalf("slow consumer: %+v, want Connected", got)
	}
	if next(t, fast).Kind != Connected {
		t.Fatal("fast consumer lost its Connected")
	}
	for i := 0; i < 5; i++ {
		if got := next(t, fast); got.Kind != Message {
			t.Fatalf("fast consumer line %d: %+v", i, got)
		}
	}
}

func TestLateConsumerStartsConnected(t *testing.T) {
	b := New(Config{}, zerolog.Nop())
	b.broadcast(Event{Kind: Connected})
	if got := next(t, b.Consumer("late", 0)); got.Kind != Connected {
		t.Errorf("late consumer: %+v", got)
	}
}

func TestDroppedOnlyOnTheEdge(t *testing.T) {
	b := New(Config{}, zerolog.Nop())
	c := b.Consumer("c", 0)
	b.broadcast(Event{Kind: Dropped}) // never connected: nothing to say
	b.broadcast(Event{Kind: Connected})
	b.broadcast(Event{Kind: Dropped})
	b.broadcast(Event{Kind: Dropped})
	if next(t, c).Kind != Connected || next(t, c).Kind != Dropped {
		t.Fatal("wrong edges")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if ev, ok := c.Next(ctx); ok {
		t.Errorf("a second Dropped was delivered: %+v", ev)
	}
}

func TestResync(t *testing.T) {
	b := New(Config{}, zerolog.Nop())
	c := b.Consumer("c", 0)
	b.broadcast(Event{Kind: Connected})
	b.broadcast(Event{Kind: Message, Line: "stale"})
	c.Resync()
	if next(t, c).Kind != Dropped || next(t, c).Kind != Connected {
		t.Error("Resync did not start the consumer over")
	}
}
