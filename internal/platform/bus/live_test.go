package bus

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Against a throwaway Redis, never the live bus: a stray line there reaches
// every Synapse process.
//
//	docker run -d --rm --name goinbound-testredis -p 127.0.0.1:56379:6379 redis:7-alpine
//	GOWORKER_TEST_REDIS=127.0.0.1:56379 go test ./internal/platform/bus/
func TestLiveReadsAndSurvivesADisconnect(t *testing.T) {
	addr := os.Getenv("GOWORKER_TEST_REDIS")
	if addr == "" {
		t.Skip("GOWORKER_TEST_REDIS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const channel = "goworker-bus-test"
	b := New(Config{Address: addr, Channel: channel}, zerolog.New(zerolog.NewTestWriter(t)))
	c := b.Consumer("test", 0)
	go b.Run(ctx)

	pub := redis.NewClient(&redis.Options{Addr: addr})
	defer pub.Close()

	expect := func(kind Kind, line string) {
		t.Helper()
		ev, ok := c.Next(ctx)
		if !ok {
			t.Fatalf("no event; wanted kind %d %q", kind, line)
		}
		if ev.Kind != kind || ev.Line != line {
			t.Fatalf("got %+v, want kind %d %q", ev, kind, line)
		}
	}
	expect(Connected, "")
	if err := pub.Publish(ctx, channel, "RDATA events master 5 []").Err(); err != nil {
		t.Fatal(err)
	}
	expect(Message, "RDATA events master 5 []")

	// Kill the subscriber's connection, as a Redis restart would. go-redis
	// reconnects underneath; the consumer must hear about the gap.
	if err := pub.Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err(); err != nil {
		t.Fatal(err)
	}
	expect(Dropped, "")
	expect(Connected, "")
	if err := pub.Publish(ctx, channel, "POSITION events master 5 6").Err(); err != nil {
		t.Fatal(err)
	}
	expect(Message, "POSITION events master 5 6")
}
