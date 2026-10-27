// Package bus is the worker's one subscription to Synapse's replication
// stream: the Redis channel named after server_name, on which every Synapse
// process publishes RDATA, POSITION, REPLICATE and the rest.
//
// One connection is read, and every line is handed to each consumer through
// a queue of its own, together with the subscription's edges: Connected when
// a subscription is confirmed (the first, or after go-redis reconnected
// underneath it), Dropped when it is lost. A consumer treats the two exactly
// as it treated its own connection doing the same: rows published in between
// are gone, so whatever it caches is disarmed until Connected.
//
// A consumer that falls behind must not hold up the others, so its queue is
// bounded. Past the bound it is resynced -- its queue cleared and replaced by
// Dropped, Connected -- which is what a slow reader of its own connection
// would have got from Redis disconnecting it.
//
// Nothing here publishes. Publishing needs a connection that is not
// subscribed; the inbound path keeps its own client for that.
package bus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Kind is what an Event carries.
type Kind int

const (
	// Connected: a subscription is confirmed. Everything before it is
	// unknown.
	Connected Kind = iota
	// Message: one command line.
	Message
	// Dropped: the subscription is lost, or this consumer fell behind.
	Dropped
)

// Event is one thing a consumer is told.
type Event struct {
	Kind Kind
	Line string
}

// Config is where the stream is.
type Config struct {
	// Address is a unix socket path or host:port.
	Address  string
	Username string
	Password string
	DB       int
	// Channel is Synapse's server_name.
	Channel string
}

// DefaultQueue is how many lines a consumer may fall behind by.
const DefaultQueue = 100000

var (
	connected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworker_bus_connected",
		Help: "1 while the replication subscription is confirmed.",
	})
	lines = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goworker_bus_lines_total",
		Help: "Replication command lines read.",
	})
	reconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goworker_bus_subscriptions_total",
		Help: "Subscriptions confirmed: the first, and one for every reconnect.",
	})
	queued = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "goworker_bus_queued_lines",
		Help: "Lines waiting for a consumer, by consumer.",
	}, []string{"consumer"})
	overflows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworker_bus_consumer_resyncs_total",
		Help: "Times a consumer fell a whole queue behind and was resynced, by consumer.",
	}, []string{"consumer"})
)

// Bus reads the stream for its consumers.
type Bus struct {
	cfg       Config
	log       zerolog.Logger
	mu        sync.Mutex
	consumers []*Consumer
	live      bool
}

// New builds a Bus. It connects in Run.
func New(cfg Config, log zerolog.Logger) *Bus {
	return &Bus{cfg: cfg, log: log}
}

// Consumer is one reader's view of the stream.
type Consumer struct {
	name  string
	max   int
	mu    sync.Mutex
	queue []Event
	ready chan struct{}
}

// Consumer registers a reader, with a queue of max lines (0: DefaultQueue).
// A consumer registered while the subscription is live starts with
// Connected, so it never waits for the next reconnect.
func (b *Bus) Consumer(name string, max int) *Consumer {
	if max <= 0 {
		max = DefaultQueue
	}
	c := &Consumer{name: name, max: max, ready: make(chan struct{}, 1)}
	b.mu.Lock()
	b.consumers = append(b.consumers, c)
	if b.live {
		c.push(Event{Kind: Connected})
	}
	b.mu.Unlock()
	return c
}

// Next waits for the next event. It reports false when ctx ends.
func (c *Consumer) Next(ctx context.Context) (Event, bool) {
	for {
		c.mu.Lock()
		if len(c.queue) > 0 {
			ev := c.queue[0]
			c.queue[0] = Event{}
			c.queue = c.queue[1:]
			if len(c.queue) == 0 {
				c.queue = nil // let the backing array go
			}
			n := len(c.queue)
			c.mu.Unlock()
			queued.WithLabelValues(c.name).Set(float64(n))
			return ev, true
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return Event{}, false
		case <-c.ready:
		}
	}
}

// Resync makes this consumer start over, as if its connection had dropped
// and come back: what it had queued is discarded. For a consumer that found
// the stream inconsistent and can no longer trust what it holds.
func (c *Consumer) Resync() {
	c.mu.Lock()
	c.queue = []Event{{Kind: Dropped}, {Kind: Connected}}
	c.mu.Unlock()
	c.signal()
}

func (c *Consumer) push(ev Event) {
	c.mu.Lock()
	if ev.Kind == Message && len(c.queue) >= c.max {
		// Behind by a whole queue: start it over rather than hold up the
		// stream for everyone, or grow without bound.
		c.queue = []Event{{Kind: Dropped}, {Kind: Connected}}
		c.mu.Unlock()
		overflows.WithLabelValues(c.name).Inc()
		c.signal()
		return
	}
	c.queue = append(c.queue, ev)
	n := len(c.queue)
	c.mu.Unlock()
	if ev.Kind == Message && n%1024 == 0 {
		queued.WithLabelValues(c.name).Set(float64(n))
	}
	c.signal()
}

func (c *Consumer) signal() {
	select {
	case c.ready <- struct{}{}:
	default:
	}
}

func (b *Bus) broadcast(ev Event) {
	b.mu.Lock()
	switch ev.Kind {
	case Connected:
		b.live = true
	case Dropped:
		if !b.live {
			b.mu.Unlock()
			return
		}
		b.live = false
	}
	cs := b.consumers
	b.mu.Unlock()
	for _, c := range cs {
		c.push(ev)
	}
}

// Run reads the stream until ctx ends, reconnecting with backoff.
func (b *Bus) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		subscribed, err := b.session(ctx)
		b.broadcast(Event{Kind: Dropped})
		connected.Set(0)
		if ctx.Err() != nil {
			return
		}
		if subscribed {
			backoff = time.Second
		}
		b.log.Warn().Err(err).Dur("retry_in", backoff).Msg("replication stream lost")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// errResubscribed ends a session go-redis silently reconnected: rows
// published while it was away are missing, so it is a drop like any other.
var errResubscribed = errors.New("redis reconnected underneath the subscription")

func (b *Bus) session(ctx context.Context) (subscribed bool, err error) {
	opts := &redis.Options{
		Addr:     b.cfg.Address,
		Username: b.cfg.Username,
		Password: b.cfg.Password,
		DB:       b.cfg.DB,
	}
	if strings.HasPrefix(b.cfg.Address, "/") {
		opts.Network = "unix"
	}
	client := redis.NewClient(opts)
	defer func() { _ = client.Close() }()

	// The main channel carries every stream. Synapse puts a few commands on
	// `<channel>/SUFFIX` sub-channels -- USER_IP is one -- which are none of
	// ours.
	sub := client.Subscribe(ctx, b.cfg.Channel)
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(ctx); err != nil {
		return false, err
	}
	reconnects.Inc()
	connected.Set(1)
	b.log.Info().Str("channel", b.cfg.Channel).Str("address", b.cfg.Address).
		Msg("following the replication stream")
	b.broadcast(Event{Kind: Connected})

	// ChannelWithSubscriptions rather than Channel: a resubscribe go-redis
	// does on its own must end the session, not pass unseen.
	ch := sub.ChannelWithSubscriptions()
	for {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return true, nil
			}
			switch v := ev.(type) {
			case *redis.Subscription:
				if v.Kind == "subscribe" {
					return true, errResubscribed
				}
			case *redis.Message:
				lines.Inc()
				b.broadcast(Event{Kind: Message, Line: v.Payload})
			}
		}
	}
}
