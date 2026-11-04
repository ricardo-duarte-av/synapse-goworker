package bus

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Publisher puts command lines on Synapse's replication bus: the channel
// named server_name, one command per message (replication/tcp/redis.py
// _async_send_command). It has its own connection, never subscribed (a
// subscribed Redis connection cannot publish), and reads nothing; what this
// process publishes it hears back on the Bus like any other process's line.
type Publisher struct {
	client  *redis.Client
	channel string
}

// NewPublisher connects as cfg says; Address and Channel as for New.
func NewPublisher(cfg Config) *Publisher {
	opts := &redis.Options{
		Addr:     cfg.Address,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if strings.HasPrefix(cfg.Address, "/") {
		opts.Network = "unix"
	}
	return &Publisher{client: redis.NewClient(opts), channel: cfg.Channel}
}

// Publish sends one command line.
func (p *Publisher) Publish(ctx context.Context, line string) error {
	return p.client.Publish(ctx, p.channel, line).Err()
}

// Ping checks the bus can be reached.
func (p *Publisher) Ping(ctx context.Context) error { return p.client.Ping(ctx).Err() }

// Close closes the connection.
func (p *Publisher) Close() error { return p.client.Close() }

// IsReplicate reports whether a line is REPLICATE: "REPLICATE " and
// ReplicateCommand.to_line(), which is empty.
func IsReplicate(line string) bool { return strings.TrimSpace(line) == "REPLICATE" }
