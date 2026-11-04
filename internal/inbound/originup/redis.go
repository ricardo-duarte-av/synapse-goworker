package originup

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/bus"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// NewBus is the replication bus's publisher, connected as homeserver.yaml's
// redis section says.
func NewBus(cfg synapsecfg.Redis, channel string) *bus.Publisher {
	addr := cfg.Addr
	if cfg.Socket != "" {
		addr = cfg.Socket
	}
	return bus.NewPublisher(bus.Config{
		Address: addr, Username: cfg.Username, Password: cfg.Password, DB: cfg.DBID, Channel: channel,
	})
}

// Listen calls onReplicate for every REPLICATE on the worker's one
// subscription until ctx ends. Nothing else is read.
func Listen(ctx context.Context, c *bus.Consumer, onReplicate func()) {
	for {
		ev, ok := c.Next(ctx)
		if !ok {
			return
		}
		if ev.Kind == bus.Message && bus.IsReplicate(ev.Line) {
			onReplicate()
		}
	}
}
