// Package sink is where a signed transaction goes: HTTP puts it on the wire.
// The interface exists so the queues can be tested without a network.
package sink

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/txn"
)

// Result is what a destination learns from an attempt.
type Result struct {
	// Delivered is true when the remote accepted the transaction.
	Delivered bool
	// StatusCode is the HTTP status, or 0 when no response arrived.
	StatusCode int
	// PDUErrors maps event id to the error a remote reported for it. Synapse
	// logs these and never retries them (transaction_manager.py:197).
	PDUErrors map[string]string
}

// Sink accepts signed transactions.
type Sink interface {
	// Send delivers a transaction.
	Send(ctx context.Context, req *txn.Request) (Result, error)
	// Mode names the implementation for startup logging, so which one is
	// running is never in doubt.
	Mode() string
}
