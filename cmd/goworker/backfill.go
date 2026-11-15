package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/inbound"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/handlers"
)

// backfiller is inbound's write mode as /messages' backfill.
type backfiller struct{ p *inbound.Processor }

func (b backfiller) MaybeBackfill(ctx context.Context, roomID string, currentDepth int64, limit int) (bool, error) {
	did, err := b.p.MaybeBackfill(ctx, roomID, currentDepth, limit)
	if errors.Is(err, inbound.ErrBackfillNotReproduced) {
		return did, fmt.Errorf("%w: %v", handlers.ErrBackfillHandBack, err)
	}
	return did, err
}
