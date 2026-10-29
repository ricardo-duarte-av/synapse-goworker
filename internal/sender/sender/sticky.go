package sender

import (
	"context"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/queue"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/txn"
)

// StickyStore is the database access the sticky event backlog needs.
type StickyStore interface {
	BackloggedStickyEvents(ctx context.Context, destination, localPattern string, nowMS int64, limit int) (*store.StickyBacklog, error)
	MarkBackloggedStickySent(ctx context.Context, destination, roomID string, maxPosition int64) error
}

// StickyBacklog is a destination's backlog of this server's MSC4354 sticky
// events, as Synapse's StickyEventBacklogTracker drains it
// (per_destination_queue.py, v1.164.1): one room at a time, up to a
// transaction's worth, oldest first, filtered as filter_events_for_server
// filters them with redact=False and filter_out_erased_senders=True -- an
// event the destination may not see is omitted rather than sent redacted,
// since redaction would strip its stickiness anyway. Progress is recorded
// only once the transaction is delivered.
type StickyBacklog struct {
	Store  StickyStore
	Events interface {
		GetEvents(ctx context.Context, ids []string) ([]store.Event, error)
	}
	// Visible is filter_events_for_server for one event.
	Visible    func(ctx context.Context, eventID, destination string) (bool, error)
	ServerName string
	// Limit is a transaction's PDUs, MAX_PDUS_PER_TRANSACTION.
	Limit int
	// Now is replaced in tests.
	Now func() time.Time
}

// Next implements queue.StickySource.
func (b *StickyBacklog) Next(ctx context.Context, destination string) ([]queue.PDU, func(context.Context) error, bool, error) {
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	limit := b.Limit
	if limit <= 0 {
		limit = 50
	}
	backlog, err := b.Store.BackloggedStickyEvents(ctx, destination,
		store.LocalUserPattern(b.ServerName), now().UnixMilli(), limit)
	if err != nil || backlog == nil {
		return nil, nil, false, err
	}
	events, err := b.Events.GetEvents(ctx, backlog.EventIDs)
	if err != nil {
		return nil, nil, false, err
	}
	byID := make(map[string]store.Event, len(events))
	for _, e := range events {
		byID[e.EventID] = e
	}
	// get_events_as_list keeps the order asked for.
	pdus := make([]queue.PDU, 0, len(backlog.EventIDs))
	for _, id := range backlog.EventIDs {
		e, ok := byID[id]
		if !ok {
			continue
		}
		visible, err := b.Visible(ctx, id, destination)
		if err != nil {
			return nil, nil, false, err
		}
		if !visible {
			continue
		}
		body, ok := txn.SerialisePDU(e.JSON)
		if !ok {
			continue
		}
		pdus = append(pdus, queue.PDU{EventID: id, StreamOrdering: e.StreamOrdering, JSON: body})
	}
	complete := func(ctx context.Context) error {
		return b.Store.MarkBackloggedStickySent(ctx, destination, backlog.RoomID, backlog.MaxPosition)
	}
	return pdus, complete, true, nil
}
