package handlers

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// The visibility filter and the redaction attachment both live in
// internal/eventfilter now, so that sliding sync uses the same ones. These are
// the shapes classic sync calls them in.
//
// Keeping the local names rather than rewriting fifteen call sites is
// deliberate: this is a move, and a move that also edits its callers is a move
// whose diff nobody reads.

// filterVisible applies visibility to a timeline, returning the events the
// caller may see with their MSC4115 membership attached.
func filterVisible(ctx context.Context, d Deps, roomID, userID string,
	events []store.TimelineEvent, isPeeking bool, nowMS int64) ([]store.TimelineEvent, []string, error) {
	return filterVisibleAlways(ctx, d, roomID, userID, events, isPeeking, nowMS, nil)
}

// filterVisibleAlways is filterVisible with an escape hatch: events in
// alwaysInclude bypass the visibility decision entirely, which is Synapse's
// `always_include_ids`.
func filterVisibleAlways(ctx context.Context, d Deps, roomID, userID string,
	events []store.TimelineEvent, isPeeking bool, nowMS int64,
	alwaysInclude map[string]bool) ([]store.TimelineEvent, []string, error) {

	res, err := eventfilter.ForClient(ctx, d.Store, roomID, userID, events, isPeeking, nowMS, alwaysInclude)
	if err != nil {
		return nil, nil, err
	}
	return res.Events, res.Memberships, nil
}

// attachRedaction marks an event as redacted and renders the redaction event
// that explains it.
func attachRedaction(stored *clientevent.Stored, redactions map[string]store.Redaction,
	timeNow int64, cfg clientevent.Config) error {
	return eventfilter.AttachRedaction(stored, redactions, timeNow, cfg)
}

// bundleAggregations works out the relations to bundle into a timeline.
func bundleAggregations(ctx context.Context, d Deps, userID string,
	messages []store.TimelineEvent, ignored map[string]bool) (map[string]eventfilter.Aggregation, []string, error) {
	return eventfilter.BundleAggregations(ctx, d.Store, userID, messages, ignored)
}

// attachAggregations writes `unsigned.m.relations` onto a serialised event.
func attachAggregations(body json.RawMessage, agg eventfilter.Aggregation,
	extra map[string]eventfilter.Aggregation, events map[string]store.StateEvent,
	timeNow int64, cfg clientevent.Config) (json.RawMessage, error) {
	return eventfilter.AttachAggregations(body, agg, extra, events, timeNow, cfg)
}

// receiptEvent renders one room's receipts as the single m.receipt event the
// legacy endpoints emit, or nil when there are none.
func receiptEvent(roomID string, rows []store.ReceiptRow, userID string,
	withThreads bool) (json.RawMessage, error) {
	return eventfilter.ReceiptEvent(roomID, rows, userID, withThreads)
}
