package handlers

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/auth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// toDeviceEvents builds the to_device section and, when it is truncated,
// winds the response's now token back.
//
// Mirrors _generate_sync_entry_for_to_device. Two things about it are easy to
// get wrong:
//
//   - The section is served on an INITIAL sync too, from position 0. A device
//     that has never synced still has an inbox waiting for it.
//   - Only 100 messages are returned at a time, and when that limit is hit the
//     now token's to_device field is REPLACED by the position of the last
//     message returned -- not left at the current stream position. The client's
//     next_batch therefore resumes mid-backlog, and the rest arrives on the
//     following sync. Leaving the token alone would skip everything past the
//     hundredth message.
//
// Returns nil when the worker is not configured to serve the section at all.
// That is not a degraded mode: see the note on deviceinbox, serving to_device
// without being able to delete it is worse than not serving it.
func toDeviceEvents(ctx context.Context, d Deps, verdict auth.Verdict,
	from int64, now *streamtoken.Token) ([]json.RawMessage, error) {

	if d.Inbox == nil || verdict.DeviceID == "" || from == now.ToDevice {
		return nil, nil
	}
	messages, next, err := d.Store.MessagesForDevice(ctx, verdict.UserID, verdict.DeviceID,
		from, now.ToDevice, store.ToDeviceLimit)
	if err != nil {
		return nil, err
	}
	now.ToDevice = next
	return messages, nil
}

// deleteAcknowledgedToDevice clears the messages this client's `since` proves
// it has already received.
//
// Synapse does this once per /sync request, before it starts waiting, and
// bounds it by since_token.to_device_key: everything at or below the position
// the client is syncing from was in a response the client demonstrably holds.
// Messages inside the window this request will return are NOT deleted here --
// they go out in this response, and the next request's `since` acknowledges
// them.
//
// A failure is logged rather than returned. The cost of not deleting is that a
// device sees a message twice; the cost of failing the sync is that it sees
// nothing at all, and a transient database error should not be the difference
// between a client working and not.
func deleteAcknowledgedToDevice(ctx context.Context, d Deps, verdict auth.Verdict, since streamtoken.Token) {
	if d.Inbox == nil || verdict.DeviceID == "" {
		return
	}
	deleted, err := d.Inbox.DeleteUpTo(ctx, verdict.UserID, verdict.DeviceID, since.ToDevice)
	if err != nil {
		d.Log.Warn().Err(err).Str("user_id", verdict.UserID).
			Str("device_id", verdict.DeviceID).Int64("up_to", since.ToDevice).
			Msg("could not delete acknowledged to-device messages")
		return
	}
	if deleted > 0 {
		metrics.ToDeviceDeleted.Add(float64(deleted))
		d.Log.Debug().Str("user_id", verdict.UserID).Str("device_id", verdict.DeviceID).
			Int("deleted", deleted).Int64("up_to", since.ToDevice).
			Msg("deleted acknowledged to-device messages")
	}
}
