package eventfilter

import (
	"encoding/json"

	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// ReceiptEvent renders one room's receipts as a single m.receipt event, or nil
// when there are none.
//
// Shared by both sync endpoints. The private-receipt rule and MSC4102's
// unthreaded-wins rule are the reason: a second copy of either would eventually
// disagree, and disagreeing about a PRIVATE receipt means showing one user's
// read position to somebody else.
//
// withThreads selects between Synapse's TWO receipt queries, which really do
// differ:
//
//   - `_get_linearized_receipts_for_room` (singular), used by
//     /rooms/{roomId}/initialSync, does not even SELECT thread_id and emits the
//     stored data unchanged.
//   - `_get_linearized_receipts_for_rooms` (plural), used by /initialSync,
//     selects thread_id and merges through ReceiptInRoom.merge_to_content,
//     which stamps `thread_id` into the data of threaded receipts and applies
//     MSC4102.
//
// So the same receipt is rendered differently by the two endpoints. Emitting
// thread_id from the singular endpoint, or omitting it from the plural one,
// differs from Synapse on every threaded receipt.
//
// Private read receipts of other users are removed in both:
// ReceiptEventSource.filter_out_private_receipts. Skipping that would publish
// exactly what m.read.private exists to keep private.
func ReceiptEvent(roomID string, rows []store.ReceiptRow, userID string, withThreads bool) (json.RawMessage, error) {
	visible := make([]store.ReceiptRow, 0, len(rows))
	for _, row := range rows {
		if row.ReceiptType == "m.read.private" && row.UserID != userID {
			continue
		}
		visible = append(visible, row)
	}

	// MSC4102: an unthreaded receipt always wins over a threaded one for the
	// same (user, event). The MSC is explicit that this drops only
	// semantically meaningless receipts.
	unthreaded := map[[2]string]bool{}
	if withThreads {
		for _, row := range visible {
			if row.ThreadID == "" {
				unthreaded[[2]string{row.UserID, row.EventID}] = true
			}
		}
	}

	content := map[string]map[string]map[string]json.RawMessage{}
	for _, row := range visible {
		data := row.Data
		if withThreads && row.ThreadID != "" {
			if unthreaded[[2]string{row.UserID, row.EventID}] {
				continue
			}
			var err error
			data, err = sjson.SetBytes(data, "thread_id", row.ThreadID)
			if err != nil {
				return nil, err
			}
		}
		byType, ok := content[row.EventID]
		if !ok {
			byType = map[string]map[string]json.RawMessage{}
			content[row.EventID] = byType
		}
		byUser, ok := byType[row.ReceiptType]
		if !ok {
			byUser = map[string]json.RawMessage{}
			byType[row.ReceiptType] = byUser
		}
		byUser[row.UserID] = data
	}
	if len(content) == 0 {
		return nil, nil
	}
	return json.Marshal(map[string]any{
		"type": "m.receipt", "room_id": roomID, "content": content,
	})
}
