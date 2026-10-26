package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// ToDeviceLimit is how many to-device messages one sync may carry.
//
// Synapse's get_messages_for_device takes limit=100 by default and /sync never
// overrides it. Hitting the limit is not an error: the response's now_token is
// wound back to the last message returned, so the client's next sync resumes
// exactly there. See MessagesForDevice.
const ToDeviceLimit = 100

// MessagesForDevice returns to-device messages for one device in (from, to],
// and the stream position the caller should use as the next `from`.
//
// Mirrors storage/databases/main/deviceinbox.py get_messages_for_device. The
// returned position is the subtle part: when fewer than `limit` rows come back
// we know the range is exhausted and return `to`, but when the limit is reached
// we return the stream id of the LAST ROW instead, because there may be more
// messages at positions we have not looked at. Returning `to` in that case
// would silently drop them -- and a dropped to-device message is a room key a
// client never receives.
//
// Message JSON is passed through verbatim rather than decoded and re-encoded,
// for the same reason event JSON is: what Synapse stored is what the client
// must see.
func (s *Store) MessagesForDevice(ctx context.Context, userID, deviceID string,
	from, to int64, limit int) ([]json.RawMessage, int64, error) {

	if deviceID == "" || from >= to || limit <= 0 {
		return nil, to, nil
	}

	const q = `
		SELECT stream_id, message_json FROM device_inbox
		WHERE user_id = $1 AND device_id = $2
		  AND $3 < stream_id AND stream_id <= $4
		ORDER BY stream_id ASC
		LIMIT $5`

	rows, err := s.query(ctx, "MessagesForDevice", q, userID, deviceID, from, to, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("store: to-device messages: %w", err)
	}
	defer rows.Close()

	var (
		messages []json.RawMessage
		last     int64
	)
	for rows.Next() {
		var (
			streamID int64
			body     []byte
		)
		if err := rows.Scan(&streamID, &body); err != nil {
			return nil, 0, fmt.Errorf("store: to-device messages: %w", err)
		}
		last = streamID
		messages = append(messages, json.RawMessage(body))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("store: to-device messages: %w", err)
	}

	if len(messages) == limit {
		return messages, last, nil
	}
	return messages, to, nil
}
