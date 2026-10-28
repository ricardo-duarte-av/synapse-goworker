package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// ReceiptRow is one row of receipts_linearized.
type ReceiptRow struct {
	ReceiptType  string
	UserID       string
	EventID      string
	InstanceName string
	StreamID     int64
	// ThreadID is empty for an unthreaded receipt. Only the multi-room query
	// reads it; see receiptEvent for why the two endpoints differ.
	ThreadID string
	Data     json.RawMessage
}

// RoomReceipts loads a room's receipts up to a position.
//
// Mirrors _get_linearized_receipts_for_room: the query bounds by the highest
// position any writer reached, and rows outside the token's per-writer range
// are dropped afterwards. Bounding by the agreed minimum instead would silently
// omit receipts a writer ahead of it has already persisted.
func (s *Store) RoomReceipts(ctx context.Context, roomID string, to streamtoken.MultiWriter) ([]ReceiptRow, error) {
	const q = `
		SELECT stream_id, COALESCE(instance_name, ''), receipt_type, user_id, event_id, data
		  FROM receipts_linearized
		 WHERE room_id = $1 AND stream_id <= $2`
	rows, err := s.query(ctx, "RoomReceipts", q, roomID, to.MaxStreamPos())
	if err != nil {
		return nil, fmt.Errorf("store: room receipts: %w", err)
	}
	defer rows.Close()

	var out []ReceiptRow
	for rows.Next() {
		var r ReceiptRow
		var data string
		if err := rows.Scan(&r.StreamID, &r.InstanceName, &r.ReceiptType,
			&r.UserID, &r.EventID, &data); err != nil {
			return nil, fmt.Errorf("store: room receipts: %w", err)
		}
		r.Data = json.RawMessage(data)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: room receipts: %w", err)
	}
	return out, nil
}

// AccountDataEntry is one account data type and its content.
type AccountDataEntry struct {
	Type    string
	Content json.RawMessage
}

// RoomAccountData loads a user's account data for one room, plus their tags.
//
// Synapse emits the tags first, as a synthetic `m.tag` event, then the stored
// room account data. Tags live in their own table rather than in
// room_account_data, so they would be missed entirely by the obvious query.
// Note: NO msc3391 filter, deliberately. `get_account_data_for_room` --
// the per-room query, used by /rooms/{roomId}/initialSync -- is a plain
// simple_select_list with no `content != '{}'` clause, while
// `get_room_account_data_for_user` -- the whole-account query behind
// /initialSync and /sync -- has one. So a deleted entry is hidden by one
// endpoint and shown by the other. The same asymmetry as the two receipt
// queries; mirror it rather than pick a side.
func (s *Store) RoomAccountData(ctx context.Context, userID, roomID string) ([]AccountDataEntry, error) {
	var out []AccountDataEntry

	tags, err := s.roomTags(ctx, userID, roomID)
	if err != nil {
		return nil, err
	}
	if tags != nil {
		out = append(out, AccountDataEntry{Type: "m.tag", Content: tags})
	}

	const q = `
		SELECT account_data_type, content
		  FROM room_account_data
		 WHERE user_id = $1 AND room_id = $2
		 ORDER BY account_data_type`
	rows, err := s.query(ctx, "RoomAccountData", q, userID, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: room account data: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e AccountDataEntry
		var content string
		if err := rows.Scan(&e.Type, &content); err != nil {
			return nil, fmt.Errorf("store: room account data: %w", err)
		}
		// Same rule as AllRoomAccountData: a stored m.tag row must not be
		// emitted beside the synthesised one. Synapse's per-room account data
		// is a map from type to content, so the synthesised tag replaces the
		// stored one rather than joining it.
		if e.Type == "m.tag" && tags != nil {
			continue
		}
		e.Content = json.RawMessage(content)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: room account data: %w", err)
	}
	return out, nil
}

// roomTags returns `{"tags": {tag: content}}`, or nil when the user has none.
func (s *Store) roomTags(ctx context.Context, userID, roomID string) (json.RawMessage, error) {
	const q = `SELECT tag, content FROM room_tags WHERE user_id = $1 AND room_id = $2`
	rows, err := s.query(ctx, "roomTags", q, userID, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: room tags: %w", err)
	}
	defer rows.Close()

	tags := map[string]json.RawMessage{}
	for rows.Next() {
		var tag, content string
		if err := rows.Scan(&tag, &content); err != nil {
			return nil, fmt.Errorf("store: room tags: %w", err)
		}
		tags[tag] = json.RawMessage(content)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: room tags: %w", err)
	}
	if len(tags) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{"tags": tags})
	if err != nil {
		return nil, fmt.Errorf("store: room tags: %w", err)
	}
	return body, nil
}

// PresenceState is one user's stored presence.
type PresenceState struct {
	UserID          string
	State           string
	LastActiveTS    int64
	HasLastActive   bool
	StatusMsg       string
	HasStatusMsg    bool
	CurrentlyActive bool
}

// Presence loads the stored presence of the given users.
//
// Users with no row are omitted. Synapse's presence handler substitutes a
// default "offline" state for them, which is a handler concern rather than a
// storage one.
func (s *Store) Presence(ctx context.Context, userIDs []string) ([]PresenceState, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT user_id, state, last_active_ts, status_msg, currently_active
		  FROM presence_stream
		 WHERE user_id = ANY($1)`
	rows, err := s.query(ctx, "Presence", q, userIDs)
	if err != nil {
		return nil, fmt.Errorf("store: presence: %w", err)
	}
	defer rows.Close()
	return scanPresence(rows)
}

// scanPresence reads presence rows. Every column but user_id is nullable, and a
// NULL is not the same as a zero: a user with no last_active_ts must have no
// last_active_ago field at all, not one reading "now".
func scanPresence(rows pgx.Rows) ([]PresenceState, error) {
	var out []PresenceState
	for rows.Next() {
		var (
			p         PresenceState
			state     *string
			lastAct   *int64
			statusMsg *string
			active    *bool
		)
		if err := rows.Scan(&p.UserID, &state, &lastAct, &statusMsg, &active); err != nil {
			return nil, fmt.Errorf("store: presence: %w", err)
		}
		if state != nil {
			p.State = *state
		}
		if lastAct != nil {
			p.LastActiveTS, p.HasLastActive = *lastAct, true
		}
		if statusMsg != nil {
			p.StatusMsg, p.HasStatusMsg = *statusMsg, true
		}
		if active != nil {
			p.CurrentlyActive = *active
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: presence: %w", err)
	}
	return out, nil
}
