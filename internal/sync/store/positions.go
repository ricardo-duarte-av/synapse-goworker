package store

import (
	"context"
	"fmt"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// CurrentToken derives a sync token from the database.
//
// It is APPROXIMATE, and the two ways it is wrong are known and measured (see
// docs/tokens.md):
//
//   - Typing is never persisted. It is an in-memory counter on the typing
//     worker, reaching other workers only over replication, so no query can
//     produce it. This returns 0.
//   - push_rules and thread_subscriptions drift above their table maxima,
//     because their id generators allocate ids that no surviving row records.
//
// The other twelve fields were verified exact against a token minted seconds
// earlier by the live sync worker.
//
// A real Synapse worker does not do this at all: it tracks positions from the
// replication stream. So will this one, from M5. Until then, anything that
// needs an exact token must be given one -- which is what the comparator's pin
// is for.
func (s *Store) CurrentToken(ctx context.Context) (streamtoken.Token, error) {
	// One round trip rather than fourteen. Each subquery is a MAX over an
	// indexed column, so the planner reduces them all to index scans.
	const q = `
		SELECT
			(SELECT COALESCE(MAX(stream_ordering), 0) FROM events),
			(SELECT COALESCE(MAX(stream_id), 0) FROM presence_stream),
			(SELECT COALESCE(MAX(stream_id), 0) FROM receipts_linearized),
			GREATEST(
				(SELECT COALESCE(MAX(stream_id), 0) FROM account_data),
				(SELECT COALESCE(MAX(stream_id), 0) FROM room_account_data),
				(SELECT COALESCE(MAX(stream_id), 0) FROM room_tags_revisions)),
			(SELECT COALESCE(MAX(stream_id), 0) FROM push_rules_stream),
			(SELECT COALESCE(MAX(stream_id), 0) FROM device_inbox),
			(SELECT COALESCE(MAX(stream_id), 0) FROM device_lists_stream),
			(SELECT COALESCE(MAX(stream_id), 0) FROM un_partial_stated_room_stream),
			(SELECT COALESCE(MAX(stream_id), 0) FROM thread_subscriptions),
			(SELECT COALESCE(MAX(stream_id), 0) FROM sticky_events),
			(SELECT COALESCE(MAX(stream_id), 0) FROM quarantined_media_changes),
			(SELECT COALESCE(MAX(stream_id), 0) FROM profile_updates)`

	var (
		events, presence, receipts, accountData, pushRules, toDevice  int64
		deviceLists, unPartialStated, threadSubs, sticky, quarantined int64
		profileUpdates                                                int64
	)
	if err := s.queryRow(ctx, "CurrentToken", q).Scan(
		&events, &presence, &receipts, &accountData, &pushRules, &toDevice,
		&deviceLists, &unPartialStated, &threadSubs, &sticky, &quarantined,
		&profileUpdates,
	); err != nil {
		return streamtoken.Token{}, fmt.Errorf("store: current token: %w", err)
	}

	return streamtoken.Token{
		Room:     streamtoken.Live(events),
		Presence: presence,
		// Typing is not in the database and never will be. See above.
		Typing:               0,
		Receipt:              streamtoken.MultiWriter{Stream: receipts},
		AccountData:          accountData,
		PushRules:            pushRules,
		ToDevice:             toDevice,
		DeviceList:           streamtoken.MultiWriter{Stream: deviceLists},
		Groups:               0, // dead field; Synapse says it "may have bogus values"
		UnPartialStatedRooms: unPartialStated,
		ThreadSubscriptions:  threadSubs,
		StickyEvents:         sticky,
		QuarantinedMedia:     streamtoken.MultiWriter{Stream: quarantined},
		ProfileUpdates:       profileUpdates,
	}, nil
}

// StreamPositions returns a lower bound for each replication stream, from the
// database.
//
// A seed only: the replication stream corrects these, and three of them cannot
// be right here. `typing` is absent entirely because it is never persisted --
// seeding it from anywhere would be inventing a number.
func (s *Store) StreamPositions(ctx context.Context) (map[string]int64, error) {
	tok, err := s.CurrentToken(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]int64{
		"events":                 tok.Room.Stream,
		"presence":               tok.Presence,
		"receipts":               tok.Receipt.Stream,
		"account_data":           tok.AccountData,
		"push_rules":             tok.PushRules,
		"to_device":              tok.ToDevice,
		"device_lists":           tok.DeviceList.Stream,
		"un_partial_stated_room": tok.UnPartialStatedRooms,
		"thread_subscriptions":   tok.ThreadSubscriptions,
		"sticky_events":          tok.StickyEvents,
		"quarantined_media":      tok.QuarantinedMedia.Stream,
		"profile_updates":        tok.ProfileUpdates,
	}, nil
}
