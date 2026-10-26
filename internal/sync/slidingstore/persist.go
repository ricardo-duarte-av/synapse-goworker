package slidingstore

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/metrics"
)

// Persist writes updated per-connection state and returns the position to hand
// the client.
//
// If nothing changed it returns the incoming position unchanged and writes
// NOTHING. That short-circuit is load-bearing rather than an optimisation:
// every new position copies the previous one's rows forward, measured at ~725
// stream rows plus ~248 room-config rows for a 654-room connection, and Element
// X holds three connections per device. A response that changed nothing must
// not cost that.
//
// previousPosition of 0 means "start a new connection", which deletes any
// existing connection for the same (user, device, conn_id) triple and cascades
// its whole history away. That is the right response to a client that has
// discarded its state: keeping the old rows would let one client accumulate a
// connection per restart.
func (s *Store) Persist(
	ctx context.Context, userID, deviceID, connID string,
	previousPosition int64, state *PerConnectionState,
) (int64, error) {
	now := s.now()
	if !state.HasUpdates(now) {
		return previousPosition, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("slidingstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	connectionKey, err := s.connectionKey(ctx, tx, userID, deviceID, connID, previousPosition, now)
	if err != nil {
		return 0, err
	}

	var position int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO sliding_sync_connection_positions (connection_key, created_ts)
		VALUES ($1, $2) RETURNING connection_position`,
		connectionKey, now).Scan(&position); err != nil {
		return 0, fmt.Errorf("slidingstore: new position: %w", err)
	}

	roomToStateID, err := s.resolveRequiredState(ctx, tx, connectionKey, previousPosition, state)
	if err != nil {
		return 0, err
	}

	if previousPosition != 0 {
		if err := copyForward(ctx, tx, position, previousPosition); err != nil {
			return 0, err
		}
	}

	if err := upsertStreams(ctx, tx, position, state); err != nil {
		return 0, err
	}
	if err := upsertRoomConfigs(ctx, tx, position, state, roomToStateID); err != nil {
		return 0, err
	}
	if err := persistLazyMembers(ctx, tx, connectionKey, position, state, now); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("slidingstore: commit: %w", err)
	}
	metrics.SlidingSyncPositionsMinted.Inc()
	return position, nil
}

// connectionKey finds the connection a position belongs to, or starts a new one.
func (s *Store) connectionKey(
	ctx context.Context, tx pgx.Tx, userID, deviceID, connID string,
	previousPosition, now int64,
) (int64, error) {
	if previousPosition != 0 {
		var key int64
		// FOR NO KEY UPDATE locks the connection row up front. Without it two
		// concurrent requests on one connection deadlock against each other on
		// the lazy-member unique index and retry forever without progress --
		// and Element X runs three connections per device, so concurrent
		// requests are the normal case rather than the exceptional one.
		err := tx.QueryRow(ctx, `
			SELECT connection_key
			  FROM sliding_sync_connection_positions
			  JOIN sliding_sync_connections USING (connection_key)
			 WHERE connection_position = $1
			   AND user_id = $2 AND effective_device_id = $3 AND conn_id = $4
			   FOR NO KEY UPDATE OF sliding_sync_connections`,
			previousPosition, userID, deviceID, connID).Scan(&key)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrUnknownPosition
		}
		if err != nil {
			return 0, fmt.Errorf("slidingstore: look up connection: %w", err)
		}
		return key, nil
	}

	// A fresh connection. Deleting the old row for this triple cascades through
	// all five dependent tables, which is the point: a client that keeps
	// starting over must not leave a connection behind each time.
	if _, err := tx.Exec(ctx, `
		DELETE FROM sliding_sync_connections
		 WHERE user_id = $1 AND effective_device_id = $2 AND conn_id = $3`,
		userID, deviceID, connID); err != nil {
		return 0, fmt.Errorf("slidingstore: clear old connection: %w", err)
	}

	var key int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO sliding_sync_connections
		  (user_id, effective_device_id, conn_id, created_ts, last_used_ts)
		VALUES ($1, $2, $3, $4, $4) RETURNING connection_key`,
		userID, deviceID, connID, now).Scan(&key); err != nil {
		return 0, fmt.Errorf("slidingstore: new connection: %w", err)
	}
	return key, nil
}

// resolveRequiredState maps each room to a required_state row, reusing existing
// rows where the encoding matches.
//
// Deduplication is what keeps this table from holding one row per room per
// connection. Even with it, it is the largest of the six on the live server.
func (s *Store) resolveRequiredState(
	ctx context.Context, tx pgx.Tx, connectionKey, previousPosition int64,
	state *PerConnectionState,
) (map[string]int64, error) {
	existing := map[string]int64{}
	if previousPosition != 0 {
		rows, err := tx.Query(ctx, `
			SELECT required_state_id, required_state
			  FROM sliding_sync_connection_required_state
			 WHERE connection_key = $1`, connectionKey)
		if err != nil {
			return nil, fmt.Errorf("slidingstore: load required state: %w", err)
		}
		for rows.Next() {
			var id int64
			var encoded string
			if err := rows.Scan(&id, &encoded); err != nil {
				rows.Close()
				return nil, fmt.Errorf("slidingstore: load required state: %w", err)
			}
			existing[encoded] = id
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("slidingstore: load required state: %w", err)
		}
	}

	roomToStateID := map[string]int64{}
	newRooms := map[string][]string{}
	for roomID, cfg := range state.RoomConfigs {
		encoded, err := EncodeRequiredState(cfg.RequiredState)
		if err != nil {
			return nil, fmt.Errorf("slidingstore: encode required state for %s: %w", roomID, err)
		}
		if id, ok := existing[encoded]; ok {
			roomToStateID[roomID] = id
			continue
		}
		newRooms[encoded] = append(newRooms[encoded], roomID)
	}

	// Sorted so a failure is reproducible and the ids a test sees are stable.
	encodings := make([]string, 0, len(newRooms))
	for encoded := range newRooms {
		encodings = append(encodings, encoded)
	}
	sort.Strings(encodings)

	for _, encoded := range encodings {
		var id int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO sliding_sync_connection_required_state (connection_key, required_state)
			VALUES ($1, $2) RETURNING required_state_id`,
			connectionKey, encoded).Scan(&id); err != nil {
			return nil, fmt.Errorf("slidingstore: insert required state: %w", err)
		}
		for _, roomID := range newRooms[encoded] {
			roomToStateID[roomID] = id
		}
	}
	return roomToStateID, nil
}

// copyForward makes the new position a complete snapshot before the changes are
// applied on top.
//
// Each position MUST be complete: hold only what one request touched, and every
// room the client was told about earlier silently reverts to NEVER, meaning its
// full state is re-sent on the next request that mentions it.
//
// This is one of TWO mechanisms that guarantee that, and Synapse carries both.
// The other is upsertStreams/upsertRoomConfigs writing the flattened state
// rather than only the changes. Verified by mutation on 2026-09-03: removing
// either alone leaves the tests green, removing both fails them. Neither is
// dead code -- they cover the same guarantee from opposite ends, one from the
// row already in the database and one from the state in memory -- but a reader
// looking for the single place completeness is enforced will not find it, so it
// is written down here instead.
func copyForward(ctx context.Context, tx pgx.Tx, position, previous int64) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO sliding_sync_connection_streams
		  (connection_position, stream, room_id, room_status, last_token)
		SELECT $1, stream, room_id, room_status, last_token
		  FROM sliding_sync_connection_streams WHERE connection_position = $2`,
		position, previous); err != nil {
		return fmt.Errorf("slidingstore: copy streams forward: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO sliding_sync_connection_room_configs
		  (connection_position, room_id, timeline_limit, required_state_id)
		SELECT $1, room_id, timeline_limit, required_state_id
		  FROM sliding_sync_connection_room_configs WHERE connection_position = $2`,
		position, previous); err != nil {
		return fmt.Errorf("slidingstore: copy room configs forward: %w", err)
	}
	return nil
}

func upsertStreams(ctx context.Context, tx pgx.Tx, position int64, state *PerConnectionState) error {
	type row struct {
		stream, roomID string
		hs             HaveSent
	}
	var rows []row
	for _, s := range []struct {
		name string
		m    *RoomStatusMap
	}{
		{StreamRooms, &state.Rooms},
		{StreamReceipts, &state.Receipts},
		{StreamAccountData, &state.AccountData},
	} {
		for roomID, hs := range s.m.All() {
			rows = append(rows, row{s.name, roomID, hs})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].stream != rows[j].stream {
			return rows[i].stream < rows[j].stream
		}
		return rows[i].roomID < rows[j].roomID
	})

	batch := &pgx.Batch{}
	for _, r := range rows {
		var token *string
		if r.hs.Status == FlagPreviously {
			t := r.hs.LastToken
			token = &t
		}
		batch.Queue(`
			INSERT INTO sliding_sync_connection_streams
			  (connection_position, stream, room_id, room_status, last_token)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (connection_position, room_id, stream)
			DO UPDATE SET room_status = EXCLUDED.room_status, last_token = EXCLUDED.last_token`,
			position, r.stream, r.roomID, string(r.hs.Status), token)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("slidingstore: upsert streams: %w", err)
	}
	metrics.SlidingSyncRowsWritten.WithLabelValues("streams").Add(float64(len(rows)))
	return nil
}

func upsertRoomConfigs(
	ctx context.Context, tx pgx.Tx, position int64,
	state *PerConnectionState, roomToStateID map[string]int64,
) error {
	if len(state.RoomConfigs) == 0 {
		return nil
	}
	roomIDs := make([]string, 0, len(state.RoomConfigs))
	for roomID := range state.RoomConfigs {
		roomIDs = append(roomIDs, roomID)
	}
	sort.Strings(roomIDs)

	batch := &pgx.Batch{}
	for _, roomID := range roomIDs {
		id, ok := roomToStateID[roomID]
		if !ok {
			return fmt.Errorf("slidingstore: no required_state id resolved for %s", roomID)
		}
		batch.Queue(`
			INSERT INTO sliding_sync_connection_room_configs
			  (connection_position, room_id, timeline_limit, required_state_id)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (connection_position, room_id)
			DO UPDATE SET timeline_limit = EXCLUDED.timeline_limit,
			              required_state_id = EXCLUDED.required_state_id`,
			position, roomID, state.RoomConfigs[roomID].TimelineLimit, id)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("slidingstore: upsert room configs: %w", err)
	}
	metrics.SlidingSyncRowsWritten.WithLabelValues("room_configs").Add(float64(len(roomIDs)))
	return nil
}

// persistLazyMembers records which lazily-loaded memberships have been sent.
//
// Two deliberate looseness decisions, both Synapse's, and both safe only
// because this is a cache whose worst failure is a member event sent twice:
//
//   - An upsert that would collide with a row written against a DIFFERENT
//     position is skipped rather than retried. That row belongs to a fork we
//     cannot yet know the fate of; losing this update just re-sends a member.
//   - An invalidation is applied across every fork without matching on
//     position, because removing an entry can only ever cost a re-send.
func persistLazyMembers(
	ctx context.Context, tx pgx.Tx, connectionKey, position int64,
	state *PerConnectionState, now int64,
) error {
	type pair struct{ roomID, userID string }
	var toUpsert, toRemove []pair

	roomIDs := make([]string, 0, len(state.LazyMembership))
	for roomID := range state.LazyMembership {
		roomIDs = append(roomIDs, roomID)
	}
	sort.Strings(roomIDs)

	for _, roomID := range roomIDs {
		changes := state.LazyMembership[roomID]
		for _, userID := range changes.ReturnedToUpdate(now) {
			toUpsert = append(toUpsert, pair{roomID, userID})
		}
		invalidated := make([]string, 0, len(changes.Invalidated))
		for userID := range changes.Invalidated {
			invalidated = append(invalidated, userID)
		}
		sort.Strings(invalidated)
		for _, userID := range invalidated {
			toRemove = append(toRemove, pair{roomID, userID})
		}
	}

	if len(toUpsert) > 0 {
		batch := &pgx.Batch{}
		for _, p := range toUpsert {
			batch.Queue(`
				INSERT INTO sliding_sync_connection_lazy_members
				  (connection_key, connection_position, room_id, user_id, last_seen_ts)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (connection_key, room_id, user_id)
				DO UPDATE SET last_seen_ts = EXCLUDED.last_seen_ts
				WHERE sliding_sync_connection_lazy_members.connection_position IS NULL
				   OR sliding_sync_connection_lazy_members.connection_position = EXCLUDED.connection_position`,
				connectionKey, position, p.roomID, p.userID, now)
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("slidingstore: upsert lazy members: %w", err)
		}
		metrics.SlidingSyncRowsWritten.WithLabelValues("lazy_members").Add(float64(len(toUpsert)))
	}

	if len(toRemove) > 0 {
		batch := &pgx.Batch{}
		for _, p := range toRemove {
			batch.Queue(`
				DELETE FROM sliding_sync_connection_lazy_members
				 WHERE connection_key = $1 AND room_id = $2 AND user_id = $3`,
				connectionKey, p.roomID, p.userID)
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("slidingstore: remove lazy members: %w", err)
		}
	}
	return nil
}

// LazyMembersSent returns which of the given users this connection has already
// been told about in a room.
func (s *Store) LazyMembersSent(
	ctx context.Context, connectionKey int64, roomID string, userIDs []string,
) (map[string]int64, error) {
	out := map[string]int64{}
	if len(userIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, last_seen_ts
		  FROM sliding_sync_connection_lazy_members
		 WHERE connection_key = $1 AND room_id = $2 AND user_id = ANY($3)`,
		connectionKey, roomID, userIDs)
	if err != nil {
		return nil, fmt.Errorf("slidingstore: load lazy members: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var userID string
		var ts int64
		if err := rows.Scan(&userID, &ts); err != nil {
			return nil, fmt.Errorf("slidingstore: load lazy members: %w", err)
		}
		out[userID] = ts
	}
	return out, rows.Err()
}

// DeleteOldConnections drops connections nobody has used in ConnectionExpiryMS.
//
// Not optional. Reading a position prunes the other positions on its
// connection, but nothing prunes a connection whose client simply went away --
// and the cascade from this one delete is what keeps all five dependent tables
// bounded. Its cost to a returning client is one M_UNKNOWN_POS.
func (s *Store) DeleteOldConnections(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM sliding_sync_connections
		 WHERE last_used_ts IS NOT NULL AND last_used_ts < $1`,
		s.now()-ConnectionExpiryMS)
	if err != nil {
		return 0, fmt.Errorf("slidingstore: reap connections: %w", err)
	}
	metrics.SlidingSyncConnectionsReaped.Add(float64(tag.RowsAffected()))
	return tag.RowsAffected(), nil
}
