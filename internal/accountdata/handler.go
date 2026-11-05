package accountdata

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/cachestream"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// ErrNotWriter is a write for a process that does not write account data.
var ErrNotWriter = errors.New("accountdata: this process is not the account-data writer")

// Callers, for metrics.
const (
	CallerClient      = "client"
	CallerReplication = "replication"
	CallerLocal       = "local"
)

// write takes a stream ID, runs fn with it, finishes the ID (failed or
// not, as MultiWriterIdGenerator's context manager does) and returns the
// stream's current token, as every AccountDataHandler write returns
// get_current_token(). viaCaches runs fn holding the caches stream, for a
// write whose transaction may add caches rows.
func (a *App) write(ctx context.Context, op, caller string, viaCaches bool,
	fn func(id int64) (changed bool, rows []cachestream.Row, err error)) (int64, error) {
	if !a.writing {
		writes.WithLabelValues(op, caller, "error").Inc()
		return 0, ErrNotWriter
	}
	id, err := a.ids.Next(ctx)
	if err != nil {
		writes.WithLabelValues(op, caller, "error").Inc()
		return 0, err
	}
	changed := true
	run := func() ([]cachestream.Row, error) {
		var rows []cachestream.Row
		var err error
		changed, rows, err = fn(id.Value)
		return rows, err
	}
	if viaCaches {
		err = a.Caches.Do(run)
	} else {
		_, err = run()
	}
	var perr *cachestream.PublishError
	if errors.As(err, &perr) {
		// The rows are committed: a lost RDATA is caught up from the
		// database at our next announcement, as any process's would be.
		cachePublishFailures.Inc()
		a.Log.Warn().Err(err).Str("op", op).Msg("account data: caches rows written but not all published")
		err = nil
	}
	if ferr := id.Finish(ctx, err == nil); err == nil && ferr != nil {
		err = ferr
	}
	if err != nil {
		writes.WithLabelValues(op, caller, "error").Inc()
		return 0, err
	}
	if changed {
		writes.WithLabelValues(op, caller, "ok").Inc()
	} else {
		writes.WithLabelValues(op, caller, "unchanged").Inc()
	}
	return a.ids.Position(), nil
}

// AddUser is add_account_data_for_user.
func (a *App) AddUser(ctx context.Context, caller, userID, dataType string, content json.RawMessage) (int64, error) {
	return a.write(ctx, "add_user", caller, dataType == TypeIgnoredUserList, func(id int64) (bool, []cachestream.Row, error) {
		// json_encoder.encode inside the transaction, after the ID.
		enc, err := pyjson.Encode(content)
		if err != nil {
			return false, nil, err
		}
		rows, err := a.Store.AddUser(ctx, a.Instance, id, userID, dataType, enc)
		return true, rows, err
	})
}

// RemoveUser is remove_account_data_for_user (MSC3391).
func (a *App) RemoveUser(ctx context.Context, caller, userID, dataType string) (int64, error) {
	return a.write(ctx, "remove_user", caller, dataType == TypeIgnoredUserList, func(id int64) (bool, []cachestream.Row, error) {
		return a.Store.RemoveUser(ctx, a.Instance, id, userID, dataType)
	})
}

// AddRoom is add_account_data_to_room.
func (a *App) AddRoom(ctx context.Context, caller, userID, roomID, dataType string, content json.RawMessage) (int64, error) {
	// json_encoder.encode before the ID is taken.
	enc, err := pyjson.Encode(content)
	if err != nil {
		writes.WithLabelValues("add_room", caller, "error").Inc()
		return 0, err
	}
	return a.write(ctx, "add_room", caller, false, func(id int64) (bool, []cachestream.Row, error) {
		return true, nil, a.Store.AddRoom(ctx, id, userID, roomID, dataType, enc)
	})
}

// RemoveRoom is remove_account_data_for_room (MSC3391).
func (a *App) RemoveRoom(ctx context.Context, caller, userID, roomID, dataType string) (int64, error) {
	return a.write(ctx, "remove_room", caller, false, func(id int64) (bool, []cachestream.Row, error) {
		updated, err := a.Store.RemoveRoom(ctx, id, userID, roomID, dataType)
		return updated, nil, err
	})
}

// AddTag is add_tag_to_room.
func (a *App) AddTag(ctx context.Context, caller, userID, roomID, tag string, content json.RawMessage) (int64, error) {
	enc, err := pyjson.Encode(content)
	if err != nil {
		writes.WithLabelValues("add_tag", caller, "error").Inc()
		return 0, err
	}
	return a.write(ctx, "add_tag", caller, false, func(id int64) (bool, []cachestream.Row, error) {
		return true, nil, a.Store.AddTag(ctx, id, userID, roomID, tag, enc)
	})
}

// RemoveTag is remove_tag_from_room.
func (a *App) RemoveTag(ctx context.Context, caller, userID, roomID, tag string) (int64, error) {
	return a.write(ctx, "remove_tag", caller, false, func(id int64) (bool, []cachestream.Row, error) {
		return true, nil, a.Store.RemoveTag(ctx, id, userID, roomID, tag)
	})
}

// AddRoomAccountData is add_account_data_to_room for this process's own
// callers (the receipts writer's read marker): no replication hop.
func (a *App) AddRoomAccountData(ctx context.Context, userID, roomID, dataType string, content json.RawMessage) error {
	_, err := a.AddRoom(ctx, CallerLocal, userID, roomID, dataType, content)
	return err
}
