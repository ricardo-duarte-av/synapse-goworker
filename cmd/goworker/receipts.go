package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamwriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/receipts"
	syncapp "github.com/ricardo-duarte-av/synapse-goworker/internal/sync/app"
)

// receiptEvents is EventHandler.get_event for the receipts writer, from
// sync's visibility code.
type receiptEvents struct{ sync *syncapp.Sync }

func (e receiptEvents) Access(ctx context.Context, userID, roomID, eventID string) (receipts.Access, error) {
	a, err := e.sync.EventAccessFor(ctx, userID, roomID, eventID)
	if err != nil {
		return 0, err
	}
	switch a {
	case syncapp.EventVisible:
		return receipts.Visible, nil
	case syncapp.EventForbidden:
		return receipts.Forbidden, nil
	}
	return receipts.Missing, nil
}

// accountDataWriter is add_account_data_to_room on a process that does not
// write account data: a random one of its writers, called with the
// positions of the streams we write, and waited on for its own.
type accountDataWriter struct {
	client    *replication.Client
	writers   []string
	instance  string
	streams   *streamwriter.Writer
	positions *replserver.Tracker
}

func (w accountDataWriter) AddRoomAccountData(ctx context.Context, userID, roomID, dataType string, content json.RawMessage) error {
	if len(w.writers) == 0 {
		return errors.New("no account_data writer")
	}
	writer := w.writers[rand.IntN(len(w.writers))]
	_, theirs, err := w.client.AddRoomAccountData(ctx, writer, userID, roomID, dataType, content,
		replication.StreamPositions{InstanceName: w.instance, Streams: w.streams.Positions()})
	if err != nil {
		return err
	}
	for stream, pos := range theirs {
		w.positions.WaitForStreamPosition(ctx, writer, stream, pos)
	}
	return nil
}
