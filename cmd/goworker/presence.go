package main

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/presencewriter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/presence"
)

// presenceBumps is bump_presence_active_time for typing and receipts: the
// presence writer in this process while it tracks presence (as a Synapse
// writer bumps itself), else the relay to the writer elsewhere. Both are
// set before any request is served.
type presenceBumps struct {
	writer *presencewriter.App
	relay  *presence.Client
}

func (b *presenceBumps) Bump(ctx context.Context, userID, deviceID string) error {
	if b.writer != nil {
		return b.writer.Bump(ctx, userID, deviceID)
	}
	return b.relay.Bump(ctx, userID, deviceID)
}

// senderPresence is the writer's states as the sender formats them.
func senderPresence(states []presencewriter.Presence) []store.PresenceState {
	out := make([]store.PresenceState, len(states))
	for i, s := range states {
		out[i] = store.PresenceState{UserID: s.UserID, State: s.State, LastActiveTS: s.LastActiveTS,
			CurrentlyActive: s.CurrentlyActive}
		if s.StatusMsg != nil {
			out[i].StatusMsg = *s.StatusMsg
		}
	}
	return out
}
