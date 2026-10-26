package handlers

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// The database's MAX(stream_ordering) can sit above an event still being
// committed. Measured on 2026-10-26: a leave was allocated 14980201 and still
// writing while 14980202 and 14980203 in other rooms committed, the table said
// 203, replication said 200, and the max of the two handed gomuks a since that
// stepped over the leave for ever. Replication must win, even when lower.
func TestOverlayTrustsReplicationOverAHigherDatabaseMax(t *testing.T) {
	db := streamtoken.Token{
		Room:       streamtoken.Live(14980203),
		Receipt:    streamtoken.MultiWriter{Stream: 90},
		ToDevice:   70,
		DeviceList: streamtoken.MultiWriter{Stream: 50},
		PushRules:  12,
	}
	positions := map[string]int64{
		replication.StreamEvents:      14980200,
		replication.StreamReceipts:    88,
		replication.StreamToDevice:    69,
		replication.StreamDeviceLists: 49,
		replication.StreamTyping:      7,
	}
	got := overlayReplication(db, func(s string) int64 { return positions[s] })

	if got.Room.String() != "s14980200" {
		t.Errorf("room = %s, want s14980200: the database max covers an uncommitted event", got.Room)
	}
	if got.Receipt.Stream != 88 || got.ToDevice != 69 || got.DeviceList.Stream != 49 {
		t.Errorf("receipt/to_device/device_list = %d/%d/%d, want 88/69/49",
			got.Receipt.Stream, got.ToDevice, got.DeviceList.Stream)
	}
	if got.Typing != 7 {
		t.Errorf("typing = %d, want 7", got.Typing)
	}
	// A stream replication has said nothing about keeps the database's value.
	if got.PushRules != 12 {
		t.Errorf("push_rules = %d, want the database's 12", got.PushRules)
	}
}

// A client may hold a token a Synapse worker minted a moment after ours.
// Handing back a lower position would make it replay; the floor stops that.
func TestNowIsBoundedBySince(t *testing.T) {
	now := streamtoken.Token{Room: streamtoken.Live(100), ToDevice: 5}
	since := streamtoken.Token{Room: streamtoken.Live(103), ToDevice: 4}

	got := now.AtLeast(since)
	if got.Room.String() != "s103" || got.ToDevice != 5 {
		t.Errorf("bounded = room %s, to_device %d; want s103 and 5", got.Room, got.ToDevice)
	}
}
