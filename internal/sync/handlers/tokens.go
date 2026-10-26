package handlers

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// replicationStreams maps a replication stream name onto the token field it
// sets.
var replicationStreams = map[string]func(*streamtoken.Token, int64){
	replication.StreamEvents:              func(t *streamtoken.Token, p int64) { t.Room = streamtoken.Live(p) },
	replication.StreamPresence:            func(t *streamtoken.Token, p int64) { t.Presence = p },
	replication.StreamTyping:              func(t *streamtoken.Token, p int64) { t.Typing = p },
	replication.StreamReceipts:            func(t *streamtoken.Token, p int64) { t.Receipt = streamtoken.MultiWriter{Stream: p} },
	replication.StreamAccountData:         func(t *streamtoken.Token, p int64) { t.AccountData = p },
	replication.StreamPushRules:           func(t *streamtoken.Token, p int64) { t.PushRules = p },
	replication.StreamToDevice:            func(t *streamtoken.Token, p int64) { t.ToDevice = p },
	replication.StreamDeviceLists:         func(t *streamtoken.Token, p int64) { t.DeviceList = streamtoken.MultiWriter{Stream: p} },
	replication.StreamUnPartialStatedRoom: func(t *streamtoken.Token, p int64) { t.UnPartialStatedRooms = p },
	replication.StreamThreadSubscriptions: func(t *streamtoken.Token, p int64) { t.ThreadSubscriptions = p },
	replication.StreamStickyEvents:        func(t *streamtoken.Token, p int64) { t.StickyEvents = p },
	replication.StreamQuarantinedMedia:    func(t *streamtoken.Token, p int64) { t.QuarantinedMedia = streamtoken.MultiWriter{Stream: p} },
	replication.StreamProfileUpdates:      func(t *streamtoken.Token, p int64) { t.ProfileUpdates = p },
}

// currentToken builds the token this response is bounded by.
//
// While replication is live, every stream it announces is taken from it and
// from nothing else. The database's MAX is not a position: a writer allocates
// stream ids before it commits, and commits several rooms concurrently, so the
// table maximum can sit ABOVE an event that is still being written. A token
// minted there steps over that event, and since every later window starts at
// the client's since, nothing ever reports it. That is how two leaves vanished
// on 2026-10-26: each was a slow transaction (the server leaving a room deletes
// all of its current state) overtaken by events in other rooms. A writer only
// announces a position once everything below it has committed, which is the
// property a token needs. See docs/tokens.md.
//
// The database still supplies any stream replication has not announced, and
// the whole token while replication is down -- approximate, as it was before
// M5, because there is nothing better to ask.
//
// A replication position can trail a token a client already holds, minted a
// moment later by one of Synapse's workers. Callers with a since raise the
// result to it with AtLeast, rather than this taking a maximum with a source
// that is not safe.
func currentToken(ctx context.Context, d Deps) (streamtoken.Token, error) {
	tok, err := d.Store.CurrentToken(ctx)
	if err != nil {
		return streamtoken.Token{}, err
	}
	if d.Replication == nil || !d.Replication.Live() {
		return tok, nil
	}
	return overlayReplication(tok, d.Replication.Position), nil
}

// overlayReplication replaces each stream replication has a position for.
//
// Replaces, never maximises: a maximum with the database is exactly the
// position that may cover an uncommitted event.
func overlayReplication(tok streamtoken.Token, position func(stream string) int64) streamtoken.Token {
	for stream, set := range replicationStreams {
		if pos := position(stream); pos > 0 {
			set(&tok, pos)
		}
	}
	return tok
}
