package app

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/notifier"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// profileWaker wakes the rooms of a user whose profile_updates row arrived,
// as Synapse's replication client does (replication/tcp/client.py: the
// rooms of the updated users, from get_rooms_for_users): a user sharing a
// room is sent the update. The notifier itself wakes only the user.
//
// The lookup runs off the replication goroutine, which must not wait on
// the database.
type profileWaker struct {
	ctx   context.Context
	db    *store.Store
	notif *notifier.Notifier
	log   zerolog.Logger
}

func (p *profileWaker) OnStreamAdvance(stream string, pos int64, _, userIDs []string) {
	if stream != replication.StreamProfileUpdates || len(userIDs) == 0 {
		return
	}
	users := append([]string(nil), userIDs...)
	go func() {
		ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
		defer cancel()
		rooms, err := p.db.RoomsOfUsers(ctx, users)
		if err != nil {
			p.log.Warn().Err(err).Msg("sync: rooms of a profile update; waking everyone")
			p.notif.OnStreamAdvance(stream, pos, nil, nil)
			return
		}
		if len(rooms) > 0 {
			p.notif.OnStreamAdvance(stream, pos, rooms, nil)
		}
	}()
}
