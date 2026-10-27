package app

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/presence"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/synapsecfg"
)

// reapSlidingConnections collects connections nobody has used for a week.
//
// Not optional. Reading a position prunes that connection's own forks and
// nothing else, so a client that simply stops coming back leaves its connection
// and every row hanging off it behind for ever. Synapse runs the same job
// hourly (CONNECTION_EXPIRY_FREQUENCY).
func reapSlidingConnections(ctx context.Context, sliding *slidingstore.Store,
	intervalMinutes int, log zerolog.Logger) {

	if intervalMinutes <= 0 {
		intervalMinutes = 60
	}
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reapCtx, cancel := context.WithTimeout(ctx, time.Minute)
			n, err := sliding.DeleteOldConnections(reapCtx)
			cancel()
			if err != nil {
				log.Error().Err(err).Msg("could not reap sliding sync connections")
				continue
			}
			if n > 0 {
				log.Info().Int64("connections", n).Msg("reaped stale sliding sync connections")
			}
		}
	}
}

// resolvePresence re-reads the presence writer's address and secret out of
// Synapse's homeserver.yaml.
//
// Handed to the presence client rather than called on a timer: the only moment
// this worker has evidence that its copy is stale is a relay that failed, and
// polling a file that changes twice a year is worse than reading it on the two
// failures that mean it changed.
//
// Presence being turned off since we started is reported as an error rather
// than as an empty writer. It leaves the existing client in place, still
// relaying to a writer that now ignores it -- which is harmless, and strictly
// better than this function's alternative of tearing down a working relay on
// the strength of one unreadable read.
func resolvePresence(path string) func() (presence.Config, error) {
	if path == "" {
		return nil
	}
	return func() (presence.Config, error) {
		sc, err := synapsecfg.Load(path)
		if err != nil {
			return presence.Config{}, err
		}
		if !sc.PresenceEnabled {
			return presence.Config{}, fmt.Errorf("presence is now disabled on this homeserver")
		}
		return presence.Config{
			Socket: sc.PresenceWriter.Socket,
			URL:    sc.PresenceWriter.URL,
			Secret: sc.ReplicationSecret,
		}, nil
	}
}
