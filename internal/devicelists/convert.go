package devicelists

import (
	"context"
	"slices"
	"time"
)

// convertBatch is get_uncoverted_outbound_room_pokes' limit.
const convertBatch = 10

// convertTimeout bounds one run of the loop's database work.
const convertTimeout = 10 * time.Minute

// HandleNewDeviceUpdate is handle_new_device_update on the first writer:
// the conversion runs in the background, and a wake while it runs makes
// it go round once more.
func (a *App) HandleNewDeviceUpdate() {
	if !a.writing {
		return
	}
	a.convMu.Lock()
	if a.converting {
		a.convNewData = true
		a.convMu.Unlock()
		return
	}
	a.converting = true
	a.convMu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), convertTimeout)
		defer cancel()
		if err := a.convert(ctx); err != nil {
			// convert clears converting itself when it finishes cleanly.
			a.convMu.Lock()
			a.converting = false
			a.convMu.Unlock()
			pokes.WithLabelValues("error").Inc()
			a.Log.Error().Err(err).Msg("device lists: converting changes to outbound pokes failed; the next change retries")
		}
	}()
}

// convert is _handle_new_device_update_async's body: from the last
// converted position, each local user's change in a room becomes pokes
// for the room's other servers (those already poked for the same stream ID
// left out), and the position moves past what was done.
func (a *App) convert(ctx context.Context) error {
	var current int64 = -1 // current_stream_id: none yet
	sent := map[string]bool{}
	stream, room, err := a.Store.ConvertedPos(ctx)
	if err != nil {
		return err
	}
	for {
		a.convMu.Lock()
		a.convNewData = false
		a.convMu.Unlock()
		maxStream := a.ids.Position()
		rows, err := a.Store.Unconverted(ctx, stream, room, maxStream, convertBatch)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			if maxStream > stream {
				stream, room = maxStream, ""
				if err := a.Store.SetConvertedPos(ctx, a.Instance, stream, room); err != nil {
					return err
				}
			}
			// The check and the end are one step, so a wake in between is
			// not lost (Synapse's loop is single-threaded and cannot race).
			a.convMu.Lock()
			if a.convNewData {
				a.convMu.Unlock()
				continue
			}
			a.converting = false
			a.convMu.Unlock()
			return nil
		}
		for _, c := range rows {
			var hosts []string
			if a.isMine(c.UserID) {
				set, err := a.Store.Hosts(ctx, c.RoomID)
				if err != nil {
					return err
				}
				for h := range set {
					if h != a.serverName {
						hosts = append(hosts, h)
					}
				}
				slices.Sort(hosts)
			}
			if current == c.StreamID {
				hosts = slices.DeleteFunc(hosts, func(h string) bool { return sent[h] })
			}
			if len(hosts) > 0 {
				var pokeID int64
				if err := a.withIDs(ctx, 1, func(ids []int64) error {
					pokeID = ids[0]
					return a.Store.AddOutboundPokes(ctx, a.Instance, ids[0], c.UserID, c.DeviceID, hosts, a.isMine(c.UserID), a.Now().UnixMilli())
				}); err != nil {
					return err
				}
				pokes.WithLabelValues("poked").Inc()
				if a.OnPokes != nil {
					a.OnPokes(pokeID)
				}
			} else {
				pokes.WithLabelValues("no_hosts").Inc()
			}
			if err := a.Store.MarkRedundant(ctx, c.UserID, c.DeviceID, c.RoomID, c.StreamID); err != nil {
				return err
			}
			if current != c.StreamID {
				clear(sent)
			}
			for _, h := range hosts {
				sent[h] = true
			}
			current = c.StreamID
		}
		last := rows[len(rows)-1]
		stream, room = last.StreamID, last.RoomID
		if err := a.Store.SetConvertedPos(ctx, a.Instance, stream, room); err != nil {
			return err
		}
	}
}
