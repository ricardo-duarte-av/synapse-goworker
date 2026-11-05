// Package roomhosts is which servers are in a room, as the EDU handlers ask
// it: is_host_joined (storage/databases/main/roommember.py:1191) and
// get_current_hosts_in_room_or_partial_state_approximation
// (storage/controllers/state.py:764).
package roomhosts

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// ErrInvalidHost is is_host_joined's Exception("Invalid host name").
var ErrInvalidHost = errors.New("Invalid host name")

const qHostJoined = `
	SELECT state_key FROM current_state_events
	WHERE membership = 'join'
		AND type = 'm.room.member'
		AND room_id = $1
		AND state_key LIKE $2
	LIMIT 1`

// HostJoined is is_host_joined: some user of host is joined to the room. A
// host with a LIKE wildcard, or a match not of that host, is ErrInvalidHost.
func HostJoined(ctx context.Context, pool *pgxpool.Pool, roomID, host string) (bool, error) {
	if strings.ContainsAny(host, "%_") {
		return false, ErrInvalidHost
	}
	var userID string
	err := pool.QueryRow(ctx, qHostJoined, roomID, "%:"+host).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if userid.Domain(userID) != host {
		return false, ErrInvalidHost
	}
	return true, nil
}

const (
	qPartialServers = `SELECT server_name FROM partial_state_rooms_servers WHERE room_id = $1`
	// get_current_hosts_in_room's PostgreSQL query.
	qCurrentHosts = `
		SELECT DISTINCT substring(state_key FROM '@[^:]*:(.*)$')
		FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND room_id = $1`
)

// Approx is get_current_hosts_in_room_or_partial_state_approximation: the
// joined hosts, and for a partial-state room the servers in it when we
// joined. The servers at join are read first, as Synapse reads them, to
// mitigate races with un-partial-stating.
func Approx(ctx context.Context, pool *pgxpool.Pool, roomID string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	for _, q := range []string{qPartialServers, qCurrentHosts} {
		rows, err := pool.Query(ctx, q, roomID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var h *string
			if err := rows.Scan(&h); err != nil {
				rows.Close()
				return nil, err
			}
			if h != nil {
				out[*h] = struct{}{}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
