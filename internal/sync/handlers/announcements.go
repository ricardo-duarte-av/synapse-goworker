package handlers

import (
	"net/http"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/server"
)

// AnnouncementsToMain hands every request to main while homeserver.yaml
// configures server announcement rooms (experimental_features.
// server_announcements.rooms, Synapse 1.165), and serves it with next
// otherwise. configured is asked on each request, so a re-read of
// homeserver.yaml applies it.
//
// None of the feature is reproduced here, and every route it wraps would
// answer differently: legacy /sync gives each local user the rooms as a
// synthetic join, with their m.server_notice tag; /sync, sliding sync,
// /events and both initialSyncs keep receipts in those rooms from everyone
// but their sender; the room state and event reads let a user who left one
// keep reading it (check_user_in_room_or_world_readable). Profile and key
// queries are not touched by it and stay native.
func AnnouncementsToMain(d Deps, configured func() bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if configured() {
			handBack(w, r, d, server.Annotate(r.Context()), "server_announcements")
			return
		}
		next.ServeHTTP(w, r)
	})
}
