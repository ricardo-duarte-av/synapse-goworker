package slidingsync

import (
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
)

// A room joined over federation with a "faster join" (MSC3706) has all of its
// state EXCEPT remote membership events until backfill finishes. Whether that
// matters depends entirely on what the request asked for.
//
// Ported from RoomSyncConfig.must_await_full_state.

// MustAwaitFullState reports whether a room's required_state can only be
// satisfied once the room is fully stated.
//
// The question is narrower than "is this room partial": partial state is
// complete for everything except REMOTE memberships, so a request that wants
// none of those is served perfectly well from it. Answering "yes" too readily
// hides a freshly joined room from the client for as long as backfill takes,
// which on a large room is minutes.
//
// isLocal decides whether a user ID belongs to this server.
func MustAwaitFullState(cfg slidingstore.RoomSyncConfig, isLocal func(string) bool) bool {
	wildcard := cfg.RequiredState[Wildcard]

	// Everything, so everything must be there.
	if wildcard[Wildcard] {
		return true
	}

	// A wildcard EVENT TYPE with a remote user as its state key asks for that
	// user's membership under every type -- which includes m.room.member.
	for stateKey := range wildcard {
		if !looksLikeUserID(stateKey) {
			continue
		}
		if !isLocal(stateKey) {
			return true
		}
	}

	members, wanted := cfg.RequiredState[memberEventType]
	if !wanted {
		// No memberships asked for at all, so partial state is complete for
		// this request.
		return false
	}

	for userID := range members {
		switch {
		case userID == Me:
			// The caller is local by definition -- they are syncing here.
		case userID == Lazy:
			// Lazy loading only ever asks for members who appear in the
			// timeline, and those events had to be authorised to be persisted,
			// so their memberships are present even in a partial room.
		case userID == Wildcard:
			// Synapse returns false here rather than true, which reads oddly
			// next to the ["*","*"] case above. It is deliberate: every
			// membership is wanted, so waiting would gain the remote ones, but
			// the client asked for a room list and gets what there is. Matching
			// it matters more than agreeing with it.
			return false
		case !isLocal(userID):
			return true
		}
	}
	return false
}

// looksLikeUserID is Synapse's check: a leading sigil and a colon.
func looksLikeUserID(s string) bool {
	return strings.HasPrefix(s, "@") && strings.Contains(s, ":")
}

// IsLocalUser builds the isLocal predicate for a server name.
func IsLocalUser(serverName string) func(string) bool {
	suffix := ":" + serverName
	return func(userID string) bool {
		return strings.HasSuffix(userID, suffix)
	}
}
