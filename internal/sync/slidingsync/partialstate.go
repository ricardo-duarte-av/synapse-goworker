package slidingsync

import (
	"fmt"
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

// errEmptyWildcardKey is Synapse's IndexError for ["*", ""]; see
// MustAwaitFullStateChecked.
var errEmptyWildcardKey = fmt.Errorf("required_state: an empty state key under a wildcard type")

// MustAwaitFullStateChecked is MustAwaitFullState with Synapse's failure
// mode: an empty state key under the wildcard type raises, which Synapse
// does for every list and subscription it builds, partial-state rooms or
// not -- a 500 for the request.
func MustAwaitFullStateChecked(cfg slidingstore.RoomSyncConfig, isLocal func(string) bool) (bool, error) {
	// possible_user_id[0] on an empty key raises IndexError, unless the
	// ["*", "*"] case returned first.
	if wildcard := cfg.RequiredState[Wildcard]; !wildcard[Wildcard] && wildcard[""] {
		return false, errEmptyWildcardKey
	}
	return MustAwaitFullState(cfg, isLocal), nil
}

// IsLocalUser builds the isLocal predicate for a server name: is_mine_id,
// which compares everything after the first colon with the server name.
func IsLocalUser(serverName string) func(string) bool {
	return func(userID string) bool {
		_, host, ok := strings.Cut(userID, ":")
		return ok && host == serverName
	}
}
