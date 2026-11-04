package pduverify

import (
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// rescindsInvite is the shape of event Synapse considers, when not in the
// room, as rescinding an invite to one of its users: someone else's leave
// for a local user.
func rescindsInvite(ev *pdu.Event, server string) bool {
	if ev.Type != "m.room.member" || ev.StateKey == nil || *ev.StateKey == ev.Sender {
		return false
	}
	if m, err := ev.Membership(); err != nil || m != "leave" {
		return false
	}
	_, domain, _ := strings.Cut(*ev.StateKey, ":")
	return domain == server
}
