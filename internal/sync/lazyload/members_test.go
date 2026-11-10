package lazyload

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

func at(s int64) streamtoken.RoomKey { return streamtoken.Live(s) }

func TestSentMembersMatchesTheEventID(t *testing.T) {
	m := NewMembers(0, 0).For(Key{UserID: "@a:x", DeviceID: "D"})
	m.MarkSent("@b:x", "$e1", at(10))
	if !m.WasSent("@b:x", "$e1") || m.WasSent("@b:x", "$e2") || m.WasSent("@c:x", "$e1") {
		t.Error("WasSent")
	}
}

// start_request: an initial sync forgets everything; a request from since
// forgets what was recorded after it, keeping the rest.
func TestStartRequest(t *testing.T) {
	m := NewMembers(0, 0).For(Key{UserID: "@a:x", DeviceID: "D"})
	m.MarkSent("@b:x", "$b", at(10))
	m.MarkSent("@c:x", "$c", at(20))
	m.MarkSent("@d:x", "$d", at(30))

	since := at(20)
	m.StartRequest(&since)
	if !m.WasSent("@b:x", "$b") || !m.WasSent("@c:x", "$c") || m.WasSent("@d:x", "$d") {
		t.Error("from 20: want b and c kept, d forgotten")
	}
	// Nothing after the new bound: a later request from the same since keeps all.
	m.StartRequest(&since)
	if !m.WasSent("@c:x", "$c") {
		t.Error("a repeated since forgot c")
	}
	m.StartRequest(nil)
	if m.WasSent("@b:x", "$b") {
		t.Error("an initial sync kept b")
	}
}

// LAZY_LOADED_MEMBERS_CACHE_MAX_SIZE, least recently used first; a match
// counts as a use.
func TestSentMembersBound(t *testing.T) {
	m := NewMembers(2, 0).For(Key{UserID: "@a:x"})
	m.MarkSent("@b:x", "$b", at(1))
	m.MarkSent("@c:x", "$c", at(2))
	m.WasSent("@b:x", "$b")
	m.MarkSent("@d:x", "$d", at(3))
	if !m.WasSent("@b:x", "$b") || m.WasSent("@c:x", "$c") || !m.WasSent("@d:x", "$d") {
		t.Error("want c evicted")
	}
	if DefaultMaxMembers != 100 {
		t.Error("LAZY_LOADED_MEMBERS_CACHE_MAX_SIZE is 100")
	}
}
