package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
)

// A state event read after the same process served it with its previous
// content carries that content, as Synapse's cached event object does; one
// never read that way, or read longer ago than the TTL, or redacted since,
// does not.
func TestCachedPrevContent(t *testing.T) {
	s := &Store{}
	now := time.Unix(1000, 0)
	s.prev().now = func() time.Time { return now }
	s.SetEventCache(2, 30*time.Minute, 0)
	s.prev().add("$a", json.RawMessage(`{"membership":"invite"}`), "@x:s")

	ev := clientevent.Stored{EventID: "$a", JSON: []byte(`{"type":"m.room.member","unsigned":{"replaces_state":"$p"}}`)}
	s.AttachCachedPrevContent(&ev)
	if got := string(ev.JSON); got != `{"type":"m.room.member","unsigned":{"replaces_state":"$p","prev_content":{"membership":"invite"},"prev_sender":"@x:s"}}` {
		t.Fatalf("cached: %s", got)
	}

	other := clientevent.Stored{EventID: "$b", JSON: []byte(`{"unsigned":{}}`)}
	s.AttachCachedPrevContent(&other)
	if string(other.JSON) != `{"unsigned":{}}` {
		t.Fatalf("never read with prev content: %s", other.JSON)
	}

	// Unread past the TTL: gone.
	now = now.Add(31 * time.Minute)
	if _, ok := s.prev().get("$a"); ok {
		t.Fatal("entry outlived cache_entry_ttl")
	}

	// Count bound, and redaction.
	s.prev().add("$1", json.RawMessage(`{}`), "@x:s")
	s.prev().add("$2", json.RawMessage(`{}`), "@x:s")
	s.prev().add("$3", json.RawMessage(`{}`), "@x:s")
	if _, ok := s.prev().get("$1"); ok {
		t.Fatal("event_cache_size not applied")
	}
	s.ForgetPrevContent("$3")
	if _, ok := s.prev().get("$3"); ok {
		t.Fatal("redacted event kept")
	}
}
