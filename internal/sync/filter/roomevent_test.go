package filter

import (
	"errors"
	"testing"
)

// What /messages reads natively, and what it leaves to Synapse because
// Python reads it by its own rules.
func TestParseRoomEventFilter(t *testing.T) {
	f, format, err := ParseRoomEventFilter([]byte(`{"types":["m.room.message"],"lazy_load_members":true,"contains_url":false,"event_format":"federation","senders":null}`), Features{})
	if err != nil || format != "federation" || !f.LazyLoadMembers || f.ContainsURL == nil || *f.ContainsURL ||
		len(f.Types) != 1 || f.Senders != nil {
		t.Fatalf("got %+v %q %v", f, format, err)
	}
	// MSC3874's fields only when enabled.
	f, _, _ = ParseRoomEventFilter([]byte(`{"org.matrix.msc3874.rel_types":["m.thread"]}`), Features{})
	if f.RelTypes != nil {
		t.Fatalf("rel_types read with MSC3874 off: %v", f.RelTypes)
	}
	for _, doc := range []string{
		`[1]`, `{"types":"m.room.message"}`, `{"lazy_load_members":1}`, `{"contains_url":null}`,
		`{"not_types":null}`, `{"org.matrix.not_labels":null}`, `{"types":[1]}`, `{"event_format":5}`,
		`{"types":["a"],"types":["b"]}`,
	} {
		if _, _, err := ParseRoomEventFilter([]byte(doc), Features{}); !errors.Is(err, ErrNotReproduced) {
			t.Errorf("%s: %v, want ErrNotReproduced", doc, err)
		}
	}
}
