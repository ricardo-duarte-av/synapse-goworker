package filter

import "testing"

func TestSyncableUserID(t *testing.T) {
	for id, want := range map[string]bool{
		"@alice:example.org":     true,
		"@Alice_+=/.:remote.org": true,  // historical but compliant: U+0021..U+007E
		"@:remote.org":           false, // empty localpart
		"@a b:remote.org":        false, // space
		"@é:remote.org":          false, // non-ASCII
		"alice:remote.org":       false, // no sigil
		"@é:example.org":         true,  // ours, whatever its localpart
		"@:example.org":          true,
		"@weird":                 true, // no domain: the whole rest is the localpart, as partition gives it
	} {
		if got := SyncableUserID(id, "example.org"); got != want {
			t.Errorf("%q: %v, want %v", id, got, want)
		}
	}
}
