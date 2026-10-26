package handlers

import "testing"

// A self-leave must reach rooms.leave on an incremental sync. Synapse's
// include_leave filter applies only to an initial sync; applying it here left
// the client believing it was still joined, so the room stayed in its room
// list and a later invite to the same room did not render.
func TestIncrementalSectionAlwaysReportsALeave(t *testing.T) {
	for _, membership := range []string{"leave", "ban"} {
		if got := incrementalSection(membership); got != "leave" {
			t.Errorf("incrementalSection(%q) = %q, want %q", membership, got, "leave")
		}
	}
}

func TestIncrementalSectionNamesTheOtherSections(t *testing.T) {
	cases := map[string]string{
		"invite":  "invite",
		"knock":   "knock",
		"join":    "",
		"":        "",
		"unknown": "",
	}
	for membership, want := range cases {
		if got := incrementalSection(membership); got != want {
			t.Errorf("incrementalSection(%q) = %q, want %q", membership, got, want)
		}
	}
}

// The mirror image of the tests above: on an INITIAL sync `include_leave` does
// apply, and a self-leave is enumerated only when the filter asks for it. A
// kick or a ban is sent either way.
func TestEnumeratesArchivedHonoursIncludeLeave(t *testing.T) {
	const me, other = "@me:example.com", "@them:example.com"
	cases := []struct {
		membership, sender string
		includeLeave       bool
		want               bool
	}{
		{"leave", me, false, false},   // left of my own accord
		{"leave", me, true, true},     // ... and asked to see it
		{"leave", other, false, true}, // kicked: always reported
		{"ban", other, false, true},   // banned: always reported
		{"ban", me, false, true},      // a self-ban is still a ban
	}
	for _, c := range cases {
		got := enumeratesArchived(c.membership, c.sender, me, c.includeLeave)
		if got != c.want {
			t.Errorf("enumeratesArchived(%q, sender=%q, include_leave=%v) = %v, want %v",
				c.membership, c.sender, c.includeLeave, got, c.want)
		}
	}
}
