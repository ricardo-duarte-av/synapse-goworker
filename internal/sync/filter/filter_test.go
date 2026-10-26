package filter

import (
	"encoding/json"
	"testing"

	"github.com/tidwall/gjson"
)

func mustNew(t *testing.T, doc string) *Collection {
	t.Helper()
	c, err := New(json.RawMessage(doc))
	if err != nil {
		t.Fatalf("New(%s): %v", doc, err)
	}
	return c
}

func ev(typ, sender, roomID, content string) Event {
	if content == "" {
		content = "{}"
	}
	return Event{
		Type: typ, Sender: sender, RoomID: roomID,
		Content: gjson.Parse(content), IsPDU: true,
	}
}

func TestDefaultTimelineLimitIsTen(t *testing.T) {
	// The default filter's limit is where /sync's ten-event timeline comes
	// from; it is not a constant in the sync code.
	if got := Default().TimelineLimit(); got != 10 {
		t.Fatalf("default timeline limit = %d, want 10", got)
	}
}

func TestEmptyListBlocksEverythingButMissingDoesNot(t *testing.T) {
	// The distinction the whole `blocks_all_*` family rests on: `"types": []`
	// matches nothing, a missing `types` matches everything. A decoder that
	// collapses the two silently disables the short-circuits.
	blocked := mustNew(t, `{"presence":{"types":[]}}`)
	if !blocked.BlocksAllPresence() {
		t.Fatal(`"types": [] should block all presence`)
	}
	open := mustNew(t, `{"presence":{}}`)
	if open.BlocksAllPresence() {
		t.Fatal("a filter with no types should block nothing")
	}
}

func TestWildcardOnlyAppliesToTypes(t *testing.T) {
	c := mustNew(t, `{"room":{"timeline":{"types":["m.room.*"]}}}`)
	if !c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!r:x", "")) {
		t.Fatal("m.room.* should match m.room.message")
	}
	if c.CheckRoomTimeline(ev("m.reaction", "@a:x", "!r:x", "")) {
		t.Fatal("m.room.* should not match m.reaction")
	}

	// Senders are compared exactly: a trailing star is a literal there.
	s := mustNew(t, `{"room":{"timeline":{"senders":["@a:x"]}}}`)
	if s.CheckRoomTimeline(ev("m.room.message", "@ab:x", "!r:x", "")) {
		t.Fatal("sender matching must be exact")
	}
}

func TestNotFieldsBeatAllowFields(t *testing.T) {
	c := mustNew(t, `{"room":{"timeline":{"types":["m.room.message"],"not_senders":["@spam:x"]}}}`)
	if !c.CheckRoomTimeline(ev("m.room.message", "@ok:x", "!r:x", "")) {
		t.Fatal("allowed type and sender should pass")
	}
	if c.CheckRoomTimeline(ev("m.room.message", "@spam:x", "!r:x", "")) {
		t.Fatal("not_senders should reject even an allowed type")
	}
}

func TestContainsURLChecksTypeNotTruthiness(t *testing.T) {
	c := mustNew(t, `{"room":{"timeline":{"contains_url":true}}}`)
	if !c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!r:x", `{"url":"mxc://x/y"}`)) {
		t.Fatal("a string url should match contains_url=true")
	}
	// Synapse asks whether the value is a string, so a numeric or empty-string
	// url is decided on its type, not on whether it looks useful.
	if c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!r:x", `{"url":42}`)) {
		t.Fatal("a non-string url must not count as containing a url")
	}
	if !c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!r:x", `{"url":""}`)) {
		t.Fatal("an empty string url is still a string")
	}
}

func TestLabelsAreOneDottedKey(t *testing.T) {
	// `org.matrix.labels` is a single content key containing dots, not a path
	// of three. Reading it as a path finds nothing and the filter matches
	// everything.
	c := mustNew(t, `{"room":{"timeline":{"org.matrix.labels":["work"]}}}`)
	if !c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!r:x",
		`{"org.matrix.labels":["work"]}`)) {
		t.Fatal("a labelled event should match its label filter")
	}
	if c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!r:x", `{}`)) {
		t.Fatal("an unlabelled event should not match a label filter")
	}
}

func TestPresenceIgnoresRoomAndContent(t *testing.T) {
	// Synapse carries presence as its own type, so only senders and types are
	// checked against it -- never rooms.
	c := mustNew(t, `{"presence":{"senders":["@a:x"]}}`)
	if !c.CheckPresence(Event{Type: "m.presence", Sender: "@a:x", IsPresence: true}) {
		t.Fatal("matching sender should pass")
	}
	if c.CheckPresence(Event{Type: "m.presence", Sender: "@b:x", IsPresence: true}) {
		t.Fatal("other sender should not pass")
	}
}

func TestRoomFilterIsANDedWithSectionFilter(t *testing.T) {
	c := mustNew(t, `{"room":{"rooms":["!keep:x"],"timeline":{"types":["m.room.message"]}}}`)
	if !c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!keep:x", "")) {
		t.Fatal("right room and right type should pass")
	}
	if c.CheckRoomTimeline(ev("m.room.message", "@a:x", "!other:x", "")) {
		t.Fatal("the room-level filter must also apply")
	}
}

func TestLazyLoadFlagsComeFromTheStateFilter(t *testing.T) {
	c := mustNew(t, `{"room":{"state":{"lazy_load_members":true,"include_redundant_members":true}}}`)
	if !c.LazyLoadMembers() || !c.IncludeRedundantMembers() {
		t.Fatal("lazy-load flags should be read from room.state")
	}
	// The same flags on the timeline filter mean nothing.
	c2 := mustNew(t, `{"room":{"timeline":{"lazy_load_members":true}}}`)
	if c2.LazyLoadMembers() {
		t.Fatal("lazy_load_members on the timeline filter must not apply")
	}
}

func TestMSC3773AliasIsGated(t *testing.T) {
	doc := json.RawMessage(`{"room":{"timeline":{"org.matrix.msc3773.unread_thread_notifications":true}}}`)
	off, err := NewWithFeatures(doc, Features{})
	if err != nil {
		t.Fatal(err)
	}
	if off.UnreadThreadNotifications() {
		t.Fatal("the unstable field must be ignored when MSC3773 is off")
	}
	on, err := NewWithFeatures(doc, Features{MSC3773: true})
	if err != nil {
		t.Fatal(err)
	}
	if !on.UnreadThreadNotifications() {
		t.Fatal("the unstable field should be honoured when MSC3773 is on")
	}
}

func TestInvalidFiltersAreRejected(t *testing.T) {
	// Structural faults are rejected on BOTH paths: a document like this is
	// not a filter at all, however it arrived.
	for _, doc := range []string{
		`{"room":{"timeline":{"limit":"ten"}}}`,
		`{"event_format":"carrier-pigeon"}`,
		`{"room":{"timeline":{"types":[1,2]}}}`,
		`[]`,
	} {
		if _, err := New(json.RawMessage(doc)); err == nil {
			t.Errorf("New(%s) should have failed", doc)
		}
		if _, err := NewInline(json.RawMessage(doc), Features{}); err == nil {
			t.Errorf("NewInline(%s) should have failed", doc)
		}
	}
}

// TestIDsAreValidatedOnlyInline pins the asymmetry that broke a real client.
//
// Synapse validates a filter when it is UPLOADED and when it arrives inline in
// the query string; `get_user_filter` reads the stored JSON back with no schema
// check at all. Re-validating on read means rejecting filters the homeserver
// has been serving for months.
func TestIDsAreValidatedOnlyInline(t *testing.T) {
	for _, doc := range []string{
		`{"room":{"timeline":{"senders":["not-a-user"]}}}`,
		`{"room":{"rooms":["not-a-room"]}}`,
	} {
		if _, err := NewInline(json.RawMessage(doc), Features{}); err == nil {
			t.Errorf("NewInline(%s) should have rejected the ID", doc)
		}
		if _, err := New(json.RawMessage(doc)); err != nil {
			t.Errorf("New(%s) must accept a stored filter unvalidated: %v", doc, err)
		}
	}
}

// TestPresenceRoomListsAreNeverValidated is gomuks's filter, verbatim.
//
// `presence` uses FILTER_SCHEMA, which declares no `rooms` or `not_rooms` at
// all and allows additional properties -- so Synapse accepts `["*"]` there even
// on the upload path, and stores it. We rejected it on every read, and the
// client retried forever against a 400.
func TestPresenceRoomListsAreNeverValidated(t *testing.T) {
	doc := json.RawMessage(
		`{"presence":{"not_rooms":["*"]},"room":{"state":{"lazy_load_members":true},` +
			`"timeline":{"lazy_load_members":true,"limit":100}}}`)

	for name, parse := range map[string]func() (*Collection, error){
		"stored": func() (*Collection, error) { return New(doc) },
		"inline": func() (*Collection, error) { return NewInline(doc, Features{}) },
	} {
		c, err := parse()
		if err != nil {
			t.Fatalf("%s: gomuks's filter must be accepted: %v", name, err)
		}
		// And the room list must not affect presence, which has no room to
		// match against: Synapse's _check skips the room test for an EDU.
		if !c.CheckPresence(Event{IsPresence: true, Sender: "@a:example.com"}) {
			t.Errorf("%s: presence was filtered out by a room list", name)
		}
	}
}

func TestRoomIDValidityCoversBothForms(t *testing.T) {
	// MSC4291 room IDs are a bare hash with no `:server` at all; rejecting
	// them would refuse filters naming any room created here, where room
	// version 12 is the default.
	for _, id := range []string{"!abc:example.com", "!NujWqysZdry6tmDvnQmm7UzsVYe-0zAv-ZaA8WRfYAA"} {
		if !ValidRoomID(id) {
			t.Errorf("ValidRoomID(%q) = false, want true", id)
		}
	}
	for _, id := range []string{"", "abc", "!", "!has/slash"} {
		if ValidRoomID(id) {
			t.Errorf("ValidRoomID(%q) = true, want false", id)
		}
	}
	// Synapse validates only the DOMAIN half of an ID with a colon in it --
	// its own comment admits it "does not reject an empty localpart" -- so a
	// junk localpart with a good domain is accepted. Matching that laxness
	// matters more than being right: rejecting a filter Synapse accepts turns
	// a working client into a 400.
	if !ValidRoomID("!bad id:example.com") {
		t.Error(`ValidRoomID("!bad id:example.com") should match Synapse and accept it`)
	}
}
