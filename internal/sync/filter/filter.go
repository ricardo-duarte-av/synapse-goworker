// Package filter implements Synapse's `/sync` filters.
//
// A filter is the client's say in what a sync response contains: which rooms,
// which event types, how many timeline events, and -- the part that matters
// most in practice -- whether member events are lazy-loaded. Element sends one
// on every request, so a worker that ignores filters answers a different
// question from the one it was asked.
//
// This is a port of synapse/api/filtering.py. The structure is kept: a
// Collection holds seven independent Filters, and the room-level `rooms` /
// `not_rooms` filter is ANDed with each section's own.
package filter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

// labelsField is where MSC2326 puts an event's labels.
const labelsField = "org.matrix.labels"

// Event is the subset of an event a filter can inspect.
//
// Filters run *before* serialisation, on the stored PDU, because Synapse
// filters EventBase objects. Everything here is therefore read from
// `event_json.json` (or, for account data and ephemeral events, from the
// synthesised dict), never from a client-facing rendering.
type Event struct {
	Type   string
	Sender string
	RoomID string
	// Content is the event's `content` object. Zero value means absent, which
	// Synapse treats as an empty mapping.
	Content gjson.Result
	// EventID is used only for `related_by_senders` / `related_by_rel_types`,
	// which are resolved by the caller against the database. Empty for
	// anything that is not a real event.
	EventID string
	// IsPDU marks a real room event. Synapse computes an event's relation type
	// only for EventBase instances, so account data -- which can carry an
	// `m.relates_to` in its content -- is never matched on `rel_types`.
	IsPDU bool
	// IsPresence marks a presence update. Synapse carries presence through
	// filters as its own type rather than as a dict, and so checks only
	// `senders` and `types` against it -- never rooms, labels, relations or
	// `contains_url`.
	IsPresence bool
}

// Filter is one section's filter: Synapse's `Filter` class.
//
// A nil slice means "unset" and matches everything; an empty non-nil slice
// means "match nothing", which is what `filters_all_*` detects. The
// distinction is load-bearing, so the JSON decoding must preserve it: `"types":
// []` and a missing `types` are different filters.
type Filter struct {
	Limit                     int
	LazyLoadMembers           bool
	IncludeRedundantMembers   bool
	UnreadThreadNotifications bool

	Types    []string
	NotTypes []string

	Rooms    []string
	NotRooms []string

	Senders    []string
	NotSenders []string

	// ContainsURL is tri-state: nil means the filter says nothing about it.
	ContainsURL *bool

	Labels    []string
	NotLabels []string

	RelatedBySenders  []string
	RelatedByRelTypes []string

	// RelTypes is MSC3874, and only populated when that MSC is enabled. It
	// filters /messages, not /sync, but the field exists on every Filter
	// because `_check_fields` looks it up unconditionally.
	RelTypes    []string
	NotRelTypes []string
}

// FiltersAllTypes reports whether the filter can never match any type.
func (f *Filter) FiltersAllTypes() bool {
	return (f.Types != nil && len(f.Types) == 0) || contains(f.NotTypes, "*")
}

// FiltersAllSenders reports whether the filter can never match any sender.
func (f *Filter) FiltersAllSenders() bool {
	return (f.Senders != nil && len(f.Senders) == 0) || contains(f.NotSenders, "*")
}

// FiltersAllRooms reports whether the filter can never match any room.
func (f *Filter) FiltersAllRooms() bool {
	return (f.Rooms != nil && len(f.Rooms) == 0) || contains(f.NotRooms, "*")
}

// HasRelationConstraint reports whether Check is not the whole story: the
// caller must additionally ask the database which events carry a matching
// relation.
func (f *Filter) HasRelationConstraint() bool {
	return len(f.RelatedBySenders) > 0 || len(f.RelatedByRelTypes) > 0
}

// Check reports whether one event passes this filter.
//
// Synapse's `_check`: for each of a fixed set of fields, reject on any match
// against `not_<field>`, then reject unless something matches `<field>` when
// `<field>` is set at all.
func (f *Filter) Check(ev Event) bool {
	if ev.IsPresence {
		// UserPresenceState carries no room, content or relation, so Synapse
		// offers `_check_fields` only these two matchers.
		return matchField(f.Senders, f.NotSenders, func(v string) bool { return ev.Sender == v }) &&
			matchField(f.Types, f.NotTypes, func(v string) bool { return matchesWildcard(ev.Type, v) })
	}

	content := ev.Content
	var labels []string
	if content.IsObject() {
		for _, l := range content.Get(escapeGJSONPath(labelsField)).Array() {
			labels = append(labels, l.String())
		}
	}

	sender := ev.Sender
	if sender == "" && content.IsObject() {
		// Presence events used to carry their user in content.user_id.
		// Synapse still looks, and so do we.
		sender = content.Get("user_id").String()
	}

	relType := ""
	if ev.IsPDU && content.IsObject() {
		relType = content.Get("m\\.relates_to.rel_type").String()
	}

	if !matchField(f.Rooms, f.NotRooms, func(v string) bool { return ev.RoomID == v }) {
		return false
	}
	if !matchField(f.Senders, f.NotSenders, func(v string) bool { return sender == v }) {
		return false
	}
	if !matchField(f.Types, f.NotTypes, func(v string) bool { return matchesWildcard(ev.Type, v) }) {
		return false
	}
	if !matchField(f.Labels, f.NotLabels, func(v string) bool { return contains(labels, v) }) {
		return false
	}
	if !matchField(f.RelTypes, f.NotRelTypes, func(v string) bool { return relType == v }) {
		return false
	}

	if f.ContainsURL != nil {
		hasURL := content.IsObject() && content.Get("url").Type == gjson.String
		if *f.ContainsURL != hasURL {
			return false
		}
	}
	return true
}

// matchField is one iteration of Synapse's `_check_fields`.
func matchField(allowed, disallowed []string, match func(string) bool) bool {
	for _, v := range disallowed {
		if match(v) {
			return false
		}
	}
	if allowed == nil {
		return true
	}
	for _, v := range allowed {
		if match(v) {
			return true
		}
	}
	return false
}

// matchesWildcard implements the trailing `*` allowed in a type filter.
//
// Only types support it, and only as a suffix: `m.room.*` matches, `*.room.x`
// does not.
func matchesWildcard(actual, want string) bool {
	if strings.HasSuffix(want, "*") {
		return strings.HasPrefix(actual, strings.TrimSuffix(want, "*"))
	}
	return actual == want
}

// FilterRooms applies the `rooms` / `not_rooms` fields to a list of room IDs.
func (f *Filter) FilterRooms(roomIDs []string) []string {
	out := make([]string, 0, len(roomIDs))
	for _, id := range roomIDs {
		if contains(f.NotRooms, id) {
			continue
		}
		if f.Rooms != nil && !contains(f.Rooms, id) {
			continue
		}
		out = append(out, id)
	}
	return out
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// escapeGJSONPath escapes a literal key for use as a gjson path.
//
// `org.matrix.labels` is a single key containing dots, not a path of three.
func escapeGJSONPath(key string) string {
	return strings.ReplaceAll(key, ".", "\\.")
}

// Collection is a whole user filter: Synapse's FilterCollection.
type Collection struct {
	room              Filter
	roomTimeline      Filter
	roomState         Filter
	roomEphemeral     Filter
	roomAccountData   Filter
	presence          Filter
	globalAccountData Filter

	IncludeLeave bool
	EventFields  []string
	EventFormat  string

	raw json.RawMessage
}

// Default is the filter applied when the client sends none.
//
// Not the zero value: every section's limit defaults to 10, which is where
// /sync's default timeline limit comes from.
func Default() *Collection { return DefaultWithFeatures(Features{}) }

// DefaultWithFeatures is Default under the given experimental flags.
func DefaultWithFeatures(feat Features) *Collection {
	c, err := NewWithFeatures(nil, feat)
	if err != nil {
		panic(err) // an empty filter cannot fail to parse
	}
	return c
}

// Raw returns the filter as the client supplied it.
func (c *Collection) Raw() json.RawMessage { return c.raw }

func (c *Collection) TimelineLimit() int  { return c.roomTimeline.Limit }
func (c *Collection) PresenceLimit() int  { return c.presence.Limit }
func (c *Collection) EphemeralLimit() int { return c.roomEphemeral.Limit }

func (c *Collection) LazyLoadMembers() bool { return c.roomState.LazyLoadMembers }
func (c *Collection) IncludeRedundantMembers() bool {
	return c.roomState.IncludeRedundantMembers
}
func (c *Collection) UnreadThreadNotifications() bool {
	return c.roomTimeline.UnreadThreadNotifications
}

// TimelineFilter and the accessors below expose the section filters so a
// caller can ask about relation constraints, which need the database.
func (c *Collection) TimelineFilter() *Filter { return &c.roomTimeline }
func (c *Collection) StateFilter() *Filter    { return &c.roomState }

func (c *Collection) BlocksAllRooms() bool { return c.room.FiltersAllRooms() }

func (c *Collection) BlocksAllPresence() bool {
	return c.presence.FiltersAllTypes() || c.presence.FiltersAllSenders()
}

func (c *Collection) BlocksAllGlobalAccountData() bool {
	return c.globalAccountData.FiltersAllTypes() || c.globalAccountData.FiltersAllSenders()
}

func (c *Collection) BlocksAllRoomEphemeral() bool {
	return c.roomEphemeral.FiltersAllTypes() || c.roomEphemeral.FiltersAllSenders() ||
		c.roomEphemeral.FiltersAllRooms()
}

func (c *Collection) BlocksAllRoomAccountData() bool {
	return c.roomAccountData.FiltersAllTypes() || c.roomAccountData.FiltersAllSenders() ||
		c.roomAccountData.FiltersAllRooms()
}

func (c *Collection) BlocksAllRoomTimeline() bool {
	return c.roomTimeline.FiltersAllTypes() || c.roomTimeline.FiltersAllSenders() ||
		c.roomTimeline.FiltersAllRooms()
}

// The Check* methods are the section filters, each ANDed with the room-level
// `rooms` / `not_rooms` filter. Synapse expresses this as nested calls --
// `_room_timeline_filter.filter(_room_filter.filter(events))` -- which is the
// same thing per event.
func (c *Collection) CheckRoomTimeline(ev Event) bool {
	return c.room.Check(ev) && c.roomTimeline.Check(ev)
}
func (c *Collection) CheckRoomState(ev Event) bool {
	return c.room.Check(ev) && c.roomState.Check(ev)
}
func (c *Collection) CheckRoomEphemeral(ev Event) bool {
	return c.room.Check(ev) && c.roomEphemeral.Check(ev)
}
func (c *Collection) CheckRoomAccountData(ev Event) bool {
	return c.room.Check(ev) && c.roomAccountData.Check(ev)
}
func (c *Collection) CheckPresence(ev Event) bool { return c.presence.Check(ev) }
func (c *Collection) CheckGlobalAccountData(ev Event) bool {
	return c.globalAccountData.Check(ev)
}

// Features are the server-side switches that change how a filter is read.
type Features struct {
	// MSC3773 makes `org.matrix.msc3773.unread_thread_notifications` an alias
	// for the stable field.
	MSC3773 bool
	// MSC3874 enables the `rel_types` fields, which filter /messages.
	MSC3874 bool
	// MSC4429 enables profile-field filtering.
	MSC4429 bool
}

// New parses a user filter.
//
// A nil or empty document is the default filter, which is the common case:
// most requests carry no filter at all.
func New(doc json.RawMessage) (*Collection, error) {
	return NewWithFeatures(doc, Features{})
}

// NewWithFeatures parses a user filter under the given experimental flags,
// WITHOUT validating user or room IDs.
//
// That is not laxness, it is Synapse's read path. `get_user_filter` hands the
// stored JSON straight to FilterCollection with no schema check at all:
// validation happens when a filter is UPLOADED, and never again. A worker that
// re-validates on read can reject a filter the homeserver has been serving
// happily for months -- which is exactly what happened the first time a real
// client pointed at this one. gomuks uploads
// `{"presence":{"not_rooms":["*"]}}`, Synapse stores it, and we answered 400
// to every sync until the client gave up.
//
// Use NewInline for a filter that arrived in the query string, which Synapse
// does validate.
func NewWithFeatures(doc json.RawMessage, feat Features) (*Collection, error) {
	return parseCollection(doc, feat, false)
}

// NewInline parses a filter that arrived inline in the query string, and
// validates it as Synapse's check_valid_filter does.
//
// The difference from a stored filter is upstream's, not ours: sync.py calls
// check_valid_filter on the inline branch and not on the stored one.
func NewInline(doc json.RawMessage, feat Features) (*Collection, error) {
	return parseCollection(doc, feat, true)
}

func parseCollection(doc json.RawMessage, feat Features, validateIDs bool) (*Collection, error) {
	if len(doc) == 0 {
		doc = json.RawMessage(`{}`)
	}
	if !gjson.ValidBytes(doc) {
		return nil, fmt.Errorf("filter is not valid JSON")
	}
	root := gjson.ParseBytes(doc)
	if !root.IsObject() {
		return nil, fmt.Errorf("filter must be an object")
	}

	c := &Collection{raw: doc}
	room := root.Get("room")

	// ROOM_FILTER_SCHEMA and ROOM_EVENT_FILTER_SCHEMA declare `rooms` and
	// `not_rooms` as room_id_array, so those are validated. FILTER_SCHEMA --
	// which is what `presence` and the top-level `account_data` use -- does
	// NOT declare them at all, and has additionalProperties: true. A room list
	// under `presence` is therefore never checked by Synapse, whatever it
	// contains.
	roomOpts := parseOpts{feat: feat, validateIDs: validateIDs, schemaHasRoomIDs: true}
	plainOpts := parseOpts{feat: feat, validateIDs: validateIDs}

	// The room-level filter carries only `rooms` and `not_rooms`; the other
	// keys under `room` are whole sub-filters of their own.
	c.room = Filter{}
	if err := parseRoomIDList(room.Get("rooms"), "room.rooms", &c.room.Rooms, roomOpts); err != nil {
		return nil, err
	}
	if err := parseRoomIDList(room.Get("not_rooms"), "room.not_rooms", &c.room.NotRooms, roomOpts); err != nil {
		return nil, err
	}

	var err error
	if c.roomTimeline, err = parseFilter(room.Get("timeline"), "room.timeline", roomOpts); err != nil {
		return nil, err
	}
	if c.roomState, err = parseFilter(room.Get("state"), "room.state", roomOpts); err != nil {
		return nil, err
	}
	if c.roomEphemeral, err = parseFilter(room.Get("ephemeral"), "room.ephemeral", roomOpts); err != nil {
		return nil, err
	}
	if c.roomAccountData, err = parseFilter(room.Get("account_data"), "room.account_data", roomOpts); err != nil {
		return nil, err
	}
	if c.presence, err = parseFilter(root.Get("presence"), "presence", plainOpts); err != nil {
		return nil, err
	}
	if c.globalAccountData, err = parseFilter(root.Get("account_data"), "account_data", plainOpts); err != nil {
		return nil, err
	}

	if v := room.Get("include_leave"); v.Exists() {
		if v.Type != gjson.True && v.Type != gjson.False {
			return nil, fmt.Errorf("room.include_leave must be a boolean")
		}
		c.IncludeLeave = v.Bool()
	}

	c.EventFormat = "client"
	if v := root.Get("event_format"); v.Exists() {
		if v.Type != gjson.String || (v.String() != "client" && v.String() != "federation") {
			return nil, fmt.Errorf("event_format must be one of 'client', 'federation'")
		}
		c.EventFormat = v.String()
	}
	if v := root.Get("event_fields"); v.Exists() {
		if err := parseStringList(v, "event_fields", &c.EventFields); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// parseOpts carries the two things that differ between call sites: whether IDs
// are validated at all, and whether this section's schema declares room IDs.
type parseOpts struct {
	feat Features
	// validateIDs is true only for a filter Synapse would run through
	// check_valid_filter -- an inline one. A stored filter is never
	// re-validated.
	validateIDs bool
	// schemaHasRoomIDs distinguishes ROOM_EVENT_FILTER_SCHEMA, which declares
	// rooms/not_rooms as room_id_array, from FILTER_SCHEMA, which does not
	// declare them at all.
	schemaHasRoomIDs bool
}

// parseFilter reads one FILTER_SCHEMA / ROOM_EVENT_FILTER_SCHEMA object.
func parseFilter(v gjson.Result, path string, opts parseOpts) (Filter, error) {
	feat := opts.feat
	f := Filter{Limit: 10}
	if !v.Exists() {
		return f, nil
	}
	if !v.IsObject() {
		return f, fmt.Errorf("%s must be an object", path)
	}
	if l := v.Get("limit"); l.Exists() {
		if l.Type != gjson.Number {
			return f, fmt.Errorf("%s.limit must be a number", path)
		}
		f.Limit = int(l.Int())
	}
	for _, b := range []struct {
		key string
		dst *bool
	}{
		{"lazy_load_members", &f.LazyLoadMembers},
		{"include_redundant_members", &f.IncludeRedundantMembers},
		{"unread_thread_notifications", &f.UnreadThreadNotifications},
	} {
		bv := v.Get(b.key)
		if !bv.Exists() {
			continue
		}
		if bv.Type != gjson.True && bv.Type != gjson.False {
			return f, fmt.Errorf("%s.%s must be a boolean", path, b.key)
		}
		*b.dst = bv.Bool()
	}
	if !f.UnreadThreadNotifications && feat.MSC3773 {
		f.UnreadThreadNotifications = v.Get("org\\.matrix\\.msc3773\\.unread_thread_notifications").Bool()
	}

	for _, l := range []struct {
		key string
		dst *[]string
	}{
		{"types", &f.Types},
		{"not_types", &f.NotTypes},
		{"related_by_senders", &f.RelatedBySenders},
		{"related_by_rel_types", &f.RelatedByRelTypes},
	} {
		if err := parseStringList(v.Get(l.key), path+"."+l.key, l.dst); err != nil {
			return f, err
		}
	}
	if err := parseStringList(v.Get("org\\.matrix\\.labels"), path+".org.matrix.labels", &f.Labels); err != nil {
		return f, err
	}
	if err := parseStringList(v.Get("org\\.matrix\\.not_labels"), path+".org.matrix.not_labels", &f.NotLabels); err != nil {
		return f, err
	}
	for _, l := range []struct {
		key string
		dst *[]string
	}{
		{"senders", &f.Senders},
		{"not_senders", &f.NotSenders},
	} {
		if err := parseUserIDList(v.Get(l.key), path+"."+l.key, l.dst, opts); err != nil {
			return f, err
		}
	}
	for _, l := range []struct {
		key string
		dst *[]string
	}{
		{"rooms", &f.Rooms},
		{"not_rooms", &f.NotRooms},
	} {
		if err := parseRoomIDList(v.Get(l.key), path+"."+l.key, l.dst, opts); err != nil {
			return f, err
		}
	}
	if u := v.Get("contains_url"); u.Exists() {
		if u.Type != gjson.True && u.Type != gjson.False {
			return f, fmt.Errorf("%s.contains_url must be a boolean", path)
		}
		b := u.Bool()
		f.ContainsURL = &b
	}
	if feat.MSC3874 {
		if err := parseStringList(v.Get("org\\.matrix\\.msc3874\\.rel_types"),
			path+".org.matrix.msc3874.rel_types", &f.RelTypes); err != nil {
			return f, err
		}
		if err := parseStringList(v.Get("org\\.matrix\\.msc3874\\.not_rel_types"),
			path+".org.matrix.msc3874.not_rel_types", &f.NotRelTypes); err != nil {
			return f, err
		}
	}
	return f, nil
}

func parseStringList(v gjson.Result, path string, dst *[]string) error {
	if !v.Exists() || v.Type == gjson.Null {
		return nil
	}
	if !v.IsArray() {
		return fmt.Errorf("%s must be an array", path)
	}
	out := []string{}
	for _, item := range v.Array() {
		if item.Type != gjson.String {
			return fmt.Errorf("%s must be an array of strings", path)
		}
		out = append(out, item.String())
	}
	*dst = out
	return nil
}

func parseUserIDList(v gjson.Result, path string, dst *[]string, opts parseOpts) error {
	if err := parseStringList(v, path, dst); err != nil {
		return err
	}
	if !opts.validateIDs {
		return nil
	}
	for _, id := range *dst {
		if !ValidUserID(id) {
			return fmt.Errorf("%q is not a valid user ID in %s", id, path)
		}
	}
	return nil
}

func parseRoomIDList(v gjson.Result, path string, dst *[]string, opts parseOpts) error {
	if err := parseStringList(v, path, dst); err != nil {
		return err
	}
	if !opts.validateIDs || !opts.schemaHasRoomIDs {
		return nil
	}
	for _, id := range *dst {
		if !ValidRoomID(id) {
			return fmt.Errorf("%q is not a valid room ID in %s", id, path)
		}
	}
	return nil
}
