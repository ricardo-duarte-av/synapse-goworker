// Package slidingsync implements MSC4186, Simplified Sliding Sync.
//
// The endpoint lets a client ask for a window of its rooms rather than all of
// them, and for exactly the state it needs in each. The server remembers what
// it has already sent, which is what internal/slidingstore holds; this package
// decides what to send.
//
// Ported from synapse/handlers/sliding_sync/. Where behaviour here looks
// arbitrary it is almost always Synapse's, and the comment says so.
package slidingsync

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Limits from Synapse's pydantic models (types/rest/client/__init__.py). They
// are validation, not tuning: a client that exceeds one gets 400 M_BAD_JSON,
// because these bound work the server does per request and an unbounded list
// count or timeline limit is a way to make one request cost everything.
const (
	MaxLists             = 100
	MaxListKeyBytes      = 64
	MaxRoomSubscriptions = 100
	MaxTimelineLimit     = 1000
)

// StateValues are the special state keys required_state understands. They are
// not state keys: they are instructions that happen to live in that position.
const (
	// Wildcard matches every type, or every state key of a type.
	Wildcard = "*"
	// Lazy asks for the members of whoever appears in the timeline, rather
	// than a fixed set. Only meaningful as an m.room.member state key.
	Lazy = "$LAZY"
	// Me is the requesting user's own membership.
	Me = "$ME"
)

// Request is the body of a sliding sync request.
//
// Pos and Timeout are NOT here. Synapse reads both from the query string
// (rest/client/sync.py: parse_integer(request, "timeout"), parse_string(
// request, "pos")) while MSC4186 specifies them as body fields. Both are
// accepted; see ParsePosAndTimeout.
type Request struct {
	// ConnID identifies one of possibly several connections a device holds.
	// Element X runs three -- "room-list", "notifications" and "" -- so this is
	// part of a connection's identity, not an optional label.
	ConnID string `json:"conn_id"`

	Lists             map[string]List          `json:"lists"`
	RoomSubscriptions map[string]RoomSubscribe `json:"room_subscriptions"`
	Extensions        *Extensions              `json:"extensions"`

	// Pos and Timeout, if the client put them in the body as MSC4186 says.
	Pos     *string `json:"pos"`
	Timeout *int    `json:"timeout"`
}

// CommonRoomParameters are shared by lists and room subscriptions.
type CommonRoomParameters struct {
	// RequiredState is a list of [type, state_key] pairs, OR'd together.
	//
	// One exception inverts the whole thing: ["*", "*"] asks for all state, and
	// any further entries then FILTER it rather than adding to it. See
	// docs/synapse-notes.md.
	RequiredState [][2]string `json:"required_state"`
	TimelineLimit int         `json:"timeline_limit"`
}

// List is one sliding window.
type List struct {
	CommonRoomParameters
	// Ranges are inclusive [start, end] index pairs into the sorted room list.
	// Absent means no window: every room in the list is returned.
	Ranges [][2]int `json:"ranges"`
	// SlowGetAllRooms makes Ranges irrelevant and returns everything.
	SlowGetAllRooms bool     `json:"slow_get_all_rooms"`
	Filters         *Filters `json:"filters"`
}

// RoomSubscribe asks for one room by ID, whether or not a window covers it.
type RoomSubscribe struct {
	CommonRoomParameters
}

// Filters narrow a list before it is sorted. All fields AND together, and an
// absent field means no filter -- NOT false.
type Filters struct {
	IsDM         *bool     `json:"is_dm"`
	Spaces       []string  `json:"spaces"`
	IsEncrypted  *bool     `json:"is_encrypted"`
	IsInvite     *bool     `json:"is_invite"`
	RoomTypes    []*string `json:"room_types"`
	NotRoomTypes []*string `json:"not_room_types"`
	RoomNameLike *string   `json:"room_name_like"`
	Tags         []string  `json:"tags"`
	NotTags      []string  `json:"not_tags"`
}

// Extensions is the extension section of the request.
//
// The two prefixed keys are what THIS Synapse accepts, verified against
// SlidingSyncBody.Extensions. Note that MSC4508's `org.matrix.msc4508.typing`
// alias is NOT among them: typing is the plain key.
type Extensions struct {
	ToDevice    *ToDeviceExtension    `json:"to_device"`
	E2EE        *E2EEExtension        `json:"e2ee"`
	AccountData *AccountDataExtension `json:"account_data"`
	Receipts    *ReceiptsExtension    `json:"receipts"`
	Typing      *TypingExtension      `json:"typing"`

	ThreadSubscriptions *ThreadSubscriptionsExtension `json:"io.element.msc4308.thread_subscriptions"`
	StickyEvents        *StickyEventsExtension        `json:"org.matrix.msc4354.sticky_events"`
	// Profiles (MSC4262) is only decoded, so a malformed body is refused as
	// Synapse's model refuses it; the extension is served only with
	// include_profile_updates_in_sync, off on both hosts (TODO.md).
	Profiles *ProfilesExtension `json:"org.matrix.msc4262.profiles"`
}

// ExtensionScope is the reserved lists/rooms targeting every room-scoped
// extension shares. `["*"]` -- the default -- means every list or every room
// subscription in the response.
type ExtensionScope struct {
	Enabled bool     `json:"enabled"`
	Lists   []string `json:"lists"`
	Rooms   []string `json:"rooms"`
}

type ToDeviceExtension struct {
	Enabled bool   `json:"enabled"`
	Limit   int    `json:"limit"`
	Since   string `json:"since"`
}

type E2EEExtension struct {
	Enabled bool `json:"enabled"`
}

type AccountDataExtension struct{ ExtensionScope }
type ReceiptsExtension struct{ ExtensionScope }
type TypingExtension struct{ ExtensionScope }

type ThreadSubscriptionsExtension struct {
	Enabled bool `json:"enabled"`
	Limit   int  `json:"limit"`
}

// StickyEventsExtension is MSC4354/MSC4480's, as Synapse 1.165 reads it
// (#20363): scoped like receipts by lists and rooms, paged through the
// connection position rather than a `since` of its own (an old client's
// `since` is ignored, as pydantic ignores an unknown field).
type StickyEventsExtension struct {
	ExtensionScope
	// Limit is NonNegativeStrictInt, default 100: an explicit 0 is kept
	// (nothing is read, and every room resumes where it was).
	Limit int `json:"limit"`
}

type ProfilesExtension struct {
	Enabled bool     `json:"enabled"`
	Fields  []string `json:"fields"`
}

// ParseRequest decodes and validates a request body.
//
// An empty body is accepted as an empty request: Synapse's pydantic model has
// no required fields, so a client may legitimately POST `{}` -- and does, when
// it only wants extensions.
func ParseRequest(body []byte) (*Request, error) {
	req := &Request{}
	if len(strings.TrimSpace(string(body))) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(body)))
		if err := dec.Decode(req); err != nil {
			return nil, fmt.Errorf("Unable to parse json: %s", err)
		}
	}
	if err := req.validate(); err != nil {
		return nil, err
	}
	if err := validateExtensionNulls(body); err != nil {
		return nil, err
	}
	req.applyDefaults(body)
	return req, nil
}

func (r *Request) validate() error {
	if len(r.Lists) > MaxLists {
		return fmt.Errorf("Max lists: %d but saw %d", MaxLists, len(r.Lists))
	}
	for key, list := range r.Lists {
		if len(key) > MaxListKeyBytes {
			return fmt.Errorf("list key %q is longer than %d bytes", key, MaxListKeyBytes)
		}
		if err := list.CommonRoomParameters.validate("list " + key); err != nil {
			return err
		}
		for _, rng := range list.Ranges {
			if rng[0] < 0 || rng[1] < 0 {
				return fmt.Errorf("list %q: ranges must not be negative", key)
			}
		}
	}
	if len(r.RoomSubscriptions) > MaxRoomSubscriptions {
		return fmt.Errorf("Max room subscriptions: %d but saw %d",
			MaxRoomSubscriptions, len(r.RoomSubscriptions))
	}
	for roomID, sub := range r.RoomSubscriptions {
		if err := sub.CommonRoomParameters.validate("room subscription " + roomID); err != nil {
			return err
		}
	}
	return nil
}

func (c CommonRoomParameters) validate(what string) error {
	if c.TimelineLimit > MaxTimelineLimit {
		return fmt.Errorf("%s: timeline_limit must be less than or equal to %d",
			what, MaxTimelineLimit)
	}
	if c.TimelineLimit < 0 {
		return fmt.Errorf("%s: timeline_limit must not be negative", what)
	}
	return nil
}

// applyDefaults fills in the values Synapse's models default rather than
// require. The extension limits differ per extension and are not a house
// default, so each is taken from its own model.
func (r *Request) applyDefaults(body []byte) {
	if r.Extensions == nil {
		return
	}
	e := r.Extensions
	if e.ToDevice != nil && e.ToDevice.Limit == 0 {
		e.ToDevice.Limit = 100
	}
	if e.ThreadSubscriptions != nil && e.ThreadSubscriptions.Limit == 0 {
		e.ThreadSubscriptions.Limit = 100
	}
	if e.StickyEvents != nil && !extensionHas(body, "org.matrix.msc4354.sticky_events", "limit") {
		e.StickyEvents.Limit = 100
	}
	for _, s := range []*ExtensionScope{
		scopeOf(e.AccountData), scopeOf(e.Receipts), scopeOf(e.Typing), scopeOf(e.StickyEvents),
	} {
		if s == nil {
			continue
		}
		if s.Lists == nil {
			s.Lists = []string{Wildcard}
		}
		if s.Rooms == nil {
			s.Rooms = []string{Wildcard}
		}
	}
}

func scopeOf(v any) *ExtensionScope {
	switch t := v.(type) {
	case *AccountDataExtension:
		if t == nil {
			return nil
		}
		return &t.ExtensionScope
	case *ReceiptsExtension:
		if t == nil {
			return nil
		}
		return &t.ExtensionScope
	case *TypingExtension:
		if t == nil {
			return nil
		}
		return &t.ExtensionScope
	case *StickyEventsExtension:
		if t == nil {
			return nil
		}
		return &t.ExtensionScope
	}
	return nil
}

// rawExtensions is the body's extensions, field by field, for what
// encoding/json cannot tell apart: an absent field from a zero or a null.
func rawExtensions(body []byte) map[string]map[string]json.RawMessage {
	var probe struct {
		Extensions map[string]map[string]json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil
	}
	return probe.Extensions
}

// extensionHas reports whether the body sets field in extension name.
func extensionHas(body []byte, name, field string) bool {
	_, ok := rawExtensions(body)[name][field]
	return ok
}

// nonNullable are the extension fields an explicit null fails since Synapse
// 1.165 (#20363 made them StrictBool and list[StrictStr] rather than
// Optional); before, a null was a falsy None. to_device's enabled still
// takes one. sticky_events' limit (NonNegativeStrictInt) never did.
var nonNullable = map[string][]string{
	"e2ee":         {"enabled"},
	"account_data": {"enabled", "lists", "rooms"},
	"receipts":     {"enabled", "lists", "rooms"},
	"typing":       {"enabled", "lists", "rooms"},
	"io.element.msc4308.thread_subscriptions": {"enabled"},
	"org.matrix.msc4354.sticky_events":        {"enabled", "limit", "lists", "rooms"},
}

// validateExtensionNulls refuses what the model refuses that encoding/json
// would take as a zero value: an explicit null where none is allowed, and a
// negative sticky events limit.
func validateExtensionNulls(body []byte) error {
	raw := rawExtensions(body)
	for _, name := range sortedStrings(nonNullable) {
		ext, ok := raw[name]
		if !ok {
			continue
		}
		for _, field := range nonNullable[name] {
			if v, ok := ext[field]; ok && strings.TrimSpace(string(v)) == "null" {
				return fmt.Errorf("extensions.%s.%s: Input should not be null", name, field)
			}
		}
	}
	if v, ok := raw["org.matrix.msc4354.sticky_events"]["limit"]; ok {
		var n int64
		if json.Unmarshal(v, &n) == nil && n < 0 {
			return fmt.Errorf("extensions.org.matrix.msc4354.sticky_events.limit: Input should be greater than or equal to 0")
		}
	}
	return nil
}

func sortedStrings[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
