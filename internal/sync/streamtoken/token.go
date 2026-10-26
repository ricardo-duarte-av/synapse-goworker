package streamtoken

import (
	"fmt"
	"strconv"
	"strings"
)

// Token is a full sync token: the value of `since`, `next_batch`, `prev_batch`,
// and the `start`/`end` of a legacy message chunk.
//
// It is 14 underscore-joined fields in a fixed order. See
// synapse/types/__init__.py:1143, whose docstring gives this example:
//
//	s2633508_17_338_6732159_1082514_541479_274711_265584_1_379_4242_4141_4343_4444
//
// Field 9, Groups, is dead: Synapse's own comment says it "is no longer used
// and may have bogus values". It is carried verbatim rather than normalised,
// because a token must round-trip to the byte.
type Token struct {
	Room                 RoomKey     // 1
	Presence             int64       // 2
	Typing               int64       // 3
	Receipt              MultiWriter // 4
	AccountData          int64       // 5
	PushRules            int64       // 6
	ToDevice             int64       // 7
	DeviceList           MultiWriter // 8
	Groups               int64       // 9  (unused by Synapse)
	UnPartialStatedRooms int64       // 10
	ThreadSubscriptions  int64       // 11
	StickyEvents         int64       // 12
	QuarantinedMedia     MultiWriter // 13
	ProfileUpdates       int64       // 14
}

// fieldCount is the number of fields in a current token. Synapse grows this
// list over time, which is why Parse pads rather than requiring exactly this
// many.
const fieldCount = 14

const separator = "_"

// Parse decodes a sync token.
//
// Missing trailing fields are padded with zero, matching Synapse's own
// from_string: tokens minted by older versions are shorter, and clients hold
// them across upgrades. A token with *more* fields than we know about is
// rejected rather than truncated -- silently dropping a field would hand back a
// token that rewinds a stream the client had already advanced past.
func Parse(s string) (Token, error) {
	if s == "" {
		return Token{}, fmt.Errorf("empty stream token")
	}
	parts := strings.Split(s, separator)
	if len(parts) > fieldCount {
		return Token{}, fmt.Errorf("stream token %q has %d fields, more than the %d this build knows",
			s, len(parts), fieldCount)
	}
	for len(parts) < fieldCount {
		parts = append(parts, "0")
	}

	var t Token
	var err error

	if t.Room, err = ParseRoomKey(parts[0]); err != nil {
		return Token{}, err
	}
	if t.Presence, err = parseInt(parts[1], "presence"); err != nil {
		return Token{}, err
	}
	if t.Typing, err = parseInt(parts[2], "typing"); err != nil {
		return Token{}, err
	}
	if t.Receipt, err = parseMulti(parts[3], "receipt"); err != nil {
		return Token{}, err
	}
	if t.AccountData, err = parseInt(parts[4], "account_data"); err != nil {
		return Token{}, err
	}
	if t.PushRules, err = parseInt(parts[5], "push_rules"); err != nil {
		return Token{}, err
	}
	if t.ToDevice, err = parseInt(parts[6], "to_device"); err != nil {
		return Token{}, err
	}
	if t.DeviceList, err = parseMulti(parts[7], "device_list"); err != nil {
		return Token{}, err
	}
	if t.Groups, err = parseInt(parts[8], "groups"); err != nil {
		return Token{}, err
	}
	if t.UnPartialStatedRooms, err = parseInt(parts[9], "un_partial_stated_rooms"); err != nil {
		return Token{}, err
	}
	if t.ThreadSubscriptions, err = parseInt(parts[10], "thread_subscriptions"); err != nil {
		return Token{}, err
	}
	if t.StickyEvents, err = parseInt(parts[11], "sticky_events"); err != nil {
		return Token{}, err
	}
	if t.QuarantinedMedia, err = parseMulti(parts[12], "quarantined_media"); err != nil {
		return Token{}, err
	}
	if t.ProfileUpdates, err = parseInt(parts[13], "profile_updates"); err != nil {
		return Token{}, err
	}
	return t, nil
}

// String renders the token.
func (t Token) String() string {
	fields := [fieldCount]string{
		t.Room.String(),
		strconv.FormatInt(t.Presence, 10),
		strconv.FormatInt(t.Typing, 10),
		t.Receipt.String(),
		strconv.FormatInt(t.AccountData, 10),
		strconv.FormatInt(t.PushRules, 10),
		strconv.FormatInt(t.ToDevice, 10),
		t.DeviceList.String(),
		strconv.FormatInt(t.Groups, 10),
		strconv.FormatInt(t.UnPartialStatedRooms, 10),
		strconv.FormatInt(t.ThreadSubscriptions, 10),
		strconv.FormatInt(t.StickyEvents, 10),
		t.QuarantinedMedia.String(),
		strconv.FormatInt(t.ProfileUpdates, 10),
	}
	return strings.Join(fields[:], separator)
}

// WithRoomKey returns a copy with only the room key replaced.
//
// This is Synapse's copy_and_replace(StreamKeyType.ROOM, ...), and it is the
// commonest token operation there is: a paginated response reports where the
// room stream reached while leaving every other stream at the position the
// caller already knows about. Building the token from scratch instead would
// silently rewind thirteen streams.
func (t Token) WithRoomKey(k RoomKey) Token {
	t.Room = k
	return t
}

// AtLeast returns a copy in which no stream is behind the same stream in floor.
//
// For a now token and the client's since. Our now token is the position
// replication has announced, and a client may hold a token minted a moment
// later by one of Synapse's own workers, or by this one while replication was
// down. Answering with a lower position would ask the client to replay what it
// already has; raising ours to theirs instead returns an empty window for that
// stream, which is what Synapse does by waiting in wait_for_stream_token.
//
// Raising is safe where taking the database's MAX would not be: a token Synapse
// minted is a persisted-up-to position, so nothing below it is still in
// flight.
//
// Typing is left alone. Its serial lives in the typing writer's memory and
// restarts at zero with it; raising ours to a client's pre-restart serial would
// pin every later token above anything the restarted writer will issue for
// days, and typing would go silent (see docs/tokens.md).
func (t Token) AtLeast(floor Token) Token {
	t.Room = t.Room.AdvancedTo(floor.Room)
	t.Presence = maxInt64(t.Presence, floor.Presence)
	t.Receipt = t.Receipt.AdvancedTo(floor.Receipt)
	t.AccountData = maxInt64(t.AccountData, floor.AccountData)
	t.PushRules = maxInt64(t.PushRules, floor.PushRules)
	t.ToDevice = maxInt64(t.ToDevice, floor.ToDevice)
	t.DeviceList = t.DeviceList.AdvancedTo(floor.DeviceList)
	t.UnPartialStatedRooms = maxInt64(t.UnPartialStatedRooms, floor.UnPartialStatedRooms)
	t.ThreadSubscriptions = maxInt64(t.ThreadSubscriptions, floor.ThreadSubscriptions)
	t.StickyEvents = maxInt64(t.StickyEvents, floor.StickyEvents)
	t.QuarantinedMedia = t.QuarantinedMedia.AdvancedTo(floor.QuarantinedMedia)
	t.ProfileUpdates = maxInt64(t.ProfileUpdates, floor.ProfileUpdates)
	return t
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func parseInt(s, field string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s position %q: %w", field, s, err)
	}
	return n, nil
}

func parseMulti(s, field string) (MultiWriter, error) {
	m, err := ParseMultiWriter(s)
	if err != nil {
		return MultiWriter{}, fmt.Errorf("invalid %s position: %w", field, err)
	}
	return m, nil
}
