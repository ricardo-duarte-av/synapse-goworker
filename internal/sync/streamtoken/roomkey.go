// Package streamtoken parses and serialises Synapse's sync tokens.
//
// A `since` or `next_batch` token is 14 underscore-joined fields recording a
// position in each of Synapse's streams. Field 1 is a RoomStreamToken with
// three possible encodings; fields 4, 8 and 13 are MultiWriterStreamTokens; the
// rest are bare integers. See synapse/types/__init__.py:1143.
//
// # Instance ids are kept as ids
//
// Synapse resolves the integer instance ids inside a vector-clock token to
// worker names on parse (via the `instance_map` table) and back to ids on
// serialise. This package keeps them as ids throughout.
//
// The round trip is identical either way -- the mapping is a bijection stored
// in the database and never rewritten -- but keeping ids means a token can be
// parsed, modified and re-serialised without touching PostgreSQL at all. That
// matters because the commonest operation on a token, replacing only the room
// key while preserving the other thirteen fields, happens on every paginated
// response. Names are needed only when comparing a token against an event's
// `instance_name` column, which is a store concern, not a token one.
package streamtoken

import (
	"fmt"
	"strconv"
	"strings"
)

// RoomKey is a position in the room event stream.
//
// It has three encodings, and which one it carries changes its meaning:
//
//   - "s123"          a live position, just after stream_ordering 123
//   - "t426-2633508"  a historical position, ordered by topological_ordering
//     first with stream_ordering as the tie-break; used when
//     paginating backwards through history
//   - "m56~2.58~3.59" a vector clock across sharded event persisters: 56 is the
//     position every writer has reached, and only writers ahead
//     of it are listed
//
// A token cannot be both historical and vector-clocked; Synapse enforces this
// in __attrs_post_init__ and so does New.
type RoomKey struct {
	// Topological is set only for the "t" form. -1 means unset, so that a
	// legitimate topological ordering of 0 is representable.
	Topological int64
	// Stream is the stream_ordering position.
	Stream int64
	// Instances lists writers ahead of Stream, in the order they appeared in
	// the token. Empty for the "s" and "t" forms.
	//
	// An ordered slice rather than a map because Synapse serialises by
	// iterating a Python dict, which preserves insertion order, and on parse
	// that order is the order in the token string. A map would reorder the
	// entries on round trip and produce a token that is semantically identical
	// but textually different -- which the comparator would report as a
	// mismatch that means nothing. There are eight writers on this deployment,
	// so a linear scan is cheaper than a map anyway.
	Instances Instances
}

// Instances is an ordered set of per-writer stream positions.
type Instances []InstancePos

// InstancePos is one writer's position. ID is the `instance_map.instance_id`
// of the worker, kept as an id rather than resolved to a name; see the package
// comment.
type InstancePos struct {
	ID  int
	Pos int64
}

// Get returns the position recorded for a writer, and whether one was.
func (in Instances) Get(id int) (int64, bool) {
	for _, e := range in {
		if e.ID == id {
			return e.Pos, true
		}
	}
	return 0, false
}

// NoTopological marks a RoomKey as live rather than historical.
const NoTopological = int64(-1)

// Live builds an "s" token.
func Live(stream int64) RoomKey {
	return RoomKey{Topological: NoTopological, Stream: stream}
}

// Historical builds a "t" token.
func Historical(topological, stream int64) RoomKey {
	return RoomKey{Topological: topological, Stream: stream}
}

// IsHistorical reports whether this is a "t" token.
func (k RoomKey) IsHistorical() bool { return k.Topological != NoTopological }

// ParseRoomKey parses the room_key field of a stream token.
func ParseRoomKey(s string) (RoomKey, error) {
	if s == "" {
		return RoomKey{}, fmt.Errorf("empty room stream token")
	}
	switch s[0] {
	case 's':
		n, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return RoomKey{}, fmt.Errorf("invalid room stream token %q: %w", s, err)
		}
		return RoomKey{Topological: NoTopological, Stream: n}, nil

	case 't':
		topo, stream, ok := strings.Cut(s[1:], "-")
		if !ok {
			return RoomKey{}, fmt.Errorf("invalid room stream token %q: expected t<topological>-<stream>", s)
		}
		t, err := strconv.ParseInt(topo, 10, 64)
		if err != nil {
			return RoomKey{}, fmt.Errorf("invalid room stream token %q: %w", s, err)
		}
		n, err := strconv.ParseInt(stream, 10, 64)
		if err != nil {
			return RoomKey{}, fmt.Errorf("invalid room stream token %q: %w", s, err)
		}
		return RoomKey{Topological: t, Stream: n}, nil

	case 'm':
		stream, instances, err := parseVectorClock(s[1:])
		if err != nil {
			return RoomKey{}, fmt.Errorf("invalid room stream token %q: %w", s, err)
		}
		return RoomKey{Topological: NoTopological, Stream: stream, Instances: instances}, nil

	default:
		return RoomKey{}, fmt.Errorf("invalid room stream token %q: expected a leading s, t or m", s)
	}
}

// String renders the room key.
//
// Writers at or below Stream are dropped, matching Synapse: we may know a
// writer has advanced without having seen a recent write from it, and listing
// it would claim a position the reader cannot rely on. A vector clock left with
// no entries degrades to the "s" form, which is what Synapse emits too.
func (k RoomKey) String() string {
	if k.IsHistorical() {
		return "t" + strconv.FormatInt(k.Topological, 10) + "-" + strconv.FormatInt(k.Stream, 10)
	}
	if entries := formatVectorClock(k.Stream, k.Instances); entries != "" {
		return "m" + strconv.FormatInt(k.Stream, 10) + "~" + entries
	}
	return "s" + strconv.FormatInt(k.Stream, 10)
}

// MaxStreamPos is the highest stream position any writer had reached.
//
// Paginating backwards from a vector clock has to start from this rather than
// from Stream, because a writer ahead of the minimum has already persisted
// events that the token covers. Synapse fetches by this bound and filters the
// rows afterwards; so must we.
func (k RoomKey) MaxStreamPos() int64 {
	max := k.Stream
	for _, e := range k.Instances {
		if e.Pos > max {
			max = e.Pos
		}
	}
	return max
}

// StreamPosForInstance is the position the given writer had reached.
//
// A writer absent from the map is assumed to be at Stream: the map lists only
// writers that are ahead.
func (k RoomKey) StreamPosForInstance(instanceID int) int64 {
	if pos, ok := k.Instances.Get(instanceID); ok {
		return pos
	}
	return k.Stream
}

// MultiWriter is a position in a stream with several writers, without the
// live/historical distinction a RoomKey carries. Receipts, device lists and
// quarantined media use it.
type MultiWriter struct {
	Stream    int64
	Instances Instances
}

// ParseMultiWriter parses a bare integer or an "m" vector clock.
func ParseMultiWriter(s string) (MultiWriter, error) {
	if s == "" {
		return MultiWriter{}, fmt.Errorf("empty stream token")
	}
	if s[0] == 'm' {
		stream, instances, err := parseVectorClock(s[1:])
		if err != nil {
			return MultiWriter{}, fmt.Errorf("invalid stream token %q: %w", s, err)
		}
		return MultiWriter{Stream: stream, Instances: instances}, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return MultiWriter{}, fmt.Errorf("invalid stream token %q: %w", s, err)
	}
	return MultiWriter{Stream: n}, nil
}

// String renders the position, degrading to a bare integer when no writer is
// ahead of Stream.
func (m MultiWriter) String() string {
	if entries := formatVectorClock(m.Stream, m.Instances); entries != "" {
		return "m" + strconv.FormatInt(m.Stream, 10) + "~" + entries
	}
	return strconv.FormatInt(m.Stream, 10)
}

// MaxStreamPos is the highest position any writer had reached.
func (m MultiWriter) MaxStreamPos() int64 {
	max := m.Stream
	for _, e := range m.Instances {
		if e.Pos > max {
			max = e.Pos
		}
	}
	return max
}

// StreamPosForInstance is the position the given writer had reached.
func (m MultiWriter) StreamPosForInstance(instanceID int) int64 {
	if pos, ok := m.Instances.Get(instanceID); ok {
		return pos
	}
	return m.Stream
}

// AdvancedTo is Synapse's copy_and_advance: a position at or after both this
// one and other, writer by writer.
func (m MultiWriter) AdvancedTo(other MultiWriter) MultiWriter {
	stream, instances := advanceVectorClock(m.Stream, m.Instances, other.Stream, other.Instances)
	return MultiWriter{Stream: stream, Instances: instances}
}

// AdvancedTo is Synapse's RoomStreamToken.copy_and_advance for live keys.
//
// A historical key is returned unchanged, in either position. Synapse raises
// on a topological key there; nothing here passes one, and a sync token whose
// room key is historical was not minted by a sync, so there is no ordering
// with a live key to restore.
func (k RoomKey) AdvancedTo(other RoomKey) RoomKey {
	if k.IsHistorical() || other.IsHistorical() {
		return k
	}
	stream, instances := advanceVectorClock(k.Stream, k.Instances, other.Stream, other.Instances)
	return RoomKey{Topological: NoTopological, Stream: stream, Instances: instances}
}

// advanceVectorClock takes the maximum of two vector clocks, writer by writer.
//
// A writer absent from one side is at that side's Stream, so it is compared
// against that rather than against zero. Entries no longer ahead of the new
// minimum are dropped, as Synapse does, and the order is this side's writers
// followed by any only the other names.
func advanceVectorClock(aStream int64, a Instances, bStream int64, b Instances) (int64, Instances) {
	stream := aStream
	if bStream > stream {
		stream = bStream
	}
	get := func(in Instances, base int64, id int) int64 {
		if pos, ok := in.Get(id); ok {
			return pos
		}
		return base
	}
	var out Instances
	add := func(id int) {
		if _, done := out.Get(id); done {
			return
		}
		pos := get(a, aStream, id)
		if other := get(b, bStream, id); other > pos {
			pos = other
		}
		if pos > stream {
			out = append(out, InstancePos{ID: id, Pos: pos})
		}
	}
	for _, e := range a {
		add(e.ID)
	}
	for _, e := range b {
		add(e.ID)
	}
	return stream, out
}

// parseVectorClock parses "<stream>~<id>.<pos>~<id>.<pos>", preserving order.
func parseVectorClock(s string) (int64, Instances, error) {
	parts := strings.Split(s, "~")
	stream, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, nil, err
	}
	var instances Instances
	for _, part := range parts[1:] {
		if part == "" {
			// Tokens of the form "m5~" exist in the wild: Synapse emitted them
			// from a bug and still has to read them back.
			continue
		}
		idStr, posStr, ok := strings.Cut(part, ".")
		if !ok {
			return 0, nil, fmt.Errorf("malformed instance entry %q", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return 0, nil, fmt.Errorf("malformed instance id %q: %w", idStr, err)
		}
		pos, err := strconv.ParseInt(posStr, 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("malformed instance position %q: %w", posStr, err)
		}
		instances = append(instances, InstancePos{ID: id, Pos: pos})
	}
	return stream, instances, nil
}

// formatVectorClock renders the writers ahead of stream, in the order held.
func formatVectorClock(stream int64, instances Instances) string {
	var b strings.Builder
	for _, e := range instances {
		if e.Pos <= stream {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('~')
		}
		b.WriteString(strconv.Itoa(e.ID))
		b.WriteByte('.')
		b.WriteString(strconv.FormatInt(e.Pos, 10))
	}
	return b.String()
}
