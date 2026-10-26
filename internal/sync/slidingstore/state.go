// Package slidingstore holds sliding sync's per-connection state: what this
// worker has already told a given client, so the next response can send only
// the difference.
//
// It is the second and last place this worker writes, and unlike the first it
// is not a read-only workload that needed one exception -- it cannot be made
// read-only at all. Even READING the state writes (see Store.GetAndClear), and
// the `pos` token a client carries is literally a sequence value from
// sliding_sync_connection_positions. The containment is therefore the same
// argument made a second time: a separate package, a separate pool, a separate
// role that owns one schema and has nothing in `public`, and a startup check
// that refuses a role which can see Synapse's tables at all. See
// deploy/sliding-sync-role.sql.
//
// A port of synapse/handlers/sliding_sync/store.py,
// synapse/types/handlers/sliding_sync.py and
// synapse/storage/databases/main/sliding_sync.py, with one deliberate
// deviation: our tables are ours, so our `pos` is not interchangeable with
// Synapse's. docs/decisions.md records why.
package slidingstore

import (
	"encoding/json"
	"sort"
)

// HaveSentFlag records whether a room has been sent down a connection.
//
// The only valid transitions are NEVER -> LIVE, LIVE -> PREVIOUSLY and
// PREVIOUSLY -> LIVE. There is no PREVIOUSLY -> NEVER: forgetting that we sent
// a room is how a client ends up never being told about it again.
type HaveSentFlag string

const (
	// FlagNever means the room has never been sent, or we have forgotten that
	// it was.
	FlagNever HaveSentFlag = "never"
	// FlagPreviously means it has been sent, but there are updates since
	// LastToken that have not been.
	FlagPreviously HaveSentFlag = "previously"
	// FlagLive means it has been sent and the client has everything.
	FlagLive HaveSentFlag = "live"
)

// HaveSent is a flag and, for FlagPreviously, the token everything up to which
// was sent.
//
// The token is a string here rather than a parsed RoomStreamToken because that
// is how the database holds it, and because the three streams this is used for
// carry three different token types. The handler parses; this package stores.
type HaveSent struct {
	Status    HaveSentFlag
	LastToken string
}

// Live is the state after sending a room in full.
func Live() HaveSent { return HaveSent{Status: FlagLive} }

// Previously is the state after choosing NOT to send a room that had updates.
func Previously(lastToken string) HaveSent {
	return HaveSent{Status: FlagPreviously, LastToken: lastToken}
}

// Never is the state of a room this connection has not heard of.
func Never() HaveSent { return HaveSent{Status: FlagNever} }

// RoomStatusMap records, for one stream, what has been sent for each room.
//
// base is the state read from the database and is never written to -- it may be
// shared. updates holds this request's changes, which is what distinguishes
// "nothing changed, reuse the client's position" from "write a new one". That
// distinction is not an optimisation: every new position invalidates cached
// state and copies a table's worth of rows forward, so spurious writes are
// expensive as well as pointless.
//
// Modelled on Synapse's ChainMap, which is the same idea.
type RoomStatusMap struct {
	base    map[string]HaveSent
	updates map[string]HaveSent
}

// NewRoomStatusMap wraps a map read from the database. The map is not copied
// and must not be written to afterwards.
func NewRoomStatusMap(base map[string]HaveSent) RoomStatusMap {
	return RoomStatusMap{base: base}
}

// HaveSentRoom reports what has been sent for a room.
func (m *RoomStatusMap) HaveSentRoom(roomID string) HaveSent {
	if hs, ok := m.updates[roomID]; ok {
		return hs
	}
	if hs, ok := m.base[roomID]; ok {
		return hs
	}
	return Never()
}

// RecordSentRooms marks rooms as fully sent.
//
// A room already LIVE is left alone rather than rewritten: rewriting it would
// register an update, which would force a new connection position for a
// response that changed nothing.
func (m *RoomStatusMap) RecordSentRooms(roomIDs []string) {
	for _, roomID := range roomIDs {
		if m.HaveSentRoom(roomID).Status == FlagLive {
			continue
		}
		m.set(roomID, Live())
	}
}

// RecordUnsentRooms marks rooms that had updates we chose not to send, so the
// next response knows where to resume from.
//
// Only a LIVE room moves. PREVIOUSLY already names a token to resume from and
// must keep it -- overwriting it with a later one would skip everything in
// between. NEVER has nothing to resume from, and claiming otherwise would tell
// a future response to send a delta to a client that has no base to apply it
// to.
func (m *RoomStatusMap) RecordUnsentRooms(roomIDs []string, fromToken string) {
	for _, roomID := range roomIDs {
		if m.HaveSentRoom(roomID).Status != FlagLive {
			continue
		}
		m.set(roomID, Previously(fromToken))
	}
}

func (m *RoomStatusMap) set(roomID string, hs HaveSent) {
	if m.updates == nil {
		m.updates = make(map[string]HaveSent)
	}
	m.updates[roomID] = hs
}

// Updates returns only this request's changes.
func (m *RoomStatusMap) Updates() map[string]HaveSent { return m.updates }

// All returns the flattened state, updates winning over base.
//
// This is what gets written: Synapse upserts the whole map rather than only the
// updates, on top of copying the previous position's rows forward. The two
// overlap completely -- see copyForward in persist.go, which records the
// mutation testing that established it -- and both are kept because both are
// Synapse's, and because each covers the guarantee from an end the other does
// not: this one from the state in memory, that one from the row already stored.
func (m *RoomStatusMap) All() map[string]HaveSent {
	out := make(map[string]HaveSent, len(m.base)+len(m.updates))
	for k, v := range m.base {
		out[k] = v
	}
	for k, v := range m.updates {
		out[k] = v
	}
	return out
}

// Len reports the flattened size.
func (m *RoomStatusMap) Len() int { return len(m.All()) }

// RoomSyncConfig is what a connection asked for in a room, as last told to it.
//
// RequiredState maps an event type to the state keys wanted for it, and the
// values are not quite state keys: `*` is a wildcard and `$LAZY` asks for
// lazy-loaded members. Resolving those is the handler's job (M12).
type RoomSyncConfig struct {
	TimelineLimit int
	RequiredState map[string]map[string]bool
}

// EncodeRequiredState serialises a required-state map the way it is stored: a
// JSON array of [type, state_key] pairs, sorted.
//
// The sort is what makes deduplication work. Two rooms asking for the same
// state must produce the same bytes, or every room gets its own row in
// sliding_sync_connection_required_state -- which on the live server is already
// the largest of the six tables by a wide margin even WITH deduplication.
func EncodeRequiredState(required map[string]map[string]bool) (string, error) {
	pairs := make([][2]string, 0, len(required))
	for eventType, stateKeys := range required {
		for stateKey := range stateKeys {
			pairs = append(pairs, [2]string{eventType, stateKey})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	b, err := json.Marshal(pairs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecodeRequiredState reverses EncodeRequiredState.
func DecodeRequiredState(s string) (map[string]map[string]bool, error) {
	var pairs [][2]string
	if err := json.Unmarshal([]byte(s), &pairs); err != nil {
		return nil, err
	}
	out := make(map[string]map[string]bool, len(pairs))
	for _, p := range pairs {
		if out[p[0]] == nil {
			out[p[0]] = make(map[string]bool)
		}
		out[p[0]][p[1]] = true
	}
	return out, nil
}

// LazyMembers records which lazily-loaded memberships a room has been given.
//
// Explicitly a cache. Losing an entry costs one member event sent twice; it can
// never cost a wrong answer, which is why entries may be evicted on a timer and
// why a conflicting write is dropped rather than retried.
type LazyMembers struct {
	// Returned maps a user whose membership this request needed to the time we
	// last recorded sending it, or nil if this is the first time.
	Returned map[string]*int64
	// Invalidated names users whose membership changed and was NOT sent, so the
	// record of having sent it must go.
	Invalidated map[string]bool
}

// ReturnedToUpdate lists the users whose timestamp needs writing: those never
// recorded, and those whose record is older than the update interval.
//
// The timestamp only exists to evict entries nothing has touched in a while, so
// it does not need to be accurate -- and writing it on every request would put
// a row per lazily-loaded member on the hot path of every sync.
func (l *LazyMembers) ReturnedToUpdate(nowMS int64) []string {
	var out []string
	for userID, lastSeen := range l.Returned {
		if lastSeen == nil || nowMS-*lastSeen >= LazyMembersUpdateIntervalMS {
			out = append(out, userID)
		}
	}
	sort.Strings(out)
	return out
}

// HasUpdates reports whether anything here is worth a database write.
func (l *LazyMembers) HasUpdates(nowMS int64) bool {
	return len(l.Invalidated) > 0 || len(l.ReturnedToUpdate(nowMS)) > 0
}

// PerConnectionState is a snapshot of what a connection has been told.
type PerConnectionState struct {
	// LastUsedMS is nil for a connection with no recorded use. Only accurate to
	// UpdateLastUsedIntervalMS.
	LastUsedMS *int64

	Rooms       RoomStatusMap
	Receipts    RoomStatusMap
	AccountData RoomStatusMap

	RoomConfigs map[string]RoomSyncConfig

	// roomConfigUpdates mirrors RoomStatusMap.updates for the configs.
	roomConfigUpdates map[string]RoomSyncConfig

	// LazyMembership is per room, and is only ever written, never read back
	// here -- the read side loads it separately.
	LazyMembership map[string]*LazyMembers
}

// SetRoomConfig records the config a room was served with.
func (p *PerConnectionState) SetRoomConfig(roomID string, cfg RoomSyncConfig) {
	if p.RoomConfigs == nil {
		p.RoomConfigs = make(map[string]RoomSyncConfig)
	}
	if p.roomConfigUpdates == nil {
		p.roomConfigUpdates = make(map[string]RoomSyncConfig)
	}
	p.RoomConfigs[roomID] = cfg
	p.roomConfigUpdates[roomID] = cfg
}

// RoomConfigUpdates returns only the configs changed this request.
func (p *PerConnectionState) RoomConfigUpdates() map[string]RoomSyncConfig {
	return p.roomConfigUpdates
}

// HasUpdates reports whether this state is worth persisting.
//
// It must not be over-eager. Every persist mints a new connection position,
// which invalidates the cached state for the old one and copies a table's worth
// of rows forward -- measured at ~725 stream rows plus ~248 room-config rows
// per position for a 654-room connection, and Element X holds three connections
// per device. A response that changed nothing must reuse the client's position.
func (p *PerConnectionState) HasUpdates(nowMS int64) bool {
	if len(p.Rooms.Updates()) > 0 || len(p.Receipts.Updates()) > 0 ||
		len(p.AccountData.Updates()) > 0 || len(p.roomConfigUpdates) > 0 {
		return true
	}
	for _, lm := range p.LazyMembership {
		if lm.HasUpdates(nowMS) {
			return true
		}
	}
	return false
}
