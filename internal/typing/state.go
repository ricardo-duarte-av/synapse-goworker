// Package typing is Synapse's typing writer (handlers/typing.py
// TypingWriterHandler): who is typing in which room, the typing stream that
// says so to every other process, PUT /rooms/{roomId}/typing/{userId}, and
// typing EDUs from other servers.
//
// It writes only while homeserver.yaml's stream_writers.typing names this
// process (SetSynapse). The state is held in memory and nowhere else, as in
// Synapse: it starts empty at serial 0 whenever this process becomes the
// writer, which every follower reads as the writer having restarted.
//
// What it does not do, because Synapse's writer does not either when it is
// not a federation sender: send EDUs. Every federation sender follows the
// stream and builds them itself; this process's sender gets the rows
// in-process (OnRow), as its own echo on the bus is dropped.
package typing

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// Synapse's timings (handlers/typing.py:64).
const (
	// FederationTimeout is how long a remote server's typing notice lasts.
	FederationTimeout = 60_000.0
	// ForgetTimeout is how often rooms with no typing change are forgotten.
	ForgetTimeout = 10 * time.Minute
	// TimeoutInterval is how often timeouts are checked (typing.py:112).
	TimeoutInterval = 5 * time.Second
	// MaxTimeout caps a client's timeout (room.py:1632).
	MaxTimeout = 120_000.0
	// DefaultTimeout is the timeout of a request that gives none.
	DefaultTimeout = 30_000.0
)

type member struct{ room, user string }

// Row is one change: the room's whole typing set at Token
// (TypingStream.TypingStreamRow).
type Row struct {
	Token  int64
	RoomID string
	Users  []string
}

// State is the writer's state. Every method is safe to call concurrently.
type State struct {
	mu sync.Mutex
	// roomTyping is _room_typing: who is typing, by room.
	roomTyping map[string]map[string]struct{}
	// roomSerials is _room_serials: the serial at which each room last
	// changed.
	roomSerials map[string]int64
	latest      int64
	// until is _member_typing_until, in ms.
	until map[member]float64
	wheel *wheel
	// roomsUpdated is _rooms_updated, for the prune.
	roomsUpdated map[string]struct{}

	now   func() float64
	onRow func(Row)
}

// NewState builds an empty state at serial 0. onRow is called for every
// change, in serial order, with the state locked: it must not call back in,
// and must be quick.
func NewState(onRow func(Row)) *State {
	return &State{
		roomTyping:   map[string]map[string]struct{}{},
		roomSerials:  map[string]int64{},
		until:        map[member]float64{},
		wheel:        newWheel(),
		roomsUpdated: map[string]struct{}{},
		now:          func() float64 { return float64(time.Now().UnixMilli()) },
		onRow:        onRow,
	}
}

func (s *State) isTypingLocked(m member) bool {
	_, ok := s.roomTyping[m.room][m.user]
	return ok
}

// Start is started_typing past its checks: the user is typing for timeout
// ms. Someone already typing has the timer moved and no new serial.
func (s *State) Start(roomID, userID string, timeout float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := member{roomID, userID}
	wasPresent := s.isTypingLocked(m)
	now := s.now()
	s.until[m] = now + timeout
	s.wheel.insert(now, m, now+timeout)
	if wasPresent {
		return
	}
	s.pushLocked(m, true)
}

// Stop is stopped_typing past its checks (_stopped_typing).
func (s *State) Stop(roomID, userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stoppedLocked(member{roomID, userID})
}

func (s *State) stoppedLocked(m member) {
	if !s.isTypingLocked(m) {
		return
	}
	delete(s.until, m)
	s.pushLocked(m, false)
}

// Remote is _recv_edu past its checks: a remote user's notice lasts
// FederationTimeout, and a repeat still takes a serial.
func (s *State) Remote(roomID, userID string, typing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := member{roomID, userID}
	now := s.now()
	s.until[m] = now + FederationTimeout
	s.wheel.insert(now, m, now+FederationTimeout)
	s.pushLocked(m, typing)
}

// pushLocked is _push_update_local: the room's set changes (or not), and
// the stream moves on regardless.
func (s *State) pushLocked(m member, typing bool) {
	set := s.roomTyping[m.room]
	if set == nil {
		set = map[string]struct{}{}
		s.roomTyping[m.room] = set
	}
	if typing {
		set[m.user] = struct{}{}
	} else {
		delete(set, m.user)
	}
	s.latest++
	s.roomSerials[m.room] = s.latest
	s.roomsUpdated[m.room] = struct{}{}
	if s.onRow != nil {
		s.onRow(Row{Token: s.latest, RoomID: m.room, Users: sortedUsers(set)})
	}
}

// HandleTimeouts is _handle_timeouts, every TimeoutInterval.
func (s *State) HandleTimeouts() (timedOut int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	// set(): a member inserted twice comes back twice.
	seen := map[member]struct{}{}
	for _, m := range s.wheel.fetch(now) {
		if _, dup := seen[m]; dup {
			continue
		}
		seen[m] = struct{}{}
		if !s.isTypingLocked(m) {
			// Synapse leaves _member_typing_until's entry behind here (a
			// remote "typing: false" sets it and nothing pops it). Nothing
			// reads it until the member types again, which sets it anew,
			// so dropping it changes nothing but memory.
			delete(s.until, m)
			continue
		}
		if until, ok := s.until[m]; !ok || until == 0 || until <= now {
			s.stoppedLocked(m)
			timedOut++
		}
	}
	return timedOut
}

// Prune is _prune_old_typing, every ForgetTimeout: rooms with no change
// since the last prune are forgotten, whoever is still in them (clients
// time typing notices out themselves).
func (s *State) Prune() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for room := range s.roomSerials {
		if _, ok := s.roomsUpdated[room]; !ok {
			delete(s.roomSerials, room)
			delete(s.roomTyping, room)
		}
	}
	s.roomsUpdated = map[string]struct{}{}
}

// CurrentToken is the latest serial.
func (s *State) CurrentToken() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

// UpdateRow is one row of get_all_typing_updates.
type UpdateRow struct {
	Token int64
	Row   json.RawMessage
}

// UpdatesSince is get_all_typing_updates (typing.py:468): each room whose
// last change is in (from, upto], as [room_id, [user_id, ...]] at that
// change's serial, and the room's typing set as it is NOW. A strict limit:
// past it the rows are cut and upto moves back to the last one kept.
func (s *State) UpdatesSince(from, upto int64, limit int) ([]UpdateRow, int64, bool) {
	if from == upto {
		return nil, upto, false
	}
	s.mu.Lock()
	type pending struct {
		serial int64
		room   string
		users  []string
	}
	var rows []pending
	for room, serial := range s.roomSerials {
		if serial != 0 && from < serial && serial <= upto {
			rows = append(rows, pending{serial, room, sortedUsers(s.roomTyping[room])})
		}
	}
	s.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].serial < rows[j].serial })

	limited := false
	if len(rows) > limit {
		rows = rows[:limit]
		upto = rows[len(rows)-1].serial
		limited = true
	}
	out := make([]UpdateRow, len(rows))
	for i, r := range rows {
		b, _ := json.Marshal([]any{r.room, r.users})
		out[i] = UpdateRow{Token: r.serial, Row: b}
	}
	return out, upto, limited
}

// Size is how many rooms, typing users and timers are held, for metrics.
func (s *State) Size() (rooms, users, timers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, set := range s.roomTyping {
		users += len(set)
	}
	return len(s.roomSerials), users, s.wheel.len()
}

// sortedUsers is the room's set as a list. Synapse's is list(set), in
// whatever order CPython's set has; followers read it as a set.
func sortedUsers(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for u := range set {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}
