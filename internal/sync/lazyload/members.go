package lazyload

import (
	"container/list"
	"sync"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// DefaultMaxMembers is LAZY_LOADED_MEMBERS_CACHE_MAX_SIZE: 100 members per
// device, not scaled by caches.global_factor, since Synapse's #20289
// (v1.164) replaced the LruCache with an OrderedDict.
const DefaultMaxMembers = 100

// MemberCache holds one SentMembers per device.
type MemberCache = Sessions[SentMembers]

// NewMembers builds a member cache. A non-positive size or TTL takes the
// default.
func NewMembers(maxPerDevice int, ttl time.Duration) *MemberCache {
	if maxPerDevice <= 0 {
		maxPerDevice = DefaultMaxMembers
	}
	return newSessions(ttl, func() *SentMembers {
		return &SentMembers{max: maxPerDevice, byUser: map[string]*list.Element{}, order: list.New()}
	})
}

// SentMembers is Synapse's LazyLoadedMembersCache (handlers/sync.py,
// v1.164.1): the membership events a lazy-loading client has been sent,
// each with the `next_batch` room position of the response that carried it.
//
// A member may only be left out if the client processed that response. A
// client that has syncs from its `next_batch` next, so a request from
// `since` means it processed every response up to `since` and none after:
// StartRequest forgets the entries recorded after `since` (a request the
// client timed out and retried, or a reload from an older persisted token),
// and an initial sync forgets everything. Least recently used first; a
// lookup that matches counts as a use.
type SentMembers struct {
	mu     sync.Mutex
	max    int
	byUser map[string]*list.Element
	order  *list.List // of *sent, least recently used at the front
	// newest bounds every entry's position, so StartRequest can skip the
	// walk when nothing was recorded after `since`; nil when empty.
	newest *streamtoken.RoomKey
}

type sent struct {
	userID, eventID string
	at              streamtoken.RoomKey
}

// StartRequest is start_request, called at the start of each sync request
// for this device that lazy-loads members; since is nil for an initial sync.
func (m *SentMembers) StartRequest(since *streamtoken.RoomKey) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if since == nil {
		m.byUser = map[string]*list.Element{}
		m.order.Init()
		m.newest = nil
		return
	}
	if m.newest == nil || m.newest.IsBeforeOrEq(*since) {
		return
	}
	for e := m.order.Front(); e != nil; {
		next := e.Next()
		if s := e.Value.(*sent); !s.at.IsBeforeOrEq(*since) {
			m.order.Remove(e)
			delete(m.byUser, s.userID)
		}
		e = next
	}
	// Everything left is at or before `since`.
	at := *since
	m.newest = &at
}

// WasSent is was_sent: whether eventID is the membership of userID last
// sent. A match counts as a use.
func (m *SentMembers) WasSent(userID, eventID string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.byUser[userID]
	if !ok || e.Value.(*sent).eventID != eventID {
		return false
	}
	m.order.MoveToBack(e)
	return true
}

// MarkSent is mark_sent: eventID is being sent in the response whose
// next_batch room position is at.
func (m *SentMembers) MarkSent(userID, eventID string, at streamtoken.RoomKey) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.byUser[userID]; ok {
		*e.Value.(*sent) = sent{userID: userID, eventID: eventID, at: at}
		m.order.MoveToBack(e)
	} else {
		m.byUser[userID] = m.order.PushBack(&sent{userID: userID, eventID: eventID, at: at})
	}
	for m.order.Len() > m.max {
		e := m.order.Front()
		m.order.Remove(e)
		delete(m.byUser, e.Value.(*sent).userID)
	}
	// Only move the bound forwards: a response that started earlier may
	// record its entries after a later one has.
	if m.newest == nil || m.newest.IsBeforeOrEq(at) {
		m.newest = &at
	}
}
