package slidingsync

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/eventfilter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// Turning a room's required_state config into actual events.
//
// The second half of `get_room_sync_data`. Three things happen here that are
// easy to conflate and must not be:
//
//   - Deciding WHICH state to fetch, from the config's wildcards, $LAZY and $ME.
//   - Fetching it, differently for an initial room (current state at the token)
//     and an incremental one (the deltas, plus whatever newly became required).
//   - Recording what the connection has now been told, so the next response can
//     send a delta rather than repeat itself.

// requiredStatePlan is what the config resolves to before anything is fetched.
type requiredStatePlan struct {
	// all means the room's entire current state.
	all bool
	// keys are exact (type, state_key) pairs.
	keys []store.StateKey
	// wildcardTypes are types wanted in full.
	wildcardTypes []string

	// lazyLoad is on when the config asked for `$LAZY` membership.
	lazyLoad bool
	// lazyUserIDs are the members the timeline makes relevant.
	lazyUserIDs map[string]bool
	// explicitUserIDs are members named outright, tracked separately because
	// lazy loading must not claim credit for having sent them.
	explicitUserIDs map[string]bool
}

// planRequiredState works out what to fetch for a room.
//
// The `["*","*"]` case and a wildcard EVENT TYPE both collapse to "fetch
// everything". Synapse notes that returning more state than asked for is
// allowed -- required_state is a minimum -- and that its StateFilter cannot
// express a wildcard event type, so it fetches all and filters afterwards. We
// do the same, and for the same reason: a type wildcard is rare and the
// alternative is a query shape that cannot use the state-group index.
func planRequiredState(
	userID string, cfg slidingstore.RoomSyncConfig,
	timeline []store.TimelineEvent, deltas map[store.StateKey]string,
	limited, initial bool,
) requiredStatePlan {

	p := requiredStatePlan{
		lazyUserIDs:     map[string]bool{},
		explicitUserIDs: map[string]bool{},
	}

	if cfg.RequiredState[Wildcard][Wildcard] || cfg.RequiredState[Wildcard] != nil {
		p.all = true
		return p
	}

	for _, eventType := range sortedKeys2(cfg.RequiredState) {
		for _, stateKey := range sortedKeys(cfg.RequiredState[eventType]) {
			switch {
			case stateKey == Wildcard:
				p.wildcardTypes = append(p.wildcardTypes, eventType)

			case eventType == memberEventType && stateKey == Lazy:
				p.lazyLoad = true
				// Everyone the client can see in the timeline is relevant: the
				// sender of every event, and the subject of every membership
				// change. Without their membership the client cannot render a
				// name against a message.
				for _, e := range timeline {
					p.lazyUserIDs[e.Sender] = true
					if e.Type == memberEventType && e.IsState {
						p.lazyUserIDs[e.StateKey] = true
					}
				}
				if limited || initial {
					// A gapped timeline: only the people in it are needed.
					for _, u := range sortedKeys(p.lazyUserIDs) {
						p.keys = append(p.keys, store.StateKey{Type: memberEventType, StateKey: u})
					}
				} else {
					// A continuous timeline: send every membership change.
					// A client that has built the full member list can then
					// keep it, which it cannot do if changes are withheld.
					p.wildcardTypes = append(p.wildcardTypes, memberEventType)
					for k := range deltas {
						if k.Type == memberEventType {
							p.lazyUserIDs[k.StateKey] = true
						}
					}
				}

			case stateKey == Lazy:
				// $LAZY is meaningless for anything but membership.

			default:
				// $ME and the user's own ID must resolve to the same thing, or
				// a client that switches between them is sent the same event
				// twice and the stored configs stop deduplicating.
				normalised := stateKey
				if stateKey == Me {
					normalised = userID
				}
				if eventType == memberEventType {
					p.explicitUserIDs[normalised] = true
				}
				p.keys = append(p.keys, store.StateKey{Type: eventType, StateKey: normalised})
			}
		}
	}

	// A member named outright is not a lazily-loaded one. Keeping them in both
	// sets would let the lazy bookkeeping record having sent somebody that the
	// explicit config is responsible for.
	for u := range p.explicitUserIDs {
		delete(p.lazyUserIDs, u)
	}
	return p
}

// wants reports whether the plan covers one state entry, for filtering a
// superset back down to what was asked for.
func (p requiredStatePlan) wants(k store.StateKey) bool {
	if p.all {
		return true
	}
	for _, t := range p.wildcardTypes {
		if k.Type == t {
			return true
		}
	}
	for _, want := range p.keys {
		if want == k {
			return true
		}
	}
	return false
}

func (p requiredStatePlan) isEmpty() bool {
	return !p.all && len(p.keys) == 0 && len(p.wildcardTypes) == 0
}

// resolveRequiredState fetches the required state and records what the
// connection has been told.
func (d Deps) resolveRequiredState(
	ctx context.Context, req RoomDataRequest, res *RoomResult,
	timeline timelineResult, deltas map[store.StateKey]string,
	initial, limited bool,
) error {
	// Invite and knock rooms have stripped state and nothing else.
	if req.Room.Membership == "invite" || req.Room.Membership == "knock" {
		d.recordRoomConfig(req, req.Config.RequiredState)
		return nil
	}

	plan := planRequiredState(req.UserID, req.Config, timeline.events, deltas, limited, initial)

	// The metadata the response needs regardless of what was asked for: the
	// heroes' memberships, and the avatar when it may have changed.
	extra := make([]store.StateKey, 0, len(res.Heroes)+1)
	for _, h := range res.Heroes {
		extra = append(extra, store.StateKey{Type: memberEventType, StateKey: h.UserID})
	}

	var stateIDs map[store.StateKey]string
	var err error

	if initial {
		fetch := append(append([]store.StateKey(nil), plan.keys...), extra...)
		if plan.all {
			stateIDs, err = d.Store.StateIDsAt(ctx, req.RoomID, req.To.Room)
		} else {
			stateIDs, err = d.Store.StateAtWithWildcards(
				ctx, req.RoomID, req.To.Room, fetch, plan.wildcardTypes)
		}
		if err != nil {
			return err
		}
	} else {
		// Incremental: the deltas are the answer, plus the current value of
		// anything that has only just become required. Without that second
		// part a client that adds a state type to required_state is sent
		// nothing until that state happens to change.
		stateIDs = map[store.StateKey]string{}
		for k, v := range deltas {
			stateIDs[k] = v
		}

		prevCfg, hasPrev := configFor(req.Previous, req.RoomID)
		if hasPrev {
			changes := ComputeRequiredStateChanges(req.UserID,
				prevCfg.RequiredState, req.Config.RequiredState,
				d.previouslyReturnedLazy(req), plan.lazyUserIDs, toDeltaSet(deltas))

			if !changes.Added.IsEmpty() {
				added, err := d.fetchFilter(ctx, req, changes.Added)
				if err != nil {
					return err
				}
				for k, v := range added {
					stateIDs[k] = v
				}
			}
			if changes.Changed != nil {
				d.recordRoomConfig(req, changes.Changed)
			} else {
				d.recordRoomConfig(req, req.Config.RequiredState)
			}
			d.recordLazyMembership(req, plan, changes)
		} else {
			d.recordRoomConfig(req, req.Config.RequiredState)
		}

		// The heroes' memberships are needed whether or not they changed.
		if len(extra) > 0 {
			heroState, err := d.Store.FilteredStateAt(ctx, req.RoomID, req.To.Room, extra)
			if err != nil {
				return err
			}
			for k, v := range heroState {
				if _, already := stateIDs[k]; !already {
					stateIDs[k] = v
				}
			}
		}
	}

	if initial {
		d.recordRoomConfig(req, req.Config.RequiredState)
		d.recordLazyMembership(req, plan, RequiredStateChanges{})
	}

	return d.serialiseState(ctx, req, res, plan, stateIDs)
}

// fetchFilter resolves a StateFilter into event IDs at the token.
func (d Deps) fetchFilter(
	ctx context.Context, req RoomDataRequest, f StateFilter,
) (map[store.StateKey]string, error) {
	if f.IsAll() {
		return d.Store.StateIDsAt(ctx, req.RoomID, req.To.Room)
	}
	var keys []store.StateKey
	var wildcards []string
	for _, e := range f.Entries() {
		if e.StateKey == nil {
			wildcards = append(wildcards, e.Type)
			continue
		}
		keys = append(keys, store.StateKey{Type: e.Type, StateKey: *e.StateKey})
	}
	return d.Store.StateAtWithWildcards(ctx, req.RoomID, req.To.Room, keys, wildcards)
}

// serialiseState loads the events and narrows them to what was actually asked
// for -- the fetch is deliberately a superset (heroes, avatar, wildcards), and
// sending that superset would leak state a client did not request.
func (d Deps) serialiseState(
	ctx context.Context, req RoomDataRequest, res *RoomResult,
	plan requiredStatePlan, stateIDs map[store.StateKey]string,
) error {
	if plan.isEmpty() || len(stateIDs) == 0 {
		d.fillHeroProfiles(ctx, req, res, stateIDs)
		return nil
	}

	wanted := make([]string, 0, len(stateIDs))
	keyOf := map[string]store.StateKey{}
	for k, eventID := range stateIDs {
		keyOf[eventID] = k
		wanted = append(wanted, eventID)
	}
	sort.Strings(wanted)

	events, err := d.Store.EventsByID(ctx, wanted, req.Room.RoomVersion)
	if err != nil {
		return err
	}
	// State is as redactable as a message: an m.room.topic in current state can
	// have been redacted just as a message can.
	redactions, err := d.Store.Redactions(ctx, wanted)
	if err != nil {
		return err
	}

	ordered := make([]store.StateKey, 0, len(stateIDs))
	for k := range stateIDs {
		if plan.wants(k) {
			ordered = append(ordered, k)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Type != ordered[j].Type {
			return ordered[i].Type < ordered[j].Type
		}
		return ordered[i].StateKey < ordered[j].StateKey
	})
	for _, k := range ordered {
		ev, ok := events[stateIDs[k]]
		if !ok {
			continue
		}
		// Serialised the same way the timeline is, and for the same reasons:
		// a v3+ PDU carries no event_id of its own, `unsigned` is rebuilt from
		// an allowlist, and a redacted event is pruned on read.
		stored := ev.Stored
		cfg := d.EventConfig(req.UserID, req.DeviceID, req.TokenID)
		if err := eventfilter.AttachRedaction(&stored, redactions, req.NowMS, cfg); err != nil {
			return err
		}
		body, err := clientevent.Serialize(stored, req.NowMS, cfg)
		if err != nil {
			return err
		}
		res.RequiredState = append(res.RequiredState, json.RawMessage(body))
	}

	d.fillHeroProfilesFrom(req, res, stateIDs, events)
	return nil
}

func (d Deps) fillHeroProfiles(
	ctx context.Context, req RoomDataRequest, res *RoomResult, stateIDs map[store.StateKey]string,
) {
	if len(res.Heroes) == 0 {
		return
	}
	var ids []string
	for _, h := range res.Heroes {
		if id, ok := stateIDs[store.StateKey{Type: memberEventType, StateKey: h.UserID}]; ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	events, err := d.Store.EventsByID(ctx, ids, req.Room.RoomVersion)
	if err != nil {
		return
	}
	d.fillHeroProfilesFrom(req, res, stateIDs, events)
}

func (d Deps) fillHeroProfilesFrom(
	req RoomDataRequest, res *RoomResult,
	stateIDs map[store.StateKey]string, events map[string]store.StateEvent,
) {
	for i := range res.Heroes {
		id, ok := stateIDs[store.StateKey{Type: memberEventType, StateKey: res.Heroes[i].UserID}]
		if !ok {
			continue
		}
		ev, ok := events[id]
		if !ok {
			continue
		}
		if name := gjsonString(ev.JSON, "content.displayname"); name != nil {
			res.Heroes[i].DisplayName = name
		}
		if url := gjsonString(ev.JSON, "content.avatar_url"); url != nil {
			res.Heroes[i].AvatarURL = url
		}
	}
}

func (d Deps) previouslyReturnedLazy(req RoomDataRequest) map[string]bool {
	// Loaded from the connection store by the caller and threaded through; an
	// empty set means "we have told this connection nothing", which is the safe
	// direction -- it re-sends a membership rather than withholding one.
	if req.PreviouslyReturnedLazy == nil {
		return map[string]bool{}
	}
	return req.PreviouslyReturnedLazy
}

func (d Deps) recordRoomConfig(req RoomDataRequest, required map[string]map[string]bool) {
	if req.New == nil {
		return
	}
	req.New.SetRoomConfig(req.RoomID, slidingstore.RoomSyncConfig{
		TimelineLimit: req.Config.TimelineLimit,
		RequiredState: required,
	})
}

func (d Deps) recordLazyMembership(
	req RoomDataRequest, plan requiredStatePlan, changes RequiredStateChanges,
) {
	if req.New == nil || !plan.lazyLoad {
		return
	}
	if req.New.LazyMembership == nil {
		req.New.LazyMembership = map[string]*slidingstore.LazyMembers{}
	}
	entry := &slidingstore.LazyMembers{
		Returned:    map[string]*int64{},
		Invalidated: changes.InvalidatedLazyMembers,
	}
	for u := range plan.lazyUserIDs {
		// nil means "first time", which is what makes the timestamp get
		// written. A user we have sent before keeps their recorded time,
		// threaded in by the caller.
		var seen *int64
		if ts, ok := req.LazyMemberLastSeen[u]; ok {
			t := ts
			seen = &t
		}
		entry.Returned[u] = seen
	}
	for u := range changes.ExtraLazyMembers {
		if _, already := entry.Returned[u]; !already {
			entry.Returned[u] = nil
		}
	}
	req.New.LazyMembership[req.RoomID] = entry
}

func toDeltaSet(deltas map[store.StateKey]string) map[store.StateKey]bool {
	out := make(map[store.StateKey]bool, len(deltas))
	for k := range deltas {
		out[k] = true
	}
	return out
}
