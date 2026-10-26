package slidingsync

import (
	"sort"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// The required-state diff: given what a connection was last told to expect in a
// room and what it is asking for now, work out what has to be fetched and sent.
//
// A port of `_required_state_changes` in synapse/handlers/sliding_sync/. It is
// the trickiest pure function in the endpoint, and the reason is one idea
// repeated in several places:
//
//	A state key is only forgotten when the client removes it AND the state
//	behind it changed.
//
// If a client drops `m.room.topic` from required_state and then adds it back,
// the server must not re-send a topic the client already has. So the effective
// config keeps the key until the topic actually changes. Everything below that
// looks like over-complication is a version of that rule.
//
// It has no I/O, which is deliberate: it is the piece most likely to be subtly
// wrong, so it is tested exhaustively before it is wired to anything.

// MaxRememberedStateKeys bounds how many state keys the effective config
// remembers per type.
//
// Synapse's number. Without a bound, a room whose membership churns grows one
// stored state key per member for ever, in a table that already holds a row per
// room per connection. With it, a client may occasionally be sent a member
// event it already had -- which is the cheap side of the trade.
const MaxRememberedStateKeys = 100

// RequiredStateChanges is what the diff produces.
type RequiredStateChanges struct {
	// Changed is the new effective required-state map to store, or nil if it
	// should be left alone.
	//
	// nil is not the same as empty. Storing an unchanged config still writes a
	// row and forces a new connection position, so "no change" has to be
	// expressible.
	Changed map[string]map[string]bool

	// Added is the extra current state to fetch and send, on top of whatever
	// the state deltas already produce.
	Added StateFilter

	// ExtraLazyMembers are users previously sent as EXPLICIT members whose
	// membership can now be remembered as lazily loaded, so lazy loading does
	// not re-send them.
	ExtraLazyMembers map[string]bool

	// InvalidatedLazyMembers are users whose membership changed and was not
	// sent, so the record of having sent it must go.
	InvalidatedLazyMembers map[string]bool
}

// ComputeRequiredStateChanges diffs a room's stored required state against the
// request's.
//
// stateDeltas is the set of (type, state_key) that changed in the token range,
// already filtered to what this user may see.
func ComputeRequiredStateChanges(
	userID string,
	prev, request map[string]map[string]bool,
	previouslyReturnedLazy, requestLazy map[string]bool,
	stateDeltas map[store.StateKey]bool,
) RequiredStateChanges {
	// Lazy members whose membership changed but which we are not sending: the
	// record of having sent them is now wrong and has to go, or the client
	// keeps a stale membership for ever.
	invalidatedLazy := map[string]bool{}
	for delta := range stateDeltas {
		if delta.Type != memberEventType {
			continue
		}
		if requestLazy[delta.StateKey] {
			// It is being sent this time, so nothing is stale.
			continue
		}
		if !previouslyReturnedLazy[delta.StateKey] {
			// Never sent, so nothing to invalidate.
			continue
		}
		invalidatedLazy[delta.StateKey] = true
	}

	if sameRequiredState(prev, request) {
		// The config did not move, so only lazy loading can need anything: the
		// members this request wants that the connection has not been given.
		var added []TypeAndKey
		for _, userID := range sortedKeys(requestLazy) {
			if !previouslyReturnedLazy[userID] {
				k := userID
				added = append(added, TypeAndKey{Type: memberEventType, StateKey: &k})
			}
		}
		return RequiredStateChanges{
			Changed:                nil,
			Added:                  StateFilterFromTypes(added),
			InvalidatedLazyMembers: invalidatedLazy,
		}
	}

	prevWildcard := prev[Wildcard]
	requestWildcard := request[Wildcard]

	// Previously fetching absolutely everything. Whatever the request is now,
	// it is a subset, so the config follows it and nothing new is needed.
	if prevWildcard[Wildcard] {
		return RequiredStateChanges{
			Changed:                request,
			Added:                  NoState(),
			InvalidatedLazyMembers: invalidatedLazy,
		}
	}

	// A type wildcard appearing or disappearing is not worth being clever
	// about: take the request wholesale, and fetch everything if it grew.
	if len(setDifference(requestWildcard, prevWildcard)) > 0 {
		return RequiredStateChanges{
			Changed:                request,
			Added:                  AllState(),
			InvalidatedLazyMembers: invalidatedLazy,
		}
	}
	if len(setDifference(prevWildcard, requestWildcard)) > 0 {
		return RequiredStateChanges{
			Changed:                request,
			Added:                  NoState(),
			InvalidatedLazyMembers: invalidatedLazy,
		}
	}

	changes := map[string]map[string]bool{}
	var added []TypeAndKey

	changedByType := map[string]map[string]bool{}
	for delta := range stateDeltas {
		if changedByType[delta.Type] == nil {
			changedByType[delta.Type] = map[string]bool{}
		}
		changedByType[delta.Type][delta.StateKey] = true
	}

	// First pass: what was ADDED to the request.
	for _, eventType := range sortedUnion(prev, request) {
		oldKeys := prev[eventType]
		requestKeys := request[eventType]
		changedKeys := changedByType[eventType]

		if sameSet(oldKeys, requestKeys) {
			continue
		}
		if len(setDifference(requestKeys, oldKeys)) == 0 {
			// Nothing added. Removals are handled in the second pass.
			continue
		}

		// $LAZY appearing is recorded immediately: from here on the lazy-member
		// tables are in use, so there is nothing to gain by pretending the
		// config has not changed.
		if eventType == memberEventType && !oldKeys[Lazy] && requestKeys[Lazy] {
			changes[eventType] = requestKeys
			continue
		}

		// The rule: a key is forgotten only if the client removed it AND the
		// state behind it changed. Otherwise remove-then-re-add would re-send
		// an event the client already has.
		invalidated := setIntersection(setDifference(oldKeys, requestKeys), changedKeys)

		// Wildcard and $LAZY are instructions rather than state keys, so they
		// are never inherited as "we already sent this one".
		inheritable := setDifference(
			setDifference(oldKeys, map[string]bool{Wildcard: true, Lazy: true}),
			invalidated)

		changes[eventType] = setUnion(requestKeys, inheritable)

		// Bound what is remembered. Past the cap, keep the requested keys and
		// backfill with an arbitrary slice of the inheritable ones -- Synapse
		// notes it would prefer the most recent, and takes an arbitrary subset
		// because a set has no order. Ours sorts, so the choice is at least
		// reproducible.
		if len(changes[eventType]) > MaxRememberedStateKeys {
			changes[eventType] = copySet(requestKeys)
			if len(requestKeys) < MaxRememberedStateKeys {
				room := MaxRememberedStateKeys - len(requestKeys)
				for _, k := range sortedKeys(inheritable) {
					if room == 0 {
						break
					}
					changes[eventType][k] = true
					room--
				}
			}
		}

		if oldKeys[Wildcard] {
			// Everything of this type was already being fetched.
			continue
		}

		if requestKeys[Wildcard] {
			added = append(added, TypeAndKey{Type: eventType})
			continue
		}
		for _, stateKey := range sortedKeys(setDifference(requestKeys, oldKeys)) {
			switch {
			case stateKey == Me:
				k := userID
				added = append(added, TypeAndKey{Type: eventType, StateKey: &k})
			case stateKey == Lazy:
				// Lazy loading is resolved outside this function, and $LAZY is
				// meaningless as a state key for any other type.
			case eventType == memberEventType:
				if !previouslyReturnedLazy[stateKey] {
					k := stateKey
					added = append(added, TypeAndKey{Type: eventType, StateKey: &k})
				}
			default:
				k := stateKey
				added = append(added, TypeAndKey{Type: eventType, StateKey: &k})
			}
		}
	}

	// Lazily-loaded members this request needs that the connection has neither
	// been given lazily nor asked for explicitly before.
	previouslyExplicitMembers := copySet(prev[memberEventType])
	if previouslyExplicitMembers[Me] {
		previouslyExplicitMembers[userID] = true
	}
	for _, required := range sortedKeys(requestLazy) {
		if previouslyReturnedLazy[required] || previouslyExplicitMembers[required] {
			continue
		}
		k := required
		added = append(added, TypeAndKey{Type: memberEventType, StateKey: &k})
	}

	addedFilter := StateFilterFromTypes(added)

	// Second pass: what was REMOVED from the request, for types whose state
	// actually changed. A removal with no state change leaves the effective
	// config alone, which is what makes remove-then-re-add free.
	for _, eventType := range sortedKeys2(changedByType) {
		changedKeys := copySet(changedByType[eventType])
		oldKeys := prev[eventType]
		requestKeys := request[eventType]

		if sameSet(oldKeys, requestKeys) {
			continue
		}

		// A client may write its own membership as either its user ID or $ME,
		// so a change to one is a change to the other.
		if changedKeys[userID] {
			changedKeys[Me] = true
		}

		invalidated := setIntersection(setDifference(oldKeys, requestKeys), changedKeys)

		if len(setDifference(requestKeys, oldKeys)) > 0 {
			// Additions were dealt with above.
			continue
		}

		if oldKeys[Wildcard] != requestKeys[Wildcard] {
			changes[eventType] = requestKeys
			continue
		}

		if eventType == memberEventType {
			hasLazy := oldKeys[Lazy] || requestKeys[Lazy]
			if oldKeys[Lazy] != requestKeys[Lazy] {
				changes[eventType] = requestKeys
				continue
			}
			// With lazy loading on, an invalidated explicit member is better
			// handled by resetting to the request: the lazy tables can carry
			// the memory instead, and churning the effective config would
			// defeat the deduplication between rooms.
			if hasLazy && len(invalidated) > 0 {
				changes[eventType] = requestKeys
				continue
			}
		}

		if len(invalidated) > 0 {
			changes[eventType] = setDifference(oldKeys, invalidated)
		}
	}

	// An explicit member that has just been dropped from the config can be
	// remembered as a lazily-loaded one instead, so lazy loading does not
	// re-send somebody the client already has -- but only if their membership
	// did not change, because then the client's copy is still right.
	extraLazy := map[string]bool{}
	if len(changes[memberEventType]) > 0 && request[memberEventType][Lazy] {
		for _, stateKey := range sortedKeys(prev[memberEventType]) {
			if stateKey == Wildcard || stateKey == Lazy {
				continue
			}
			if stateKey == Me {
				stateKey = userID
			}
			if !stateDeltas[store.StateKey{Type: memberEventType, StateKey: stateKey}] {
				extraLazy[stateKey] = true
			}
		}
	}

	var newMap map[string]map[string]bool
	if len(changes) > 0 {
		newMap = map[string]map[string]bool{}
		for eventType, keys := range prev {
			newMap[eventType] = copySet(keys)
		}
		for eventType, keys := range changes {
			if len(keys) > 0 {
				newMap[eventType] = copySet(keys)
			} else {
				// An empty set and an absent type must not be
				// distinguishable, or two equivalent configs encode
				// differently and deduplication stops working.
				delete(newMap, eventType)
			}
		}
	}

	return RequiredStateChanges{
		Changed:                newMap,
		Added:                  addedFilter,
		ExtraLazyMembers:       extraLazy,
		InvalidatedLazyMembers: invalidatedLazy,
	}
}

const memberEventType = "m.room.member"

func sameRequiredState(a, b map[string]map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for eventType, keys := range a {
		if !sameSet(keys, b[eventType]) {
			return false
		}
	}
	return true
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func setDifference(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		if !b[k] {
			out[k] = true
		}
	}
	return out
}

func setIntersection(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		if b[k] {
			out[k] = true
		}
	}
	return out
}

func setUnion(a, b map[string]bool) map[string]bool {
	out := copySet(a)
	for k := range b {
		out[k] = true
	}
	return out
}

func copySet(a map[string]bool) map[string]bool {
	out := make(map[string]bool, len(a))
	for k := range a {
		out[k] = true
	}
	return out
}

// The sorted helpers exist so the function is deterministic. Synapse iterates
// sets and dicts, which in Python is insertion-ordered for dicts and arbitrary
// for sets; here every iteration that can affect the OUTPUT is ordered, so the
// same inputs always produce the same answer and a failing test is
// reproducible.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys2(m map[string]map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedUnion(a, b map[string]map[string]bool) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	return sortedKeys(seen)
}
