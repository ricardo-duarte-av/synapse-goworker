package slidingsync

import "sort"

// StateFilter names the state a response still needs to fetch.
//
// A port of the parts of synapse/types/state.py that
// _required_state_changes uses: All (fetch everything), None (fetch nothing),
// and a set of types where a nil state-key set means "every key of this type".
//
// Three states rather than two, and the difference between "no types" and "all
// types" is the whole point: an empty filter means the response needs no extra
// state, where an all filter means it needs the room's entire current state.
// Conflating them either sends nothing or sends everything.
type StateFilter struct {
	all bool
	// types maps an event type to the state keys wanted. A nil value means
	// every state key of that type.
	types map[string]map[string]bool
}

// TypeAndKey is one entry of a state filter. A nil StateKey means every state
// key of that type.
type TypeAndKey struct {
	Type     string
	StateKey *string
}

// AllState fetches the room's entire current state.
func AllState() StateFilter { return StateFilter{all: true} }

// NoState fetches nothing.
func NoState() StateFilter { return StateFilter{} }

// StateFilterFromTypes builds a filter from explicit entries.
func StateFilterFromTypes(entries []TypeAndKey) StateFilter {
	if len(entries) == 0 {
		return NoState()
	}
	f := StateFilter{types: map[string]map[string]bool{}}
	for _, e := range entries {
		if e.StateKey == nil {
			// A wildcard for this type wins over any specific keys already
			// recorded for it, and nothing narrower may be added afterwards.
			f.types[e.Type] = nil
			continue
		}
		if keys, seen := f.types[e.Type]; seen && keys == nil {
			continue
		}
		if f.types[e.Type] == nil {
			f.types[e.Type] = map[string]bool{}
		}
		f.types[e.Type][*e.StateKey] = true
	}
	return f
}

// IsAll reports whether the whole current state is wanted.
func (f StateFilter) IsAll() bool { return f.all }

// IsEmpty reports whether nothing is wanted.
func (f StateFilter) IsEmpty() bool { return !f.all && len(f.types) == 0 }

// Wants reports whether a filter covers one state entry.
func (f StateFilter) Wants(eventType, stateKey string) bool {
	if f.all {
		return true
	}
	keys, ok := f.types[eventType]
	if !ok {
		return false
	}
	if keys == nil {
		return true
	}
	return keys[stateKey]
}

// Entries returns the filter's contents in a stable order, for tests and for
// building a query.
func (f StateFilter) Entries() []TypeAndKey {
	out := make([]TypeAndKey, 0, len(f.types))
	types := make([]string, 0, len(f.types))
	for t := range f.types {
		types = append(types, t)
	}
	sort.Strings(types)
	for _, t := range types {
		keys := f.types[t]
		if keys == nil {
			out = append(out, TypeAndKey{Type: t})
			continue
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		for i := range names {
			out = append(out, TypeAndKey{Type: t, StateKey: &names[i]})
		}
	}
	return out
}
