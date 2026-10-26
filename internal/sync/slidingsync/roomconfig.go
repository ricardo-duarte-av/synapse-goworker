package slidingsync

import "github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"

// NewRoomSyncConfig normalises a list's or subscription's required_state into
// the map form the connection store holds.
//
// The normalisation is not cosmetic. required_state entries are OR'd, so a
// wildcard subsumes anything it covers, and leaving the subsumed entries in
// would make two requests asking for the same thing produce different stored
// configs -- which defeats the deduplication AND makes _required_state_changes
// see a difference where there is none, re-sending state the client already has.
//
// Ported from RoomSyncConfig.from_room_config.
func NewRoomSyncConfig(p CommonRoomParameters) slidingstore.RoomSyncConfig {
	required := map[string]map[string]bool{}

	for _, entry := range p.RequiredState {
		stateType, stateKey := entry[0], entry[1]

		// A wildcard on this exact state key already covers it.
		if required[Wildcard][stateKey] {
			continue
		}
		// A wildcard state key for this type already covers it.
		if required[stateType][Wildcard] {
			continue
		}

		// ["*","*"] subsumes everything, and nothing after it can add.
		if stateType == Wildcard && stateKey == Wildcard {
			return slidingstore.RoomSyncConfig{
				TimelineLimit: p.TimelineLimit,
				RequiredState: map[string]map[string]bool{Wildcard: {Wildcard: true}},
			}
		}

		// A wildcard TYPE for a given state key covers that key under every
		// type, so drop the narrower entries it subsumes.
		if stateType == Wildcard {
			dropStateKey(required, stateKey)
		}

		if stateKey == Wildcard {
			// A wildcard state key covers every key of this type.
			required[stateType] = map[string]bool{Wildcard: true}
			continue
		}
		if required[stateType] == nil {
			required[stateType] = map[string]bool{}
		}
		required[stateType][stateKey] = true
	}

	return slidingstore.RoomSyncConfig{
		TimelineLimit: p.TimelineLimit,
		RequiredState: required,
	}
}

// CombineRoomSyncConfig returns the union of two configs.
//
// A room can appear in several lists at once, and each may ask for different
// state and a different timeline depth. The client gets the superset: the
// highest timeline limit and the union of the required state. Sending less
// would mean one list's presence in the response silently degrading another's.
//
// Ported from RoomSyncConfig.combine_room_sync_config.
func CombineRoomSyncConfig(a, b slidingstore.RoomSyncConfig) slidingstore.RoomSyncConfig {
	timelineLimit := a.TimelineLimit
	if b.TimelineLimit > timelineLimit {
		timelineLimit = b.TimelineLimit
	}

	required := map[string]map[string]bool{}
	for stateType, keys := range a.RequiredState {
		required[stateType] = map[string]bool{}
		for key := range keys {
			required[stateType][key] = true
		}
	}

	for stateType, keys := range b.RequiredState {
		if required[Wildcard][Wildcard] {
			break
		}
		if required[stateType][Wildcard] {
			continue
		}
		if stateType == Wildcard && keys[Wildcard] {
			return slidingstore.RoomSyncConfig{
				TimelineLimit: timelineLimit,
				RequiredState: map[string]map[string]bool{Wildcard: {Wildcard: true}},
			}
		}
		for stateKey := range keys {
			if required[Wildcard][stateKey] {
				continue
			}
			if stateType == Wildcard {
				dropStateKey(required, stateKey)
			}
			if stateKey == Wildcard {
				required[stateType] = map[string]bool{Wildcard: true}
				break
			}
			if required[stateType] == nil {
				required[stateType] = map[string]bool{}
			}
			required[stateType][stateKey] = true
		}
	}

	return slidingstore.RoomSyncConfig{TimelineLimit: timelineLimit, RequiredState: required}
}

// dropStateKey removes one state key from every type, leaving no empty types
// behind -- an empty set and an absent type must not be distinguishable, or the
// encoding differs for equivalent configs and deduplication stops working.
func dropStateKey(required map[string]map[string]bool, stateKey string) {
	for stateType, keys := range required {
		delete(keys, stateKey)
		if len(keys) == 0 {
			delete(required, stateType)
		}
	}
}
