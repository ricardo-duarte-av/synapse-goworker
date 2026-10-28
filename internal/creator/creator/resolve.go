package creator

import (
	"context"
	"errors"
	"slices"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/writer"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/stateres"
)

// stateBefore is the state before a new event, as calculate_context_info
// gets it from resolve_state_groups_for_events (state/__init__.py,
// v1.163.0): Synapse's _StateCacheEntry.
type stateBefore struct {
	state pdu.StateMap
	// group is the existing state group that holds the state: the prev
	// events' one group, or one that a resolution matched exactly. 0 when
	// the state needs a new group.
	group int64
	// prev and delta are the group's edge (get_state_group_delta) for an
	// existing group, or what a new group is built on
	// (_make_state_cache_entry's closest match). hasPrev is false for a new
	// group stored whole, and for an exact match, whose entry has no edge.
	prev    int64
	delta   pdu.StateMap
	hasPrev bool
	// inputs are the prev events' groups, when there are several: a
	// resolution bumps them all before relying on any.
	inputs []int64
}

// resolveStateBefore is resolve_state_groups_for_events over the prev
// events, and resolve_state_groups' choice of a group for the result.
func (c *Creator) resolveStateBefore(ctx context.Context, roomID string, rv *pdu.RoomVersion, prevs []string) (*stateBefore, *stop) {
	seen := map[int64]bool{}
	var groups []int64
	for _, id := range prevs {
		g, err := c.o.Store.StateGroupForEvent(ctx, id)
		if err != nil {
			// An outlier, or unknown: Synapse raises.
			return nil, fb("no_state_group")
		}
		if !seen[g] {
			seen[g] = true
			groups = append(groups, g)
		}
	}
	if len(groups) == 1 {
		state, err := c.o.Store.StateForGroup(ctx, groups[0])
		if err != nil {
			return nil, fb("db_error")
		}
		prev, hasPrev, delta, err := c.o.Store.StateDelta(ctx, groups[0])
		if err != nil {
			return nil, fb("db_error")
		}
		return &stateBefore{state: state, group: groups[0], prev: prev, delta: delta, hasPrev: hasPrev}, nil
	}

	// Several groups: resolve. The order of the sets does not change the
	// result; the order the groups are tried in below is a Python set's in
	// Synapse, random per process, so any is Synapse's.
	slices.Sort(groups)
	sets := make([]pdu.StateMap, len(groups))
	for i, g := range groups {
		s, err := c.o.Store.StateForGroup(ctx, g)
		if err != nil {
			return nil, fb("db_error")
		}
		sets[i] = s
	}
	resolved, err := stateres.Resolve(ctx, roomID, rv, sets, c.o.Store)
	if err != nil {
		var raise *pdu.RaiseError
		if errors.As(err, &raise) {
			// Synapse raises too: its 500.
			return nil, fb("state_res_raises")
		}
		c.o.Log.Warn().Err(err).Str("room_id", roomID).Msg("Could not resolve the state before a new event")
		return nil, fb("state_res")
	}
	sb := makeStateEntry(resolved, groups, sets)
	sb.inputs = groups
	return sb, nil
}

// makeStateEntry is _make_state_cache_entry: an input group holding exactly
// the resolved events is reused; otherwise the new state is the input group
// it differs least from plus a delta, or whole when every group has a key
// the result lacks.
func makeStateEntry(resolved pdu.StateMap, groups []int64, sets []pdu.StateMap) *stateBefore {
	ids := map[string]bool{}
	for _, id := range resolved {
		ids[id] = true
	}
	for i, s := range sets {
		if len(ids) != len(s) {
			continue
		}
		same := true
		for _, id := range s {
			if !ids[id] {
				same = false
				break
			}
		}
		if same {
			return &stateBefore{state: resolved, group: groups[i]}
		}
	}
	sb := &stateBefore{state: resolved}
	var best pdu.StateMap
	for i, old := range sets {
		removes := false
		for k := range old {
			if _, ok := resolved[k]; !ok {
				removes = true
				break
			}
		}
		if removes {
			// Deltas cannot remove keys.
			continue
		}
		d := pdu.StateMap{}
		for k, v := range resolved {
			if old[k] != v {
				d[k] = v
			}
		}
		// `if not delta_ids or len(n_delta_ids) < len(delta_ids)`.
		if len(best) == 0 || len(d) < len(best) {
			sb.prev, sb.hasPrev, best = groups[i], true, d
		}
	}
	sb.delta = best
	return sb
}

// stateRows is a state map as rows to store.
func stateRows(m pdu.StateMap) []writer.StateRow {
	rows := make([]writer.StateRow, 0, len(m))
	for k, v := range m {
		rows = append(rows, writer.StateRow{Type: k.Type, StateKey: k.StateKey, EventID: v})
	}
	return rows
}
