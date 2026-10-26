package handlers

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// roomSnapshot is the messages/state pair both initialSync endpoints build.
type roomSnapshot struct {
	Chunk []json.RawMessage
	Start streamtoken.RoomKey
	State []json.RawMessage
}

// buildRoomSnapshot renders a joined room's recent timeline and current state.
//
// Shared by /rooms/{roomId}/initialSync and /initialSync, which differ in what
// they wrap around this but agree exactly on what goes inside it.
func buildRoomSnapshot(ctx context.Context, d Deps, room store.RoomForUser,
	userID string, end streamtoken.RoomKey, limit int, timeNow int64,
	requester clientevent.Requester) (roomSnapshot, error) {

	var snap roomSnapshot

	state, err := d.Store.CurrentState(ctx, room.RoomID, room.RoomVersion)
	if err != nil {
		return snap, err
	}
	messages, start, err := d.Store.RecentEvents(ctx, room.RoomID, room.RoomVersion, limit, end)
	if err != nil {
		return snap, err
	}
	snap.Start = start

	// Visibility before serialisation: nothing that fails this check should be
	// rendered at all, let alone written to a response buffer.
	//
	// is_peeking is false: callers only reach here for a room the user is a
	// member of.
	messages, memberships, err := filterVisible(ctx, d, room.RoomID, userID, messages, false, timeNow)
	if err != nil {
		return snap, err
	}

	// Timeline only: see the note in RoomInitialSync on Synapse's shared event
	// cache leaking prev_content into state reads.
	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return snap, err
	}

	cfg := clientevent.Config{Format: clientevent.FormatV1, Requester: requester,
		MSC4354Enabled: d.MSC4354Enabled}

	// Redaction is applied on read: a redacted event keeps its original body in
	// storage until a background job censors it, so serving stored JSON
	// unchanged publishes what the redaction removed.
	//
	// State events too, not just the timeline. Synapse redacts in the storage
	// layer, so every event it hands out is already pruned -- a redacted
	// m.room.topic in the current state is just as redacted as a message.
	ids := timelineEventIDs(messages)
	for _, ev := range state {
		ids = append(ids, ev.EventID)
	}
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return snap, err
	}

	snap.State = make([]json.RawMessage, 0, len(state))
	for _, ev := range state {
		// Synapse wraps state with FilteredEvent.state(), whose membership is
		// None, so the state block carries no unsigned.membership.
		stored := ev.Stored
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return snap, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return snap, err
		}
		snap.State = append(snap.State, body)
	}

	snap.Chunk = make([]json.RawMessage, 0, len(messages))
	for i, ev := range messages {
		stored := ev.Stored
		stored.Membership = memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return snap, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return snap, err
		}
		snap.Chunk = append(snap.Chunk, body)
	}
	return snap, nil
}

func timelineEventIDs(messages []store.TimelineEvent) []string {
	out := make([]string, len(messages))
	for i, ev := range messages {
		out[i] = ev.EventID
	}
	return out
}

// accountDataEvents renders account data entries as client events.
func accountDataEvents(entries []store.AccountDataEntry) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(entries))
	for _, e := range entries {
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, nil
}

// presenceEvents renders m.presence events.
//
// Mirrors format_user_presence_state: last_active_ago and status_msg appear
// only when set, currently_active only when the user is online.
func presenceEvents(states []store.PresenceState, timeNow int64) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(states))
	for _, p := range states {
		content := map[string]any{"presence": p.State, "user_id": p.UserID}
		// Synapse guards these with plain Python truthiness -- `if
		// state.last_active_ts:` -- so a stored 0 is omitted just like a NULL,
		// and an empty status message is omitted just like a missing one.
		// Treating NULL as the only absence emits `last_active_ago` equal to
		// the whole epoch.
		if p.HasLastActive && p.LastActiveTS != 0 {
			content["last_active_ago"] = timeNow - p.LastActiveTS
		}
		if p.HasStatusMsg && p.StatusMsg != "" {
			content["status_msg"] = p.StatusMsg
		}
		if p.State == "online" {
			content["currently_active"] = p.CurrentlyActive
		}
		body, err := json.Marshal(map[string]any{"type": "m.presence", "content": content})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, nil
}

// typingEvent renders the m.typing EDU for a room, or nil when nobody is.
//
// This is the one part of a sync response that exists ONLY in memory. Typing is
// never written down: Synapse keeps it in a counter on the typing worker, and
// it reaches everyone else over the replication stream. Before this worker
// followed that stream it could not produce the section at all -- syncdiff
// counted it as a known gap.
//
// Nothing is reported while the subscription is unhealthy: a stale typist list
// would leave a room showing somebody typing forever, which is worse than
// showing nobody.
func typingEvent(d Deps, roomID string) (json.RawMessage, error) {
	if d.Replication == nil || !d.Replication.Live() {
		return nil, nil
	}
	// An EMPTY list is an event, not an absence. Synapse's _make_event_for
	// builds `{"user_ids": []}` for any room whose typing serial moved, and
	// that empty event is the only thing that tells a client somebody STOPPED
	// typing. Returning nil instead leaves the indicator on the screen for
	// ever -- there is no timeout on the client side, and no later sync will
	// mention the room again until somebody types in it once more.
	//
	// The caller decides which rooms to ask about; this only renders.
	users := d.Replication.TypingIn(roomID)
	if users == nil {
		users = []string{}
	}
	return json.Marshal(map[string]any{
		"type":    "m.typing",
		"content": map[string]any{"user_ids": users},
	})
}
