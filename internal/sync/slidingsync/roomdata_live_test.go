package slidingsync

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/slidingstore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// Per-room results, compared field by field against a real Synapse sync worker.
//
// Only an INITIAL request is compared. An incremental one is a delta against
// per-connection state, and the two sides keep that state in different tables
// with different positions, so there is no way to put them in the same place
// without driving both through the same sequence of requests -- which is
// syncdiff's job (M12, still to come), not a unit test's.
//
//	GOSYNC_TEST_DSN=... GOSYNC_LIVE_REF_SOCKET=... \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token \
//	GOSYNC_PARITY_USER=@goworker:aguiarvieira.pt \
//	  go test ./internal/slidingsync/ -run LiveRoomData -v

type refRoom struct {
	Name          *string           `json:"name"`
	Avatar        *string           `json:"avatar"`
	Initial       bool              `json:"initial"`
	IsDM          bool              `json:"is_dm"`
	RequiredState []json.RawMessage `json:"required_state"`
	Timeline      []json.RawMessage `json:"timeline"`
	Limited       *bool             `json:"limited"`
	BumpStamp     *int64            `json:"bump_stamp"`
	JoinedCount   *int              `json:"joined_count"`
	InvitedCount  *int              `json:"invited_count"`
	NumLive       *int              `json:"num_live"`
	Heroes        []struct {
		UserID      string  `json:"user_id"`
		DisplayName *string `json:"displayname"`
	} `json:"heroes"`
	NotificationCount int `json:"notification_count"`
	HighlightCount    int `json:"highlight_count"`
}

func refRooms(t *testing.T, body map[string]any) map[string]refRoom {
	t.Helper()
	c, token := refClient(t)
	body["conn_id"] = "gosync-roomdata-" + t.Name()
	raw := refRaw(t, c, token, body)
	var parsed struct {
		Rooms map[string]refRoom `json:"rooms"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed.Rooms
}

func stateKeysOf(events []json.RawMessage) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, gjson.GetBytes(e, "type").String()+"/"+gjson.GetBytes(e, "state_key").String())
	}
	sort.Strings(out)
	return out
}

func eventIDsOf(events []json.RawMessage) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, gjson.GetBytes(e, "event_id").String())
	}
	return out
}

func TestLiveRoomDataParity(t *testing.T) {
	d, _, now, ctx := liveDeps(t)
	nowMS := time.Now().UnixMilli()
	userID, deviceID := refWhoami(t)
	if want := os.Getenv("GOSYNC_PARITY_USER"); want != "" && want != userID {
		t.Fatalf("the token belongs to %s, not %s", userID, want)
	}

	const timelineLimit = 5
	required := [][2]string{
		{"m.room.name", ""},
		{"m.room.topic", ""},
		{"m.room.encryption", ""},
	}

	// A wide window on purpose. With five rooms the sample was every room the
	// test account has at `shared` history visibility, and a build that threw
	// the visibility filter's results away compared clean -- verified by
	// mutation, 2026-09-03. The rooms that can tell the difference are the ones
	// further down the list.
	//
	// Order is irrelevant here: rooms are matched by ID, not by position, so the
	// full-range sort skip (comparability.md, source 10) does not apply.
	body := map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges":         [][2]int{{0, 49}},
			"required_state": required,
			"timeline_limit": timelineLimit,
		}},
	}
	theirs := refRooms(t, body)
	if len(theirs) == 0 {
		t.Skip("reference returned no rooms")
	}

	req := &Request{Lists: map[string]List{"all": {
		CommonRoomParameters: CommonRoomParameters{
			RequiredState: required, TimelineLimit: timelineLimit,
		},
		Ranges: [][2]int{{0, 49}},
	}}}
	lists, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := d.Store.SlidingJoinedRooms(ctx, lists.Lists["all"].Ops[0].RoomIDs)
	if err != nil {
		t.Fatal(err)
	}

	compared, tolerated := 0, 0
	for _, roomID := range lists.Lists["all"].Ops[0].RoomIDs {
		theirRoom, ok := theirs[roomID]
		if !ok {
			t.Errorf("%s is in our window but not in the reference response", roomID)
			continue
		}
		m, hasMeta := meta[roomID]
		newState := &slidingstore.PerConnectionState{}

		ours, err := GetRoomData(ctx, d, RoomDataRequest{
			UserID:   userID,
			RoomID:   roomID,
			Config:   lists.Relevant[roomID],
			Room:     lists.Membership[roomID],
			Meta:     m,
			HasMeta:  hasMeta,
			From:     nil,
			To:       now,
			NowMS:    nowMS,
			DeviceID: deviceID,
			Previous: &slidingstore.PerConnectionState{},
			New:      newState,
			IsDM:     lists.DMRooms[roomID],
		})
		if err != nil {
			t.Fatalf("%s: %v", roomID, err)
		}
		compared++

		t.Run(roomID, func(t *testing.T) {
			if !ours.Initial || !theirRoom.Initial {
				t.Errorf("initial = %v, reference %v; both should be true for a first request",
					ours.Initial, theirRoom.Initial)
			}
			comparePtrString(t, "name", ours.Name, theirRoom.Name)
			comparePtrString(t, "avatar", ours.Avatar, theirRoom.Avatar)
			comparePtrInt(t, "joined_count", ours.JoinedCount, theirRoom.JoinedCount)
			comparePtrInt(t, "invited_count", ours.InvitedCount, theirRoom.InvitedCount)
			if ours.IsDM != theirRoom.IsDM {
				t.Errorf("is_dm = %v, reference %v", ours.IsDM, theirRoom.IsDM)
			}
			if ours.NotificationCount != theirRoom.NotificationCount ||
				ours.HighlightCount != theirRoom.HighlightCount {
				t.Errorf("counts = %d/%d, reference %d/%d (both are dummy zeros upstream)",
					ours.NotificationCount, ours.HighlightCount,
					theirRoom.NotificationCount, theirRoom.HighlightCount)
			}

			// required_state is compared by (type, state_key): the events
			// themselves are byte-identical stored JSON on both sides, and what
			// is being tested here is the SELECTION.
			gotKeys, wantKeys := stateKeysOf(ours.RequiredState), stateKeysOf(theirRoom.RequiredState)
			if !equalStrings(gotKeys, wantKeys) {
				t.Errorf("required_state = %v, reference %v", gotKeys, wantKeys)
			}

			// Timeline compared by event ID and order. A message landing
			// between the two requests would show up here as a genuine
			// difference, which is why the test account is a quiet one.
			gotIDs, wantIDs := eventIDsOf(ours.Timeline), eventIDsOf(theirRoom.Timeline)
			if !equalStrings(gotIDs, wantIDs) {
				t.Errorf("timeline = %v, reference %v", gotIDs, wantIDs)
			} else {
				// And the bodies. `unsigned` is dropped from both sides for
				// now: we do not yet populate `membership` (MSC4115) or
				// `transaction_id`, because both come from the visibility
				// filter and the requester's device, and neither is wired into
				// this path yet. Recorded in docs/milestones.md; this
				// comparison tightens to include `unsigned` when they are.
				for i := range gotIDs {
					g, w, leaked := normalise(t, ours.Timeline[i], theirRoom.Timeline[i])
					tolerated += leaked
					if g != w {
						t.Errorf("timeline event %s differs:\n  ours %s\n  ref  %s", gotIDs[i], g, w)
					}
				}
			}

			// Same for the state events.
			if equalStrings(gotKeys, wantKeys) {
				oursByKey := byStateKey(ours.RequiredState)
				theirsByKey := byStateKey(theirRoom.RequiredState)
				for k, g := range oursByKey {
					w, ok := theirsByKey[k]
					if !ok {
						continue
					}
					gs, ws, leaked := normalise(t, g, w)
					tolerated += leaked
					if gs != ws {
						t.Errorf("required_state %s differs:\n  ours %s\n  ref  %s", k, gs, ws)
					}
				}
			}

			if (ours.Limited == nil) != (theirRoom.Limited == nil) {
				t.Errorf("limited presence differs: %v vs %v", ours.Limited, theirRoom.Limited)
			} else if ours.Limited != nil && *ours.Limited != *theirRoom.Limited {
				t.Errorf("limited = %v, reference %v", *ours.Limited, *theirRoom.Limited)
			}

			comparePtrInt64(t, "bump_stamp", ours.BumpStamp, theirRoom.BumpStamp)

			var gotHeroes, wantHeroes []string
			for _, h := range ours.Heroes {
				gotHeroes = append(gotHeroes, h.UserID)
			}
			for _, h := range theirRoom.Heroes {
				wantHeroes = append(wantHeroes, h.UserID)
			}
			if !equalStrings(gotHeroes, wantHeroes) {
				t.Errorf("heroes = %v, reference %v", gotHeroes, wantHeroes)
			}
		})

		// The room config must have been recorded, or the next response repeats
		// everything.
		if _, ok := newState.RoomConfigs[roomID]; !ok {
			t.Errorf("%s: no room config recorded for the connection", roomID)
		}
	}
	t.Logf("compared %d rooms, tolerated %d prev_content/prev_sender cache leaks (either side)",
		compared, tolerated)
}

// Lazy-loaded members are what a real client asks for, and the selection
// differs from the explicit case in a way worth checking against the reference.
func TestLiveRoomDataParityWithLazyMembers(t *testing.T) {
	d, _, now, ctx := liveDeps(t)
	nowMS := time.Now().UnixMilli()
	userID := parityUser(t)

	required := [][2]string{{"m.room.member", "$LAZY"}, {"m.room.name", ""}}
	body := map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges": [][2]int{{0, 49}}, "required_state": required, "timeline_limit": 5,
		}},
	}
	theirs := refRooms(t, body)
	if len(theirs) == 0 {
		t.Skip("reference returned no rooms")
	}

	req := &Request{Lists: map[string]List{"all": {
		CommonRoomParameters: CommonRoomParameters{RequiredState: required, TimelineLimit: 5},
		Ranges:               [][2]int{{0, 49}},
	}}}
	lists, err := ComputeRoomLists(ctx, d, userID, req, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := d.Store.SlidingJoinedRooms(ctx, lists.Lists["all"].Ops[0].RoomIDs)
	if err != nil {
		t.Fatal(err)
	}

	for _, roomID := range lists.Lists["all"].Ops[0].RoomIDs {
		theirRoom, ok := theirs[roomID]
		if !ok {
			continue
		}
		m, hasMeta := meta[roomID]
		newState := &slidingstore.PerConnectionState{}
		ours, err := GetRoomData(ctx, d, RoomDataRequest{
			UserID: userID, RoomID: roomID,
			Config: lists.Relevant[roomID], Room: lists.Membership[roomID],
			Meta: m, HasMeta: hasMeta, To: now, NowMS: nowMS,
			Previous: &slidingstore.PerConnectionState{}, New: newState,
			IsDM: lists.DMRooms[roomID],
		})
		if err != nil {
			t.Fatalf("%s: %v", roomID, err)
		}

		got, want := stateKeysOf(ours.RequiredState), stateKeysOf(theirRoom.RequiredState)
		if !equalStrings(got, want) {
			t.Errorf("%s: lazy required_state = %v, reference %v", roomID, got, want)
		}

		// Everyone in the timeline must have their membership included, or the
		// client cannot put a name to a message.
		senders := map[string]bool{}
		for _, e := range ours.Timeline {
			senders[gjson.GetBytes(e, "sender").String()] = true
		}
		included := map[string]bool{}
		for _, e := range ours.RequiredState {
			if gjson.GetBytes(e, "type").String() == "m.room.member" {
				included[gjson.GetBytes(e, "state_key").String()] = true
			}
		}
		for sender := range senders {
			if !included[sender] {
				t.Errorf("%s: %s sent a timeline event but their membership was not included",
					roomID, sender)
			}
		}
		if newState.LazyMembership[roomID] == nil && len(senders) > 0 {
			t.Errorf("%s: lazy membership was not recorded for the connection", roomID)
		}
	}
}

func comparePtrString(t *testing.T, field string, got, want *string) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s = %v, reference %v", field, deref(got), deref(want))
	case *got != *want:
		t.Errorf("%s = %q, reference %q", field, *got, *want)
	}
}

func comparePtrInt(t *testing.T, field string, got, want *int) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s presence differs: %v vs %v", field, got, want)
	case *got != *want:
		t.Errorf("%s = %d, reference %d", field, *got, *want)
	}
}

func comparePtrInt64(t *testing.T, field string, got, want *int64) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s presence differs: %v vs %v", field, got, want)
	case *got != *want:
		t.Errorf("%s = %d, reference %d", field, *got, *want)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<absent>"
	}
	return *s
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ = store.StateKey{}

// normalise prepares two events for comparison and reports how many tolerated
// differences it removed.
//
// Two things are dropped, and only two:
//
//   - `unsigned.age`, from both sides. It is recomputed from the wall clock on
//     each side and can never match (comparability.md, source 1).
//   - `unsigned.prev_content` and `prev_sender`, from BOTH sides. Synapse
//     writes these into its shared cached event -- its own comment says "This
//     mutates the cached event, but that's fine" -- so whether they appear
//     depends on whether some other request happened to load that event first.
//
// Both sides, and that is a correction to what synapse-notes.md said. It
// recorded the leak as upstream-only, because that is the only direction
// classic sync can show: classic sync calls AttachPrevContent deliberately, so
// OUR side is never the surprising one there. Sliding sync does not, and over
// 30 rooms of the second account this produced 128 events where the reference
// had the fields and we did not, and ONE where we had them and it did not
// ($15131056291173STGyq:t2l.io, an m.room.topic). The non-determinism runs both
// ways; asserting one direction would make this test flake.
func normalise(t *testing.T, ours, ref json.RawMessage) (string, string, int) {
	t.Helper()
	a, b := decode(t, ours), decode(t, ref)
	dropClockDerived(a)
	dropClockDerived(b)

	leaked := dropPrevContent(a) + dropPrevContent(b)
	return encodeJSON(t, a), encodeJSON(t, b), leaked
}

func decode(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// clockDerived are the `unsigned` fields each side recomputes from its own wall
// clock, so they can never match: `age`, and MSC4354's remaining sticky
// lifetime. Measured drift between the two sides on this deployment: 85 ms and
// 86 ms respectively -- small, and unbounded in principle.
var clockDerived = []string{"age", "msc4354_sticky_duration_ttl_ms"}

// dropClockDerived removes those fields RECURSIVELY.
//
// Recursively because a bundle nests whole events: a thread's latest reply and
// a `redacted_because` each carry their own `unsigned`, computed from the same
// clock and differing by the same handful of milliseconds. Dropping only the
// outer one leaves three "differences" that are nothing of the kind -- which is
// exactly what the first version of this test reported.
func dropClockDerived(v map[string]any) {
	for key, val := range v {
		switch t := val.(type) {
		case map[string]any:
			if key == "unsigned" {
				for _, f := range clockDerived {
					delete(t, f)
				}
			}
			dropClockDerived(t)
			if key == "unsigned" && len(t) == 0 {
				delete(v, key)
			}
		case []any:
			for _, item := range t {
				if m, ok := item.(map[string]any); ok {
					dropClockDerived(m)
				}
			}
		}
	}
}

func encodeJSON(t *testing.T, v map[string]any) string {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func byStateKey(events []json.RawMessage) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, e := range events {
		k := gjson.GetBytes(e, "type").String() + "/" + gjson.GetBytes(e, "state_key").String()
		out[k] = e
	}
	return out
}

func dropPrevContent(v map[string]any) int {
	u, ok := v["unsigned"].(map[string]any)
	if !ok {
		return 0
	}
	n := 0
	for _, field := range []string{"prev_content", "prev_sender"} {
		if _, present := u[field]; present {
			delete(u, field)
			n++
		}
	}
	if len(u) == 0 {
		delete(v, "unsigned")
	}
	return n
}
