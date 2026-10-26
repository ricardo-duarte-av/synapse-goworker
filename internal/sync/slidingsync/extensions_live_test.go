package slidingsync

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

// The extensions, compared against a real Synapse sync worker.
//
// Driven through the WORKER rather than through Build, because three of the
// seven depend on per-connection state and two more on what the room section
// contains -- so the thing worth comparing is a whole response, twice, with the
// position fed back.
//
//	GOSYNC_LIVE_SS_SOCKET=/tmp/claude-1001/gs-ss.sock \
//	GOSYNC_LIVE_REF_SOCKET=/var/sockets/nginx/av-sync-worker-2.sock \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token-2 \
//	  go test ./internal/slidingsync/ -run LiveExtensions -v
//
// NOT read-only: `to_device` deletes on whichever side is asked for it, so only
// OUR side is asked. Point it at a test account.

func TestLiveExtensionsParity(t *testing.T) {
	ours := os.Getenv("GOSYNC_LIVE_SS_SOCKET")
	if ours == "" {
		t.Skip("GOSYNC_LIVE_SS_SOCKET not set; needs a running worker with sliding_sync enabled")
	}
	c, token := refClient(t)

	body := func(connID string) map[string]any {
		return map[string]any{
			"conn_id": connID,
			"lists": map[string]any{"all": map[string]any{
				"ranges": [][2]int{{0, 4}}, "required_state": [][2]string{{"m.room.name", ""}},
				"timeline_limit": 3,
			}},
			"extensions": map[string]any{
				"e2ee":         map[string]any{"enabled": true},
				"account_data": map[string]any{"enabled": true},
				"receipts":     map[string]any{"enabled": true},
				"typing":       map[string]any{"enabled": true},
				"io.element.msc4308.thread_subscriptions": map[string]any{"enabled": true, "limit": 20},
				"org.matrix.msc4354.sticky_events":        map[string]any{"enabled": true, "limit": 20},
			},
		}
	}

	var oursPos, refPos string
	for round := 1; round <= 2; round++ {
		o := postSliding(t, c, ours, token, body("ext-test-ours"), oursPos)
		r := postSliding(t, c, os.Getenv("GOSYNC_LIVE_REF_SOCKET"), token, body("ext-test-ref"), refPos)
		oursPos, refPos = o.Pos, r.Pos

		t.Run(roundName(round), func(t *testing.T) {
			for _, key := range extensionKeys(o.Extensions, r.Extensions) {
				a, aOK := o.Extensions[key]
				b, bOK := r.Extensions[key]
				switch {
				case !aOK && !bOK:
				case !aOK:
					t.Errorf("%s: absent from ours, present in the reference", key)
				case !bOK:
					t.Errorf("%s: present in ours, absent from the reference", key)
				default:
					compareExtension(t, key, a, b, round)
				}
			}
		})
	}
}

type slidingResponse struct {
	Pos        string                     `json:"pos"`
	Extensions map[string]json.RawMessage `json:"extensions"`
}

func postSliding(t *testing.T, c interface{}, sock, token string,
	body map[string]any, pos string) slidingResponse {
	t.Helper()
	raw := refRawTo(t, sock, token, body, pos)
	var parsed slidingResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("%s: %v", sock, err)
	}
	if parsed.Pos == "" {
		t.Fatalf("%s returned no pos: %s", sock, string(raw)[:200])
	}
	return parsed
}

func roundName(n int) string {
	if n == 1 {
		return "initial"
	}
	return "incremental"
}

func extensionKeys(a, b map[string]json.RawMessage) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func compareExtension(t *testing.T, key string, ours, ref json.RawMessage, round int) {
	t.Helper()

	switch key {
	case "account_data":
		// The `global` list has no defined order: Synapse builds it by
		// iterating a dict, and ours sorts. Compared as a set; see
		// comparability.md source 12.
		compareAccountData(t, ours, ref)
		return

	case "typing":
		// Which rooms appear depends on the typing STREAM, which lives only in
		// memory: a worker that started a minute ago has not seen the clears
		// that Synapse's typing worker remembers from hours back. So an
		// initial request legitimately reports fewer rooms here until the view
		// fills in -- the same warm-up gap classic sync has, in a new place.
		// What must never differ is a room BOTH report.
		compareTyping(t, ours, ref, round)
		return

	case "org.matrix.msc4354.sticky_events":
		// `age` and the remaining sticky lifetime are recomputed per request on
		// each side (comparability.md source 11).
		if a, b := stripClock(t, ours), stripClock(t, ref); a != b {
			t.Errorf("%s differs:\n  ours %s\n  ref  %s", key, a, b)
		}
		return
	}

	if key == "receipts" {
		// Room-scoped, so the same scope caveat applies.
		type section struct {
			Rooms map[string]json.RawMessage `json:"rooms"`
		}
		var a, b section
		if err := json.Unmarshal(ours, &a); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(ref, &b); err != nil {
			t.Fatal(err)
		}
		compareRoomScoped(t, "receipts.rooms", a.Rooms, b.Rooms, func(roomID string) bool {
			return canonical(t, a.Rooms[roomID]) == canonical(t, b.Rooms[roomID])
		})
		return
	}

	if a, b := canonical(t, ours), canonical(t, ref); a != b {
		t.Errorf("%s differs:\n  ours %s\n  ref  %s", key, a, b)
	}
}

func compareAccountData(t *testing.T, ours, ref json.RawMessage) {
	t.Helper()
	type section struct {
		Global []json.RawMessage            `json:"global"`
		Rooms  map[string][]json.RawMessage `json:"rooms"`
	}
	var a, b section
	if err := json.Unmarshal(ours, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(ref, &b); err != nil {
		t.Fatal(err)
	}

	as, bs := canonicalSet(t, a.Global), canonicalSet(t, b.Global)
	if !reflect.DeepEqual(as, bs) {
		t.Errorf("account_data.global differs as a SET (not merely in order):\n"+
			"  ours-only %v\n  ref-only  %v", missing(as, bs), missing(bs, as))
	}

	compareRoomScoped(t, "account_data.rooms", toRaw(a.Rooms), toRaw(b.Rooms),
		func(roomID string) bool {
			return reflect.DeepEqual(canonicalSet(t, a.Rooms[roomID]), canonicalSet(t, b.Rooms[roomID]))
		})
}

// compareRoomScoped compares a room-keyed extension section over the rooms BOTH
// sides described, and reports a differing scope separately.
//
// The scope is not comparable on a busy account: a room-scoped extension covers
// whatever is in the response's window, and a list ordered by activity
// reorders between two requests as soon as anything happens. On the 654-room
// account the two sides described five rooms each and disagreed on two of them
// -- comparability source 1, arriving through a new door.
//
// The rooms both sides DID describe are still compared in full, so on a quiet
// account the test keeps all of its strength.
func compareRoomScoped(t *testing.T, what string, ours, ref map[string]json.RawMessage,
	equal func(roomID string) bool) {
	t.Helper()

	var oursOnly, refOnly int
	for roomID := range ours {
		if _, both := ref[roomID]; !both {
			oursOnly++
			continue
		}
		if !equal(roomID) {
			t.Errorf("%s[%s] differs", what, roomID)
		}
	}
	for roomID := range ref {
		if _, both := ours[roomID]; !both {
			refOnly++
		}
	}
	if oursOnly > 0 || refOnly > 0 {
		t.Logf("%s: scopes differ (%d ours-only, %d reference-only) -- the window is "+
			"ordered by activity and reordered between the two requests", what, oursOnly, refOnly)
	}
}

func compareTyping(t *testing.T, ours, ref json.RawMessage, round int) {
	t.Helper()
	type section struct {
		Rooms map[string]json.RawMessage `json:"rooms"`
	}
	var a, b section
	if err := json.Unmarshal(ours, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(ref, &b); err != nil {
		t.Fatal(err)
	}
	for roomID, av := range a.Rooms {
		bv, ok := b.Rooms[roomID]
		if !ok {
			// Ours reporting a room the reference does not is the direction
			// that cannot be explained by warm-up, so it stays a failure.
			t.Errorf("typing.rooms[%s]: we report it, the reference does not", roomID)
			continue
		}
		if canonical(t, av) != canonical(t, bv) {
			t.Errorf("typing.rooms[%s] differs:\n  ours %s\n  ref  %s",
				roomID, canonical(t, av), canonical(t, bv))
		}
	}
	if extra := len(b.Rooms) - len(a.Rooms); extra > 0 {
		t.Logf("reference reports %d typing room(s) we do not: the replication "+
			"typing view has only been warm since this worker started", extra)
	}
}

func toRaw(m map[string][]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(m))
	for k := range m {
		out[k] = nil
	}
	return out
}

func canonicalSet(t *testing.T, xs []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, canonical(t, x))
	}
	sort.Strings(out)
	return out
}

func missing(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	return out
}

// canonical re-encodes with sorted keys so two equal objects compare equal.
func canonical(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func stripClock(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	v := decode(t, raw)
	dropClockDerived(v)
	return encodeJSON(t, v)
}

// TestLiveE2EEWithoutLists compares `e2ee.device_lists` on a connection that
// has NO lists, over a window rewound far enough to contain device changes.
//
// This is how Element X asks for it: a dedicated encryption connection with
// only `to_device` and `e2ee`. The parity test above cannot see the shape --
// its list covers every room of a small test account, and its two rounds are
// seconds apart -- and that is how a device-list section scoped to the lists'
// rooms reached production. The Element X connection was told about the
// caller's own devices and nobody else's.
//
// Sets, not lists: neither side orders `changed` or `left`. Read-only on
// Synapse's side; no `to_device`, so nothing is deleted.
//
// Exact on @test. On the 654-room account expect a few differences in
// `changed`, all of one shape: a bridged user who joined one shared room and
// joined-then-left another inside the window. Synapse's answer for each is
// only reachable if it skipped one of the two rooms' member deltas -- which
// its stream-change cache, not the database, decides. Ours follows the
// database. Either way the cost is one user's keys re-fetched or not.
func TestLiveE2EEWithoutLists(t *testing.T) {
	ours := os.Getenv("GOSYNC_LIVE_SS_SOCKET")
	if ours == "" {
		t.Skip("GOSYNC_LIVE_SS_SOCKET not set; needs a running worker with sliding_sync enabled")
	}
	c, token := refClient(t)
	ref := os.Getenv("GOSYNC_LIVE_REF_SOCKET")

	body := func(connID string) map[string]any {
		return map[string]any{
			"conn_id":    connID,
			"extensions": map[string]any{"e2ee": map[string]any{"enabled": true}},
		}
	}

	// A current position from the reference, then wound back. Connection
	// position 0 is "no connection state", which both sides accept with any
	// stream token -- exactly what Element X sends on its encryption connection.
	now := postSliding(t, c, ref, token, body("e2ee-nolists-seed"), "")
	pos, err := ParsePos(now.Pos)
	if err != nil {
		t.Fatal(err)
	}
	from := pos.StreamToken
	from.Room.Stream -= 5000
	from.Room.Instances = nil
	from.DeviceList.Stream -= 20000
	from.DeviceList.Instances = nil
	rewound := Pos{StreamToken: from}.String()

	deviceLists := func(r slidingResponse) (changed, left map[string]bool) {
		var e struct {
			DeviceLists *struct {
				Changed []string `json:"changed"`
				Left    []string `json:"left"`
			} `json:"device_lists"`
		}
		if err := json.Unmarshal(r.Extensions["e2ee"], &e); err != nil {
			t.Fatal(err)
		}
		if e.DeviceLists == nil {
			t.Fatal("e2ee.device_lists absent on a request with a pos")
		}
		changed, left = map[string]bool{}, map[string]bool{}
		for _, u := range e.DeviceLists.Changed {
			changed[u] = true
		}
		for _, u := range e.DeviceLists.Left {
			left[u] = true
		}
		return changed, left
	}

	oc, ol := deviceLists(postSliding(t, c, ours, token, body("e2ee-nolists-ours"), rewound))
	rc, rl := deviceLists(postSliding(t, c, ref, token, body("e2ee-nolists-ref"), rewound))

	// A window with nothing in it proves nothing; say so rather than pass.
	if len(rc) == 0 {
		t.Skip("reference reports no device-list changes in the rewound window; widen it")
	}
	for name, pair := range map[string][2]map[string]bool{"changed": {oc, rc}, "left": {ol, rl}} {
		o, r := pair[0], pair[1]
		for u := range r {
			if !o[u] {
				t.Errorf("device_lists.%s: %s missing from ours (%d ours, %d reference)",
					name, u, len(o), len(r))
			}
		}
		for u := range o {
			if !r[u] {
				t.Errorf("device_lists.%s: %s present only in ours", name, u)
			}
		}
	}
}
