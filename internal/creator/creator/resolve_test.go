package creator

import (
	"net/http"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// A message on a forked DAG: the state is resolved across both prev
// events' groups, and stored as a new group on the closer one.
func TestMessageOnAFork(t *testing.T) {
	h := newHarness(t)
	ida, idb := h.f.fork(t, [3]string{"m.room.topic", "", `{"topic":"a"}`}, [3]string{"m.room.name", "", `{"name":"b"}`})
	rec, out := h.send(t, http.MethodPut, "f1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,state_group,participation,stage" {
		t.Errorf("writes %s", got)
	}
	if len(h.w.bumped) != 2 || h.w.bumped[0] != 8 || h.w.bumped[1] != 9 {
		t.Errorf("bumped %v, want the inputs", h.w.bumped)
	}
	// Both groups differ from the result by one event; the first is the
	// base, and the delta is the other side's event.
	if h.w.groupPrev != 8 || len(h.w.groupDelta) != 1 || h.w.groupDelta[0].EventID != idb {
		t.Errorf("new group on %d + %v", h.w.groupPrev, h.w.groupDelta)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.prev_events":                `["` + ida + `","` + idb + `"]`,
		"context.state_group":              "8",
		"context.state_group_before_event": "8",
		"context.state_group_deltas":       `[[8,8,[]],[8,8,[["m.room.name","","` + idb + `"]]]]`,
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if d := e.Get("event.depth").Int(); d != h.f.events[ida].Depth+1 {
		t.Errorf("depth %d", d)
	}
}

// A state event whose prevs resolve to exactly one of their groups reuses
// it, and the context has no edge for it.
func TestStateEventOnAForkThatResolvesToAGroup(t *testing.T) {
	h := newHarness(t)
	// Two topics at the same depth: the resolution keeps one, which is
	// then exactly that side's group.
	ida, idb := h.f.fork(t, [3]string{"m.room.topic", "", `{"topic":"a"}`}, [3]string{"m.room.topic", "", `{"topic":"b"}`})
	rec, out := h.put(t, stateReq("m.room.name", ""), "/state/m.room.name/", `{"name":"n"}`)
	if out.Result != "created" || rec.Code != 200 {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	before := e.Get("context.state_group_before_event").Int()
	if before != 8 && before != 9 {
		t.Fatalf("before %d, want a reused input group", before)
	}
	if got := strings.Join(h.w.calls, ","); got != "bump,state_group,stage" {
		t.Errorf("writes %s", got)
	}
	if n := e.Get("context.state_group_deltas.#").Int(); n != 1 {
		t.Errorf("%d deltas for a reused group: %s", n, e.Get("context.state_group_deltas").Raw)
	}
	if got := e.Get("event.unsigned.replaces_state").Str; got != "" {
		t.Errorf("replaces_state %q for a new key", got)
	}
	// The topic in the event's auth? No: but the new group is a delta of
	// the name on the reused group.
	if h.w.groupPrev != before || len(h.w.groupDelta) != 1 || h.w.groupDelta[0].Type != "m.room.name" {
		t.Errorf("after group on %d + %v", h.w.groupPrev, h.w.groupDelta)
	}
	_, _ = ida, idb
}

func TestMakeStateEntry(t *testing.T) {
	k := func(t string) pdu.StateKey { return pdu.StateKey{Type: t} }
	resolved := pdu.StateMap{k("a"): "$1", k("b"): "$2", k("c"): "$3"}
	for name, tc := range map[string]struct {
		sets        []pdu.StateMap
		group, prev int64
		delta       int
		hasPrev     bool
	}{
		"exact":    {[]pdu.StateMap{{k("a"): "$1"}, {k("a"): "$1", k("b"): "$2", k("c"): "$3"}}, 2, 0, 0, false},
		"closest":  {[]pdu.StateMap{{k("a"): "$1"}, {k("a"): "$1", k("b"): "$2"}}, 0, 2, 1, true},
		"tie":      {[]pdu.StateMap{{k("a"): "$1", k("b"): "$x"}, {k("a"): "$1", k("c"): "$x"}}, 0, 1, 2, true},
		"removes":  {[]pdu.StateMap{{k("a"): "$1", k("z"): "$9"}, {k("a"): "$1", k("b"): "$2", k("d"): "$4"}}, 0, 0, 0, false},
		"one base": {[]pdu.StateMap{{k("a"): "$1", k("z"): "$9"}, {k("b"): "$2"}}, 0, 2, 2, true},
	} {
		sb := makeStateEntry(resolved, []int64{1, 2}, tc.sets)
		if sb.group != tc.group || sb.prev != tc.prev || len(sb.delta) != tc.delta || sb.hasPrev != tc.hasPrev {
			t.Errorf("%s: %+v", name, sb)
		}
	}
}
