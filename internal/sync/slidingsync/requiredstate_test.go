package slidingsync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// testdata/required_state_changes.json is Synapse's OWN test table for
// `_required_state_changes`, extracted mechanically from
// tests/handlers/test_sliding_sync.py (class RequiredStateChangesTestCase).
//
// Copying the reference implementation's own tests rather than writing our own
// from a reading of the code is the point: this function has twenty-odd
// branches whose interactions are the whole difficulty, and the people who
// wrote it have already enumerated the cases that matter. Our tests would
// mostly encode our reading of the source, which is exactly what needs
// checking.
//
// Each case runs TWICE, as Synapse's does: once with the state deltas and once
// with none. That second run is not padding -- the entire "a key is forgotten
// only if the client removed it AND the state changed" rule is invisible
// without it, because with no deltas nothing is ever invalidated.
//
// Regenerate after a Synapse upgrade; the extractor is in the commit that added
// this file.

type conformanceCase struct {
	Name        string `json:"name"`
	Description string `json:"description"`

	Prev    map[string][]string `json:"prev"`
	Request map[string][]string `json:"request"`

	StateDeltas []struct {
		Type     string `json:"type"`
		StateKey string `json:"state_key"`
	} `json:"state_deltas"`

	PreviouslyReturnedLazy []string `json:"previously_returned_lazy"`
	RequestLazy            []string `json:"request_lazy"`

	WithDeltas    conformanceExpect `json:"with_deltas"`
	WithoutDeltas conformanceExpect `json:"without_deltas"`
}

type conformanceExpect struct {
	Changed map[string][]string `json:"changed"`
	Added   struct {
		Kind    string `json:"kind"`
		Entries []struct {
			Type     string  `json:"type"`
			StateKey *string `json:"state_key"`
		} `json:"entries"`
	} `json:"added"`
	ExtraLazy       []string `json:"extra_lazy"`
	InvalidatedLazy []string `json:"invalidated_lazy"`
}

func TestRequiredStateChangesConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "required_state_changes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []conformanceCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 30 {
		t.Fatalf("only %d cases loaded; the extraction is incomplete", len(cases))
	}

	const userID = "@user:test"
	seen := map[string]int{}
	for _, tc := range cases {
		// Synapse's table has a duplicated label; keep both and disambiguate.
		seen[tc.Name]++
		name := tc.Name
		if seen[tc.Name] > 1 {
			name = tc.Name + "#2"
		}

		t.Run(name, func(t *testing.T) {
			if tc.Description != "" {
				t.Log(tc.Description)
			}
			deltas := map[store.StateKey]bool{}
			for _, d := range tc.StateDeltas {
				deltas[store.StateKey{Type: d.Type, StateKey: d.StateKey}] = true
			}

			t.Run("without state deltas", func(t *testing.T) {
				got := ComputeRequiredStateChanges(userID,
					toSets(tc.Prev), toSets(tc.Request),
					toSet(tc.PreviouslyReturnedLazy), toSet(tc.RequestLazy),
					map[store.StateKey]bool{})
				checkConformance(t, got, tc.WithoutDeltas)
			})

			t.Run("with state deltas", func(t *testing.T) {
				got := ComputeRequiredStateChanges(userID,
					toSets(tc.Prev), toSets(tc.Request),
					toSet(tc.PreviouslyReturnedLazy), toSet(tc.RequestLazy),
					deltas)
				checkConformance(t, got, tc.WithDeltas)
			})
		})
	}
}

func checkConformance(t *testing.T, got RequiredStateChanges, want conformanceExpect) {
	t.Helper()

	// nil and empty are different: nil means "leave the stored config alone",
	// and storing an unchanged config forces a new connection position.
	if want.Changed == nil {
		if got.Changed != nil {
			t.Errorf("changed = %v, want nil (no change to store)", fromSets(got.Changed))
		}
	} else {
		if got.Changed == nil {
			t.Errorf("changed = nil, want %v", want.Changed)
		} else if diff := compareMaps(fromSets(got.Changed), want.Changed); diff != "" {
			t.Errorf("changed %s", diff)
		}
	}

	wantAll := want.Added.Kind == "all"
	wantNone := want.Added.Kind == "none"
	switch {
	case wantAll && !got.Added.IsAll():
		t.Errorf("added = %v, want ALL state", describeFilter(got.Added))
	case wantNone && !got.Added.IsEmpty():
		t.Errorf("added = %v, want no state", describeFilter(got.Added))
	case !wantAll && !wantNone:
		if got.Added.IsAll() {
			t.Errorf("added = ALL, want %v", want.Added.Entries)
			break
		}
		gotEntries := describeFilter(got.Added)
		wantEntries := make([]string, 0, len(want.Added.Entries))
		for _, e := range want.Added.Entries {
			if e.StateKey == nil {
				wantEntries = append(wantEntries, e.Type+"/*")
			} else {
				wantEntries = append(wantEntries, e.Type+"/"+*e.StateKey)
			}
		}
		sort.Strings(wantEntries)
		if !reflect.DeepEqual(gotEntries, wantEntries) {
			t.Errorf("added = %v, want %v", gotEntries, wantEntries)
		}
	}

	if diff := compareStringSets(got.ExtraLazyMembers, want.ExtraLazy); diff != "" {
		t.Errorf("extra lazy members %s", diff)
	}
	if diff := compareStringSets(got.InvalidatedLazyMembers, want.InvalidatedLazy); diff != "" {
		t.Errorf("invalidated lazy members %s", diff)
	}
}

func describeFilter(f StateFilter) []string {
	if f.IsAll() {
		return []string{"ALL"}
	}
	out := []string{}
	for _, e := range f.Entries() {
		if e.StateKey == nil {
			out = append(out, e.Type+"/*")
		} else {
			out = append(out, e.Type+"/"+*e.StateKey)
		}
	}
	sort.Strings(out)
	return out
}

func toSets(m map[string][]string) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for k, vs := range m {
		out[k] = toSet(vs)
	}
	return out
}

func toSet(vs []string) map[string]bool {
	out := map[string]bool{}
	for _, v := range vs {
		out[v] = true
	}
	return out
}

func fromSets(m map[string]map[string]bool) map[string][]string {
	out := map[string][]string{}
	for k, vs := range m {
		out[k] = sortedKeys(vs)
	}
	return out
}

func compareMaps(got, want map[string][]string) string {
	for k := range want {
		sort.Strings(want[k])
	}
	if reflect.DeepEqual(got, want) {
		return ""
	}
	return "= " + mustJSON(got) + ", want " + mustJSON(want)
}

func compareStringSets(got map[string]bool, want []string) string {
	g := sortedKeys(got)
	if len(g) == 0 && len(want) == 0 {
		return ""
	}
	sort.Strings(want)
	if reflect.DeepEqual(g, want) {
		return ""
	}
	return "= " + mustJSON(g) + ", want " + mustJSON(want)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<unmarshalable>"
	}
	return string(b)
}

// The two tests Synapse writes procedurally rather than in the table, ported
// directly. They cover the remembered-keys cap, which the table does not reach
// -- verified by mutation: removing the cap entirely leaves all 62 table
// assertions green.

func manyKeys(prefix string, n int) map[string]bool {
	out := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		out[prefix+itoa(i)] = true
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// Ported from test_limit_retained_previous_state_keys.
func TestRememberedStateKeysAreCapped(t *testing.T) {
	for _, tc := range []struct {
		name      string
		eventType string
		extra     []string
	}{
		{"an arbitrary type", "type", nil},
		{"membership", memberEventType, nil},
		{"lazy-loaded membership", memberEventType, []string{Lazy}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := manyKeys("prev_state_key", MaxRememberedStateKeys-30)
			request := manyKeys("state_key", 50)
			for _, e := range tc.extra {
				prev[e] = true
				request[e] = true
			}

			got := ComputeRequiredStateChanges("@user:test",
				map[string]map[string]bool{tc.eventType: prev},
				map[string]map[string]bool{tc.eventType: request},
				nil, nil, nil)

			if got.Changed == nil {
				t.Fatal("changed = nil, want a new config")
			}
			kept := got.Changed[tc.eventType]

			// Naively backfilling can overlap with the requested keys, so the
			// result can be slightly under the cap -- never over it.
			if len(kept) > MaxRememberedStateKeys {
				t.Errorf("remembered %d keys, cap is %d", len(kept), MaxRememberedStateKeys)
			}
			if len(kept) < MaxRememberedStateKeys-len(tc.extra) {
				t.Errorf("remembered %d keys, want at least %d",
					len(kept), MaxRememberedStateKeys-len(tc.extra))
			}

			// Everything requested must survive: dropping a requested key
			// means never sending state the client just asked for.
			for k := range request {
				if !kept[k] {
					t.Fatalf("requested key %q was dropped by the cap", k)
				}
			}

			// The remainder is backfilled from what was previously sent.
			remaining := setDifference(kept, request)
			if len(remaining) == 0 {
				t.Fatal("nothing was backfilled from the previous keys")
			}
			for k := range remaining {
				if len(k) < 5 || k[:5] != "prev_" {
					t.Errorf("backfilled with %q, which was never previously sent", k)
				}
			}
		})
	}
}

// Ported from test_request_more_state_keys_than_remember_limit.
func TestRequestingMoreKeysThanTheCapKeepsThemAll(t *testing.T) {
	prev := manyKeys("prev_state_key", MaxRememberedStateKeys-30)
	request := manyKeys("state_key", MaxRememberedStateKeys+20)

	got := ComputeRequiredStateChanges("@user:test",
		map[string]map[string]bool{"type": prev},
		map[string]map[string]bool{"type": request},
		nil, nil, nil)

	if got.Changed == nil {
		t.Fatal("changed = nil, want a new config")
	}
	kept := got.Changed["type"]
	for k := range request {
		if !kept[k] {
			t.Fatalf("requested key %q was dropped; the cap bounds what is REMEMBERED "+
				"from previous requests, never what was just asked for", k)
		}
	}
	// And nothing is backfilled, because there is no room.
	if extra := setDifference(kept, request); len(extra) != 0 {
		t.Errorf("backfilled %v despite the request already exceeding the cap", sortedKeys(extra))
	}
}

// Three cases Synapse's table does not reach, each found by a mutation that
// left all 62 of its assertions green.

// A type that gains one key and loses another in the same request exercises the
// FIRST pass's invalidation, which the table only reaches through the second.
// The removed key's state did not change, so it must still be remembered --
// otherwise a client that reshuffles its required_state is re-sent state it
// already has.
func TestAddAndRemoveInOneRequestKeepsUnchangedKeys(t *testing.T) {
	prev := map[string]map[string]bool{"type1": {"keep": true, "drop": true}}
	request := map[string]map[string]bool{"type1": {"keep": true, "added": true}}

	t.Run("no state changed", func(t *testing.T) {
		got := ComputeRequiredStateChanges("@user:test", prev, request, nil, nil, nil)
		if got.Changed == nil {
			t.Fatal("changed = nil, want a new config")
		}
		if !got.Changed["type1"]["drop"] {
			t.Errorf("changed = %v; `drop` was removed from the request but its state "+
				"never changed, so it must still be remembered -- otherwise re-adding "+
				"it re-sends an event the client already has", sortedKeys(got.Changed["type1"]))
		}
		if !got.Changed["type1"]["added"] || !got.Changed["type1"]["keep"] {
			t.Errorf("changed = %v, want it to include the requested keys",
				sortedKeys(got.Changed["type1"]))
		}
	})

	t.Run("the removed key's state changed", func(t *testing.T) {
		deltas := map[store.StateKey]bool{{Type: "type1", StateKey: "drop"}: true}
		got := ComputeRequiredStateChanges("@user:test", prev, request, nil, nil, deltas)
		if got.Changed == nil {
			t.Fatal("changed = nil, want a new config")
		}
		if got.Changed["type1"]["drop"] {
			t.Errorf("changed = %v; `drop` was removed AND its state changed, so it "+
				"must be forgotten", sortedKeys(got.Changed["type1"]))
		}
	})
}

// $LAZY is an instruction, not a state key, and it is only meaningful for
// membership. A client sending it for another type must not make us go looking
// for a state event whose state key is the literal "$LAZY".
func TestLazyIsIgnoredForNonMembershipTypes(t *testing.T) {
	got := ComputeRequiredStateChanges("@user:test",
		map[string]map[string]bool{},
		map[string]map[string]bool{"m.room.topic": {Lazy: true}},
		nil, nil, nil)

	for _, e := range got.Added.Entries() {
		if e.StateKey != nil && *e.StateKey == Lazy {
			t.Fatalf("added %s/%s: $LAZY was treated as a state key to fetch",
				e.Type, *e.StateKey)
		}
	}
}

// Previously fetching ("*","*") means any narrower request is already covered,
// so nothing new is fetched even though the config changes.
func TestNarrowingFromEverythingFetchesNothing(t *testing.T) {
	got := ComputeRequiredStateChanges("@user:test",
		map[string]map[string]bool{Wildcard: {Wildcard: true}},
		map[string]map[string]bool{"m.room.name": {"": true}},
		nil, nil, nil)

	if !got.Added.IsEmpty() {
		t.Errorf("added = %v, want nothing: we were already fetching everything",
			describeFilter(got.Added))
	}
	if got.Changed == nil || !got.Changed["m.room.name"][""] {
		t.Errorf("changed = %v, want the narrowed request", got.Changed)
	}
}
