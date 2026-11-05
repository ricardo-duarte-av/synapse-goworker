package pushwriter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/streamid"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
)

func TestParseAndMatch(t *testing.T) {
	for p, want := range map[string]string{
		"/_matrix/client/v3/pushrules/":                         "pushrules/",
		"/_matrix/client/api/v1/pushrules/global/room/%21r%3Ax": "pushrules/global/room/!r:x",
		"/_matrix/client/r0/pushrules/global/override/a%2Fb":    "pushrules/global/override/a/b",
	} {
		got, ok := parse(p)
		if !ok || got != want {
			t.Errorf("%s: %q %v", p, got, ok)
		}
	}
	for _, p := range []string{"/_matrix/client/v3/pushrules", "/_matrix/client/v4/pushrules/", "/_matrix/client/v3/x/pushrules/"} {
		if _, ok := parse(p); ok {
			t.Errorf("%s matched", p)
		}
	}
	if Match("POST", "/_matrix/client/v3/pushrules/") {
		t.Error("POST matched")
	}
}

func TestRuleSpec(t *testing.T) {
	for p, want := range map[string]string{
		"pushrules/global/override/x":         "override|x||false",
		"pushrules/global/override/x/enabled": "override|x|enabled|true",
		"pushrules/global/override/x/":        "override|x||false",
	} {
		s, err := ruleSpec(strings.Split(p, "/"))
		got := s.template + "|" + s.ruleID + "|" + s.attr + "|"
		if s.hasAttr {
			got += "true"
		} else {
			got += "false"
		}
		if err != nil || got != want {
			t.Errorf("%s: %s %v", p, got, err)
		}
	}
	for _, p := range []string{"pushrules", "pushrules/device/x/y", "pushrules/global", "pushrules/global/override", "pushrules/global/override/"} {
		if _, err := ruleSpec(strings.Split(p, "/")); err == nil {
			t.Errorf("%s accepted", p)
		}
	}
}

func TestRuleTuple(t *testing.T) {
	for _, c := range []struct {
		template, body, conds, acts, err string
	}{
		{"room", `{"actions":["notify"]}`, `[{"kind":"event_match","key":"room_id","pattern":"!r:x"}]`, `["notify"]`, ""},
		{"content", `{"pattern":"café","actions":[{"set_tweak":"sound","value":"default"}]}`,
			`[{"kind":"event_match","key":"content.body","pattern":"caf` + "\\" + `u00e9"}]`, `[{"set_tweak":"sound","value":"default"}]`, ""},
		{"override", `{"conditions":[{"kind":"x"}],"actions":[]}`, `[{"kind":"x"}]`, `[]`, ""},
		// Python's `in`: a dict's keys, a string's substrings.
		{"override", `{"conditions":{"kind":1},"actions":["notify"]}`, `{"kind":1}`, `["notify"]`, ""},
		{"override", `{"conditions":["kind"],"actions":["notify"]}`, `["kind"]`, `["notify"]`, ""},
		{"override", `{"actions":[]}`, "", "", "Missing 'conditions'"},
		{"override", `{"conditions":[{"x":1}],"actions":[]}`, "", "", "Condition without 'kind'"},
		{"content", `{"actions":[]}`, "", "", "Content rule missing 'pattern'"},
		{"room", `{}`, "", "", "No actions found"},
		{"room", `{"actions":"notify"}`, "", "", "No actions found"},
		{"room", `{"actions":[{"x":1}]}`, "", "", "Unrecognised action {'x': 1}"},
		{"room", `{"actions":["bad"]}`, "", "", "Unrecognised action bad"},
		{"room", `{"actions":[[1, true]]}`, "", "", "Unrecognised action [1, True]"},
		{"postcontent", `{}`, "", "", "user-defined rules using `postcontent` are not accepted"},
		{"override", `[]`, "", "", "Missing 'conditions'"},
		{"override", `5`, "", "", "pushwriter: argument of type is not iterable"},
	} {
		conds, acts, err := ruleTuple(c.template, "!r:x", gjson.Parse(c.body))
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%s %s: %v, want %q", c.template, c.body, err, c.err)
			}
			continue
		}
		if err != nil || conds != c.conds || acts != c.acts {
			t.Errorf("%s %s: %s %s %v", c.template, c.body, conds, acts, err)
		}
	}
}

func TestGetFilter(t *testing.T) {
	a := &App{}
	rules := json.RawMessage(`{"global":{"override":[{"rule_id":"a","enabled":true,"actions":[]},{"rule_id":"a","enabled":false}],"room":[]}}`)
	for path, want := range map[string]string{
		"pushrules/":                          string(rules),
		"pushrules/global/":                   `{"override":[{"rule_id":"a","enabled":true,"actions":[]},{"rule_id":"a","enabled":false}],"room":[]}`,
		"pushrules/global/room/":              `[]`,
		"pushrules/global/override/a":         `{"rule_id":"a","enabled":false}`,
		"pushrules/global/override/a/enabled": `{"enabled":false}`,
	} {
		got, err := a.get(rules, path)
		if err != nil || string(got) != want {
			t.Errorf("%s: %s %v", path, got, err)
		}
	}
	for path, want := range map[string]string{
		"pushrules/global":                    "Unrecognised request: You probably wanted a trailing slash",
		"pushrules/global/room":               "Unrecognised request: You probably wanted a trailing slash",
		"pushrules/device/":                   "Unrecognized request",
		"pushrules/global/sender/":            "Unrecognized request",
		"pushrules/global/override/b":         "Not found",
		"pushrules/global/override/a/nope":    "Unrecognized request",
		"pushrules/global/override/a/actions": "Unrecognized request", // the last match has none
	} {
		_, err := a.get(rules, path)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", path, err, want)
		}
	}
}

type copyStore struct {
	Store
	rules   []pushrules.UserRule
	enabled map[string]bool
	added   []string
}

func (c *copyStore) UserRules(context.Context, string) ([]pushrules.UserRule, map[string]bool, error) {
	return c.rules, c.enabled, nil
}

func (c *copyStore) AddRule(_ context.Context, _ Limits, _ Write, _, ruleID string, class int, conds, acts, before, after string) error {
	c.added = append(c.added, ruleID+" "+conds+" "+acts)
	return nil
}

func TestParses(t *testing.T) {
	for _, c := range []struct {
		c, a string
		ok   bool
	}{
		{`[]`, `[]`, true}, {`{"kind":1}`, `[]`, false}, {`["kind"]`, `[]`, true}, {`[1]`, `[1]`, true},
		{`[]`, `["bad"]`, false}, {`[]`, `{}`, false}, {`[]`, `["dont_notify","coalesce",{"x":1}]`, true}, {`"kind"`, `[]`, false},
	} {
		if got := parses(c.c, c.a); got != c.ok {
			t.Errorf("%s %s: %v", c.c, c.a, got)
		}
	}
}

type memDB struct{ seq int64 }

func (m *memDB) DeleteStale(context.Context, string, []string) error         { return nil }
func (m *memDB) LastValue(context.Context, string) (int64, error)            { return m.seq, nil }
func (m *memDB) UpsertPosition(context.Context, string, string, int64) error { return nil }
func (m *memDB) NextVal(context.Context, string) (int64, error)              { m.seq++; return m.seq, nil }

func TestCopyRules(t *testing.T) {
	st := &copyStore{
		rules: []pushrules.UserRule{
			{RuleID: "global/override/mine", PriorityClass: 5, Conditions: `[{"kind":"event_match","key":"room_id","pattern":"!old:x"},{"kind":"other"}]`, Actions: `["notify"]`},
			{RuleID: "global/room/!old:x", PriorityClass: 3, Conditions: `[{"kind":"event_match","key":"room_id","pattern":"!old:x"}]`, Actions: `[]`},
			{RuleID: "global/room/!other:x", PriorityClass: 3, Conditions: `[{"kind":"event_match","key":"room_id","pattern":"!other:x"}]`, Actions: `[]`},
			{RuleID: "global/room/!off:x", PriorityClass: 3, Conditions: `[{"kind":"event_match","key":"room_id","pattern":"!old:x"}]`, Actions: `[]`},
			{RuleID: "global/override/.m.rule.master", PriorityClass: -1, Conditions: `[]`, Actions: `[]`},
		},
		enabled: map[string]bool{"global/room/!off:x": false},
	}
	hs := &synapsecfg.Config{}
	a := &App{Options: Options{Store: st, Limits: DefaultLimits, Log: zerolog.Nop(), Synapse: func() *synapsecfg.Config { return hs }}}
	g, err := streamid.Open(context.Background(), &memDB{}, streamid.Options{Stream: PositionsName, Instance: "gw", Sequence: Sequence, Writers: []string{"gw"}})
	if err != nil {
		t.Fatal(err)
	}
	a.ids = g
	if err := a.copyRules(context.Background(), "!old:x", "!new:x", "@u:x"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`global/override/!new:x [{"kind":"event_match","key":"room_id","pattern":"!new:x"},{"kind":"other"}] ["notify"]`,
		`global/room/!new:x [{"kind":"event_match","key":"room_id","pattern":"!new:x"}] []`,
	}
	if strings.Join(st.added, "\n") != strings.Join(want, "\n") {
		t.Errorf("added\n%s", strings.Join(st.added, "\n"))
	}
}
