package pushrules

import (
	"encoding/json"
	"testing"
)

const user = "@alice:example.com"

var allOn = Features{
	MSC1767Enabled: true, MSC3381PollsEnabled: true, MSC3664Enabled: true,
	MSC4028PushEncryptedEvents: true, MSC4210Enabled: true, MSC4306Enabled: true,
}

func format(t *testing.T, rules []UserRule, enabled map[string]bool, f Features) map[string][]map[string]any {
	t.Helper()
	body, err := Format(user, rules, enabled, f)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	var parsed struct {
		Global map[string][]map[string]any `json:"global"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}
	return parsed.Global
}

func ids(rules []map[string]any) []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		out = append(out, r["rule_id"].(string))
	}
	return out
}

func find(rules []map[string]any, id string) map[string]any {
	for _, r := range rules {
		if r["rule_id"] == id {
			return r
		}
	}
	return nil
}

// Every priority class appears, empty ones included: a client distinguishes
// "no rules of this kind" from "this server does not report them".
func TestAllClassesPresent(t *testing.T) {
	g := format(t, nil, nil, allOn)
	for _, class := range allClasses {
		if _, ok := g[class]; !ok {
			t.Errorf("class %q missing from the output", class)
		}
	}
}

// pattern_type is a placeholder so the base ruleset can be shared between
// users. A client must never see it: it wants the pattern.
func TestPlaceholdersAreSubstituted(t *testing.T) {
	g := format(t, nil, nil, Features{MSC1767Enabled: true, MSC3664Enabled: true})

	// .m.rule.contains_user_name matches on the localpart.
	content := find(g["content"], ".m.rule.contains_user_name")
	if content == nil {
		t.Fatal("contains_user_name should be present when MSC4210 is off")
	}
	if content["pattern"] != "alice" {
		t.Errorf("pattern = %v, want the localpart", content["pattern"])
	}
	if _, ok := content["pattern_type"]; ok {
		t.Error("pattern_type must not reach the client")
	}

	// .m.rule.invite_for_me matches state_key against the full user id.
	invite := find(g["override"], ".m.rule.invite_for_me")
	conds := invite["conditions"].([]any)
	var found bool
	for _, c := range conds {
		cond := c.(map[string]any)
		if cond["key"] == "state_key" {
			found = true
			if cond["pattern"] != user {
				t.Errorf("state_key pattern = %v, want %q", cond["pattern"], user)
			}
			if _, ok := cond["pattern_type"]; ok {
				t.Error("pattern_type must not reach the client")
			}
		}
	}
	if !found {
		t.Error("invite_for_me should have a state_key condition")
	}
}

// The rule id a client sees is the last segment; the namespace is internal.
func TestRuleIDsAreUnscoped(t *testing.T) {
	g := format(t, nil, nil, allOn)
	for class, rules := range g {
		for _, id := range ids(rules) {
			if len(id) > 0 && id[0] != '.' {
				t.Errorf("%s: rule id %q should start with a dot", class, id)
			}
		}
	}
}

// MSC4210 REMOVES the legacy mention rules, the opposite sense to the other
// flags. Getting the direction wrong changes every user's ruleset.
func TestMSC4210RemovesLegacyMentionRules(t *testing.T) {
	on := format(t, nil, nil, allOn)
	if find(on["override"], ".m.rule.contains_display_name") != nil {
		t.Error("contains_display_name should be removed when MSC4210 is on")
	}
	if find(on["content"], ".m.rule.contains_user_name") != nil {
		t.Error("contains_user_name should be removed when MSC4210 is on")
	}

	off := allOn
	off.MSC4210Enabled = false
	g := format(t, nil, nil, off)
	if find(g["override"], ".m.rule.contains_display_name") == nil {
		t.Error("contains_display_name should be present when MSC4210 is off")
	}
}

func TestExperimentalRulesAreGated(t *testing.T) {
	none := format(t, nil, nil, Features{})
	if find(none["override"], ".im.nheko.msc3664.reply") != nil {
		t.Error("the msc3664 rule should be hidden when the flag is off")
	}
	if find(none["override"], ".org.matrix.msc4028.encrypted_event") != nil {
		t.Error("the msc4028 rule should be hidden when the flag is off")
	}
	if len(none["postcontent"]) != 0 {
		t.Error("msc4306 postcontent rules should be hidden when the flag is off")
	}

	on := format(t, nil, nil, allOn)
	if find(on["override"], ".im.nheko.msc3664.reply") == nil {
		t.Error("the msc3664 rule should appear when the flag is on")
	}
	if len(on["postcontent"]) == 0 {
		t.Error("msc4306 postcontent rules should appear when the flag is on")
	}
}

// A rule's enabled state comes from push_rules_enable when present, and from
// the rule's own default otherwise. .m.rule.master is the one base rule that
// defaults to disabled -- it is the "silence everything" switch.
func TestEnabledState(t *testing.T) {
	g := format(t, nil, nil, allOn)
	master := find(g["override"], ".m.rule.master")
	if master["enabled"] != false {
		t.Errorf(".m.rule.master should default to disabled, got %v", master["enabled"])
	}

	g = format(t, nil, map[string]bool{"global/override/.m.rule.master": true}, allOn)
	master = find(g["override"], ".m.rule.master")
	if master["enabled"] != true {
		t.Error("an explicit enable should win over the default")
	}
}

// A user rule whose id matches a base rule replaces that rule's ACTIONS in
// place, keeping its conditions and its position. Appending it instead would
// both duplicate the rule and move it, changing evaluation order.
func TestUserRuleOverridingABaseRule(t *testing.T) {
	before := ids(format(t, nil, nil, allOn)["override"])

	g := format(t, []UserRule{{
		RuleID: "global/override/.m.rule.master", PriorityClass: 5,
		Conditions: "[]", Actions: `["notify"]`,
	}}, nil, allOn)

	after := ids(g["override"])
	if len(after) != len(before) {
		t.Errorf("override count changed from %d to %d: the rule was duplicated",
			len(before), len(after))
	}
	master := find(g["override"], ".m.rule.master")
	if master["default"] != true {
		t.Error("an overridden base rule is still a default rule")
	}
	actions := master["actions"].([]any)
	if len(actions) != 1 || actions[0] != "notify" {
		t.Errorf("actions = %v, want the user's", actions)
	}
}

// A user's own rules sit in front of the base rules of the same class.
func TestUserRulesComeFirst(t *testing.T) {
	g := format(t, []UserRule{{
		RuleID: "mine", PriorityClass: 5, Conditions: "[]", Actions: `["notify"]`,
	}}, nil, allOn)
	got := ids(g["override"])
	// .m.rule.master is prepended before user rules; everything else follows.
	if len(got) < 2 || got[0] != ".m.rule.master" || got[1] != "mine" {
		t.Errorf("order = %v, want master, then the user's rule", got[:min(3, len(got))])
	}
	if find(g["override"], "mine")["default"] != false {
		t.Error("a user's own rule is not a default rule")
	}
}

// room and sender rules are identified by the pattern of their one condition:
// a `room` rule is named by the room it applies to.
func TestRoomRuleIDComesFromItsCondition(t *testing.T) {
	g := format(t, []UserRule{{
		RuleID: "!abc:example.com", PriorityClass: 3,
		Conditions: `[{"kind":"event_match","key":"room_id","pattern":"!abc:example.com"}]`,
		Actions:    `["notify"]`,
	}}, nil, allOn)
	if len(g["room"]) != 1 {
		t.Fatalf("room rules = %v", g["room"])
	}
	r := g["room"][0]
	if r["rule_id"] != "!abc:example.com" {
		t.Errorf("rule_id = %v", r["rule_id"])
	}
	if _, ok := r["conditions"]; ok {
		t.Error("a room rule reports no conditions: they follow from its kind and id")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
