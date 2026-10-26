// Package pushrules assembles the `m.push_rules` account data a sync reports.
//
// Synapse does not store a user's ruleset; it stores only their *deviations*
// from a built-in base ruleset, and rebuilds the whole thing on every read. So
// reporting push rules means reproducing that base ruleset exactly and
// interleaving the user's rows into it in the right places -- see baserules.go.
package pushrules

import (
	"encoding/json"
	"fmt"
	"strings"
)

// UserRule is one row of the `push_rules` table.
type UserRule struct {
	RuleID        string
	PriorityClass int
	Priority      int
	// Conditions and Actions are the stored JSON, passed through untouched.
	Conditions string
	Actions    string
}

// Features mirrors the experimental flags that gate individual base rules.
//
// Synapse's own field names, so they paste across from homeserver.yaml. Getting
// one wrong adds or removes a rule from every user's ruleset.
type Features struct {
	MSC1767Enabled             bool
	MSC3381PollsEnabled        bool
	MSC3664Enabled             bool
	MSC4028PushEncryptedEvents bool
	MSC4210Enabled             bool
	MSC4306Enabled             bool
}

// priorityClassNames maps a priority class to its template name, and is also
// the set of keys that always appear in the output -- Synapse emits every
// class, empty ones included.
var priorityClassNames = map[int]string{
	1: "underride",
	2: "sender",
	3: "room",
	4: "content",
	5: "override",
	6: "postcontent", // MSC4306
}

// allClasses is the order the empty arrays are created in. JSON objects are
// unordered, so this only decides which keys exist, not their order.
var allClasses = []string{"underride", "sender", "room", "postcontent", "content", "override"}

// Format builds the `m.push_rules` account data content for a user.
//
// userRules must be sorted by (priority_class DESC, priority DESC), which is
// what Synapse does before handing them to the Rust.
func Format(userID string, userRules []UserRule, enabled map[string]bool, f Features) (json.RawMessage, error) {
	localpart := localpartOf(userID)

	// A user rule whose id matches a base rule does not sit alongside it: it
	// *replaces that base rule's actions in place*, keeping the base rule's own
	// conditions and position. Appending it instead would both duplicate the
	// rule and move it, changing evaluation order.
	overriddenActions := map[string]string{}
	byClass := map[int][]UserRule{}
	for _, r := range userRules {
		if _, isBase := baseRuleIDs[r.RuleID]; isBase {
			overriddenActions[r.RuleID] = r.Actions
			continue
		}
		byClass[r.PriorityClass] = append(byClass[r.PriorityClass], r)
	}

	// PushRules::iter's order. Every entry is either a base rule or a user one,
	// and the sequence is what a client evaluates.
	var ordered []Rule
	appendBase := func(rules []Rule) {
		for _, r := range rules {
			if actions, ok := overriddenActions[r.RuleID]; ok {
				r.Actions = []string{actions}
				r.ruleFlags.overriddenRaw = true
			}
			ordered = append(ordered, r)
		}
	}
	appendUser := func(class int) {
		for _, u := range byClass[class] {
			ordered = append(ordered, Rule{
				RuleID:        u.RuleID,
				PriorityClass: u.PriorityClass,
				Conditions:    []string{u.Conditions},
				Actions:       []string{u.Actions},
				ruleFlags:     ruleFlags{rawArrays: true},
			})
		}
	}

	appendBase(basePrependOverride)
	appendUser(5)
	appendBase(baseAppendOverride)
	appendUser(4)
	appendBase(baseAppendContent)
	appendBase(baseAppendPostcontent)
	appendUser(3)
	appendUser(2)
	appendUser(1)
	appendBase(baseAppendUnderride)

	out := map[string][]json.RawMessage{}
	for _, name := range allClasses {
		out[name] = []json.RawMessage{}
	}

	for _, r := range ordered {
		if !f.allows(r.RuleID) {
			continue
		}
		name, ok := priorityClassNames[r.PriorityClass]
		if !ok {
			// Synapse warns and drops an unrecognised class rather than
			// failing the whole request.
			continue
		}
		body, err := r.template(name, userID, localpart, enabledFor(enabled, r))
		if err != nil {
			return nil, err
		}
		if body == nil {
			// A content rule with no usable pattern is skipped, as
			// _rule_to_template does.
			continue
		}
		out[name] = append(out[name], body)
	}

	content, err := json.Marshal(map[string]any{"global": out})
	if err != nil {
		return nil, fmt.Errorf("pushrules: encode: %w", err)
	}
	return content, nil
}

func enabledFor(enabled map[string]bool, r Rule) bool {
	if v, ok := enabled[r.RuleID]; ok {
		return v
	}
	return r.DefaultEnabled
}

// allows applies the experimental gates that hide individual base rules.
func (f Features) allows(ruleID string) bool {
	if !f.MSC1767Enabled &&
		(strings.Contains(ruleID, "org.matrix.msc1767") || strings.Contains(ruleID, "org.matrix.msc3933")) {
		return false
	}
	if !f.MSC3664Enabled && ruleID == "global/override/.im.nheko.msc3664.reply" {
		return false
	}
	if !f.MSC3381PollsEnabled && strings.Contains(ruleID, "org.matrix.msc3930") {
		return false
	}
	if !f.MSC4028PushEncryptedEvents && ruleID == "global/override/.org.matrix.msc4028.encrypted_event" {
		return false
	}
	// Note the inverted sense: MSC4210 *removes* the legacy mention rules.
	if f.MSC4210Enabled &&
		(ruleID == "global/override/.m.rule.contains_display_name" ||
			ruleID == "global/content/.m.rule.contains_user_name" ||
			ruleID == "global/override/.m.rule.roomnotif") {
		return false
	}
	if !f.MSC4306Enabled && strings.Contains(ruleID, "/.io.element.msc4306.rule.") {
		return false
	}
	return true
}

func localpartOf(userID string) string {
	s := strings.TrimPrefix(userID, "@")
	if i := strings.IndexByte(s, ':'); i >= 0 {
		return s[:i]
	}
	return s
}
