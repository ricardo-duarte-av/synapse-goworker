package pushrules

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// baseRuleIDs is every rule id in the built-in ruleset, for spotting a user
// rule that overrides one.
var baseRuleIDs = func() map[string]struct{} {
	out := map[string]struct{}{}
	for _, group := range [][]Rule{
		basePrependOverride, baseAppendOverride, baseAppendContent,
		baseAppendPostcontent, baseAppendUnderride,
	} {
		for _, r := range group {
			out[r.RuleID] = struct{}{}
		}
	}
	return out
}()

// rawArrays marks a rule whose Conditions/Actions hold one JSON *array* each
// (as stored in the push_rules table) rather than one element per entry.
// overriddenRaw marks a base rule whose actions were replaced by a user's,
// which arrive in the same array form.
type ruleFlags struct {
	rawArrays     bool
	overriddenRaw bool
}

// template renders one rule in the client format.
//
// A port of _rule_to_template plus _convert_type_to_value: the shape depends on
// the rule's kind, and `pattern_type`/`value_type` are placeholders that are
// substituted for the requesting user and then removed. A client never sees
// them -- they exist so the base ruleset can be shared between users.
func (r Rule) template(className, userID, localpart string, enabled bool) (json.RawMessage, error) {
	conditions, err := r.jsonArray(r.Conditions, r.rawArrays)
	if err != nil {
		return nil, err
	}
	actions, err := r.jsonArray(r.Actions, r.rawArrays || r.overriddenRaw)
	if err != nil {
		return nil, err
	}

	ruleID := unscopedRuleID(r.RuleID)
	out := map[string]any{}

	switch className {
	case "override", "underride", "postcontent":
		out["conditions"] = substituteAll(conditions, userID, localpart)
		out["actions"] = actions

	case "sender", "room":
		out["actions"] = actions
		// The rule id for these kinds is the pattern of their single condition:
		// a `room` rule is identified by the room it applies to.
		if len(conditions) == 0 {
			return nil, fmt.Errorf("pushrules: %s rule %q has no conditions", className, r.RuleID)
		}
		pattern := gjson.GetBytes(conditions[0], "pattern")
		if !pattern.Exists() {
			return nil, fmt.Errorf("pushrules: %s rule %q has no pattern", className, r.RuleID)
		}
		ruleID = pattern.String()

	case "content":
		// Synapse drops a content rule that does not have exactly one
		// condition, or whose condition has neither a pattern nor a
		// pattern_type. Returning nil skips it, as _rule_to_template does.
		if len(conditions) != 1 {
			return nil, nil
		}
		out["actions"] = actions
		cond := conditions[0]
		switch {
		case gjson.GetBytes(cond, "pattern").Exists():
			out["pattern"] = gjson.GetBytes(cond, "pattern").Value()
		case gjson.GetBytes(cond, "pattern_type").Exists():
			// Substituted at the top level of the rule, not inside a
			// condition: a content rule reports its pattern directly.
			v := substituteType(gjson.GetBytes(cond, "pattern_type").String(), userID, localpart)
			if v == "" {
				return nil, nil
			}
			out["pattern"] = v
		default:
			return nil, nil
		}

	default:
		return nil, fmt.Errorf("pushrules: unexpected class %q", className)
	}

	out["rule_id"] = ruleID
	out["default"] = r.isBase()
	out["enabled"] = enabled

	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("pushrules: encode rule %q: %w", r.RuleID, err)
	}
	return body, nil
}

// jsonArray normalises a rule's stored form into a list of JSON values.
func (r Rule) jsonArray(entries []string, packedAsArray bool) ([]json.RawMessage, error) {
	if packedAsArray {
		if len(entries) == 0 {
			return nil, nil
		}
		var out []json.RawMessage
		if err := json.Unmarshal([]byte(entries[0]), &out); err != nil {
			return nil, fmt.Errorf("pushrules: rule %q: %w", r.RuleID, err)
		}
		return out, nil
	}
	out := make([]json.RawMessage, 0, len(entries))
	for _, e := range entries {
		out = append(out, json.RawMessage(e))
	}
	return out, nil
}

func (r Rule) isBase() bool {
	_, ok := baseRuleIDs[r.RuleID]
	return ok
}

// substituteAll replaces the pattern_type / value_type placeholders in each
// condition with the requesting user's id or localpart, and drops the
// placeholder key.
func substituteAll(conditions []json.RawMessage, userID, localpart string) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(conditions))
	for _, c := range conditions {
		out = append(out, substituteCondition(c, userID, localpart))
	}
	return out
}

func substituteCondition(cond json.RawMessage, userID, localpart string) json.RawMessage {
	for _, key := range []string{"pattern", "value"} {
		typeKey := key + "_type"
		t := gjson.GetBytes(cond, typeKey)
		if !t.Exists() {
			continue
		}
		v := substituteType(t.String(), userID, localpart)
		next, err := sjson.DeleteBytes(cond, typeKey)
		if err != nil {
			continue
		}
		cond = next
		if v == "" {
			// An unrecognised placeholder leaves no value behind, matching
			// Synapse: it pops the type key and only sets the value for the
			// two it knows.
			continue
		}
		if next, err := sjson.SetBytes(cond, key, v); err == nil {
			cond = next
		}
	}
	return cond
}

func substituteType(typeValue, userID, localpart string) string {
	switch typeValue {
	case "user_id":
		return userID
	case "user_localpart":
		return localpart
	}
	return ""
}

// unscopedRuleID strips the `global/override/` namespace a rule is stored
// under: clients see only the last segment.
func unscopedRuleID(ruleID string) string {
	if i := strings.LastIndexByte(ruleID, '/'); i >= 0 {
		return ruleID[i+1:]
	}
	return ruleID
}
