package pushwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientreq"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
)

// priorityClass is PRIORITY_CLASS_MAP.
var priorityClass = map[string]int{
	"underride": 1, "sender": 2, "room": 3, "postcontent": 6, "content": 4, "override": 5,
}

// spec is RuleSpec.
type spec struct {
	template, ruleID, attr string
	hasAttr                bool
}

var errUnrecognized = clientreq.New(400, "M_UNRECOGNIZED", "Unrecognized request")

// ruleSpec is _rule_spec_from_path, of the unquoted path split on '/'.
func ruleSpec(path []string) (spec, *clientreq.Error) {
	if len(path) < 2 || path[0] != "pushrules" || path[1] != "global" {
		return spec{}, errUnrecognized
	}
	path = path[2:]
	if len(path) == 0 {
		return spec{}, errUnrecognized
	}
	s := spec{template: path[0]}
	path = path[1:]
	if len(path) == 0 || path[0] == "" {
		return spec{}, errUnrecognized
	}
	s.ruleID = path[0]
	path = path[1:]
	if len(path) > 0 && path[0] != "" {
		s.attr, s.hasAttr = path[0], true
	}
	return s, nil
}

func (s spec) namespaced() string { return "global/" + s.template + "/" + s.ruleID }

// errType is a TypeError or AttributeError Synapse would raise: a 500.
type errType struct{ msg string }

func (e *errType) Error() string { return "pushwriter: " + e.msg }

// pyIn is Python's `key in v` for a JSON value: a dict's keys, a list's
// elements (equal strings only), a substring; TypeError for the rest.
func pyIn(v gjson.Result, key string) (bool, error) {
	switch {
	case v.IsObject():
		return v.Get(gjsonKey(key)).Exists(), nil
	case v.IsArray():
		found := false
		v.ForEach(func(_, x gjson.Result) bool {
			found = x.Type == gjson.String && x.Str == key
			return !found
		})
		return found, nil
	case v.Type == gjson.String:
		return strings.Contains(v.Str, key), nil
	}
	return false, &errType{"argument of type is not iterable"}
}

// pyGet is v[key] for a dict; TypeError for anything else.
func pyGet(v gjson.Result, key string) (gjson.Result, error) {
	if !v.IsObject() {
		return gjson.Result{}, &errType{"indices must be integers"}
	}
	return v.Get(gjsonKey(key)), nil
}

// pyIter is `for x in v`: a dict's keys, a list's elements, a string's
// characters; TypeError for the rest.
func pyIter(v gjson.Result) ([]gjson.Result, error) {
	var out []gjson.Result
	switch {
	case v.IsObject():
		v.ForEach(func(k, _ gjson.Result) bool { out = append(out, k); return true })
	case v.IsArray():
		out = v.Array()
	case v.Type == gjson.String:
		for _, r := range v.Str {
			b, _ := json.Marshal(string(r))
			out = append(out, gjson.ParseBytes(b))
		}
	default:
		return nil, &errType{"object is not iterable"}
	}
	return out, nil
}

// invalidRule is InvalidRuleException.
type invalidRule struct{ msg string }

func (e *invalidRule) Error() string { return e.msg }

// checkActions is check_actions.
func checkActions(actions gjson.Result) error {
	if !actions.IsArray() {
		return &invalidRule{"No actions found"}
	}
	for _, a := range actions.Array() {
		if a.Type == gjson.String && (a.Str == "notify" || a.Str == "dont_notify" || a.Str == "coalesce") {
			continue
		}
		if a.IsObject() && a.Get("set_tweak").Exists() {
			continue
		}
		s, err := pyjson.Str([]byte(a.Raw))
		if err != nil {
			return err
		}
		return &invalidRule{"Unrecognised action " + s}
	}
	return nil
}

// ruleTuple is _rule_tuple_from_request_object: the conditions and actions
// as json_encoder writes them.
func ruleTuple(template, ruleID string, req gjson.Result) (string, string, error) {
	if template == "postcontent" {
		return "", "", clientreq.New(400, "M_INVALID_PARAM", "user-defined rules using `postcontent` are not accepted")
	}
	var conditions string
	switch template {
	case "override", "underride":
		in, err := pyIn(req, "conditions")
		if err != nil {
			return "", "", err
		}
		if !in {
			return "", "", &invalidRule{"Missing 'conditions'"}
		}
		c, err := pyGet(req, "conditions")
		if err != nil {
			return "", "", err
		}
		items, err := pyIter(c)
		if err != nil {
			return "", "", err
		}
		for _, x := range items {
			has, err := pyIn(x, "kind")
			if err != nil {
				return "", "", err
			}
			if !has {
				return "", "", &invalidRule{"Condition without 'kind'"}
			}
		}
		if conditions, err = pyjson.Encode([]byte(c.Raw)); err != nil {
			return "", "", err
		}
	case "room", "sender":
		key := "room_id"
		if template == "sender" {
			key = "user_id"
		}
		// Synapse's dict literal, in its order: kind, key, pattern.
		p, _ := json.Marshal(ruleID)
		enc, err := pyjson.Encode([]byte(`[{"kind":"event_match","key":"` + key + `","pattern":` + string(p) + `}]`))
		if err != nil {
			return "", "", err
		}
		conditions = enc
	case "content":
		in, err := pyIn(req, "pattern")
		if err != nil {
			return "", "", err
		}
		if !in {
			return "", "", &invalidRule{"Content rule missing 'pattern'"}
		}
		pat, err := pyGet(req, "pattern")
		if err != nil {
			return "", "", err
		}
		if conditions, err = pyjson.Encode([]byte(`[{"kind":"event_match","key":"content.body","pattern":` + pat.Raw + `}]`)); err != nil {
			return "", "", err
		}
	default:
		return "", "", &invalidRule{"Unknown rule template: " + template}
	}
	in, err := pyIn(req, "actions")
	if err != nil {
		return "", "", err
	}
	if !in {
		return "", "", &invalidRule{"No actions found"}
	}
	acts, err := pyGet(req, "actions")
	if err != nil {
		return "", "", err
	}
	if err := checkActions(acts); err != nil {
		return "", "", err
	}
	actions, err := pyjson.Encode([]byte(acts.Raw))
	if err != nil {
		return "", "", err
	}
	return conditions, actions, nil
}

// add is add_push_rule: the limits, then the write under a stream ID.
func (a *App) add(ctx context.Context, userID, ruleID string, class int, conditions, actions, before, after string) error {
	if n := len(ruleID); n > a.Limits.RuleIDLength {
		return &RuleError{413, "M_UNKNOWN", fmt.Sprintf("Push rule ID length exceeds server limit (%d bytes > %d bytes).", n, a.Limits.RuleIDLength)}
	}
	if size := len(conditions) + len(actions); size > a.Limits.RuleSize {
		return &RuleError{413, "M_UNKNOWN", fmt.Sprintf("Push rule size exceeds server limit (%d bytes > %d bytes).", size, a.Limits.RuleSize)}
	}
	err := a.stored(ctx, func(w Write) error {
		return a.Store.AddRule(ctx, a.Limits, w, userID, ruleID, class, conditions, actions, before, after)
	})
	outcome(writesFor("add"), err)
	return err
}

func writesFor(op string) func(string) { return func(o string) { writes.WithLabelValues(op, o).Inc() } }

func outcome(count func(string), err error) {
	var rerr *RuleError
	switch {
	case err == nil:
		count("ok")
	case errors.As(err, &rerr), errors.Is(err, ErrRuleNotFound), errors.Is(err, ErrNoRow):
		count("refused")
	default:
		count("error")
	}
}

// put is PushRuleRestServlet.handle_put.
func (a *App) put(ctx context.Context, userID, path string, body []byte, before, after string) *clientreq.Error {
	s, cerr := ruleSpec(strings.Split(path, "/"))
	if cerr != nil {
		return cerr
	}
	class, ok := priorityClass[s.template]
	if !ok {
		return clientreq.New(400, "M_UNKNOWN", "Unknown template: "+s.template)
	}
	if strings.ContainsAny(s.ruleID, "/\\") {
		return clientreq.New(400, "M_UNKNOWN", "rule_id may not contain slashes")
	}
	body, err := parseValue(body)
	if err != nil {
		return clientreq.New(400, "M_NOT_JSON", "Content not JSON.")
	}
	content := gjson.ParseBytes(body)
	if s.hasAttr {
		err := a.setAttr(ctx, userID, s, content)
		var inv *invalidRule
		switch {
		case err == nil:
			return nil
		case errors.As(err, &inv):
			return clientreq.New(400, "M_UNKNOWN", "Invalid actions: "+inv.msg)
		case errors.Is(err, ErrRuleNotFound):
			return clientreq.New(404, "M_NOT_FOUND", "Unknown rule")
		}
		return a.asClient(err)
	}
	if strings.HasPrefix(s.ruleID, ".") {
		return clientreq.New(400, "M_UNKNOWN", "cannot add new rule_ids that start with '.'")
	}
	conditions, actions, err := ruleTuple(s.template, s.ruleID, content)
	if err != nil {
		var inv *invalidRule
		if errors.As(err, &inv) {
			return clientreq.New(400, "M_UNKNOWN", inv.msg)
		}
		return a.asClient(err)
	}
	if before != "" {
		before = "global/" + s.template + "/" + before
	}
	if after != "" {
		after = "global/" + s.template + "/" + after
	}
	return a.asClient(a.add(ctx, userID, s.namespaced(), class, conditions, actions, before, after))
}

// setAttr is PushRulesHandler.set_rule_attr.
func (a *App) setAttr(ctx context.Context, userID string, s spec, val gjson.Result) error {
	if s.attr != "enabled" && s.attr != "actions" {
		return errUnrecognized
	}
	ruleID := s.namespaced()
	isDefault := strings.HasPrefix(s.ruleID, ".")
	if isDefault && !a.served[ruleID] {
		return ErrRuleNotFound // "Unknown rule %r": the servlet says "Unknown rule"
	}
	if s.attr == "enabled" {
		if val.IsObject() && val.Get("enabled").Exists() {
			val = val.Get("enabled")
		}
		if val.Type != gjson.True && val.Type != gjson.False {
			return clientreq.New(400, "M_UNKNOWN", "Value for 'enabled' must be boolean")
		}
		op := "disable"
		if val.Type == gjson.True {
			op = "enable"
		}
		err := a.stored(ctx, func(w Write) error {
			return a.Store.SetEnabled(ctx, w, userID, ruleID, val.Type == gjson.True, isDefault)
		})
		outcome(writesFor(op), err)
		return err
	}
	if !val.IsObject() {
		return clientreq.New(400, "M_UNKNOWN", "Value must be a dict")
	}
	acts := val.Get("actions")
	if !acts.IsArray() {
		return clientreq.New(400, "M_UNKNOWN", "Value for 'actions' must be dict")
	}
	if err := checkActions(acts); err != nil {
		return err
	}
	enc, err := pyjson.Encode([]byte(acts.Raw))
	if err != nil {
		return err
	}
	err = a.stored(ctx, func(w Write) error {
		return a.Store.SetActions(ctx, a.Limits, w, userID, ruleID, enc, isDefault)
	})
	outcome(writesFor("actions"), err)
	return err
}

// del is PushRuleRestServlet.handle_delete.
func (a *App) del(ctx context.Context, userID, path string) *clientreq.Error {
	s, cerr := ruleSpec(strings.Split(path, "/"))
	if cerr != nil {
		return cerr
	}
	err := a.stored(ctx, func(w Write) error { return a.Store.DeleteRule(ctx, w, userID, s.namespaced()) })
	outcome(writesFor("delete"), err)
	if errors.Is(err, ErrNoRow) {
		return clientreq.New(404, "M_NOT_FOUND", "Not found")
	}
	return a.asClient(err)
}

// asClient renders an error as Synapse would.
func (a *App) asClient(err error) *clientreq.Error {
	if err == nil {
		return nil
	}
	var cerr *clientreq.Error
	if errors.As(err, &cerr) {
		return cerr
	}
	var rerr *RuleError
	if errors.As(err, &rerr) {
		return clientreq.New(rerr.Status, rerr.Errcode, rerr.Msg)
	}
	a.Log.Error().Err(err).Msg("push rules: request failed")
	return clientreq.Internal
}

// parseValue is parse_json_value_from_request: any JSON value, with
// json.loads' dict semantics.
func parseValue(body []byte) ([]byte, error) {
	b := []byte(strings.TrimSpace(string(body)))
	if !utf8.Valid(b) || !json.Valid(b) {
		return nil, errors.New("not JSON")
	}
	return pyjson.Dedupe(b)
}

// trailingSlash is SLIGHTLY_PEDANTIC_TRAILING_SLASH_ERROR.
var trailingSlash = clientreq.New(400, "M_UNRECOGNIZED", "Unrecognised request: You probably wanted a trailing slash")

// get is PushRuleRestServlet.on_GET: the ruleset, or the part of it the
// path names.
func (a *App) get(rules json.RawMessage, path string) (json.RawMessage, *clientreq.Error) {
	parts := strings.Split(path, "/")[1:]
	if len(parts) == 0 {
		return nil, trailingSlash
	}
	root := gjson.ParseBytes(rules)
	switch parts[0] {
	case "":
		return rules, nil
	case "global":
	default:
		return nil, errUnrecognized
	}
	ruleset := root.Get("global")
	parts = parts[1:]
	if len(parts) == 0 {
		return nil, trailingSlash
	}
	if parts[0] == "" {
		return json.RawMessage(ruleset.Raw), nil
	}
	kind := ruleset.Get(gjsonKey(parts[0]))
	if !kind.Exists() {
		return nil, errUnrecognized
	}
	parts = parts[1:]
	if len(parts) == 0 {
		return nil, trailingSlash
	}
	if parts[0] == "" {
		return json.RawMessage(kind.Raw), nil
	}
	var rule gjson.Result
	for _, r := range kind.Array() {
		if r.Get("rule_id").Str == parts[0] {
			rule = r // the last match, as the loop has no break
		}
	}
	if !rule.Exists() {
		return nil, clientreq.New(404, "M_NOT_FOUND", "Not found")
	}
	parts = parts[1:]
	if len(parts) == 0 {
		return json.RawMessage(rule.Raw), nil
	}
	attr := rule.Get(gjsonKey(parts[0]))
	if !attr.Exists() {
		return nil, errUnrecognized
	}
	k, _ := json.Marshal(parts[0])
	return json.RawMessage(`{` + string(k) + `:` + attr.Raw + `}`), nil
}

// errUnparseable is PushRule.from_db raising ("parsing conditions",
// "parsing actions") on a stored rule: get_push_rules_for_user then fails
// for the user, and so does everything built on it (the GETs, the copy).
var errUnparseable = errors.New("pushwriter: a stored push rule does not parse (RuntimeError: parsing conditions/actions)")

// parses is whether Synapse's Rust PushRule.from_db accepts a stored rule:
// conditions must be a list (any element: an unknown condition), actions a
// list whose strings are actions it knows. The API stores conditions that
// fail this (a dict or a string, which iterate as `in` allows).
func parses(conditions, actions string) bool {
	c, a := gjson.Parse(conditions), gjson.Parse(actions)
	if !c.IsArray() || !a.IsArray() {
		return false
	}
	ok := true
	a.ForEach(func(_, x gjson.Result) bool {
		if x.Type == gjson.String {
			ok = x.Str == "notify" || x.Str == "dont_notify" || x.Str == "coalesce"
		}
		return ok
	})
	return ok
}

// loadable is get_push_rules_for_user succeeding.
func (a *App) loadable(ctx context.Context, userID string) ([]pushrules.UserRule, map[string]bool, error) {
	rules, enabled, err := a.Store.UserRules(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range rules {
		if !parses(r.Conditions, r.Actions) {
			return nil, nil, errUnparseable
		}
	}
	return rules, enabled, nil
}

// copyRules is copy_push_rules_from_room_to_room_for_user: each enabled
// rule of the user's with a room_id condition on the old room, added again
// for the new one (its own stream ID, at the top of its class).
func (a *App) copyRules(ctx context.Context, oldRoom, newRoom, userID string) error {
	rules, enabled, err := a.loadable(ctx, userID)
	if err != nil {
		return err
	}
	for _, r := range pushrules.UserRulesInOrder(rules) {
		if e, ok := enabled[r.RuleID]; ok && !e {
			continue
		}
		conds := gjson.Parse(r.Conditions)
		match := false
		conds.ForEach(func(_, c gjson.Result) bool {
			match = c.Get("key").Str == "room_id" && c.Get("key").Type == gjson.String &&
				c.Get("pattern").Type == gjson.String && c.Get("pattern").Str == oldRoom
			return !match
		})
		if !match {
			continue
		}
		scope := r.RuleID
		if i := strings.LastIndexByte(scope, '/'); i >= 0 {
			scope = scope[:i]
		} else {
			scope = ""
		}
		newID := scope + "/" + newRoom
		var parts []string
		for _, c := range conds.Array() {
			raw := c.Raw
			if c.Get("key").Type == gjson.String && c.Get("key").Str == "room_id" {
				p, _ := json.Marshal(newRoom)
				raw = setMember(c, "pattern", string(p))
			}
			parts = append(parts, raw)
		}
		enc, err := pyjson.Encode([]byte("[" + strings.Join(parts, ",") + "]"))
		if err != nil {
			return err
		}
		if err := a.add(ctx, userID, newID, r.PriorityClass, enc, r.Actions, "", ""); err != nil {
			return err
		}
		copies.WithLabelValues("rule_copied").Inc()
	}
	return nil
}

// setMember replaces (or appends) one member of an object, keeping the
// order: dict(condition) then new_condition["pattern"] = new_room_id.
func setMember(obj gjson.Result, key, value string) string {
	var b strings.Builder
	b.WriteByte('{')
	first, done := true, false
	obj.ForEach(func(k, v gjson.Result) bool {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(k.Raw)
		b.WriteByte(':')
		if k.Str == key {
			b.WriteString(value)
			done = true
		} else {
			b.WriteString(v.Raw)
		}
		return true
	})
	if !done {
		if !first {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(key)
		b.Write(k)
		b.WriteByte(':')
		b.WriteString(value)
	}
	b.WriteByte('}')
	return b.String()
}

// gjsonKey escapes a key for gjson's path syntax.
func gjsonKey(k string) string {
	out := make([]byte, 0, len(k))
	for i := 0; i < len(k); i++ {
		switch c := k[i]; c {
		case '\\', '.', '*', '?', '|', '#', '@', '!', '=', '<', '>', '%':
			out = append(out, '\\', c)
		default:
			out = append(out, c)
		}
	}
	return string(out)
}
