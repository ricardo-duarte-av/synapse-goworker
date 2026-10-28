package creator

import (
	"context"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/acl"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// EventValidator (events/validator.py, v1.163.0): validate_builder, which
// create_event runs on the builder, and validate_new, which runs once the
// event is built. Each check answers nil when the content passes, Synapse's
// refusal when it does not, and a fallback where Python would do something
// not reproduced here. The vectors in testdata/oracle_validator.json are
// Synapse's own answers (TestValidatorAgainstSynapse).

// canonicalJSONMaxInt is CANONICALJSON_MAX_INT; the minimum is its negative.
const canonicalJSONMaxInt = 1<<53 - 1

// maxAliasLength is MAX_ALIAS_LENGTH (api/constants.py).
const maxAliasLength = 255

// jsonInt is a JSON number json.loads makes an int (no fraction, no
// exponent).
var jsonInt = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

// validateBuilder is validate_builder for the event a request asks for.
func validateBuilder(in *input, content gjson.Result, room string) *stop {
	state := in.stateKey != nil
	ensureStrings := func(keys ...string) *stop {
		for _, k := range keys {
			v, ok := pdu.Field(content, k)
			if !ok {
				return badRequest("invalid_"+typeLabel(in.typ), "'"+k+"' not in content").stop()
			}
			if v.Type != gjson.String {
				return badRequest("invalid_"+typeLabel(in.typ), "'"+k+"' not a string type").stop()
			}
		}
		return nil
	}
	ensureState := func() *stop {
		if !state {
			return badRequest("type_state_only", "'"+in.typ+"' must be state events").stop()
		}
		return nil
	}
	switch in.typ {
	case "m.room.message":
		return ensureStrings("body", "msgtype")
	case "m.room.topic":
		if s := ensureStrings("topic"); s != nil {
			return s
		}
		return ensureState()
	case "m.room.name":
		if s := ensureStrings("name"); s != nil {
			return s
		}
		return ensureState()
	case "m.room.member":
		// Reached only by a delayed member event: update_membership builds
		// a valid one.
		v, ok := pdu.Field(content, "membership")
		if !ok {
			return badRequest("invalid_member", "Content has not membership key").stop()
		}
		if v.Type != gjson.String || !membershipList[v.Str] {
			return badRequest("invalid_member", "Invalid membership key").stop()
		}
		return ensureState()
	case "m.room.tombstone":
		r, ok := pdu.Field(content, "replacement_room")
		if !ok {
			return badRequest("invalid_tombstone", "Content has no replacement_room key").stop()
		}
		if r.Type == gjson.String && r.Str == room {
			return badRequest("invalid_tombstone", "Tombstone cannot reference the room it was sent in").stop()
		}
		return ensureState()
	}
	return nil
}

// typeLabel names an event type in a metrics label.
func typeLabel(typ string) string { return strings.TrimPrefix(typ, "m.room.") }

// validateNew is validate_new past validate_builder: canonical JSON, the
// checks of aliases, retention, server ACL and power levels events, and
// m.mentions.
func (c *Creator) validateNew(in *input, raw []byte, content gjson.Result) *stop {
	// builder.build first: make_event_from_dict encodes the event (an
	// infinite float cannot be) and hands it to Rust, whose parser stops at
	// 128 levels, the event's own included.
	if s := validateCanonicalJSON(content); s != nil && s.ref != nil && s.ref.status == 500 {
		return s
	}
	if jsonDepth(content) >= maxContentDepth {
		return badRequest("deep_json", "Invalid event JSON", "M_BAD_JSON").stop()
	}
	if s := validateCanonicalJSON(content); s != nil {
		return s
	}
	switch in.typ {
	case "m.room.aliases":
		if s := validateAliases(content); s != nil {
			return s
		}
	case "m.room.retention":
		if s := validateRetention(in.stateKey != nil, content); s != nil {
			return s
		}
	case "m.room.server_acl":
		if s := validateServerACL(raw, c.o.ServerName); s != nil {
			return s
		}
	case "m.room.power_levels":
		errs, ok := powerLevelsErrors(content)
		if !ok {
			return fb("invalid_power_levels")
		}
		if e := bestMatch(errs); e != nil {
			return schemaRefusal("invalid_power_levels", e).stop()
		}
	}
	// `if "m.mentions" in event.content`: present, null included.
	if m, ok := pdu.Field(content, "m.mentions"); ok {
		errs, ok := mentionsErrors(m)
		if !ok {
			return fb("invalid_mentions")
		}
		if e := bestMatch(errs); e != nil {
			return schemaRefusal("invalid_mentions", e).stop()
		}
	}
	return nil
}

// validateCanonicalJSON is validate_canonicaljson over the content, in
// order: the first float or integer out of range is refused. A number that
// is infinite as a float never gets that far: the event cannot be built
// (make_event_from_dict cannot encode it), Synapse's 500.
func validateCanonicalJSON(content gjson.Result) *stop {
	var walk func(v gjson.Result) *stop
	inf := false
	var first *stop
	walk = func(v gjson.Result) *stop {
		switch {
		case v.Type == gjson.Number:
			if !jsonInt.MatchString(v.Raw) {
				if f, err := strconv.ParseFloat(v.Raw, 64); err != nil || math.IsInf(f, 0) {
					inf = true
				}
				if first == nil {
					first = badRequest("non_canonical_json", "Bad JSON value: float", "M_BAD_JSON").stop()
				}
				return nil
			}
			n, err := strconv.ParseInt(v.Raw, 10, 64)
			if (err != nil || n > canonicalJSONMaxInt || n < -canonicalJSONMaxInt) && first == nil {
				first = badRequest("non_canonical_json", "JSON integer out of range", "M_BAD_JSON").stop()
			}
		case v.IsArray(), v.IsObject():
			v.ForEach(func(_, x gjson.Result) bool {
				walk(x)
				return true
			})
		}
		return nil
	}
	walk(content)
	if inf {
		return internalError("float_overflow").stop()
	}
	return first
}

// validateAliases is validate_new's length check: `for alias in
// content["aliases"]: if len(alias) > MAX_ALIAS_LENGTH`. Python iterates a
// string's characters and an object's keys; what len() or the loop cannot
// take is a TypeError, Synapse's 500.
func validateAliases(content gjson.Result) *stop {
	v, ok := pdu.Field(content, "aliases")
	if !ok {
		return nil
	}
	tooLong := badRequest("alias_too_long", "Can't create aliases longer than 255 characters", "M_INVALID_PARAM").stop()
	switch {
	case v.Type == gjson.String:
		// One character each.
		return nil
	case v.IsObject():
		var s *stop
		v.ForEach(func(k, _ gjson.Result) bool {
			if utf8.RuneCountInString(k.Str) > maxAliasLength {
				s = tooLong
			}
			return s == nil
		})
		if s != nil && loneSurrogate(v.Raw) {
			return fb("invalid_aliases")
		}
		return s
	case v.IsArray():
		for _, a := range v.Array() {
			n := 0
			switch {
			case a.Type == gjson.String:
				if loneSurrogate(a.Raw) {
					return fb("invalid_aliases")
				}
				n = utf8.RuneCountInString(a.Str)
			case a.IsArray():
				n = len(a.Array())
			case a.IsObject():
				a.ForEach(func(_, _ gjson.Result) bool { n++; return true })
			default:
				return internalError("invalid_aliases").stop()
			}
			if n > maxAliasLength {
				return tooLong
			}
		}
		return nil
	}
	return internalError("invalid_aliases").stop()
}

// validateRetention is _validate_retention: a state event, with integer
// min_lifetime and max_lifetime when given, and min <= max.
func validateRetention(state bool, content gjson.Result) *stop {
	if !state {
		return badRequest("retention_not_state", "must be a state event").stop()
	}
	lifetime := func(k string) (n int64, has bool, s *stop) {
		v := pdu.Get(content, k)
		if !v.Exists() || v.Type == gjson.Null {
			return 0, false, nil
		}
		// type(x) is not int: bools are refused; floats were, as
		// non-canonical JSON.
		if !isPyInt(v) {
			return 0, true, badRequest("invalid_retention", "'"+k+"' must be an integer", "M_BAD_JSON").stop()
		}
		n, err := strconv.ParseInt(v.Raw, 10, 64)
		if err != nil {
			return 0, true, fb("invalid_retention")
		}
		return n, true, nil
	}
	lo, hasLo, s := lifetime("min_lifetime")
	if s != nil {
		return s
	}
	hi, hasHi, s := lifetime("max_lifetime")
	if s != nil {
		return s
	}
	if hasLo && hasHi && lo > hi {
		return badRequest("invalid_retention", "'min_lifetime' can't be greater than 'max_lifetime", "M_BAD_JSON").stop()
	}
	return nil
}

// validateServerACL is validate_new's refusal of an ACL that denies the
// local server (server_acl_evaluator_from_event, then
// server_matches_acl_event).
func validateServerACL(raw []byte, serverName string) *stop {
	a, err := acl.ParseServerACL([]byte(`{"content":` + string(raw) + `}`))
	if err != nil {
		return fb("invalid_server_acl")
	}
	if !a.Allowed(serverName) {
		return badRequest("acl_denies_us", "Can't create an ACL event that denies the local server").stop()
	}
	return nil
}

// maxContentDepth is the nesting of an event's content, counting the content
// object, at which Synapse cannot build the event (serde_json's recursion
// limit of 128, with the event around it), checked in its container.
const maxContentDepth = 127

// jsonDepth is how many containers deep a value goes: 0 for a scalar.
func jsonDepth(v gjson.Result) int {
	if !v.IsObject() && !v.IsArray() {
		return 0
	}
	d := 0
	v.ForEach(func(_, x gjson.Result) bool {
		d = max(d, jsonDepth(x))
		return true
	})
	return d + 1
}

// isPyInt is an int as json.loads decodes it: never a bool, a float or a
// string.
func isPyInt(v gjson.Result) bool {
	return v.Type == gjson.Number && jsonInt.MatchString(v.Raw)
}

// publicRoom is room_stats_state.join_rules being public, read from the
// state before the event.
func (c *Creator) publicRoom(ctx context.Context, before pdu.StateMap) (bool, error) {
	id, ok := before[pdu.StateKey{Type: "m.room.join_rules"}]
	if !ok {
		return false, nil
	}
	evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{id})
	if err != nil {
		return false, err
	}
	se := evs[id]
	if se == nil {
		return false, nil
	}
	rule := pdu.Get(se.Content, "join_rule")
	return rule.Type == gjson.String && rule.Str == "public", nil
}
