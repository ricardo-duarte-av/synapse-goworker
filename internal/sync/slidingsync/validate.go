package slidingsync

import (
	"fmt"
	"math/big"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// BodyError is a request Synapse refuses before doing anything: 400, with
// the errcode and message parse_and_validate_json_object_from_request gives.
type BodyError struct {
	ErrCode string
	Message string
}

func (e *BodyError) Error() string { return e.Message }

// ValidateBody is parse_json_object_from_request(allow_empty_body=False)
// then validate_json_object(content, SlidingSyncBody): Synapse's pydantic
// model (types/rest/client/__init__.py) ported rule for rule -- every field
// strict, extra keys ignored, fields checked in the model's order and dict
// entries in the body's, all errors collected -- and the message
// _format_pydantic_error builds from them. testdata/validate.json holds
// Synapse's own answers for the cases this was checked against.
func ValidateBody(body []byte) *BodyError {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return &BodyError{ErrCode: "M_NOT_JSON", Message: "Content not JSON."}
	}
	root := gjson.ParseBytes(body)
	if !root.IsObject() {
		return &BodyError{ErrCode: "M_BAD_JSON", Message: "Content must be a JSON object."}
	}
	v := &validator{}
	v.body(root, start(root))
	return v.result()
}

// verr is one pydantic error: its type and the path _format_pydantic_error
// derives from its loc by walking the request body.
type verr struct {
	typ, path, msg string
}

type validator struct {
	errs []verr
}

func (v *validator) add(typ string, p loc, msg string) {
	v.errs = append(v.errs, verr{typ: typ, path: p.path, msg: msg})
}

// result is validate_json_object's SynapseError: the errcode from the one
// error's type when there is exactly one, M_BAD_JSON otherwise; the messages
// deduplicated in order and joined with "; ".
func (v *validator) result() *BodyError {
	if len(v.errs) == 0 {
		return nil
	}
	code := "M_BAD_JSON"
	if len(v.errs) == 1 {
		switch v.errs[0].typ {
		case "missing":
			code = "M_MISSING_PARAM"
		case "value_error":
			code = "M_INVALID_PARAM"
		}
	}
	var parts []string
	seen := map[string]bool{}
	for _, e := range v.errs {
		var msg string
		switch e.typ {
		case "missing":
			msg = fmt.Sprintf("Missing required field '%s'", e.path)
		case "value_error":
			msg = e.msg
		case "model_type":
			msg = fmt.Sprintf("'%s' must be an object", e.path)
		default:
			msg = fmt.Sprintf("'%s': %s", e.path, e.msg)
		}
		if !seen[msg] {
			seen[msg] = true
			parts = append(parts, msg)
		}
	}
	return &BodyError{ErrCode: code, Message: strings.Join(parts, "; ")}
}

// loc is a position in pydantic's loc together with the path the formatter
// has built for it: a segment extends the path only while the body can be
// followed -- a string into an object, an index into an array -- and the
// first segment that cannot be followed stops it for good.
type loc struct {
	path   string
	cur    gjson.Result
	broken bool
}

func start(root gjson.Result) loc { return loc{cur: root} }

func (l loc) key(name string) loc {
	if l.broken || !l.cur.IsObject() {
		return loc{path: l.path, broken: true}
	}
	return loc{path: l.path + "." + name, cur: entry(l.cur, name)}
}

func (l loc) index(i int) loc {
	if l.broken || !l.cur.IsArray() {
		return loc{path: l.path, broken: true}
	}
	arr := l.cur.Array()
	out := loc{path: fmt.Sprintf("%s[%d]", l.path, i)}
	if i < len(arr) {
		out.cur = arr[i]
	}
	return out
}

// entry is a dict lookup as Python's json gives it: the last of duplicate keys.
func entry(obj gjson.Result, name string) gjson.Result {
	var out gjson.Result
	obj.ForEach(func(k, val gjson.Result) bool {
		if k.String() == name {
			out = val
		}
		return true
	})
	return out
}

// entries are an object's keys in first-seen order, each with its last value.
func entries(obj gjson.Result) (keys []string, vals map[string]gjson.Result) {
	vals = map[string]gjson.Result{}
	obj.ForEach(func(k, val gjson.Result) bool {
		name := k.String()
		if _, ok := vals[name]; !ok {
			keys = append(keys, name)
		}
		vals[name] = val
		return true
	})
	return keys, vals
}

// field is one model field: absent means the default, so only a required
// field can fail by not being there.
func field(obj gjson.Result, name string) (gjson.Result, bool) {
	_, vals := entries(obj)
	val, ok := vals[name]
	return val, ok
}

func isNull(r gjson.Result) bool { return r.Type == gjson.Null }

// isInt is a StrictInt: a JSON integer literal, never a float or a bool.
func isInt(r gjson.Result) bool {
	if r.Type != gjson.Number {
		return false
	}
	return !strings.ContainsAny(r.Raw, ".eE")
}

func bigInt(r gjson.Result) *big.Int {
	n, _ := new(big.Int).SetString(r.Raw, 10)
	if n == nil {
		n = new(big.Int)
	}
	return n
}

// The model's field types.

func (v *validator) strictInt(r gjson.Result, p loc, ge, le *int64) bool {
	if !isInt(r) {
		v.add("int_type", p, "Input should be a valid integer")
		return false
	}
	n := bigInt(r)
	if ge != nil && n.Cmp(big.NewInt(*ge)) < 0 {
		v.add("greater_than_equal", p, fmt.Sprintf("Input should be greater than or equal to %d", *ge))
		return false
	}
	if le != nil && n.Cmp(big.NewInt(*le)) > 0 {
		v.add("less_than_equal", p, fmt.Sprintf("Input should be less than or equal to %d", *le))
		return false
	}
	return true
}

func (v *validator) strictBool(r gjson.Result, p loc) {
	if r.Type != gjson.True && r.Type != gjson.False {
		v.add("bool_type", p, "Input should be a valid boolean")
	}
}

func (v *validator) strictStr(r gjson.Result, p loc) bool {
	if r.Type != gjson.String {
		v.add("string_type", p, "Input should be a valid string")
		return false
	}
	return true
}

func (v *validator) strList(r gjson.Result, p loc, nullableItems bool) {
	if !r.IsArray() {
		v.add("list_type", p, "Input should be a valid list")
		return
	}
	for i, item := range r.Array() {
		if nullableItems && isNull(item) {
			continue
		}
		v.strictStr(item, p.index(i))
	}
}

// pair is tuple[X, X] (not strict): an array, its first two items checked,
// a missing one reported as missing, and more than two refused outright.
func (v *validator) pair(r gjson.Result, p loc, item func(gjson.Result, loc)) {
	if !r.IsArray() {
		v.add("tuple_type", p, "Input should be a valid tuple")
		return
	}
	arr := r.Array()
	if len(arr) > 2 {
		v.add("too_long", p, fmt.Sprintf("Tuple should have at most 2 items after validation, not %d", len(arr)))
		return
	}
	for i := 0; i < 2; i++ {
		if i < len(arr) {
			item(arr[i], p.index(i))
		} else {
			v.add("missing", p.index(i), "")
		}
	}
}

// model checks that a value is an object before its fields are.
func (v *validator) model(r gjson.Result, p loc, fields func(gjson.Result, loc)) {
	if !r.IsObject() {
		v.add("model_type", p, "")
		return
	}
	fields(r, p)
}

// optional is `X | None`: null is accepted, absent takes the default.
func optional(obj gjson.Result, name string, p loc, check func(gjson.Result, loc)) {
	val, ok := field(obj, name)
	if !ok || isNull(val) {
		return
	}
	check(val, p.key(name))
}

// defaulted is a field with a default that is not nullable.
func defaulted(obj gjson.Result, name string, p loc, check func(gjson.Result, loc)) {
	if val, ok := field(obj, name); ok {
		check(val, p.key(name))
	}
}

func (v *validator) required(obj gjson.Result, name string, p loc, check func(gjson.Result, loc)) {
	val, ok := field(obj, name)
	if !ok {
		v.add("missing", p.key(name), "")
		return
	}
	check(val, p.key(name))
}

var (
	zero = int64(0)
	max  = int64(MaxTimelineLimit)
)

// SlidingSyncBody.
func (v *validator) body(r gjson.Result, p loc) {
	optional(r, "conn_id", p, func(x gjson.Result, q loc) { v.strictStr(x, q) })
	optional(r, "lists", p, func(x gjson.Result, q loc) {
		if !x.IsObject() {
			v.add("dict_type", q, "Input should be a valid dictionary")
			return
		}
		before := len(v.errs)
		keys, vals := entries(x)
		for _, k := range keys {
			// dict[constr(max_length=64), SlidingSyncList]: the key, then the
			// value. A key's loc ends in the "[key]" marker.
			if utf8.RuneCountInString(k) > MaxListKeyBytes {
				v.add("string_too_long", q.key(k).key("[key]"),
					fmt.Sprintf("String should have at most %d characters", MaxListKeyBytes))
			}
			v.model(vals[k], q.key(k), v.list)
		}
		// lists_length_check runs only once the dict itself validated.
		if len(v.errs) == before && len(keys) > MaxLists {
			v.add("assertion_error", q, fmt.Sprintf("Assertion failed, Max lists: %d but saw %d", MaxLists, len(keys)))
		}
	})
	optional(r, "room_subscriptions", p, func(x gjson.Result, q loc) {
		if !x.IsObject() {
			v.add("dict_type", q, "Input should be a valid dictionary")
			return
		}
		keys, vals := entries(x)
		for _, k := range keys {
			v.model(vals[k], q.key(k), v.common)
		}
	})
	optional(r, "extensions", p, func(x gjson.Result, q loc) { v.model(x, q, v.extensions) })
}

// CommonRoomParameters.
func (v *validator) common(r gjson.Result, p loc) {
	v.required(r, "required_state", p, func(x gjson.Result, q loc) {
		if !x.IsArray() {
			v.add("list_type", q, "Input should be a valid list")
			return
		}
		for i, item := range x.Array() {
			v.pair(item, q.index(i), func(s gjson.Result, sq loc) { v.strictStr(s, sq) })
		}
	})
	v.required(r, "timeline_limit", p, func(x gjson.Result, q loc) { v.strictInt(x, q, nil, &max) })
}

// SlidingSyncList.
func (v *validator) list(r gjson.Result, p loc) {
	v.common(r, p)
	optional(r, "ranges", p, func(x gjson.Result, q loc) {
		if !x.IsArray() {
			v.add("list_type", q, "Input should be a valid list")
			return
		}
		for i, item := range x.Array() {
			v.pair(item, q.index(i), func(n gjson.Result, nq loc) { v.strictInt(n, nq, &zero, nil) })
		}
	})
	optional(r, "slow_get_all_rooms", p, func(x gjson.Result, q loc) { v.strictBool(x, q) })
	optional(r, "filters", p, func(x gjson.Result, q loc) { v.model(x, q, v.filters) })
}

// SlidingSyncList.Filters.
func (v *validator) filters(r gjson.Result, p loc) {
	b := func(x gjson.Result, q loc) { v.strictBool(x, q) }
	s := func(x gjson.Result, q loc) { v.strictStr(x, q) }
	strs := func(x gjson.Result, q loc) { v.strList(x, q, false) }
	types := func(x gjson.Result, q loc) { v.strList(x, q, true) }
	optional(r, "is_dm", p, b)
	optional(r, "spaces", p, strs)
	optional(r, "is_encrypted", p, b)
	optional(r, "is_invite", p, b)
	optional(r, "room_types", p, types)
	optional(r, "not_room_types", p, types)
	optional(r, "room_name_like", p, s)
	optional(r, "tags", p, strs)
	optional(r, "not_tags", p, strs)
}

const (
	stickyKey   = "org.matrix.msc4354.sticky_events"
	profilesKey = "org.matrix.msc4262.profiles"
	threadsKey  = "io.element.msc4308.thread_subscriptions"
	absentTag   = "json-or-python[json=function-plain[_reject_from_json()],python=is-instance[AbsentType]]"
)

// Extensions.
func (v *validator) extensions(r gjson.Result, p loc) {
	b := func(x gjson.Result, q loc) { v.strictBool(x, q) }
	strs := func(x gjson.Result, q loc) { v.strList(x, q, false) }
	scoped := func(x gjson.Result, q loc) {
		defaulted(x, "enabled", q, b)
		defaulted(x, "lists", q, strs)
		defaulted(x, "rooms", q, strs)
	}
	optional(r, "to_device", p, func(x gjson.Result, q loc) {
		v.model(x, q, func(m gjson.Result, mq loc) {
			optional(m, "enabled", mq, b)
			defaulted(m, "limit", mq, func(n gjson.Result, nq loc) { v.strictInt(n, nq, nil, nil) })
			optional(m, "since", mq, func(s gjson.Result, sq loc) {
				if v.strictStr(s, sq) && !pythonInt(s.String()) {
					v.add("value_error", sq, "'extensions.to_device.since' is invalid (should look like an int)")
				}
			})
		})
	})
	optional(r, "e2ee", p, func(x gjson.Result, q loc) {
		v.model(x, q, func(m gjson.Result, mq loc) { defaulted(m, "enabled", mq, b) })
	})
	for _, name := range []string{"account_data", "receipts", "typing"} {
		optional(r, name, p, func(x gjson.Result, q loc) { v.model(x, q, scoped) })
	}
	optional(r, threadsKey, p, func(x gjson.Result, q loc) {
		v.model(x, q, func(m gjson.Result, mq loc) {
			defaulted(m, "enabled", mq, b)
			defaulted(m, "limit", mq, func(n gjson.Result, nq loc) { v.strictInt(n, nq, nil, nil) })
		})
	})
	// `Model | AbsentType`: a key that is present must hold the model; if
	// it does not, both members of the union report.
	if x, ok := field(r, stickyKey); ok {
		v.union(x, p.key(stickyKey), "StickyEventsExtension", func(m gjson.Result, mq loc) {
			defaulted(m, "enabled", mq, b)
			defaulted(m, "limit", mq, func(n gjson.Result, nq loc) { v.strictInt(n, nq, &zero, nil) })
			defaulted(m, "lists", mq, strs)
			defaulted(m, "rooms", mq, strs)
		})
	}
	if x, ok := field(r, profilesKey); ok {
		v.union(x, p.key(profilesKey), "ProfilesExtension", func(m gjson.Result, mq loc) {
			defaulted(m, "enabled", mq, b)
			// fields is itself `list[str] | AbsentType`.
			if f, ok := field(m, "fields"); ok {
				fq := mq.key("fields")
				sub := &validator{}
				sub.strList(f, fq, false)
				if len(sub.errs) > 0 {
					v.errs = append(v.errs, sub.errs...)
					v.add("is_instance_of", fq, "Input should be an instance of AbsentType")
				}
			}
		})
	}
}

// union validates `Model | AbsentType`. Every loc inside a member starts with
// the member's tag, which the formatter can follow only into an object and
// never past it: the model member's errors all land on "<path>.<Tag>", the
// absent member's on "<path>.<absent tag>" (or on <path> for a value that is
// not an object).
func (v *validator) union(r gjson.Result, p loc, tag string, fields func(gjson.Result, loc)) {
	before := len(v.errs)
	member := p.key(tag)
	member.broken = true // nothing past the tag can be followed
	if r.IsObject() {
		fields(r, member)
	} else {
		v.add("model_type", member, "")
	}
	if len(v.errs) == before {
		return
	}
	v.add("is_instance_of", p.key(absentTag), "Input should be an instance of AbsentType")
}

// PythonIntASCII is Python's int() on bytes (parse_integer reads the query
// argument undecoded): ASCII whitespace, an optional sign, ASCII digits with
// single underscores between them. Values beyond 2**53 are refused.
func PythonIntASCII(s string) (int, bool) {
	t := strings.Trim(s, " \t\n\r\v\f")
	neg := false
	if t != "" && (t[0] == '+' || t[0] == '-') {
		neg = t[0] == '-'
		t = t[1:]
	}
	if t == "" {
		return 0, false
	}
	n, prevDigit := 0, false
	for i := 0; i < len(t); i++ {
		c := t[i]
		switch {
		case c >= '0' && c <= '9':
			if n > (1<<53)/10 {
				return 0, false
			}
			n = n*10 + int(c-'0')
			prevDigit = true
		case c == '_' && prevDigit:
			prevDigit = false
		default:
			return 0, false
		}
	}
	if !prevDigit {
		return 0, false
	}
	if neg {
		n = -n
	}
	return n, true
}

// pythonInt is whether Python's int(s) accepts s: surrounding whitespace, an
// optional sign, then decimal digits (any script's), with single underscores
// allowed between digits.
func pythonInt(s string) bool {
	s = strings.TrimFunc(s, unicode.IsSpace)
	if s != "" && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	prevDigit := false
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Nd, r):
			prevDigit = true
		case r == '_' && prevDigit:
			prevDigit = false
		default:
			return false
		}
	}
	return prevDigit
}
