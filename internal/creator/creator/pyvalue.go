package creator

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"unicode"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// Synapse writes some refusals with Python's repr of a JSON value, as
// json.loads decoded it: jsonschema's messages, and some of
// _check_power_levels'. pyRepr reproduces it for the values that can reach
// those messages; ok is false where it cannot be sure, and the request is
// then Synapse's.

// loneSurrogate reports whether raw JSON escapes a lone UTF-16 surrogate.
// Python keeps one in the string, where Go has U+FFFD, and Synapse cannot
// encode it. A high surrogate escape followed at once by a low one is a
// pair, one character to both (py_scanstring).
func loneSurrogate[T string | []byte](raw T) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		u, ok := unicodeEscape(raw, i)
		if !ok {
			i++ // \\, \" and the like
			continue
		}
		switch {
		case u >= 0xdc00 && u <= 0xdfff:
			return true
		case u >= 0xd800 && u <= 0xdbff:
			if lo, ok := unicodeEscape(raw, i+6); !ok || lo < 0xdc00 || lo > 0xdfff {
				return true
			}
			i += 6
		}
		i += 5
	}
	return false
}

// unicodeEscape is the \uXXXX escape at raw[i:], if there is one.
func unicodeEscape[T string | []byte](raw T, i int) (rune, bool) {
	if i+6 > len(raw) || raw[i] != '\\' || raw[i+1] != 'u' {
		return 0, false
	}
	n, err := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
	return rune(n), err == nil
}

// pyRepr is repr(json.loads(v.Raw)).
func pyRepr(v gjson.Result) (string, bool) {
	var b strings.Builder
	if !writeRepr(&b, v) {
		return "", false
	}
	return b.String(), true
}

func writeRepr(b *strings.Builder, v gjson.Result) bool {
	switch v.Type {
	case gjson.Null:
		b.WriteString("None")
	case gjson.True:
		b.WriteString("True")
	case gjson.False:
		b.WriteString("False")
	case gjson.Number:
		// Floats are refused as non-canonical JSON before any message that
		// shows a value; only integers are written.
		if !jsonInt.MatchString(v.Raw) {
			return false
		}
		n := strings.TrimPrefix(v.Raw, "-")
		if n == "0" {
			b.WriteString("0")
		} else {
			b.WriteString(v.Raw)
		}
	case gjson.String:
		if loneSurrogate(v.Raw) {
			return false
		}
		s, ok := reprString(v.Str)
		if !ok {
			return false
		}
		b.WriteString(s)
	case gjson.JSON:
		if v.IsArray() {
			b.WriteByte('[')
			ok := true
			for i, x := range v.Array() {
				if i > 0 {
					b.WriteString(", ")
				}
				if ok = writeRepr(b, x); !ok {
					break
				}
			}
			b.WriteByte(']')
			return ok
		}
		b.WriteByte('{')
		ok, first := true, true
		v.ForEach(func(k, x gjson.Result) bool {
			if !first {
				b.WriteString(", ")
			}
			first = false
			if ok = writeRepr(b, k); !ok {
				return false
			}
			b.WriteString(": ")
			ok = writeRepr(b, x)
			return ok
		})
		b.WriteByte('}')
		return ok
	default:
		return false
	}
	return true
}

// reprString is unicode_repr (Objects/unicodeobject.c).
func reprString(s string) (string, bool) {
	quote := byte('\'')
	if strings.IndexByte(s, '\'') >= 0 && strings.IndexByte(s, '"') < 0 {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < ' ' || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x7f:
			b.WriteRune(r)
		case unicode.IsPrint(r):
			b.WriteRune(r)
		case !unprintable(r):
			// Unassigned in Go's Unicode tables, perhaps not in Python's.
			return "", false
		case r <= 0xff:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r <= 0xffff:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteByte(quote)
	return b.String(), true
}

// unprintable is the categories str.isprintable refuses in every Unicode
// version: other (but not unassigned) and separators.
func unprintable(r rune) bool {
	return unicode.In(r, unicode.Cc, unicode.Cf, unicode.Co, unicode.Cs, unicode.Zs, unicode.Zl, unicode.Zp)
}

// schemaError is a jsonschema ValidationError: where, and its message.
type schemaError struct {
	path []string
	msg  string
	// indexed is a path ending in an array index, which
	// _validation_error_to_api_error cannot join: Synapse's 500.
	indexed bool
}

// bestMatch is jsonschema.exceptions.best_match with by_relevance (4.26):
// the shallowest error, then the greatest path among siblings.
func bestMatch(errs []schemaError) *schemaError {
	var best *schemaError
	for i := range errs {
		e := &errs[i]
		if best == nil || len(e.path) < len(best.path) ||
			len(e.path) == len(best.path) && comparePaths(e.path, best.path) > 0 {
			best = e
		}
	}
	return best
}

func comparePaths(a, b []string) int {
	for i := range a {
		if c := strings.Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}

// schemaRefusal is _validation_error_to_api_error.
func schemaRefusal(reason string, e *schemaError) *refusal {
	if e.indexed {
		return internalError(reason)
	}
	msg := e.msg
	if len(e.path) > 0 {
		msg = `"` + e.path[len(e.path)-1] + `": ` + msg
	}
	return &refusal{reason: reason, status: 400, errcode: "M_BAD_JSON", msg: msg}
}

// typeError is jsonschema's type message: "<repr> is not of type '<type>'".
func typeError(path []string, v gjson.Result, typ string) (schemaError, bool) {
	r, ok := pyRepr(v)
	return schemaError{path: path, msg: r + " is not of type '" + typ + "'"}, ok
}

var (
	plInts    = []string{"ban", "events_default", "invite", "kick", "redact", "state_default", "users_default"}
	plObjects = []string{"events", "notifications", "users"}
)

// powerLevelsErrors validates POWER_LEVELS_SCHEMA. Integers out of range are
// refused before, as non-canonical JSON. ok is false when an error's message
// cannot be written.
func powerLevelsErrors(content gjson.Result) ([]schemaError, bool) {
	var errs []schemaError
	for _, k := range plInts {
		if v, ok := pdu.Field(content, k); ok && !isPyInt(v) {
			e, ok := typeError([]string{k}, v, "integer")
			if !ok {
				return nil, false
			}
			errs = append(errs, e)
		}
	}
	for _, k := range plObjects {
		v, ok := pdu.Field(content, k)
		if !ok {
			continue
		}
		if !v.IsObject() {
			e, ok := typeError([]string{k}, v, "object")
			if !ok {
				return nil, false
			}
			errs = append(errs, e)
			continue
		}
		good := true
		v.ForEach(func(key, x gjson.Result) bool {
			if !isPyInt(x) {
				var e schemaError
				if e, good = typeError([]string{k, key.Str}, x, "integer"); good {
					errs = append(errs, e)
				}
			}
			return good
		})
		if !good {
			return nil, false
		}
	}
	return errs, true
}

// mentionsErrors validates MENTIONS_SCHEMA against content["m.mentions"],
// with Synapse's type checker: a string is a Sequence, so an array.
func mentionsErrors(m gjson.Result) ([]schemaError, bool) {
	if !m.IsObject() {
		e, ok := typeError(nil, m, "object")
		return []schemaError{e}, ok
	}
	var errs []schemaError
	if u, ok := pdu.Field(m, "user_ids"); ok {
		switch {
		case u.IsArray():
			for i, x := range u.Array() {
				if x.Type != gjson.String {
					errs = append(errs, schemaError{path: []string{"user_ids", strconv.Itoa(i)}, indexed: true})
				}
			}
		case u.Type == gjson.String:
		default:
			e, ok := typeError([]string{"user_ids"}, u, "array")
			if !ok {
				return nil, false
			}
			errs = append(errs, e)
		}
	}
	if r, ok := pdu.Field(m, "room"); ok && r.Type != gjson.True && r.Type != gjson.False {
		e, ok := typeError([]string{"room"}, r, "boolean")
		if !ok {
			return nil, false
		}
		errs = append(errs, e)
	}
	return errs, true
}

// pyEqual is == between two values json.loads decoded: bools are the ints
// 1 and 0, ints and floats compare by value, dicts by their items, lists
// by their elements.
func pyEqual(a, b gjson.Result) bool {
	an, aNum := pyNumber(a)
	bn, bNum := pyNumber(b)
	switch {
	case aNum || bNum:
		return aNum && bNum && an.Cmp(bn) == 0
	case a.Type == gjson.String || b.Type == gjson.String:
		return a.Type == b.Type && a.Str == b.Str
	case a.Type == gjson.Null || b.Type == gjson.Null:
		return a.Type == b.Type
	case a.IsArray() || b.IsArray():
		if !a.IsArray() || !b.IsArray() {
			return false
		}
		x, y := a.Array(), b.Array()
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if !pyEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case a.IsObject() && b.IsObject():
		x, y := a.Map(), b.Map()
		if len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !pyEqual(v, w) {
				return false
			}
		}
		return true
	}
	return false
}

// pyNumber is a number, or a bool as its int, exactly.
func pyNumber(v gjson.Result) (*big.Rat, bool) {
	switch v.Type {
	case gjson.True:
		return big.NewRat(1, 1), true
	case gjson.False:
		return new(big.Rat), true
	case gjson.Number:
		r, ok := new(big.Rat).SetString(v.Raw)
		if !ok {
			return nil, false
		}
		if !jsonInt.MatchString(v.Raw) {
			// A float: what float(text) is, exactly.
			f, err := strconv.ParseFloat(v.Raw, 64)
			if err != nil {
				return nil, false
			}
			if r = new(big.Rat); r.SetFloat64(f) == nil {
				// inf: equal to nothing a canonical event holds.
				return nil, false
			}
		}
		return r, true
	}
	return nil, false
}
