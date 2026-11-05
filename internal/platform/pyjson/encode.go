package pyjson

import (
	"errors"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// ErrOutOfRange is a float Python's json reads as inf, which Synapse's
// json_encoder (allow_nan=False) refuses to write: "Out of range float
// values are not JSON compliant".
var ErrOutOfRange = errors.New("pyjson: Out of range float values are not JSON compliant")

// Encode is json_encoder.encode(json.loads(raw)), Synapse's
// json.JSONEncoder(allow_nan=False, separators=(",", ":")): what Synapse
// stores when it writes a value it parsed, such as a receipt's data.
//
//   - objects keep their keys in the order received, a repeated key once,
//     at its first position, with its last value (dict assignment);
//   - strings are ensure_ascii: everything outside ' '..'~' as \uXXXX,
//     lowercase, astral characters as surrogate pairs, and a lone
//     surrogate escape kept as it came;
//   - a number with neither '.' nor an exponent is a Python int, written
//     as its value ("-0" is "0"); any other is a float, written as
//     float.__repr__ ("1e5" is "100000.0", "1e16" is "1e+16"), and one
//     out of range is ErrOutOfRange.
func Encode(raw []byte) (string, error) {
	return encode(raw, options{ensureASCII: true})
}

// ErrLoneSurrogate is a lone surrogate in a string, which canonicaljson's
// .encode("utf-8") refuses (UnicodeEncodeError).
var ErrLoneSurrogate = errors.New("pyjson: a lone surrogate cannot be encoded as UTF-8")

// CanonicalLen is len(encode_canonical_json(json.loads(raw))): canonicaljson
// 2.0's ensure_ascii=False, sort_keys=True, compact JSON, as UTF-8 bytes,
// with Python's float repr. Synapse sizes to-device messages and EDUs by it.
func CanonicalLen(raw []byte) (int, error) {
	s, err := Canonical(raw)
	return len(s), err
}

// Canonical is encode_canonical_json(json.loads(raw)) as a string.
func Canonical(raw []byte) (string, error) {
	return encode(raw, options{sortKeys: true})
}

type options struct {
	ensureASCII bool
	sortKeys    bool
}

func encode(raw []byte, o options) (string, error) {
	if !gjson.ValidBytes(raw) {
		return "", ErrInvalid
	}
	var sb strings.Builder
	if err := encodeValue(&sb, gjson.ParseBytes(raw), o); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func encodeValue(sb *strings.Builder, v gjson.Result, o options) error {
	switch {
	case v.IsObject():
		type member struct {
			key   []rune
			value gjson.Result
		}
		var order []string
		members := map[string]*member{}
		var err error
		v.ForEach(func(k, x gjson.Result) bool {
			key, kerr := decodeString(k.Raw)
			if kerr != nil {
				err = kerr
				return false
			}
			id := string(runesKey(key))
			if m, ok := members[id]; ok {
				m.value = x
			} else {
				members[id] = &member{key, x}
				order = append(order, id)
			}
			return true
		})
		if err != nil {
			return err
		}
		if o.sortKeys {
			// sorted() of str keys: by code point.
			sort.Slice(order, func(i, j int) bool {
				return slices.Compare(members[order[i]].key, members[order[j]].key) < 0
			})
		}
		sb.WriteByte('{')
		for i, id := range order {
			if i > 0 {
				sb.WriteByte(',')
			}
			m := members[id]
			if err := writeStr(sb, m.key, o); err != nil {
				return err
			}
			sb.WriteByte(':')
			if err := encodeValue(sb, m.value, o); err != nil {
				return err
			}
		}
		sb.WriteByte('}')
	case v.IsArray():
		sb.WriteByte('[')
		i := 0
		var err error
		v.ForEach(func(_, x gjson.Result) bool {
			if i > 0 {
				sb.WriteByte(',')
			}
			i++
			err = encodeValue(sb, x, o)
			return err == nil
		})
		if err != nil {
			return err
		}
		sb.WriteByte(']')
	case v.Type == gjson.String:
		s, err := decodeString(v.Raw)
		if err != nil {
			return err
		}
		if err := writeStr(sb, s, o); err != nil {
			return err
		}
	case v.Type == gjson.Number:
		return writeNumber(sb, v.Raw)
	case v.Type == gjson.True:
		sb.WriteString("true")
	case v.Type == gjson.False:
		sb.WriteString("false")
	default:
		sb.WriteString("null")
	}
	return nil
}

// runesKey makes a map key of decoded code points, lone surrogates
// included (which a Go string cannot hold as such).
func runesKey(rs []rune) []byte {
	out := make([]byte, 0, len(rs)*4)
	for _, r := range rs {
		out = append(out, byte(r>>24), byte(r>>16), byte(r>>8), byte(r))
	}
	return out
}

// decodeString is a JSON string literal as Python holds it: code points,
// an escaped surrogate pair joined into one, a lone one kept.
func decodeString(lit string) ([]rune, error) {
	if len(lit) < 2 || lit[0] != '"' || lit[len(lit)-1] != '"' {
		return nil, ErrInvalid
	}
	s := lit[1 : len(lit)-1]
	var out []rune
	for i := 0; i < len(s); {
		c := s[i]
		if c != '\\' {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size <= 1 {
				return nil, ErrInvalid
			}
			out = append(out, r)
			i += size
			continue
		}
		if i+1 >= len(s) {
			return nil, ErrInvalid
		}
		switch s[i+1] {
		case '"', '\\', '/':
			out = append(out, rune(s[i+1]))
			i += 2
		case 'b':
			out, i = append(out, '\b'), i+2
		case 'f':
			out, i = append(out, '\f'), i+2
		case 'n':
			out, i = append(out, '\n'), i+2
		case 'r':
			out, i = append(out, '\r'), i+2
		case 't':
			out, i = append(out, '\t'), i+2
		case 'u':
			if i+6 > len(s) {
				return nil, ErrInvalid
			}
			n, err := strconv.ParseUint(s[i+2:i+6], 16, 32)
			if err != nil {
				return nil, ErrInvalid
			}
			r := rune(n)
			i += 6
			// json.decoder: a high surrogate followed by an escaped low one
			// is one character.
			if r >= 0xd800 && r <= 0xdbff && i+6 <= len(s) && s[i] == '\\' && s[i+1] == 'u' {
				if lo, err := strconv.ParseUint(s[i+2:i+6], 16, 32); err == nil && lo >= 0xdc00 && lo <= 0xdfff {
					r = 0x10000 + (r-0xd800)<<10 + (rune(lo) - 0xdc00)
					i += 6
				}
			}
			out = append(out, r)
		default:
			return nil, ErrInvalid
		}
	}
	return out, nil
}

func writeStr(sb *strings.Builder, s []rune, o options) error {
	if o.ensureASCII {
		writeString(sb, s)
		return nil
	}
	return writeStringUTF8(sb, s)
}

// writeStringUTF8 is py_encode_basestring (ensure_ascii=False): only '"',
// '\\' and the control characters escaped, everything else as itself.
func writeStringUTF8(sb *strings.Builder, s []rune) error {
	const hex = "0123456789abcdef"
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		default:
			switch {
			case r < 0x20:
				sb.WriteString(`\u00`)
				sb.WriteByte(hex[r>>4])
				sb.WriteByte(hex[r&0xf])
			case r >= 0xd800 && r <= 0xdfff:
				return ErrLoneSurrogate
			default:
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return nil
}

// writeString is py_encode_basestring_ascii.
func writeString(sb *strings.Builder, s []rune) {
	const hex = "0123456789abcdef"
	u := func(r rune) {
		sb.WriteString(`\u`)
		sb.WriteByte(hex[r>>12&0xf])
		sb.WriteByte(hex[r>>8&0xf])
		sb.WriteByte(hex[r>>4&0xf])
		sb.WriteByte(hex[r&0xf])
	}
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		default:
			switch {
			case r >= ' ' && r <= '~':
				sb.WriteRune(r)
			case r >= 0x10000:
				v := r - 0x10000
				u(0xd800 | (v >> 10 & 0x3ff))
				u(0xdc00 | (v & 0x3ff))
			default:
				u(r)
			}
		}
	}
	sb.WriteByte('"')
}

func writeNumber(sb *strings.Builder, lit string) error {
	if !strings.ContainsAny(lit, ".eE") {
		// A Python int: its value, of any size.
		neg := strings.HasPrefix(lit, "-")
		digits := strings.TrimLeft(strings.TrimPrefix(lit, "-"), "0")
		if digits == "" {
			sb.WriteByte('0')
			return nil
		}
		if neg {
			sb.WriteByte('-')
		}
		sb.WriteString(digits)
		return nil
	}
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return ErrInvalid
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return ErrOutOfRange
	}
	sb.WriteString(FloatRepr(f))
	return nil
}

// FloatRepr is float.__repr__: the shortest digits that read back as f, in
// positional notation for exponents -4 to 15 and scientific otherwise, with
// ".0" on an integral positional value and a two-digit-minimum exponent.
func FloatRepr(f float64) string {
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64) // -d.ddde±XX
	neg := e[0] == '-'
	if neg {
		e = e[1:]
	}
	mant, expStr, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expStr)
	digits := strings.Replace(mant, ".", "", 1)
	var out string
	if exp < -4 || exp >= 16 {
		out = digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		sign := "+"
		if exp < 0 {
			sign, exp = "-", -exp
		}
		es := strconv.Itoa(exp)
		if len(es) < 2 {
			es = "0" + es
		}
		out += "e" + sign + es
	} else {
		point := exp + 1 // digits before the point
		switch {
		case point <= 0:
			out = "0." + strings.Repeat("0", -point) + digits
		case point >= len(digits):
			out = digits + strings.Repeat("0", point-len(digits)) + ".0"
		default:
			out = digits[:point] + "." + digits[point:]
		}
	}
	if neg {
		return "-" + out
	}
	return out
}
