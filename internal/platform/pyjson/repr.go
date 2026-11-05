package pyjson

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/tidwall/gjson"
)

// Repr is repr(json.loads(raw)): what Synapse's error messages show of a
// value they format with %r, or with %s of a container.
func Repr(raw []byte) (string, error) {
	if !gjson.ValidBytes(raw) {
		return "", ErrInvalid
	}
	var sb strings.Builder
	if err := reprValue(&sb, gjson.ParseBytes(raw)); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// Str is str(json.loads(raw)): a string as itself, anything else as Repr.
func Str(raw []byte) (string, error) {
	v := gjson.ParseBytes(raw)
	if v.Type == gjson.String {
		s, err := decodeString(v.Raw)
		if err != nil {
			return "", err
		}
		return string(s), nil
	}
	return Repr(raw)
}

// ReprString is repr of a Python str.
func ReprString(s string) string {
	var sb strings.Builder
	reprRunes(&sb, []rune(s))
	return sb.String()
}

func reprValue(sb *strings.Builder, v gjson.Result) error {
	switch {
	case v.IsObject():
		// dict semantics: a repeated key once, at its first place, last value.
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
		sb.WriteByte('{')
		for i, id := range order {
			if i > 0 {
				sb.WriteString(", ")
			}
			reprRunes(sb, members[id].key)
			sb.WriteString(": ")
			if err := reprValue(sb, members[id].value); err != nil {
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
				sb.WriteString(", ")
			}
			i++
			err = reprValue(sb, x)
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
		reprRunes(sb, s)
	case v.Type == gjson.Number:
		var nb strings.Builder
		if err := writeNumber(&nb, v.Raw); err != nil {
			if err == ErrOutOfRange {
				if strings.HasPrefix(v.Raw, "-") {
					sb.WriteString("-inf")
				} else {
					sb.WriteString("inf")
				}
				return nil
			}
			return err
		}
		sb.WriteString(nb.String())
	case v.Type == gjson.True:
		sb.WriteString("True")
	case v.Type == gjson.False:
		sb.WriteString("False")
	default:
		sb.WriteString("None")
	}
	return nil
}

// reprRunes is unicode_repr: single quotes unless the string holds a ' and
// no ", backslash escapes for the quote, '\\' and \t \n \r, and \x, \u or
// \U for what str.isprintable refuses.
func reprRunes(sb *strings.Builder, s []rune) {
	quote := '\''
	hasSingle, hasDouble := false, false
	for _, r := range s {
		hasSingle = hasSingle || r == '\''
		hasDouble = hasDouble || r == '"'
	}
	if hasSingle && !hasDouble {
		quote = '"'
	}
	sb.WriteRune(quote)
	for _, r := range s {
		switch {
		case r == quote || r == '\\':
			sb.WriteByte('\\')
			sb.WriteRune(r)
		case r == '\t':
			sb.WriteString(`\t`)
		case r == '\n':
			sb.WriteString(`\n`)
		case r == '\r':
			sb.WriteString(`\r`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(sb, `\x%02x`, r)
		case r >= 0xd800 && r <= 0xdfff, r > 0x7f && !unicode.IsPrint(r):
			switch {
			case r <= 0xff:
				fmt.Fprintf(sb, `\x%02x`, r)
			case r <= 0xffff:
				fmt.Fprintf(sb, `\u%04x`, r)
			default:
				fmt.Fprintf(sb, `\U%08x`, r)
			}
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteRune(quote)
}
