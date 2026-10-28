// Package pyjson decodes JSON as Python's json module does where gjson
// differs: an object with a repeated key holds it once, at its first
// position, with its last value (a dict assignment). gjson finds the first.
package pyjson

import (
	"bytes"
	"errors"

	"github.com/tidwall/gjson"
)

// ErrInvalid is input that is not JSON.
var ErrInvalid = errors.New("pyjson: invalid JSON")

// HasDuplicateKeys reports a repeated key in any object. Keys are compared
// decoded, as Python compares them: "a" and "a" are one key.
func HasDuplicateKeys(v gjson.Result) bool {
	dup := false
	switch {
	case v.IsObject():
		seen := map[string]bool{}
		v.ForEach(func(k, x gjson.Result) bool {
			if seen[k.Str] || HasDuplicateKeys(x) {
				dup = true
				return false
			}
			seen[k.Str] = true
			return true
		})
	case v.IsArray():
		v.ForEach(func(_, x gjson.Result) bool {
			dup = HasDuplicateKeys(x)
			return !dup
		})
	}
	return dup
}

// Dedupe is raw as json.loads holds it: every object's repeated keys
// collapsed. Input without repeated keys comes back unchanged.
func Dedupe(raw []byte) ([]byte, error) {
	if !gjson.ValidBytes(raw) {
		return nil, ErrInvalid
	}
	v := gjson.ParseBytes(raw)
	if !HasDuplicateKeys(v) {
		return raw, nil
	}
	var b bytes.Buffer
	write(&b, v)
	return b.Bytes(), nil
}

func write(b *bytes.Buffer, v gjson.Result) {
	switch {
	case v.IsObject():
		type member struct{ key, value gjson.Result }
		var order []string
		members := map[string]*member{}
		v.ForEach(func(k, x gjson.Result) bool {
			if m, ok := members[k.Str]; ok {
				m.value = x
			} else {
				members[k.Str] = &member{k, x}
				order = append(order, k.Str)
			}
			return true
		})
		b.WriteByte('{')
		for i, k := range order {
			if i > 0 {
				b.WriteByte(',')
			}
			m := members[k]
			b.WriteString(m.key.Raw)
			b.WriteByte(':')
			write(b, m.value)
		}
		b.WriteByte('}')
	case v.IsArray():
		b.WriteByte('[')
		i := 0
		v.ForEach(func(_, x gjson.Result) bool {
			if i > 0 {
				b.WriteByte(',')
			}
			i++
			write(b, x)
			return true
		})
		b.WriteByte(']')
	default:
		b.WriteString(v.Raw)
	}
}
