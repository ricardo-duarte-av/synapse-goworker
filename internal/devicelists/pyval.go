package devicelists

import (
	"errors"
	"math/big"
	"strings"

	"github.com/tidwall/gjson"
)

// Python's operations on JSON values, as the cross-signing code applies
// them to a request body nobody has validated. errPython is the
// TypeError, AttributeError or ValueError Synapse would raise there: a 500.
var errPython = errors.New("devicelists: a Python error on the request body (Synapse answers 500)")

// pyField is a dict's member by its (unescaped) key.
func pyField(v gjson.Result, key string) gjson.Result { return v.Get(gjson.Escape(key)) }

// pyIn is `key in v`: a dict's keys, a list's elements (only an equal
// string matches a string), a substring; TypeError for the rest.
func pyIn(v gjson.Result, key string) (bool, error) {
	switch {
	case v.IsObject():
		return pyField(v, key).Exists(), nil
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
	return false, errPython
}

// pyIndex is v[key] with a string key: a dict's member (ok false is the
// KeyError), and a TypeError for anything else.
func pyIndex(v gjson.Result, key string) (gjson.Result, bool, error) {
	if !v.IsObject() {
		return gjson.Result{}, false, errPython
	}
	r := pyField(v, key)
	return r, r.Exists(), nil
}

// pyEqual is == between two JSON values as Python holds them: True is 1,
// an int equals the float of the same value exactly, dicts compare by
// keys and values whatever their order.
func pyEqual(a, b gjson.Result) bool {
	an, aok := pyNumber(a)
	bn, bok := pyNumber(b)
	if aok || bok {
		return aok && bok && an.Cmp(bn) == 0
	}
	switch {
	case a.Type == gjson.String || b.Type == gjson.String:
		return a.Type == b.Type && a.Str == b.Str
	case a.Type == gjson.Null || b.Type == gjson.Null:
		return a.Type == b.Type
	case a.IsArray() && b.IsArray():
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
		return pyDictEqual(a, b, nil)
	}
	return false
}

// pyDictEqual compares two dicts without the keys in skip (the
// {k: v for k, v in d.items() if k not in skip} of _check_device_signature).
func pyDictEqual(a, b gjson.Result, skip []string) bool {
	skipped := func(k string) bool {
		for _, s := range skip {
			if k == s {
				return true
			}
		}
		return false
	}
	na, nb := 0, 0
	equal := true
	a.ForEach(func(k, v gjson.Result) bool {
		if skipped(k.Str) {
			return true
		}
		na++
		w := pyField(b, k.Str)
		if !w.Exists() || !pyEqual(v, w) {
			equal = false
		}
		return equal
	})
	if !equal {
		return false
	}
	b.ForEach(func(k, _ gjson.Result) bool {
		if !skipped(k.Str) {
			nb++
		}
		return true
	})
	return na == nb
}

// pyNumber is a bool, int or float as an exact rational (a float as the
// double Python would hold).
func pyNumber(v gjson.Result) (*big.Rat, bool) {
	switch v.Type {
	case gjson.True:
		return big.NewRat(1, 1), true
	case gjson.False:
		return new(big.Rat), true
	case gjson.Number:
		if strings.ContainsAny(v.Raw, ".eE") {
			r := new(big.Rat)
			if r.SetFloat64(v.Float()) == nil {
				return nil, false
			}
			return r, true
		}
		r, ok := new(big.Rat).SetString(v.Raw)
		return r, ok
	}
	return nil, false
}

// pyB64Decode is unpaddedbase64.decode_base64: the input padded to a
// multiple of four, both alphabets accepted, and binascii's lenient
// decoding as Python 3.13 (Synapse's image) does it: characters outside
// the alphabet skipped, and pads too, except that an unfinished last
// quad needs enough of them after it.
func pyB64Decode(s string) ([]byte, error) {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return nil, errors.New("UnicodeEncodeError")
		}
	}
	pad := 3 - ((len(s) + 3) % 4)
	s += strings.Repeat("=", pad)
	var out []byte
	quad, pads := 0, 0
	var left byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '=' {
			pads++
			continue
		}
		var v byte
		switch {
		case c >= 'A' && c <= 'Z':
			v = c - 'A'
		case c >= 'a' && c <= 'z':
			v = c - 'a' + 26
		case c >= '0' && c <= '9':
			v = c - '0' + 52
		case c == '+' || c == '-':
			v = 62
		case c == '/' || c == '_':
			v = 63
		default:
			continue
		}
		pads = 0
		switch quad {
		case 0:
			quad, left = 1, v
		case 1:
			out = append(out, left<<2|v>>4)
			quad, left = 2, v&0x0f
		case 2:
			out = append(out, left<<4|v>>2)
			quad, left = 3, v&0x03
		case 3:
			out = append(out, left<<6|v)
			quad, left = 0, 0
		}
	}
	if quad == 1 || (quad != 0 && quad+pads < 4) {
		return nil, errors.New("binascii.Error")
	}
	return out, nil
}
