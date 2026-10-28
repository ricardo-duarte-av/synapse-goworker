package creator

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/pyjson"
)

// The request parsing of http/servlet.py (v1.163.0).

// maxJSONDepth bounds the nesting served here. Python's decoder and
// encoder recurse, and fail past about 9,996 levels (CPython 3.13's C
// recursion limit); beyond this margin the request is Synapse's. Far
// sooner, at 127, the event cannot be built (eventDepthRefusal).
const maxJSONDepth = 9000

// maxIntDigits is sys.int_info.default_max_str_digits: json.loads refuses a
// longer integer.
const maxIntDigits = 4300

var (
	notJSON     = badRequest("not_json", "Content not JSON.", "M_NOT_JSON")
	notAnObject = badRequest("not_object", "Content must be a JSON object.", "M_BAD_JSON")
)

// parseJSONObject is parse_json_object_from_request: the body, decoded as
// UTF-8 and then json_decoder decodes it, must be an object. With
// allowEmpty, an empty body is {}. Repeated keys are collapsed as Python
// collapses them; a lone surrogate, which Python keeps and cannot encode, is
// Synapse's.
func parseJSONObject(body []byte, allowEmpty bool) ([]byte, *stop) {
	if len(body) == 0 && allowEmpty {
		return []byte("{}"), nil
	}
	if !utf8.Valid(body) {
		return nil, notJSON.stop()
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	depth, longInt, tokens := 0, false, 0
	for ; ; tokens++ {
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) && tokens > 0 {
			break
		}
		if depth > maxJSONDepth {
			return nil, fb("deep_json")
		}
		if err != nil || (tokens > 0 && depth == 0) {
			// Nothing, or more than one value: "Extra data".
			return nil, notJSON.stop()
		}
		switch tok.Kind() {
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		case '0':
			n := strings.TrimPrefix(tok.String(), "-")
			if !strings.ContainsAny(n, ".eE") && len(n) > maxIntDigits {
				longInt = true
			}
		}
	}
	if longInt {
		return nil, fb("long_integer")
	}
	v := gjson.ParseBytes(body)
	if !v.IsObject() {
		return nil, notAnObject.stop()
	}
	// A repeated key: Python's dict keeps the last value.
	body, err := pyjson.Dedupe(body)
	if err != nil {
		return nil, notJSON.stop()
	}
	if loneSurrogate(body) {
		return nil, fb("surrogate_escape")
	}
	return body, nil
}

// parseIntegerParam is parse_integer for an optional, non-negative query
// parameter, given as present.
func parseIntegerParam(name, value string) (int64, *stop) {
	n, ok, exact := pyIntBytes(value)
	switch {
	case !ok:
		return 0, badRequest("bad_param", "Query parameter "+name+" must be an integer", "M_INVALID_PARAM").stop()
	case !exact:
		// Beyond int64.
		return 0, fb("bad_param")
	case n < 0:
		return 0, badRequest("bad_param", "Query parameter "+name+" must be a positive integer.", "M_INVALID_PARAM").stop()
	}
	return n, nil
}

// pyIntBytes is int(b) for ASCII bytes: surrounding whitespace, a sign, and
// underscores between digits. exact is false when the value does not fit in
// an int64.
func pyIntBytes(s string) (n int64, ok, exact bool) {
	s = strings.Trim(s, " \t\n\r\v\f")
	digits := strings.TrimLeft(s, "+-")
	if len(s)-len(digits) > 1 || digits == "" {
		return 0, false, false
	}
	var b strings.Builder
	for i := 0; i < len(digits); i++ {
		switch d := digits[i]; {
		case d >= '0' && d <= '9':
			b.WriteByte(d)
		case d == '_' && i > 0 && i < len(digits)-1 && digits[i-1] != '_':
		default:
			return 0, false, false
		}
	}
	v, err := strconv.ParseInt(s[:len(s)-len(digits)]+b.String(), 10, 64)
	if err != nil {
		return 0, true, false
	}
	return v, true, true
}
