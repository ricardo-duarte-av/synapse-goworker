package clientevent

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// OnlyFields reduces an event to the paths a filter's `event_fields` names.
//
// Port of `only_fields` in rust/src/events/serialize.rs. The output is built up
// from nothing rather than pruned down, so a path naming something the event
// does not have is simply skipped -- it never produces a null or an empty
// object.
//
// A path is dot-separated, and a literal dot or backslash inside a key is
// escaped with a backslash. `content.body` names the body inside content;
// `content.m\.relates_to` names a single key called `m.relates_to`.
func OnlyFields(event []byte, fields []string) ([]byte, error) {
	out := []byte(`{}`)
	for _, field := range fields {
		parts := splitField(field)
		if len(parts) == 0 {
			continue
		}
		value, ok := lookup(event, parts)
		if !ok {
			continue
		}
		var err error
		if out, err = sjson.SetRawBytes(out, sjsonPath(parts), []byte(value.Raw)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// lookup walks a decoded path, refusing to descend through a non-object.
//
// Synapse drills down with `match sub.get(parent) { Some(Value::Object(obj)) =>
// ... , _ => return }`: a scalar part-way along the path is a miss, not an
// error.
func lookup(event []byte, parts []string) (gjson.Result, bool) {
	cur := gjson.ParseBytes(event)
	for i, part := range parts {
		if !cur.IsObject() {
			return gjson.Result{}, false
		}
		next := cur.Get(escapePath(part))
		if !next.Exists() {
			return gjson.Result{}, false
		}
		if i == len(parts)-1 {
			return next, true
		}
		cur = next
	}
	return gjson.Result{}, false
}

// splitField splits a dotted path on unescaped dots and unescapes each part.
func splitField(field string) []string {
	var parts []string
	var cur strings.Builder
	escaped := false
	for _, r := range field {
		switch {
		case escaped:
			// Only `\.` and `\\` are escapes; any other `\x` keeps the
			// backslash, which is what the Rust does.
			if r != '.' && r != '\\' {
				cur.WriteRune('\\')
			}
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '.':
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if escaped {
		// A trailing lone backslash is kept verbatim.
		cur.WriteRune('\\')
	}
	return append(parts, cur.String())
}

// escapePath renders one already-unescaped key as a gjson path component.
func escapePath(part string) string {
	r := strings.NewReplacer(".", `\.`, "*", `\*`, "?", `\?`, "#", `\#`)
	return r.Replace(part)
}

// sjsonPath renders a decoded path for sjson, which uses the same escaping.
func sjsonPath(parts []string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, escapePath(p))
	}
	return strings.Join(out, ".")
}
