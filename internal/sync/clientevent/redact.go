package clientevent

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Redact strips an event down to the fields that survive redaction.
//
// A close port of `redact` in rust/src/events/utils.rs. Redaction is applied
// **on read**, not only when the redaction arrives: a redacted event keeps its
// original body in `event_json` until a background job censors it, and Synapse
// censors in place only after `redaction_retention_period`. On this server
// thousands of redacted events still carry their original content, so serving
// stored JSON unchanged publishes exactly what the redaction was meant to
// remove.
//
// The allowlist is per room version, and the differences are not cosmetic: an
// `m.room.aliases` event keeps its aliases in versions 1 to 5 and loses them
// afterwards; a create event keeps its whole content from version 11.
func Redact(event []byte, rv RoomVersion) ([]byte, error) {
	eventType := gjson.GetBytes(event, "type").String()

	allowed := map[string]bool{
		"event_id": true, "sender": true, "room_id": true, "hashes": true,
		"signatures": true, "content": true, "type": true, "state_key": true,
		"depth": true, "prev_events": true, "origin_server_ts": true,
	}
	if !rv.UpdatedRedactionRules {
		// Earlier versions kept three more.
		allowed["prev_state"] = true
		allowed["membership"] = true
		allowed["origin"] = true
	}
	if rv.MSC4242StateDags {
		allowed["prev_state_events"] = true
	} else {
		allowed["auth_events"] = true
	}

	content := gjson.GetBytes(event, "content")
	newContent := []byte(`{}`)
	keep := func(field string) {
		if v := content.Get(field); v.Exists() {
			if next, err := sjson.SetRawBytes(newContent, escapeKey(field), []byte(v.Raw)); err == nil {
				newContent = next
			}
		}
	}

	switch eventType {
	case "m.room.member":
		keep("membership")
		if rv.RestrictedJoinRuleFix {
			keep("join_authorised_via_users_server")
		}
		if rv.UpdatedRedactionRules {
			// The `signed` block of a third-party invite is authorisation
			// material, so it survives.
			if tpi := content.Get("third_party_invite"); tpi.IsObject() {
				inner := []byte(`{}`)
				if signed := tpi.Get("signed"); signed.Exists() {
					inner, _ = sjson.SetRawBytes(inner, "signed", []byte(signed.Raw))
				}
				newContent, _ = sjson.SetRawBytes(newContent, "third_party_invite", inner)
			}
		}
	case "m.room.create":
		if rv.UpdatedRedactionRules {
			// MSC2176: a create event's content cannot be redacted at all.
			content.ForEach(func(k, _ gjson.Result) bool {
				keep(k.String())
				return true
			})
		}
		if !rv.ImplicitRoomCreator {
			keep("creator")
		}
		if rv.RoomIDsAsHashes {
			// The room ID is derived from this event's hash, so it cannot
			// carry one.
			delete(allowed, "room_id")
		}
	case "m.room.join_rules":
		keep("join_rule")
		if rv.RestrictedJoinRule {
			keep("allow")
		}
	case "m.room.power_levels":
		for _, f := range []string{"users", "users_default", "events",
			"events_default", "state_default", "ban", "kick", "redact"} {
			keep(f)
		}
		if rv.UpdatedRedactionRules {
			keep("invite")
		}
	case "m.room.aliases":
		if rv.SpecialCaseAliasesAuth {
			keep("aliases")
		}
	case "m.room.history_visibility":
		keep("history_visibility")
	case "m.room.redaction":
		if rv.UpdatedRedactionRules {
			keep("redacts")
		}
	}

	out := []byte(`{}`)
	var err error
	gjson.ParseBytes(event).ForEach(func(k, v gjson.Result) bool {
		if !allowed[k.String()] {
			return true
		}
		var next []byte
		next, err = sjson.SetRawBytes(out, escapeKey(k.String()), []byte(v.Raw))
		if err != nil {
			return false
		}
		out = next
		return true
	})
	if err != nil {
		return nil, err
	}

	if rv.MSC3389RelationRedactions {
		if rel := content.Get("m\\.relates_to"); rel.IsObject() {
			inner := []byte(`{}`)
			any := false
			for _, f := range []string{"rel_type", "event_id"} {
				if v := rel.Get(f); v.Exists() {
					inner, _ = sjson.SetRawBytes(inner, f, []byte(v.Raw))
					any = true
				}
			}
			if any {
				newContent, _ = sjson.SetRawBytes(newContent, `m\.relates_to`, inner)
			}
		}
	}

	if out, err = sjson.SetRawBytes(out, "content", newContent); err != nil {
		return nil, err
	}

	// Redaction does not clear `unsigned`, it rebuilds it: two fields are
	// copied onto the pruned event and everything else is dropped.
	newUnsigned := []byte(`{}`)
	if unsigned := gjson.GetBytes(event, "unsigned"); unsigned.IsObject() {
		for _, f := range []string{"age_ts", "replaces_state"} {
			if v := unsigned.Get(f); v.Exists() {
				newUnsigned, _ = sjson.SetRawBytes(newUnsigned, f, []byte(v.Raw))
			}
		}
	}
	if out, err = sjson.SetRawBytes(out, "unsigned", newUnsigned); err != nil {
		return nil, err
	}
	return out, nil
}

// escapeKey escapes a JSON key for sjson's path syntax, where a dot means
// nesting. Matrix keys routinely contain dots -- `m.relates_to` is the obvious
// one -- so an unescaped key silently creates a nested object instead of the
// field it names.
func escapeKey(key string) string {
	out := make([]byte, 0, len(key)+4)
	for i := 0; i < len(key); i++ {
		switch key[i] {
		case '.', '*', '?', '\\':
			out = append(out, '\\')
		}
		out = append(out, key[i])
	}
	return string(out)
}
