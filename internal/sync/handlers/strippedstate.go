package handlers

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// strippedStateEvents builds the events list of an invited or knocked room:
// the stripped state stored in the membership event's unsigned block,
// followed by the membership event itself, stripped too.
//
// key is "invite_room_state" or "knock_room_state". Ported from
// encode_invited and encode_knocked in Synapse's rest/client/sync.py, which
// pop the key out of `unsigned` and append strip_event of the membership event
// (since v1.162, #19723; until then the whole serialised event). The stripped
// state is the only thing that tells a client what the room is called; leave
// it inside `unsigned` and the invite arrives with no name, no avatar and no
// room type, because no client looks for it there.
//
// A value that is not a list is treated as empty, as Synapse does. Spliced,
// not re-encoded: the stored JSON is copied through exactly.
func strippedStateEvents(body []byte, key string) ([]json.RawMessage, error) {
	events := []json.RawMessage{}
	if v := gjson.GetBytes(body, "unsigned."+key); v.IsArray() {
		v.ForEach(func(_, ev gjson.Result) bool {
			events = append(events, json.RawMessage(ev.Raw))
			return true
		})
	}
	stripped, err := stripEvent(body)
	if err != nil {
		return nil, fmt.Errorf("strip %s: %w", key, err)
	}
	return append(events, stripped), nil
}

// stripEvent is strip_event (events/utils.py): only type, state_key, content
// and sender, in that order.
func stripEvent(body []byte) (json.RawMessage, error) {
	out := []byte(`{}`)
	var err error
	for _, k := range []string{"type", "state_key", "content", "sender"} {
		raw := "null"
		if v := gjson.GetBytes(body, k); v.Exists() {
			raw = v.Raw
		}
		if out, err = sjson.SetRawBytes(out, k, []byte(raw)); err != nil {
			return nil, err
		}
	}
	return out, nil
}
