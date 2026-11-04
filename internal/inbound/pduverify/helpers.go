package pduverify

import (
	"context"
	"encoding/json"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// What the push port left behind for the rest of the checker.

// asSynapseReads loads events as get_events returns them: an event whose
// redaction Synapse honours (RedactedOnLoad, MSC4293's included) in its
// redacted form.
func (s *Verifier) asSynapseReads(ctx context.Context, ids []string) (map[string]*pdu.Event, error) {
	evs, err := s.st.GetEvents(ctx, ids)
	if err != nil {
		return nil, err
	}
	present := make([]string, 0, len(evs))
	for id := range evs {
		present = append(present, id)
	}
	redacted, err := s.st.RedactedOnLoad(ctx, present)
	if err != nil {
		return nil, err
	}
	for id, e := range evs {
		if !redacted[id] {
			continue
		}
		pruned, err := pdu.Redact(e.JSON, e.Room)
		if err != nil {
			return nil, err
		}
		pe, err := pdu.ParseEvent(pruned, id, e.Room, e.Rejected)
		if err != nil {
			return nil, err
		}
		evs[id] = pe
	}
	return evs, nil
}

// jsonNormal round-trips v through JSON, so it compares equal to decoded
// JSON.
func jsonNormal(v any) any {
	b, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}

// pyTruthy is bool(v) for a decoded JSON value; absent or null is false.
func pyTruthy(v gjson.Result) bool {
	switch v.Type {
	case gjson.Null:
		return false
	case gjson.False:
		return false
	case gjson.True:
		return true
	case gjson.Number:
		return v.Num != 0
	case gjson.String:
		return v.Str != ""
	}
	if v.IsArray() {
		return len(v.Array()) > 0
	}
	if v.IsObject() {
		return len(v.Map()) > 0
	}
	return false
}
