package queue

import (
	"encoding/json"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sender/txn"
)

// An update arriving while an earlier one of the same user (presence), key
// (typing) or slot (receipt) is in flight must not merge into the taken EDU,
// which is dequeued after delivery: it was lost that way.
func TestUpdateDuringSendIsNotMergedIntoTheTakenEDU(t *testing.T) {
	d := NewDestination(Config{Name: "remote"})
	enc := func(s string) func(int64) json.RawMessage {
		return func(int64) json.RawMessage { return json.RawMessage(`"` + s + `"`) }
	}
	d.EnqueuePresence(PresenceEntry{UserID: "@a:x", Encode: enc("online")})
	d.EnqueueKeyedEDU("room", txn.EDU{Type: txn.EDUTypeTyping, Content: json.RawMessage(`{"typing":true}`)})
	d.EnqueueReceipt("!r", "m.read", "@a:x", "", json.RawMessage(`{"n":1}`))
	d.mu.Lock()
	if _, err := d.takeLocked(); err != nil {
		t.Fatal(err)
	}
	d.mu.Unlock()
	d.EnqueuePresence(PresenceEntry{UserID: "@a:x", Encode: enc("unavailable")})
	d.EnqueueKeyedEDU("room", txn.EDU{Type: txn.EDUTypeTyping, Content: json.RawMessage(`{"typing":false}`)})
	d.EnqueueReceipt("!r", "m.read", "@a:x", "", json.RawMessage(`{"n":2}`))
	d.dequeue(0, 3)
	if len(d.pendingEDUs) != 3 {
		t.Fatalf("%d EDUs left, want the three newer updates", len(d.pendingEDUs))
	}
	if got := string(d.pendingEDUs[0].Presence["@a:x"].Encode(0)); got != `"unavailable"` {
		t.Errorf("presence %s", got)
	}
	if got := string(d.pendingEDUs[1].Unit.Content); got != `{"typing":false}` {
		t.Errorf("typing %s", got)
	}
	if got := string(d.pendingEDUs[2].Receipts["!r"]["m.read"]["@a:x"].content); got != `{"n":2}` {
		t.Errorf("receipt %s", got)
	}
}
