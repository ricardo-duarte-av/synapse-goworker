package fedfetch

import "testing"

// A copy the remote redacted keeps the pruned event's keys; one whose
// content was changed does not.
func TestSameKeys(t *testing.T) {
	pruned := `{"type":"m.room.message","content":{},"unsigned":{},"sender":"@a:x"}`
	if !sameKeys([]byte(`{"type":"m.room.message","content":{},"sender":"@a:x","unsigned":{"age":5}}`), []byte(pruned)) {
		t.Error("a redacted copy counted as tampered")
	}
	if sameKeys([]byte(`{"type":"m.room.message","content":{"body":"x"},"sender":"@a:x"}`), []byte(pruned)) {
		t.Error("changed content not counted as tampered")
	}
	if sameKeys([]byte(`{"type":"m.room.message","content":{},"sender":"@a:x","extra":1}`), []byte(pruned)) {
		t.Error("an extra top-level key not counted as tampered")
	}
}
