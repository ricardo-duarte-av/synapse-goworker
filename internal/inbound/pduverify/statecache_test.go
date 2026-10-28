package pduverify

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

func TestDiffAfterOverlay(t *testing.T) {
	k := func(t, s string) pdu.StateKey { return pdu.StateKey{Type: t, StateKey: s} }
	before := pdu.StateMap{k("m.room.create", ""): "$c", k("m.room.member", "@a"): "$a1"}
	own := k("m.room.member", "@a")
	stored := pdu.StateMap{k("m.room.create", ""): "$c", k("m.room.member", "@a"): "$a2"}
	if d := diffAfter(before, &own, "$a2", stored); len(d) != 0 {
		t.Errorf("the event replacing a key: %v", d)
	}
	newKey := k("m.room.member", "@b")
	stored2 := pdu.StateMap{k("m.room.create", ""): "$c", k("m.room.member", "@a"): "$a1", newKey: "$b"}
	if d := diffAfter(before, &newKey, "$b", stored2); len(d) != 0 {
		t.Errorf("the event adding a key: %v", d)
	}
	if d := diffAfter(before, nil, "", stored2); len(d) != 1 {
		t.Errorf("a stored key we lack was not reported: %v", d)
	}
	if len(before) != 2 || before[own] != "$a1" {
		t.Error("diffAfter changed the state before")
	}
}
