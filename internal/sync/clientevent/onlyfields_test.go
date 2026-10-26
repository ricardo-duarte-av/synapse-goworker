package clientevent

import (
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func fields(t *testing.T, event string, want []string) map[string]any {
	t.Helper()
	out, err := OnlyFields([]byte(event), want)
	if err != nil {
		t.Fatalf("OnlyFields: %v", err)
	}
	m, _ := gjson.ParseBytes(out).Value().(map[string]any)
	return m
}

func TestOnlyFieldsKeepsNamedPathsAndDropsTheRest(t *testing.T) {
	got := fields(t, `{"type":"m.room.message","sender":"@a:x",
		"content":{"body":"hi","msgtype":"m.text"},"unsigned":{"age":5}}`,
		[]string{"type", "content.body"})
	want := map[string]any{
		"type":    "m.room.message",
		"content": map[string]any{"body": "hi"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestOnlyFieldsSkipsMissingPathsRatherThanEmittingNull(t *testing.T) {
	// The output is built up from nothing, so a path the event does not have
	// contributes nothing -- not a null, and not an empty parent object.
	got := fields(t, `{"type":"m.room.message"}`, []string{"type", "content.body", "nope"})
	want := map[string]any{"type": "m.room.message"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestOnlyFieldsWillNotDescendThroughAScalar(t *testing.T) {
	got := fields(t, `{"content":"a string"}`, []string{"content.body"})
	if len(got) != 0 {
		t.Fatalf("got %#v, want nothing", got)
	}
}

func TestOnlyFieldsEscaping(t *testing.T) {
	// A dot inside a key is escaped with a backslash. Without that, the one
	// key `m.relates_to` reads as a three-level path and never matches.
	got := fields(t, `{"content":{"m.relates_to":{"rel_type":"m.thread"},"body":"x"}}`,
		[]string{`content.m\.relates_to.rel_type`})
	want := map[string]any{
		"content": map[string]any{"m.relates_to": map[string]any{"rel_type": "m.thread"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestOnlyFieldsMergesSiblingsIntoOneParent(t *testing.T) {
	got := fields(t, `{"content":{"body":"hi","msgtype":"m.text","extra":1}}`,
		[]string{"content.body", "content.msgtype"})
	want := map[string]any{
		"content": map[string]any{"body": "hi", "msgtype": "m.text"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestSplitField(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"a.b", []string{"a", "b"}},
		{`a\.b`, []string{"a.b"}},
		{`a\\.b`, []string{`a\`, "b"}},
		// Any other backslash escape is left alone, backslash included.
		{`a\qb`, []string{`a\qb`}},
		{"", []string{""}},
	} {
		if got := splitField(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("splitField(%q) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
}
