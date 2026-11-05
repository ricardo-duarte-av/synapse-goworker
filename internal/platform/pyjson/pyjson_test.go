package pyjson

import "testing"

// The answers are json.loads', re-encoded with json.dumps(separators=(",", ":")).
func TestDedupe(t *testing.T) {
	for in, want := range map[string]string{
		`{"a":1}`:             `{"a":1}`,
		`{"a":1,"b":2,"a":3}`: `{"a":3,"b":2}`,
		`{"a":{"x":1,"x":2},"b":[{"y":1,"y":[1,2]}]}`: `{"a":{"x":2},"b":[{"y":[1,2]}]}`,
		`{"a":1,"a":2}`:          `{"a":2}`,
		`{"run":"x","run":"y"}`:  `{"run":"y"}`,
		` {"a" : 1 , "a" : 2 } `: `{"a":2}`,
	} {
		got, err := Dedupe([]byte(in))
		if err != nil {
			t.Fatal(err)
		}
		if in == `{"a":1}` && string(got) != in {
			t.Errorf("unchanged input re-encoded: %s", got)
		}
		if string(got) != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}
