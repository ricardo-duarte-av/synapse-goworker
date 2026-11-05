package pyjson

import (
	"bufio"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strconv"
	"testing"
)

// testdata/encode.jsonl is [input, json_encoder.encode(json.loads(input))]
// per line, written by CPython 3.11's json module with Synapse's
// json.JSONEncoder(allow_nan=False, separators=(",", ":")).
func TestEncodeMatchesPython(t *testing.T) {
	f, err := os.Open("testdata/encode.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var pair [2]string
		if err := json.Unmarshal(sc.Bytes(), &pair); err != nil {
			t.Fatal(err)
		}
		got, err := Encode([]byte(pair[0]))
		if err != nil || got != pair[1] {
			t.Errorf("Encode(%s)\n got  %s (%v)\n want %s", pair[0], got, err, pair[1])
		}
		n++
	}
	if n == 0 {
		t.Fatal("no cases")
	}
}

func TestEncodeOutOfRange(t *testing.T) {
	if _, err := Encode([]byte(`{"ts":1e400}`)); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("got %v", err)
	}
	if _, err := Encode([]byte(`{`)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid: %v", err)
	}
}

// FloatRepr agrees with Go's shortest digits for every float: only the
// notation is Python's.
func TestFloatReprRoundTrips(t *testing.T) {
	for _, f := range []float64{1, 0.1, 1e-5, 1e-4, 1e15, 1e16, 123456789.123, math.MaxFloat64, math.SmallestNonzeroFloat64, -2.5e-7} {
		s := FloatRepr(f)
		back, err := strconv.ParseFloat(s, 64)
		if err != nil || back != f {
			t.Errorf("%v -> %q -> %v (%v)", f, s, back, err)
		}
	}
}

// testdata/canonical.jsonl is [input, encode_canonical_json(json.loads(input))]
// per line (null where it raises), written by canonicaljson 2.0 in the
// Synapse 1.164.1 image.
func TestCanonicalMatchesPython(t *testing.T) {
	f, err := os.Open("testdata/canonical.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var pair [2]*string
		if err := json.Unmarshal(sc.Bytes(), &pair); err != nil {
			t.Fatal(err)
		}
		got, err := Canonical([]byte(*pair[0]))
		switch {
		case pair[1] == nil:
			if err == nil {
				t.Errorf("Canonical(%s) = %s, want an error", *pair[0], got)
			}
		case err != nil || got != *pair[1]:
			t.Errorf("Canonical(%s)\n got  %s (%v)\n want %s", *pair[0], got, err, *pair[1])
		}
		if l, lerr := CanonicalLen([]byte(*pair[0])); err == nil && (lerr != nil || l != len(got)) {
			t.Errorf("CanonicalLen(%s) = %d", *pair[0], l)
		}
		n++
	}
	if n == 0 {
		t.Fatal("no cases")
	}
}
