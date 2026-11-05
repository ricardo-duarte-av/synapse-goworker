package devicelists

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// TestValidateKeyUpload replays Synapse 1.164.1's own validator
// (validate_json_object(body, KeyUploadRequestBody), run in its image):
// each line is [body, null | [status, errcode, error] | ["exc", type, msg]].
func TestValidateKeyUpload(t *testing.T) {
	f, err := os.Open("testdata/keyupload.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var line [2]json.RawMessage
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		n++
		want := gjson.ParseBytes(line[1])
		cerr, err := validateKeyUpload(gjson.ParseBytes(line[0]))
		switch {
		case want.Type == gjson.Null:
			if cerr != nil || err != nil {
				t.Errorf("%s: got %v %v, want valid", line[0], cerr, err)
			}
		case want.Get("0").Str == "exc":
			if err == nil {
				t.Errorf("%s: got %v, want a TypeError", line[0], cerr)
			}
		default:
			if err != nil || cerr == nil || cerr.Status != int(want.Get("0").Int()) || cerr.Body["errcode"] != want.Get("1").Str || cerr.Body["error"] != want.Get("2").Str {
				t.Errorf("%s:\n got %+v %v\nwant %s", line[0], cerr, err, line[1])
			}
		}
	}
	if n < 40 {
		t.Fatalf("%d cases", n)
	}
}

// TestPyB64Decode replays unpaddedbase64.decode_base64 (Synapse 1.164.1's
// image): each line is [input, hex or null for an exception].
func TestPyB64Decode(t *testing.T) {
	b, err := os.ReadFile("testdata/b64.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		c := gjson.Parse(line)
		got, err := pyB64Decode(c.Get("0").Str)
		want := c.Get("1")
		switch {
		case want.Type == gjson.Null && err == nil:
			t.Errorf("%q: got %x, want an error", c.Get("0").Str, got)
		case want.Type != gjson.Null && (err != nil || hex.EncodeToString(got) != want.Str):
			t.Errorf("%q: got %x %v, want %s", c.Get("0").Str, got, err, want.Str)
		}
	}
}

func TestPyEqual(t *testing.T) {
	for _, c := range []struct {
		a, b string
		eq   bool
	}{
		{`1`, `1.0`, true}, {`true`, `1`, true}, {`false`, `0.0`, true}, {`true`, `2`, false},
		{`10000000000000000001`, `1e19`, false}, {`10000000000000000000`, `1e19`, true},
		{`"a"`, `"a"`, true}, {`null`, `null`, true}, {`null`, `0`, false},
		{`{"a":1,"b":[1,2]}`, `{"b":[1.0,2],"a":true}`, true}, {`{"a":1}`, `{"a":1,"b":2}`, false},
		{`[1]`, `[1,1]`, false}, {`"1"`, `1`, false},
	} {
		if got := pyEqual(gjson.Parse(c.a), gjson.Parse(c.b)); got != c.eq {
			t.Errorf("%s == %s: got %v", c.a, c.b, got)
		}
	}
}
