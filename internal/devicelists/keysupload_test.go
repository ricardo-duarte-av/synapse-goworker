package devicelists

import (
	"bufio"
	"encoding/json"
	"os"
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
