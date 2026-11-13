package slidingsync

import (
	"encoding/json"
	"os"
	"testing"
)

// Synapse 1.165.0's own answers (validate_json_object(body, SlidingSyncBody),
// run in its image) for every case in testdata/validate.json.
func TestValidateBodyAgainstSynapse(t *testing.T) {
	raw, err := os.ReadFile("testdata/validate.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Body    json.RawMessage `json:"body"`
		OK      bool            `json:"ok"`
		ErrCode string          `json:"errcode"`
		Error   string          `json:"error"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 140 {
		t.Fatalf("only %d cases read", len(cases))
	}
	for _, c := range cases {
		got := ValidateBody(c.Body)
		switch {
		case c.OK && got != nil:
			t.Errorf("%s\n  Synapse accepts it; got %s %s", trunc(c.Body), got.ErrCode, got.Message)
		case !c.OK && got == nil:
			t.Errorf("%s\n  Synapse: %s %s; accepted", trunc(c.Body), c.ErrCode, c.Error)
		case !c.OK && (got.ErrCode != c.ErrCode || got.Message != c.Error):
			t.Errorf("%s\n  Synapse: %s %s\n  got:     %s %s", trunc(c.Body), c.ErrCode, c.Error, got.ErrCode, got.Message)
		}
	}
}

func TestValidateBodyNotJSON(t *testing.T) {
	for body, want := range map[string]string{
		"": "M_NOT_JSON Content not JSON.", "{nope": "M_NOT_JSON Content not JSON.",
		"[]": "M_BAD_JSON Content must be a JSON object.", "null": "M_BAD_JSON Content must be a JSON object.",
	} {
		got := ValidateBody([]byte(body))
		if got == nil || got.ErrCode+" "+got.Message != want {
			t.Errorf("%q: %+v, want %s", body, got, want)
		}
	}
}

func trunc(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}
