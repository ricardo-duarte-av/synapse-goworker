package slidingsync

import "testing"

// The MSC4262 profiles extension is validated as Synapse's model validates
// it, even with profile updates off (SlidingSyncBody, v1.164, #20003).
func TestParseRequestProfilesExtension(t *testing.T) {
	for body, ok := range map[string]bool{
		`{"extensions":{"org.matrix.msc4262.profiles":{"enabled":true,"fields":["displayname"]}}}`: true,
		`{"extensions":{"org.matrix.msc4262.profiles":{}}}`:                                        true,
		`{"extensions":{"org.matrix.msc4262.profiles":{"enabled":"yes"}}}`:                         false,
		`{"extensions":{"org.matrix.msc4262.profiles":{"fields":[1]}}}`:                            false,
	} {
		if _, err := ParseRequest([]byte(body)); (err == nil) != ok {
			t.Errorf("%s: err %v, want ok %v", body, err, ok)
		}
	}
}
