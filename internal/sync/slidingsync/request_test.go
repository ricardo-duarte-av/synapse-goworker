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

// The sticky events extension as Synapse 1.165 reads it (#20363): lists
// and rooms default to everything, limit to 100 but an explicit 0 stays,
// and an old client's `since` is ignored.
func TestStickyEventsRequest(t *testing.T) {
	ext := func(body string) *StickyEventsExtension {
		t.Helper()
		req, err := ParseRequest([]byte(`{"extensions":{"org.matrix.msc4354.sticky_events":` + body + `}}`))
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		return req.Extensions.StickyEvents
	}
	if e := ext(`{"enabled":true,"since":"sticky_5"}`); e.Limit != 100 || len(e.Lists) != 1 || e.Lists[0] != Wildcard ||
		len(e.Rooms) != 1 || e.Rooms[0] != Wildcard {
		t.Errorf("defaults: %+v", e)
	}
	if e := ext(`{"enabled":true,"limit":0,"lists":["a"],"rooms":[]}`); e.Limit != 0 || len(e.Lists) != 1 || e.Rooms == nil || len(e.Rooms) != 0 {
		t.Errorf("explicit: %+v", e)
	}
}

// Since Synapse 1.165 an explicit null fails these StrictBool and
// list[StrictStr] fields, where it was a falsy None; to_device's enabled
// still takes one.
func TestExtensionNulls(t *testing.T) {
	for _, body := range []string{
		`{"e2ee":{"enabled":null}}`,
		`{"account_data":{"enabled":null}}`,
		`{"receipts":{"lists":null}}`,
		`{"typing":{"rooms":null}}`,
		`{"io.element.msc4308.thread_subscriptions":{"enabled":null}}`,
		`{"org.matrix.msc4354.sticky_events":{"limit":null}}`,
		`{"org.matrix.msc4354.sticky_events":{"limit":-1}}`,
	} {
		if _, err := ParseRequest([]byte(`{"extensions":` + body + `}`)); err == nil {
			t.Errorf("%s accepted", body)
		}
	}
	if _, err := ParseRequest([]byte(`{"extensions":{"to_device":{"enabled":null}}}`)); err != nil {
		t.Errorf("to_device null: %v", err)
	}
}
