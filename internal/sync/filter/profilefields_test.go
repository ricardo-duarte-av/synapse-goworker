package filter

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestProfileFields(t *testing.T) {
	on := Features{MSC4429: true}
	for _, c := range []struct {
		doc    string
		feat   Features
		inline bool
		want   string
		bad    bool
	}{
		{`{"org.matrix.msc4429.profile_fields":{"ids":["displayname","m.status","displayname"]}}`, on, true, "[displayname m.status]", false},
		{`{"org.matrix.msc4429.profile_fields":{"ids":["displayname"]}}`, Features{}, true, "[]", false},
		{`{"org.matrix.msc4429.profile_fields":{}}`, on, true, "[]", false},
		{`{"org.matrix.msc4429.profile_fields":{"ids":[1]}}`, on, true, "", true},
		{`{"org.matrix.msc4429.profile_fields":{"ids":[1,"a.b"]}}`, on, false, "[a.b]", false},
		{`{"org.matrix.msc4429.profile_fields":"nope"}`, on, false, "[]", false},
		{`{"org.matrix.msc4429.profile_fields":"nope"}`, on, true, "", true},
	} {
		parse := NewWithFeatures
		if c.inline {
			parse = NewInline
		}
		got, err := parse(json.RawMessage(c.doc), c.feat)
		if (err != nil) != c.bad {
			t.Errorf("%s: %v", c.doc, err)
			continue
		}
		if err == nil && fmt.Sprint(append([]string{}, got.ProfileFields...)) != c.want {
			t.Errorf("%s: %v, want %s", c.doc, got.ProfileFields, c.want)
		}
	}
}
