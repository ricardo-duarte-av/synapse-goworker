package handlers

import (
	"context"
	"net/http/httptest"
	"testing"
)

// MSC4222, stable since Synapse v1.164: the stable parameter wins, the
// unstable one still works and keeps its field name, and a value that is
// not "true" or "false" is parse_boolean's 400.
func TestParseUseStateAfter(t *testing.T) {
	for query, want := range map[string]struct {
		on, unstable bool
		err          string
	}{
		"":                       {},
		"?use_state_after=true":  {on: true},
		"?use_state_after=false": {},
		"?org.matrix.msc4222.use_state_after=true":                       {on: true, unstable: true},
		"?use_state_after=true&org.matrix.msc4222.use_state_after=true":  {on: true},
		"?use_state_after=false&org.matrix.msc4222.use_state_after=true": {on: true, unstable: true},
		"?use_state_after=1":                      {err: "Boolean query parameter 'use_state_after' must be one of ['true', 'false']"},
		"?org.matrix.msc4222.use_state_after=yes": {err: "Boolean query parameter 'org.matrix.msc4222.use_state_after' must be one of ['true', 'false']"},
	} {
		on, unstable, err := parseUseStateAfter(httptest.NewRequest("GET", "/_matrix/client/v3/sync"+query, nil))
		switch {
		case want.err != "" && (err == nil || err.Error != want.err || err.ErrCode != "M_INVALID_PARAM"):
			t.Errorf("%q: error %+v, want %q", query, err, want.err)
		case want.err == "" && (err != nil || on != want.on || unstable != want.unstable):
			t.Errorf("%q: on %v unstable %v err %+v, want %+v", query, on, unstable, err, want)
		}
	}
}

func TestStateKeyName(t *testing.T) {
	ctx := context.Background()
	unstable := context.WithValue(ctx, unstableStateAfterKey{}, true)
	for _, tc := range []struct {
		ctx  context.Context
		on   bool
		want string
	}{
		{ctx, false, "state"},
		{unstable, false, "state"},
		{ctx, true, "state_after"},
		{unstable, true, "org.matrix.msc4222.state_after"},
	} {
		if got := stateKeyName(tc.ctx, tc.on); got != tc.want {
			t.Errorf("on %v, unstable %v: %q, want %q", tc.on, tc.ctx != ctx, got, tc.want)
		}
	}
}
