package sink

import (
	"testing"
)

func TestCountUnitsHandlesJunk(t *testing.T) {
	for _, body := range []string{``, `{}`, `not json`, `{"pdus":null}`} {
		if p, e := countUnits([]byte(body)); p != 0 || e != 0 {
			t.Errorf("%q counted %d pdus and %d edus", body, p, e)
		}
	}
}
