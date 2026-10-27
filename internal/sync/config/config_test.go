package config

import "testing"

func TestZeroValueIsValid(t *testing.T) {
	var c Config
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.TimelineLimitCap() != 100 {
		t.Errorf("default timeline cap %d, want Synapse's 100", c.TimelineLimitCap())
	}
}

// 0 and -1 both mean something, so neither may be read as "unset".
func TestTimelineLimitZeroAndNoCap(t *testing.T) {
	for _, v := range []int{0, -1} {
		c := Config{FilterTimelineLimit: &v}
		if c.TimelineLimitCap() != v {
			t.Errorf("cap %d read as %d", v, c.TimelineLimitCap())
		}
	}
}

func TestValidateRejectsNegatives(t *testing.T) {
	c := Config{Auth: Auth{MaxEntries: -1}}
	if c.Validate() == nil {
		t.Error("a negative auth bound was accepted")
	}
}
