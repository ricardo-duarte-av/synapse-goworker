package config

import "testing"

func TestDefaultsAreValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatal(err)
	}
	if !Defaults().MarksOriginsUp() {
		t.Error("origins in backoff are not marked up by default")
	}
}

func TestValidateRefusesUnknownChoices(t *testing.T) {
	c := Defaults()
	c.MissingEvents = "maybe"
	if c.Validate() == nil {
		t.Error("an unknown missing_events was accepted")
	}
	c = Defaults()
	c.Fallback = "main"
	if c.Validate() == nil {
		t.Error("an unknown fallback was accepted")
	}
}
