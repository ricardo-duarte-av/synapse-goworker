package config

import "testing"

func TestDefaultsAreValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRefusesZeroLimits(t *testing.T) {
	c := Defaults()
	c.MaxPDUsPerTransaction = 0
	if err := c.Validate(); err == nil {
		t.Error("a zero PDU limit was accepted")
	}
	c = Defaults()
	c.PresenceBatchStates = -1
	if err := c.Validate(); err == nil {
		t.Error("a negative presence batch was accepted")
	}
}
