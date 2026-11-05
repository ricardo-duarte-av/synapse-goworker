package synapsecfg

import "testing"

func TestFederationTLS(t *testing.T) {
	tl, err := federationTLS([]byte("federation_certificate_verification_whitelist: ['*.example.com', 'lon?don.test']\n"))
	if err != nil || !tl.Verify {
		t.Fatal(tl, err)
	}
	for host, want := range map[string]bool{"a.EXAMPLE.com": false, "example.com": true, "london.test": true,
		"lonxdon.test": false, "lon.don.test": false, "x.example.com.evil": true} {
		if got := tl.VerifyFor(host); got != want {
			t.Errorf("%s: %v", host, got)
		}
	}
	off, _ := federationTLS([]byte("federation_verify_certificates: false\n"))
	if off.VerifyFor("anything") {
		t.Error("verification on")
	}
	if _, err := federationTLS([]byte("federation_certificate_verification_whitelist: ['ünï.test']\n")); err == nil {
		t.Error("IDNA accepted")
	}
}
