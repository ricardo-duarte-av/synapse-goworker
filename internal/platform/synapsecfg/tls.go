package synapsecfg

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// FederationTLS is config/tls.py's client side: whether outbound
// federation verifies certificates, and the servers it does not verify.
type FederationTLS struct {
	// Verify is federation_verify_certificates (default true).
	Verify bool
	// Unverified is federation_certificate_verification_whitelist, each
	// glob as glob_to_regex makes it (anchored, case-insensitive).
	Unverified []*regexp.Regexp
}

type tlsRaw struct {
	Verify    *bool    `yaml:"federation_verify_certificates"`
	Whitelist []string `yaml:"federation_certificate_verification_whitelist"`
}

func federationTLS(b []byte) (FederationTLS, error) {
	var r tlsRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return FederationTLS{}, fmt.Errorf("synapsecfg: federation TLS: %w", err)
	}
	t := FederationTLS{Verify: deref(r.Verify, true)}
	for _, g := range r.Whitelist {
		for _, c := range g {
			if c > 0x7f {
				return FederationTLS{}, fmt.Errorf("synapsecfg: IDNA domain names are not allowed in the federation_certificate_verification_whitelist: %s", g)
			}
		}
		t.Unverified = append(t.Unverified, GlobToRegex(g))
	}
	return t, nil
}

// GlobToRegex is matrix_common's glob_to_regex: '*' any run, '?' one
// character, everything else literal; whole string, case-insensitive.
func GlobToRegex(glob string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString(`(?is)\A`)
	for _, r := range glob {
		switch r {
		case '*':
			b.WriteString(`.*?`)
		case '?':
			b.WriteString(`.`)
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString(`\z`)
	return regexp.MustCompile(b.String())
}

// VerifyFor is whether a federation connection to host checks its
// certificate.
func (t FederationTLS) VerifyFor(host string) bool {
	if !t.Verify {
		return false
	}
	for _, re := range t.Unverified {
		if re.MatchString(host) {
			return false
		}
	}
	return true
}
