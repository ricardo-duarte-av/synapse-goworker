// Package userid is what Synapse checks of a user ID string
// (types/__init__.py): UserID.from_string's errors, its domain, and
// is_compliant_user_id.
package userid

import (
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// ParseError is UserID.from_string's SynapseError(400, ..., M_INVALID_PARAM).
type ParseError struct{ Msg string }

func (e *ParseError) Error() string { return e.Msg }

// Check is UserID.from_string succeeding: an '@' and a ':' after it.
func Check(s string) *ParseError {
	if !strings.HasPrefix(s, "@") {
		return &ParseError{"Expected UserID string to start with '@'"}
	}
	if !strings.Contains(s[1:], ":") {
		return &ParseError{"Expected UserID of the form '@localname:domain'"}
	}
	return nil
}

// Domain is UserID.from_string(s).domain (get_domain_from_id): everything
// after the first colon.
func Domain(s string) string {
	_, domain, _ := strings.Cut(s, ":")
	return domain
}

// Compliant is is_compliant_user_id: an '@', and a localpart that is not
// empty and all U+0021 to U+007E.
func Compliant(s string) bool {
	if !strings.HasPrefix(s, "@") {
		return false
	}
	local, _, _ := strings.Cut(s[1:], ":")
	if local == "" {
		return false
	}
	for _, r := range local {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

// Valid is UserID.is_valid: from_string succeeds, and the domain is a
// valid server name (parse_and_validate_server_name).
func Valid(s string) bool {
	return Check(s) == nil && pdu.ValidServerName(Domain(s))
}
