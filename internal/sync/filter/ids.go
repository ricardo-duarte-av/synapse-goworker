package filter

import "strings"

// ValidUserID reports whether a string is a user ID Synapse would accept in a
// filter.
//
// This mirrors `UserID.is_valid`, which is deliberately lax: it checks the
// sigil, that a domain is present, and that the domain parses. It does *not*
// reject an empty localpart or an over-long ID -- there are non-compliant user
// IDs in the wild and rejecting them here would break filters that name them.
func ValidUserID(s string) bool {
	if !strings.HasPrefix(s, "@") {
		return false
	}
	_, domain, ok := strings.Cut(s[1:], ":")
	return ok && validServerName(domain)
}

// ValidRoomID reports whether a string is a room ID Synapse would accept.
//
// Two forms exist: `!localpart:domain` for room versions before MSC4291, and
// `!<base64 event id>` for those after it, which have no domain at all.
func ValidRoomID(s string) bool {
	if !strings.HasPrefix(s, "!") {
		return false
	}
	if strings.Contains(s, ":") {
		_, domain, _ := strings.Cut(s[1:], ":")
		return validServerName(domain)
	}
	// An MSC4291 room ID is the create event's ID: unpadded URL-safe base64.
	rest := s[1:]
	if rest == "" {
		return false
	}
	for _, r := range rest {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// validServerName is a permissive check on the domain half of an ID.
//
// Synapse runs `parse_and_validate_server_name`, which accepts a hostname, an
// IPv4 literal or a bracketed IPv6 literal, each with an optional port. The
// check here is looser: it rejects the empty string, a leading colon and
// obvious junk, which is what actually distinguishes a typo'd filter from a
// real one. A deliberately malformed-but-plausible domain is accepted here and
// rejected by Synapse -- recorded in README.md as a deviation.
func validServerName(domain string) bool {
	if domain == "" {
		return false
	}
	// Strip a port, if any. An IPv6 literal is bracketed, so a colon inside
	// brackets is not a port separator.
	if strings.HasPrefix(domain, "[") {
		end := strings.Index(domain, "]")
		if end < 0 {
			return false
		}
		host := domain[1:end]
		if host == "" {
			return false
		}
		rest := domain[end+1:]
		return rest == "" || (strings.HasPrefix(rest, ":") && allDigits(rest[1:]))
	}
	if host, port, ok := strings.Cut(domain, ":"); ok {
		return host != "" && allDigits(port) && port != ""
	}
	return !strings.ContainsAny(domain, " \t\n/")
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// CompliantUserID is is_compliant_user_id (types/__init__.py, v1.164.1): the
// localpart is not empty and every character is between U+0021 and U+007E.
// Never fails on a malformed ID; one without the sigil is not compliant.
func CompliantUserID(s string) bool {
	if !strings.HasPrefix(s, "@") {
		return false
	}
	localpart, _, _ := strings.Cut(s[1:], ":")
	if localpart == "" {
		return false
	}
	for _, r := range localpart {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

// IsMineID is HomeServer.is_mine_id: what follows the first colon is this
// server's name.
func IsMineID(s, serverName string) bool {
	_, host, ok := strings.Cut(s, ":")
	return ok && host == serverName
}

// SyncableUserID is the filter Synapse v1.164 puts on the users a sync
// reports in device_lists and in presence for newly visible users (#20338):
// local users, and remote ones whose ID is compliant.
func SyncableUserID(s, serverName string) bool {
	return IsMineID(s, serverName) || CompliantUserID(s)
}
