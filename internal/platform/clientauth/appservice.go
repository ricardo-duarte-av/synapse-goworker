package clientauth

import (
	"fmt"
	"net/netip"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// appService is the part of Synapse's ApplicationService that decides who a
// request is from (appservice/__init__.py, config/appservice.py).
type appService struct {
	id     string
	token  string
	sender string // "@" + sender_localpart + ":" + server_name

	// users is the users namespace, each regex anchored only at the start:
	// Python's re.match, which does not require the match to reach the end.
	users []*regexp.Regexp

	// ipRanges is ip_range_whitelist; nil means any address.
	ipRanges []netip.Prefix
}

// interestedInUser is is_interested_in_user: the sender, or any user
// namespace, exclusive or not.
func (as *appService) interestedInUser(userID string) bool {
	if userID == as.sender {
		return true
	}
	for _, re := range as.users {
		if re.MatchString(userID) {
			return true
		}
	}
	return false
}

// allowsIP is the ip_range_whitelist check in get_appservice_user.
func (as *appService) allowsIP(ip string) bool {
	if as.ipRanges == nil {
		return true
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	for _, p := range as.ipRanges {
		if p.Contains(addr.Unmap()) {
			return true
		}
	}
	return false
}

// appServices is what was loaded: the services this worker can authenticate
// natively, and the tokens of those it cannot. A request with one of the
// latter goes to /whoami, so Synapse's own regex engine decides.
type appServices struct {
	byToken     map[string]*appService
	unsupported map[string]string // as_token -> why
}

// loadAppServices is load_appservices: every file, in order, validated as
// Synapse validates it. Synapse refuses to start on a registration it cannot
// load, so one that fails here is one this Synapse did not load either -- or
// one changed on disk since it started -- and is an error, not a skip.
func loadAppServices(serverName string, files []string) (*appServices, error) {
	out := &appServices{byToken: map[string]*appService{}, unsupported: map[string]string{}}
	seenIDs := map[string]string{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("appservice: %w", err)
		}
		as, why, err := parseAppService(serverName, b)
		if err != nil {
			return nil, fmt.Errorf("appservice %s: %w", f, err)
		}
		if prev, ok := seenIDs[as.id]; ok {
			return nil, fmt.Errorf("appservice %s: id %q is also %s's", f, as.id, prev)
		}
		seenIDs[as.id] = f
		if _, dup := out.byToken[as.token]; dup {
			return nil, fmt.Errorf("appservice %s: as_token reused", f)
		}
		if _, dup := out.unsupported[as.token]; dup {
			return nil, fmt.Errorf("appservice %s: as_token reused", f)
		}
		if why != "" {
			out.unsupported[as.token] = why
			continue
		}
		out.byToken[as.token] = as
	}
	return out, nil
}

// parseAppService is _load_appservice, for the fields read here. A non-empty
// why means the registration is valid but not reproducible natively.
func parseAppService(serverName string, b []byte) (*appService, string, error) {
	var info map[string]any
	if err := yaml.Unmarshal(b, &info); err != nil {
		return nil, "", err
	}
	for _, field := range []string{"id", "as_token", "hs_token", "sender_localpart"} {
		if _, ok := info[field].(string); !ok {
			return nil, "", fmt.Errorf("required string field: %q", field)
		}
	}
	localpart := info["sender_localpart"].(string)
	if !quoteSafe(localpart) {
		return nil, "", fmt.Errorf("sender_localpart needs characters which are not URL encoded")
	}
	as := &appService{
		id:     info["id"].(string),
		token:  info["as_token"].(string),
		sender: "@" + localpart + ":" + serverName,
	}

	namespaces, ok := info["namespaces"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("requires 'namespaces' object")
	}
	why := ""
	for _, ns := range []string{"users", "aliases", "rooms"} {
		entries, _ := namespaces[ns].([]any)
		for _, e := range entries {
			obj, ok := e.(map[string]any)
			if !ok {
				return nil, "", fmt.Errorf("namespace entry in %s is not an object", ns)
			}
			regex, ok := obj["regex"].(string)
			if !ok {
				return nil, "", fmt.Errorf("missing/bad type 'regex' key in %s", ns)
			}
			if _, ok := obj["exclusive"].(bool); !ok {
				return nil, "", fmt.Errorf("missing/bad type 'exclusive' key in %s", ns)
			}
			if ns != "users" {
				continue
			}
			// re.match anchors at the start only. A Python regex RE2
			// cannot compile (a lookaround, a backreference) leaves the
			// service to Synapse rather than to an approximation.
			re, err := regexp.Compile(`^(?:` + regex + `)`)
			if err != nil {
				why = fmt.Sprintf("user regex %q: %v", regex, err)
				continue
			}
			as.users = append(as.users, re)
		}
	}

	// IPSet accepts addresses and CIDRs alike.
	if list, ok := info["ip_range_whitelist"].([]any); ok && len(list) > 0 {
		for _, v := range list {
			s, _ := v.(string)
			p, err := parsePrefix(s)
			if err != nil {
				return nil, "", fmt.Errorf("ip_range_whitelist: %w", err)
			}
			as.ipRanges = append(as.ipRanges, p)
		}
	}
	return as, why, nil
}

func parsePrefix(s string) (netip.Prefix, error) {
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		return p.Masked(), err
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, err
	}
	return netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()), nil
}

// quoteSafe is urllib.parse.quote(s) == s: nothing outside letters, digits,
// "_.-~" and quote's default safe "/".
func quoteSafe(s string) bool {
	for _, c := range []byte(s) {
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("_.-~/", c) >= 0:
		default:
			return false
		}
	}
	return true
}
