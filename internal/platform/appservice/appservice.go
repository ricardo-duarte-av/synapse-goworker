// Package appservice reads the application service registrations Synapse is
// configured with (config/appservice.py): which users an appservice claims
// exclusively, as Synapse never computes push actions for them
// (bulk_push_rule_evaluator.py:193), and who an appservice's token may act
// as (api/auth/base.py, get_appservice_user).
package appservice

import (
	"fmt"
	"net/netip"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type registration struct {
	ID               string `yaml:"id"`
	ASToken          string `yaml:"as_token"`
	SenderLocalpart  string `yaml:"sender_localpart"`
	RateLimited      any    `yaml:"rate_limited"`
	IPRangeWhitelist any    `yaml:"ip_range_whitelist"`
	Namespaces       struct {
		Users []struct {
			Exclusive bool   `yaml:"exclusive"`
			Regex     string `yaml:"regex"`
		} `yaml:"users"`
	} `yaml:"namespaces"`
}

// ExclusiveUsers is _make_exclusive_regex over the registrations: the
// exclusive user namespaces joined, matched as Python's re.match does -- at
// the start of the user ID, not necessarily to its end. nil when none.
func ExclusiveUsers(files []string) (*regexp.Regexp, error) {
	var parts []string
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("appservice: %w", err)
		}
		var r registration
		if err := yaml.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("appservice: %s: %w", f, err)
		}
		for _, u := range r.Namespaces.Users {
			if u.Exclusive && u.Regex != "" {
				parts = append(parts, "("+u.Regex+")")
			}
		}
	}
	if len(parts) == 0 {
		return nil, nil
	}
	re, err := regexp.Compile(`^(?:` + strings.Join(parts, "|") + `)`)
	if err != nil {
		// A Python regex Go cannot compile (lookarounds, say).
		return nil, fmt.Errorf("appservice: exclusive user regex: %w", err)
	}
	return re, nil
}

// Service is an ApplicationService, as far as a request from it goes
// (appservice/__init__.py, config/appservice.py).
type Service struct {
	ID     string
	Token  string
	Sender string // "@" + sender_localpart + ":" + server_name
	// RateLimited is rate_limited: true unless the registration says a
	// bool false.
	RateLimited bool
	// IPRanges is ip_range_whitelist being set: a request from elsewhere is
	// not the appservice's.
	IPRanges bool
	// Unsupported says why its requests cannot be judged natively: a user
	// namespace regex RE2 cannot compile (a lookaround, a backreference).
	// Empty when they can.
	Unsupported string

	// users is the users namespace, each regex anchored only at the start:
	// Python's re.match, which does not require the match to reach the end.
	users []*regexp.Regexp
	// ipRanges is ip_range_whitelist; nil means any address.
	ipRanges []netip.Prefix
}

// Load is load_appservices: every file, in order, validated as Synapse
// validates it, the services by as_token. Synapse refuses to start on a
// registration it cannot load, so one that fails here is one this Synapse did
// not load either -- or one changed on disk since it started -- and is an
// error, not a skip.
func Load(files []string, serverName string) (map[string]*Service, error) {
	out := map[string]*Service{}
	seenIDs := map[string]string{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("appservice: %w", err)
		}
		s, err := parse(serverName, b)
		if err != nil {
			return nil, fmt.Errorf("appservice %s: %w", f, err)
		}
		if prev, ok := seenIDs[s.ID]; ok {
			return nil, fmt.Errorf("appservice %s: id %q is also %s's", f, s.ID, prev)
		}
		seenIDs[s.ID] = f
		if _, dup := out[s.Token]; dup {
			return nil, fmt.Errorf("appservice %s: as_token reused", f)
		}
		out[s.Token] = s
	}
	return out, nil
}

// parse is _load_appservice, for the fields read here.
func parse(serverName string, b []byte) (*Service, error) {
	var info map[string]any
	if err := yaml.Unmarshal(b, &info); err != nil {
		return nil, err
	}
	for _, field := range []string{"id", "as_token", "hs_token", "sender_localpart"} {
		if _, ok := info[field].(string); !ok {
			return nil, fmt.Errorf("required string field: %q", field)
		}
	}
	localpart := info["sender_localpart"].(string)
	if !quoteSafe(localpart) {
		return nil, fmt.Errorf("sender_localpart needs characters which are not URL encoded")
	}
	s := &Service{
		ID:          info["id"].(string),
		Token:       info["as_token"].(string),
		Sender:      "@" + localpart + ":" + serverName,
		RateLimited: true,
	}
	if rl, ok := info["rate_limited"].(bool); ok {
		s.RateLimited = rl
	}

	namespaces, ok := info["namespaces"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("requires 'namespaces' object")
	}
	for _, ns := range []string{"users", "aliases", "rooms"} {
		entries, _ := namespaces[ns].([]any)
		for _, e := range entries {
			obj, ok := e.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("namespace entry in %s is not an object", ns)
			}
			regex, ok := obj["regex"].(string)
			if !ok {
				return nil, fmt.Errorf("missing/bad type 'regex' key in %s", ns)
			}
			if _, ok := obj["exclusive"].(bool); !ok {
				return nil, fmt.Errorf("missing/bad type 'exclusive' key in %s", ns)
			}
			if ns != "users" {
				continue
			}
			// re.match anchors at the start only. A Python regex RE2
			// cannot compile leaves the service to Synapse rather than to
			// an approximation.
			re, err := regexp.Compile(`^(?:` + regex + `)`)
			if err != nil {
				s.Unsupported = fmt.Sprintf("user regex %q: %v", regex, err)
				continue
			}
			s.users = append(s.users, re)
		}
	}

	// `if as_info.get("ip_range_whitelist")`: anything truthy. IPSet accepts
	// addresses and CIDRs alike.
	switch v := info["ip_range_whitelist"].(type) {
	case nil:
	case []any:
		s.IPRanges = len(v) > 0
		for _, e := range v {
			str, _ := e.(string)
			p, err := parsePrefix(str)
			if err != nil {
				return nil, fmt.Errorf("ip_range_whitelist: %w", err)
			}
			s.ipRanges = append(s.ipRanges, p)
		}
	case string:
		s.IPRanges = v != ""
	default:
		s.IPRanges = true
	}
	return s, nil
}

// InterestedInUser is is_interested_in_user: the sender, or any user
// namespace, exclusive or not.
func (s *Service) InterestedInUser(userID string) bool {
	if userID == s.Sender {
		return true
	}
	for _, re := range s.users {
		if re.MatchString(userID) {
			return true
		}
	}
	return false
}

// AllowsIP is the ip_range_whitelist check in get_appservice_user.
func (s *Service) AllowsIP(ip string) bool {
	if s.ipRanges == nil {
		return true
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	for _, p := range s.ipRanges {
		if p.Contains(addr.Unmap()) {
			return true
		}
	}
	return false
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

// Registry is the registrations, by as_token and by ID.
type Registry struct {
	byToken map[string]*Service
	byID    map[string]*Service
	all     []*Service
}

// NewRegistry indexes what Load read.
func NewRegistry(services map[string]*Service) *Registry {
	r := &Registry{byToken: services, byID: map[string]*Service{}}
	for _, s := range services {
		r.byID[s.ID] = s
		r.all = append(r.all, s)
	}
	return r
}

// ByToken is the service whose as_token it is, or nil.
func (r *Registry) ByToken(token string) *Service { return r.byToken[token] }

// ByID is the service with that ID, or nil.
func (r *Registry) ByID(id string) *Service { return r.byID[id] }

// All is every service.
func (r *Registry) All() []*Service { return r.all }

// InUserNamespace is is_user_in_namespace. ok is false when a namespace
// could not be compiled, and the answer is unknown.
func (s *Service) InUserNamespace(userID string) (in, ok bool) {
	for _, re := range s.users {
		if re.MatchString(userID) {
			return true, true
		}
	}
	return false, s.Unsupported == ""
}
