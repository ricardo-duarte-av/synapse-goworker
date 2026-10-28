// Package appservice reads the application service registrations Synapse is
// configured with (config/appservice.py): which users an appservice claims
// exclusively, as Synapse never computes push actions for them
// (bulk_push_rule_evaluator.py:193), and who an appservice's token may act
// as (api/auth/base.py, get_appservice_user).
package appservice

import (
	"fmt"
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

// Service is an ApplicationService, as far as a request from it goes.
type Service struct {
	ID     string
	Token  string
	Sender string
	// RateLimited is rate_limited: true unless the registration says a
	// bool false.
	RateLimited bool
	// IPRanges is ip_range_whitelist being set: a request from elsewhere is
	// not the appservice's.
	IPRanges bool
	users    []*regexp.Regexp
	// broken is a namespace regex Go cannot compile.
	broken bool
}

// Load reads the registrations: the services by as_token.
func Load(files []string, serverName string) (map[string]*Service, error) {
	out := map[string]*Service{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("appservice: %w", err)
		}
		var r registration
		if err := yaml.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("appservice: %s: %w", f, err)
		}
		if r.ASToken == "" {
			continue
		}
		s := &Service{ID: r.ID, Token: r.ASToken, Sender: "@" + r.SenderLocalpart + ":" + serverName, RateLimited: true}
		if v, ok := r.RateLimited.(bool); ok {
			s.RateLimited = v
		}
		// `if as_info.get("ip_range_whitelist")`: anything truthy.
		switch v := r.IPRangeWhitelist.(type) {
		case nil:
		case []any:
			s.IPRanges = len(v) > 0
		case string:
			s.IPRanges = v != ""
		default:
			s.IPRanges = true
		}
		for _, u := range r.Namespaces.Users {
			// re.match: anchored at the start only.
			re, err := regexp.Compile(`^(?:` + u.Regex + `)`)
			if err != nil {
				s.broken = true
				continue
			}
			s.users = append(s.users, re)
		}
		out[s.Token] = s
	}
	return out, nil
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
	return false, !s.broken
}
