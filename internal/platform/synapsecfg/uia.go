package synapsecfg

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// UIA is what user-interactive authentication reads (config/auth.py,
// config/sso.py, config/password_auth_providers.py).
type UIA struct {
	// PasswordForLogin and PasswordForReauth are password_config.enabled
	// (true, false or "only_for_reauth"); LocalDB its localdb_enabled,
	// Pepper its pepper.
	PasswordForLogin, PasswordForReauth bool
	LocalDB                             bool
	Pepper                              string
	// SessionTimeoutMS is ui_auth.session_timeout.
	SessionTimeoutMS int64
	// SSO is any SSO provider configured (OIDC, SAML2, CAS): a user with
	// an external ID is offered m.login.sso, which only main can run.
	SSO bool
	// PasswordProviders is any password_providers entry: their login
	// types and checks are Python modules.
	PasswordProviders bool
}

type uiaRaw struct {
	PasswordConfig    yaml.Node `yaml:"password_config"`
	UIAuth            yaml.Node `yaml:"ui_auth"`
	OIDCProviders     yaml.Node `yaml:"oidc_providers"`
	OIDCConfig        yaml.Node `yaml:"oidc_config"`
	SAML2Config       yaml.Node `yaml:"saml2_config"`
	CASConfig         yaml.Node `yaml:"cas_config"`
	PasswordProviders yaml.Node `yaml:"password_providers"`
}

// truthy is Python's bool() of a YAML value.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return true
}

// configured is an SSO section present and not disabled. Synapse's own
// defaults differ by section; any section that could be enabled sends
// UIA to main, which can only cost main the request.
func configured(n yaml.Node) bool {
	var v any
	if n.Decode(&v) != nil || !truthy(v) {
		return false
	}
	if m, ok := v.(map[string]any); ok {
		if e, set := m["enabled"]; set && !truthy(e) {
			return false
		}
	}
	return true
}

func uiaSettings(b []byte) (UIA, error) {
	var r uiaRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return UIA{}, fmt.Errorf("synapsecfg: password_config: %w", err)
	}
	u := UIA{PasswordForLogin: true, PasswordForReauth: true, LocalDB: true}
	pc := lastWins(&r.PasswordConfig)
	if e, ok := pc["enabled"]; ok {
		u.PasswordForReauth = truthy(e)
		u.PasswordForLogin = truthy(e) && e != "only_for_reauth"
	}
	if l, ok := pc["localdb_enabled"]; ok {
		u.LocalDB = truthy(l)
	}
	if p, ok := pc["pepper"].(string); ok {
		u.Pepper = p
	}
	if r.UIAuth.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(r.UIAuth.Content); i += 2 {
			if r.UIAuth.Content[i].Value == "session_timeout" && r.UIAuth.Content[i+1].Tag != "!!null" {
				ms, err := durationNode(*r.UIAuth.Content[i+1])
				if err != nil {
					return UIA{}, fmt.Errorf("synapsecfg: ui_auth.session_timeout: %w", err)
				}
				u.SessionTimeoutMS = ms
			}
		}
	}
	var providers []any
	if r.OIDCProviders.Decode(&providers) == nil {
		for _, p := range providers {
			if m, ok := p.(map[string]any); ok {
				if e, set := m["enabled"]; set && !truthy(e) {
					continue
				}
			}
			u.SSO = true
		}
	}
	u.SSO = u.SSO || configured(r.OIDCConfig) || configured(r.SAML2Config) || configured(r.CASConfig)
	var pp []any
	u.PasswordProviders = r.PasswordProviders.Decode(&pp) == nil && len(pp) > 0
	return u, nil
}
