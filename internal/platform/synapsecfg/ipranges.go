package synapsecfg

import (
	"fmt"
	"net/netip"
	"strings"

	"gopkg.in/yaml.v3"
)

// defaultIPRangeBlocklist is DEFAULT_IP_RANGE_BLOCKLIST (config/server.py,
// v1.163.0).
var defaultIPRangeBlocklist = []string{
	"127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10",
	"192.0.0.0/24", "169.254.0.0/16", "192.88.99.0/24", "198.18.0.0/15", "192.0.2.0/24",
	"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "::1/128", "fe80::/10", "fc00::/7",
	"2001:db8::/32", "ff00::/8", "fec0::/10",
}

// IPRanges is a federation IP block and allow list: an address is refused
// when it is in Block and not in Allow.
type IPRanges struct {
	Block, Allow []netip.Prefix
}

// Blocked reports whether outbound federation may not connect to ip.
func (r IPRanges) Blocked(ip netip.Addr) bool {
	ip = ip.Unmap()
	in := func(ps []netip.Prefix) bool {
		for _, p := range ps {
			if p.Contains(ip) {
				return true
			}
		}
		return false
	}
	return in(r.Block) && !in(r.Allow)
}

type ipRangesRaw struct {
	Blacklist           *[]string `yaml:"ip_range_blacklist"`
	Whitelist           []string  `yaml:"ip_range_whitelist"`
	FederationBlacklist *[]string `yaml:"federation_ip_range_blacklist"`
}

// federationIPRanges is federation_ip_range_blocklist and its allow list:
// federation_ip_range_blacklist when given (with no allow list), else
// ip_range_blacklist (default DEFAULT_IP_RANGE_BLOCKLIST) and
// ip_range_whitelist. 0.0.0.0 and :: are always blocked.
func federationIPRanges(b []byte) (IPRanges, error) {
	var r ipRangesRaw
	if err := yaml.Unmarshal(b, &r); err != nil {
		return IPRanges{}, fmt.Errorf("synapsecfg: ip ranges: %w", err)
	}
	block := defaultIPRangeBlocklist
	var allow []string
	switch {
	case r.FederationBlacklist != nil:
		block = *r.FederationBlacklist
	case r.Blacklist != nil:
		block, allow = *r.Blacklist, r.Whitelist
	default:
		allow = r.Whitelist
	}
	out := IPRanges{}
	var err error
	if out.Block, err = prefixes(append(append([]string{}, block...), "0.0.0.0", "::")); err != nil {
		return IPRanges{}, err
	}
	if out.Allow, err = prefixes(allow); err != nil {
		return IPRanges{}, err
	}
	return out, nil
}

func prefixes(ss []string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, s := range ss {
		if !strings.Contains(s, "/") {
			a, err := netip.ParseAddr(s)
			if err != nil {
				return nil, fmt.Errorf("synapsecfg: ip range %q: %w", s, err)
			}
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return nil, fmt.Errorf("synapsecfg: ip range %q: %w", s, err)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}
