package synapsecfg

import "fmt"

// outboundRestrictedTo is config/workers.py's outbound_federation_restricted_to:
// when the list is not empty, worker_replication_secret must be set and
// every instance must be in instance_map, or Synapse refuses to start. The
// check is on the map's names after "main" became "master", so the main
// process is listed as "master" ("main" is refused there too). An empty
// list is not checked: nobody may federate.
func outboundRestrictedTo(r raw, cfg *Config) ([]string, error) {
	if r.OutboundFederationRestrictedTo == nil {
		return nil, nil
	}
	list := append([]string{}, (*r.OutboundFederationRestrictedTo)...)
	if len(list) == 0 {
		return list, nil
	}
	if cfg.ReplicationSecret == "" {
		return nil, fmt.Errorf("synapsecfg: `worker_replication_secret` must be configured when using `outbound_federation_restricted_to`; Synapse refuses this too")
	}
	for _, name := range list {
		if _, ok := cfg.InstanceMap[name]; !ok {
			return nil, fmt.Errorf("synapsecfg: Instance %q is configured in 'outbound_federation_restricted_to' but does not appear in `instance_map` config; Synapse refuses this too", name)
		}
	}
	return list, nil
}

// FederatesDirectly is whether instance talks to other servers itself
// (OutboundFederationRestrictedTo.__contains__): always when the setting
// is unset, else when it is listed.
func (c *Config) FederatesDirectly(instance string) bool {
	if c.OutboundFederationRestrictedTo == nil {
		return true
	}
	for _, n := range c.OutboundFederationRestrictedTo {
		if n == instance {
			return true
		}
	}
	return false
}

// FederationProxies is where an instance that does not federate directly
// sends its requests: the listed instances' instance_map addresses, in
// file order.
func (c *Config) FederationProxies() []Instance {
	out := make([]Instance, 0, len(c.OutboundFederationRestrictedTo))
	for _, n := range c.OutboundFederationRestrictedTo {
		if inst, ok := c.InstanceMap[n]; ok {
			out = append(out, inst)
		}
	}
	return out
}
