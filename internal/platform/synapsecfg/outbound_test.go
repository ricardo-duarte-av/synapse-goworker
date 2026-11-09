package synapsecfg

import (
	"strings"
	"testing"
)

const outboundBase = `
server_name: example.org
worker_replication_secret: "s3cret"
instance_map:
  main:
    path: /sock/main
  goworker:
    host: goworker
    port: 9300
    tls: true
`

func TestOutboundRestrictedTo(t *testing.T) {
	for _, tc := range []struct {
		name, extra string
		wantNil     bool
		direct      map[string]bool
		proxies     []string
	}{
		{"absent", "", true, map[string]bool{"x": true, "goworker": true}, nil},
		{"null", "outbound_federation_restricted_to: null\n", true, map[string]bool{"x": true}, nil},
		{"empty", "outbound_federation_restricted_to: []\n", false, map[string]bool{"x": false, "goworker": false}, nil},
		{"listed", "outbound_federation_restricted_to: [goworker, master]\n", false,
			map[string]bool{"goworker": true, "master": true, "x": false}, []string{"https://goworker:9300", "/sock/main"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustParse(t, outboundBase+tc.extra)
			if (cfg.OutboundFederationRestrictedTo == nil) != tc.wantNil {
				t.Fatalf("restricted = %#v", cfg.OutboundFederationRestrictedTo)
			}
			for inst, want := range tc.direct {
				if got := cfg.FederatesDirectly(inst); got != want {
					t.Errorf("FederatesDirectly(%q) = %v", inst, got)
				}
			}
			var got []string
			for _, p := range cfg.FederationProxies() {
				got = append(got, p.URL+p.Socket)
			}
			if strings.Join(got, ",") != strings.Join(tc.proxies, ",") {
				t.Errorf("proxies = %v, want %v", got, tc.proxies)
			}
		})
	}
}

// Synapse refuses to start on these (config/workers.py).
func TestOutboundRestrictedToRefused(t *testing.T) {
	for name, y := range map[string]string{
		"not in instance_map": outboundBase + "outbound_federation_restricted_to: [elsewhere]\n",
		// The map holds main as "master".
		"main by its yaml name": outboundBase + "outbound_federation_restricted_to: [main]\n",
		"no secret":             "server_name: example.org\ninstance_map:\n  goworker:\n    path: /s\noutbound_federation_restricted_to: [goworker]\n",
	} {
		if _, err := parse([]byte(y), "hs.yaml"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
