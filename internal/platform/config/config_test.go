package config

import (
	"os"
	"strings"
	"testing"
)

const minimal = `
instance_name: goworker-1
listen: { socket: /tmp/goworker.sock }
upstream: { sockets: [/tmp/main.sock] }
`

func TestMinimalConfigLoads(t *testing.T) {
	c, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if c.HomeserverYAML != "/etc/synapse/homeserver.yaml" || c.Upstream.TimeoutSeconds != 180 {
		t.Errorf("defaults not applied: %+v", c)
	}
	if !c.Database.ManagesRoles() || c.Inbound.MissingEvents != "fetch" {
		t.Error("production defaults not applied")
	}
}

// There is no switch for what the worker does: a components block, a mode
// or a shadow section is refused, not ignored.
func TestNoComponentOrModeSwitches(t *testing.T) {
	for _, extra := range []string{
		"components: { sync: true }",
		"mode: primary",
		"shadow: { enabled: false }",
		"inbound: { modes: { edu: native } }",
		"federation_reads: { endpoints: { event: native } }",
	} {
		if _, err := Parse([]byte(minimal + extra + "\n")); err == nil {
			t.Errorf("accepted %q", extra)
		}
	}
}

func TestValidationRejects(t *testing.T) {
	for _, bad := range []string{
		strings.Replace(minimal, "instance_name: goworker-1", "", 1),
		strings.Replace(minimal, "upstream: { sockets: [/tmp/main.sock] }", "", 1),
		minimal + "listen: { socket: /a, addr: ':1' }\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("accepted:\n%s", bad)
		}
	}
}

func TestDSNFor(t *testing.T) {
	base, synapse := "host=/s dbname=d", "host=pgcat user=synapse"
	for _, tc := range []struct {
		p    Pool
		want string
	}{
		{Pool{DSN: "host=x user=y"}, "host=x user=y"},
		{Pool{User: "gopro_ro"}, "host=/s dbname=d user=gopro_ro"},
		{Pool{}, base},
	} {
		if got := DSNFor(tc.p, base, synapse); got != tc.want {
			t.Errorf("DSNFor(%+v) = %q, want %q", tc.p, got, tc.want)
		}
	}
	if got := DSNFor(Pool{}, "", synapse); got != synapse {
		t.Errorf("no base: %q, want homeserver.yaml's", got)
	}
}

func TestExampleConfigParses(t *testing.T) {
	b, err := os.ReadFile("../../../deploy/goworker.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(b); err != nil {
		t.Fatal(err)
	}
}

// The caches must leave room for requests in flight.
func TestCachesMustFitTheMemoryLimit(t *testing.T) {
	if _, err := Parse([]byte(minimal + "memory: { limit_mb: 800 }\n")); err == nil {
		t.Error("default caches (~880MB) were accepted under an 800MB limit")
	}
	if _, err := Parse([]byte(minimal + "memory: { limit_mb: 3072 }\n")); err != nil {
		t.Errorf("default caches under 3GB: %v", err)
	}
	if _, err := Parse([]byte(minimal)); err != nil {
		t.Errorf("no limit: %v", err)
	}
}
