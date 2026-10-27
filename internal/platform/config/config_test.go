package config

import (
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
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
	if c.Database.Pools.Read != 16 || c.Inbound.MissingEvents != "fetch" {
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
		"database: { manage_roles: true }",
		"inbound: { state_cache_entries: 400000 }",
		"federation_reads: { caches: { state_groups_mb: 768 } }",
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

func TestDSNDefaultsToHomeserverYAML(t *testing.T) {
	synapse := "host=matrix-pgcat port=6432 user=synapse dbname=synapse-db"
	if got := (Database{}).DSNOr(synapse); got != synapse {
		t.Errorf("unset dsn = %q, want homeserver.yaml's", got)
	}
	if got := (Database{DSN: "host=/s"}).DSNOr(synapse); got != "host=/s" {
		t.Errorf("set dsn = %q", got)
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
		t.Error("default caches (~1.1GB) were accepted under an 800MB limit")
	}
	if _, err := Parse([]byte(minimal + "memory: { limit_mb: 3072 }\n")); err != nil {
		t.Errorf("default caches under 3GB: %v", err)
	}
	if _, err := Parse([]byte(minimal)); err != nil {
		t.Errorf("no limit: %v", err)
	}
}

// A part's level overrides log.level for that part only; a part not named,
// and the process's own lines, keep the default.
func TestLogLevelPerComponent(t *testing.T) {
	c, err := Parse([]byte(minimal + "log: { level: warn, components: { sender: error, sync: info } }\n"))
	if err != nil {
		t.Fatal(err)
	}
	for comp, want := range map[string]zerolog.Level{
		"sender": zerolog.ErrorLevel, "sync": zerolog.InfoLevel,
		"inbound": zerolog.WarnLevel, "": zerolog.WarnLevel,
	} {
		if got := c.Log.LevelFor(comp); got != want {
			t.Errorf("LevelFor(%q) = %v, want %v", comp, got, want)
		}
	}
	for _, bad := range []string{
		"log: { components: { federation: info } }", // not a part's name
		"log: { components: { sender: loud } }",
		"log: { level: loud }",
	} {
		if _, err := Parse([]byte(minimal + bad + "\n")); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
