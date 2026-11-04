package main

import (
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

func TestCheckReplication(t *testing.T) {
	const me = "av-goworker-1"
	hs := func(inst *synapsecfg.Instance, secret string) *synapsecfg.Config {
		c := &synapsecfg.Config{InstanceMap: map[string]synapsecfg.Instance{}, ReplicationSecret: secret}
		if inst != nil {
			inst.Name = me
			c.InstanceMap[me] = *inst
		}
		return c
	}
	sock := config.Replication{Socket: "/run/goworker/repl.sock"}
	port := config.Replication{Addr: ":9093"}

	for _, c := range []struct {
		name   string
		listen config.Replication
		hs     *synapsecfg.Config
		// want is a substring of each finding, warn or not, in order.
		want []string
		warn []bool
	}{
		{"neither", config.Replication{}, hs(nil, "s"), nil, nil},
		{"listed, no listener", config.Replication{}, hs(&synapsecfg.Instance{Socket: "/x"}, "s"),
			[]string{"will fail"}, []bool{true}},
		{"listener, not listed", sock, hs(nil, "s"), []string{"no Synapse process calls it yet"}, []bool{false}},
		{"same socket", sock, hs(&synapsecfg.Instance{Socket: "/run/goworker/./repl.sock"}, "s"), nil, nil},
		{"other socket", sock, hs(&synapsecfg.Instance{Socket: "/run/other.sock"}, "s"),
			[]string{"is not listen.replication.socket"}, []bool{true}},
		{"socket vs port", sock, hs(&synapsecfg.Instance{URL: "http://goworker:9093"}, "s"),
			[]string{"is a socket"}, []bool{true}},
		{"port vs socket", port, hs(&synapsecfg.Instance{Socket: "/x"}, "s"), []string{"is a port"}, []bool{true}},
		{"same port", port, hs(&synapsecfg.Instance{URL: "http://goworker:9093"}, "s"), nil, nil},
		{"other port", port, hs(&synapsecfg.Instance{URL: "http://goworker:9094"}, "s"),
			[]string{"(9094) is not"}, []bool{true}},
		{"tls", port, hs(&synapsecfg.Instance{URL: "https://goworker:9093"}, "s"), []string{"TLS"}, []bool{true}},
		{"no secret", sock, hs(&synapsecfg.Instance{Socket: "/run/goworker/repl.sock"}, ""),
			[]string{"worker_replication_secret is unset"}, []bool{true}},
	} {
		got := checkReplication(me, c.listen, c.hs)
		if len(got) != len(c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
			continue
		}
		for i, f := range got {
			if !strings.Contains(f.msg, c.want[i]) || f.warn != c.warn[i] {
				t.Errorf("%s: finding %d is %+v, want %q warn=%v", c.name, i, f, c.want[i], c.warn[i])
			}
		}
	}
}
