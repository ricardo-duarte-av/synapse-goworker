package main

import (
	"net"
	"net/url"
	"path/filepath"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/config"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// replicationFinding is one thing the replication listener and
// homeserver.yaml disagree on. warn is false for a normal state worth a
// line in the log, true for one where some Synapse process will fail to
// reach us or reach us unchecked.
type replicationFinding struct {
	warn bool
	msg  string
}

// checkReplication compares listen.replication with what homeserver.yaml
// tells every Synapse process about this one: its instance_map entry and
// worker_replication_secret. None of it stops the worker starting (CLAUDE.md:
// a missing capability is a warning); each is logged at start and on every
// re-read of homeserver.yaml.
func checkReplication(instance string, l config.Replication, hs *synapsecfg.Config) []replicationFinding {
	var out []replicationFinding
	inst, listed := hs.Instance(instance)
	switch {
	case !l.Enabled() && !listed:
		return nil
	case !l.Enabled():
		return []replicationFinding{{true, "homeserver.yaml's instance_map names this process, and listen.replication " +
			"is unset: every Synapse process that calls it will fail"}}
	case !listed:
		out = append(out, replicationFinding{false, "the replication listener is open, and instance_map does not " +
			"name this process: no Synapse process calls it yet"})
	case l.Socket != "" && inst.Socket == "":
		out = append(out, replicationFinding{true, "instance_map gives this process a host and port, and " +
			"listen.replication is a socket: Synapse processes will not reach it"})
	case l.Socket != "" && filepath.Clean(inst.Socket) != filepath.Clean(l.Socket):
		out = append(out, replicationFinding{true, "instance_map's path for this process (" + inst.Socket +
			") is not listen.replication.socket (" + l.Socket + "); check both are the same socket, seen from " +
			"two containers"})
	case l.Addr != "" && inst.URL == "":
		out = append(out, replicationFinding{true, "instance_map gives this process a path, and " +
			"listen.replication is a port: Synapse processes will not reach it"})
	case l.Addr != "":
		u, err := url.Parse(inst.URL)
		_, port, perr := net.SplitHostPort(l.Addr)
		switch {
		case err != nil || perr != nil:
			out = append(out, replicationFinding{true, "cannot compare instance_map's " + inst.URL +
				" with listen.replication.addr " + l.Addr})
		case u.Scheme == "https":
			out = append(out, replicationFinding{true, "instance_map sets tls for this process, and the " +
				"replication listener does not serve TLS: Synapse processes will not reach it"})
		case u.Port() != port:
			out = append(out, replicationFinding{true, "instance_map's port for this process (" + u.Port() +
				") is not listen.replication.addr's (" + port + ")"})
		}
	}
	if hs.ReplicationSecret == "" {
		out = append(out, replicationFinding{true, "worker_replication_secret is unset: the replication listener " +
			"accepts every request, as Synapse's own listeners do then"})
	}
	return out
}

func logReplication(log zerolog.Logger, instance string, l config.Replication, hs *synapsecfg.Config) {
	for _, f := range checkReplication(instance, l, hs) {
		ev := log.Info()
		if f.warn {
			ev = log.Warn()
		}
		ev.Msg(f.msg)
	}
}
