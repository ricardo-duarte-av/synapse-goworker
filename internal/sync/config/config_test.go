package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

const minimal = `
server_name: example.com
listen:
  socket: /var/sockets/gosync-worker.sock
database:
  dsn: "host=/var/sockets user=gosync_ro dbname=synapse-db"
auth:
  whoami_socket: /var/sockets/nginx/av-request-worker-1.sock
`

func TestMinimalConfigLoads(t *testing.T) {
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.ServerName != "example.com" {
		t.Errorf("server_name = %q", cfg.ServerName)
	}
	if got := cfg.Listen.SocketMode; got != "0660" {
		t.Errorf("socket_mode default = %q, want 0660", got)
	}
	if got := cfg.Metrics.Addr; got != ":9201" {
		t.Errorf("metrics.addr default = %q", got)
	}
	if got := cfg.Database.Conns(); got != 16 {
		t.Errorf("max_conns default = %d", got)
	}
	if got := cfg.Auth.Positive(); got != 5*time.Minute {
		t.Errorf("positive_ttl default = %v", got)
	}
}

// The replication channel is named after the homeserver. Defaulting it to
// server_name matters more than it looks: subscribing to the wrong channel
// raises no error and delivers no traffic, so the worker would simply never
// wake a long-poll and look merely idle.
func TestReplicationChannelDefaultsToServerName(t *testing.T) {
	cfg, err := Parse([]byte(minimal + `replication:
  enabled: true
  address: /var/sockets/keydb
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Replication.Channel != "example.com" {
		t.Errorf("channel = %q, want example.com", cfg.Replication.Channel)
	}
}

// A typo in a socket path is the kind of mistake that would otherwise surface
// as "why does nothing connect".
func TestUnknownFieldIsRejected(t *testing.T) {
	_, err := Parse([]byte(minimal + "\nlisten_socket: /oops\n"))
	if err == nil {
		t.Fatal("expected an error for an unknown field")
	}
	if !strings.Contains(err.Error(), "listen_socket") {
		t.Errorf("error should name the offending field, got: %v", err)
	}
}

func TestValidationRejects(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"no server name", "listen:\n  addr: \":1\"\ndatabase:\n  dsn: x\nauth:\n  whoami_url: http://x\n", "server_name"},
		{"both listen forms", "server_name: e.com\nlisten:\n  socket: /a\n  addr: \":1\"\ndatabase:\n  dsn: x\nauth:\n  whoami_url: http://x\n", "exactly one"},
		{"neither listen form", "server_name: e.com\ndatabase:\n  dsn: x\nauth:\n  whoami_url: http://x\n", "exactly one"},
		{"no dsn", "server_name: e.com\nlisten:\n  addr: \":1\"\nauth:\n  whoami_url: http://x\n", "dsn is required"},
		{"both auth forms", "server_name: e.com\nlisten:\n  addr: \":1\"\ndatabase:\n  dsn: x\nauth:\n  whoami_socket: /a\n  whoami_url: http://b\n", "exactly one"},
		{"neither auth form", "server_name: e.com\nlisten:\n  addr: \":1\"\ndatabase:\n  dsn: x\n", "exactly one"},
		{"replication without address", minimal + "replication:\n  enabled: true\n", "address is required"},
		{"bad socket mode", "server_name: e.com\nlisten:\n  socket: /a\n  socket_mode: \"98\"\ndatabase:\n  dsn: x\nauth:\n  whoami_url: http://x\n", "socket_mode"},
		{"both reference forms", minimal + "reference:\n  socket: /a\n  url: http://b\n", "at most one"},
		{"to_device without dsn", minimal + "to_device:\n  enabled: true\n", "dsn is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestParsedSocketMode(t *testing.T) {
	l := Listen{SocketMode: "0666"}
	got, err := l.ParsedSocketMode()
	if err != nil {
		t.Fatalf("ParsedSocketMode: %v", err)
	}
	if got != 0o666 {
		t.Errorf("mode = %o, want 666", got)
	}
}

// The example config must stay loadable: it is the documentation, and a stale
// one is worse than none.
func TestExampleConfigParses(t *testing.T) {
	data, err := readExample()
	if err != nil {
		t.Skipf("example config not readable: %v", err)
	}
	if _, err := Parse(data); err != nil {
		t.Fatalf("deploy/gosync-worker.example.yaml does not parse: %v", err)
	}
}

// readExample loads the shipped example configuration.
func readExample() ([]byte, error) {
	return os.ReadFile("../../deploy/gosync-worker.example.yaml")
}
