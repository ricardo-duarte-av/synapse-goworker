package synapsecfg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "homeserver.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The shape of the live deployment, reduced. This is the case that has to keep
// working across Synapse upgrades and worker rebalances.
func TestResolvesTheLiveDeploymentShape(t *testing.T) {
	cfg, err := Load(write(t, `
server_name: "aguiarvieira.pt"
worker_replication_secret: "sekrit"
presence:
  enabled: true
instance_map:
    main:
        path: "/var/sockets/av-synapse-replication.sock"
    av-edu-worker:
        path: "/var/sockets/av-edu-worker.sock"
stream_writers:
  presence:
    - av-edu-worker
  typing:
    - av-edu-worker
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "aguiarvieira.pt" {
		t.Errorf("server_name = %q", cfg.ServerName)
	}
	if cfg.ReplicationSecret != "sekrit" {
		t.Error("replication secret not read")
	}
	if !cfg.PresenceEnabled {
		t.Error("presence should be enabled")
	}
	if cfg.PresenceWriter.Name != "av-edu-worker" {
		t.Errorf("writer = %q", cfg.PresenceWriter.Name)
	}
	if cfg.PresenceWriter.Socket != "/var/sockets/av-edu-worker.sock" {
		t.Errorf("socket = %q", cfg.PresenceWriter.Socket)
	}
	if cfg.PresenceWriter.URL != "" {
		t.Errorf("url = %q, want empty for a socket instance", cfg.PresenceWriter.URL)
	}
}

// Absent stream_writers means the main process, per Synapse's defaults. A
// worker that assumed a dedicated writer would look in instance_map for a name
// nobody configured.
func TestPresenceDefaultsToTheMainProcess(t *testing.T) {
	cfg, err := Load(write(t, `
server_name: "e.com"
instance_map:
    main:
        path: "/var/sockets/main.sock"
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PresenceWriter.Name != MainProcessInstanceName {
		t.Errorf("writer = %q, want the main process", cfg.PresenceWriter.Name)
	}
	if cfg.PresenceWriter.Socket != "/var/sockets/main.sock" {
		t.Errorf("socket = %q", cfg.PresenceWriter.Socket)
	}
}

// _instance_to_list_converter accepts a bare string as well as a list.
func TestStreamWriterAcceptsABareString(t *testing.T) {
	cfg, err := Load(write(t, `
instance_map:
    edu:
        path: "/s/edu.sock"
stream_writers:
  presence: edu
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PresenceWriter.Name != "edu" {
		t.Errorf("writer = %q", cfg.PresenceWriter.Name)
	}
}

func TestTcpInstance(t *testing.T) {
	for _, tc := range []struct{ tls, want string }{
		{"false", "http://10.0.0.5:9093"},
		{"true", "https://10.0.0.5:9093"},
	} {
		cfg, err := Load(write(t, `
instance_map:
    edu:
        host: 10.0.0.5
        port: 9093
        tls: `+tc.tls+`
stream_writers:
  presence: edu
`))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.PresenceWriter.URL != tc.want {
			t.Errorf("tls=%s url = %q, want %q", tc.tls, cfg.PresenceWriter.URL, tc.want)
		}
		if cfg.PresenceWriter.Socket != "" {
			t.Errorf("socket = %q, want empty for a TCP instance", cfg.PresenceWriter.Socket)
		}
	}
}

// A bare number is MILLISECONDS to Synapse. Reading it as seconds would give a
// relay interval a thousand times too long, which presents as presence simply
// not working rather than as a misparse.
func TestDurationsUseSynapsesUnits(t *testing.T) {
	cases := []struct {
		yaml string
		want time.Duration
	}{
		{"30000", 30 * time.Second},
		{"30", 30 * time.Millisecond},
		{`"30s"`, 30 * time.Second},
		{`"5m"`, 5 * time.Minute},
		{`"2h"`, 2 * time.Hour},
		{`"1d"`, 24 * time.Hour},
		{`"1w"`, 7 * 24 * time.Hour},
	}
	for _, tc := range cases {
		cfg, err := Load(write(t, `
instance_map:
    main:
        path: /s.sock
presence:
  sync_online_timeout: `+tc.yaml+`
`))
		if err != nil {
			t.Fatalf("%s: %v", tc.yaml, err)
		}
		if cfg.SyncOnlineTimeout != tc.want {
			t.Errorf("%s -> %v, want %v", tc.yaml, cfg.SyncOnlineTimeout, tc.want)
		}
	}
}

func TestDefaultsWhenTimersAreAbsent(t *testing.T) {
	cfg, err := Load(write(t, "instance_map:\n    main:\n        path: /s.sock\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SyncOnlineTimeout != DefaultSyncOnlineTimeout {
		t.Errorf("sync_online_timeout = %v", cfg.SyncOnlineTimeout)
	}
	if cfg.LastActiveGranularity != DefaultLastActiveGranularity {
		t.Errorf("last_active_granularity = %v", cfg.LastActiveGranularity)
	}
	if !cfg.PresenceEnabled {
		t.Error("presence.enabled defaults to true in Synapse")
	}
}

func TestPresenceCanBeDisabled(t *testing.T) {
	cfg, err := Load(write(t, `
presence:
  enabled: false
instance_map:
    main:
        path: /s.sock
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PresenceEnabled {
		t.Error("presence.enabled: false was not read")
	}
}

// Each of these is a config this worker cannot act on. Failing at startup with
// the reason beats starting and quietly not relaying presence.
func TestRefusesConfigsItCannotActdOn(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{
			name: "writer missing from instance_map",
			body: "stream_writers:\n  presence: edu\ninstance_map:\n    main:\n        path: /s\n",
			want: "not in instance_map",
		},
		{
			name: "two presence writers",
			body: "stream_writers:\n  presence: [a, b]\ninstance_map:\n    a:\n        path: /a\n    b:\n        path: /b\n",
			want: "exactly one",
		},
		{
			name: "instance with no address",
			body: "stream_writers:\n  presence: edu\ninstance_map:\n    edu:\n        tls: true\n",
			want: "neither a path nor a host",
		},
		{
			name: "unparseable duration",
			body: "instance_map:\n    main:\n        path: /s\npresence:\n  sync_online_timeout: \"soon\"\n",
			want: "not a duration",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(write(t, tc.body))
			if err == nil {
				t.Fatal("accepted a config it cannot act on")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// A missing file must say so plainly: it is the likeliest deployment mistake,
// since the worker runs in a container and homeserver.yaml has to be mounted.
func TestMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/homeserver.yaml"); err == nil {
		t.Fatal("accepted a missing file")
	}
}

// Everything else in homeserver.yaml is ignored, so options added by a Synapse
// upgrade cannot stop this parsing.
func TestUnknownKeysAreIgnored(t *testing.T) {
	cfg, err := Load(write(t, `
server_name: e.com
some_future_option:
  nested: [1, 2, 3]
listeners:
  - port: 8008
    type: http
instance_map:
    main:
        path: /s.sock
`))
	if err != nil {
		t.Fatalf("an unrelated key broke parsing: %v", err)
	}
	if cfg.ServerName != "e.com" {
		t.Errorf("server_name = %q", cfg.ServerName)
	}
}
