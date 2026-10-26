package synapsecfg

import (
	"os"
	"testing"
)

// Resolving the real homeserver.yaml. Gated on an env var so it never runs in
// CI, and it never prints the secret -- only whether one was found.
func TestLiveResolvesTheDeployedConfig(t *testing.T) {
	path := os.Getenv("GOSYNC_SYNAPSE_CONFIG")
	if path == "" {
		t.Skip("GOSYNC_SYNAPSE_CONFIG not set")
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("server_name         = %s", cfg.ServerName)
	t.Logf("presence enabled    = %v", cfg.PresenceEnabled)
	t.Logf("presence writer     = %s", cfg.PresenceWriter.Name)
	t.Logf("writer socket       = %s", cfg.PresenceWriter.Socket)
	t.Logf("writer url          = %q", cfg.PresenceWriter.URL)
	t.Logf("replication secret  = %d chars (not printed)", len(cfg.ReplicationSecret))
	t.Logf("sync_online_timeout = %v", cfg.SyncOnlineTimeout)
	t.Logf("last_active_granul. = %v", cfg.LastActiveGranularity)

	if cfg.PresenceWriter.Socket == "" && cfg.PresenceWriter.URL == "" {
		t.Error("resolved no address for the presence writer")
	}
	if cfg.ReplicationSecret == "" {
		t.Error("resolved no replication secret")
	}
}
