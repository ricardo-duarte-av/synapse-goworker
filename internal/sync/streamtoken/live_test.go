package streamtoken

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestLiveTokensRoundTrip takes tokens minted by the real Synapse sync worker
// and checks they survive Parse -> String unchanged.
//
// The unit tests use tokens captured by hand, which go stale: Synapse adds
// stream fields over time, and a token that gained a fifteenth would be
// rejected by Parse with nothing in CI to notice. This test asks the running
// server instead, so an upgrade that changes the token shape is caught by the
// suite rather than by a client.
//
// Gated on env vars and skipped otherwise, so it never fails CI:
//
//	GOSYNC_LIVE_REF_SOCKET=/var/sockets/nginx/av-sync-worker-2.sock \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token \
//	  go test ./internal/streamtoken -run Live -v
func TestLiveTokensRoundTrip(t *testing.T) {
	socket := os.Getenv("GOSYNC_LIVE_REF_SOCKET")
	tokenFile := os.Getenv("GOSYNC_LIVE_TOKEN_FILE")
	if socket == "" || tokenFile == "" {
		t.Skip("GOSYNC_LIVE_REF_SOCKET and GOSYNC_LIVE_TOKEN_FILE not set; skipping live test")
	}
	raw, err := os.ReadFile(expandHome(tokenFile))
	if err != nil {
		t.Skipf("cannot read %s: %v", tokenFile, err)
	}
	accessToken := strings.TrimSpace(string(raw))

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}

	get := func(path string) map[string]any {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "http://localhost"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d", path, resp.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("GET %s: decoding: %v", path, err)
		}
		return body
	}

	// set_presence=offline: a sync marks the user online and emits USER_SYNC
	// over replication. A test must not perturb the deployment it measures.
	sync := get("/_matrix/client/v3/sync?timeout=0&set_presence=offline")

	var tokens []string
	if nb, ok := sync["next_batch"].(string); ok {
		tokens = append(tokens, nb)
	} else {
		t.Fatal("/sync returned no next_batch")
	}
	// prev_batch tokens are minted by a different code path (pagination rather
	// than the sync snapshot) and are the ones that carry the topological form.
	if rooms, ok := sync["rooms"].(map[string]any); ok {
		if join, ok := rooms["join"].(map[string]any); ok {
			for _, room := range join {
				r, _ := room.(map[string]any)
				tl, _ := r["timeline"].(map[string]any)
				if pb, ok := tl["prev_batch"].(string); ok {
					tokens = append(tokens, pb)
				}
			}
		}
	}

	if len(tokens) < 2 {
		t.Logf("only %d token(s) available; the account may have no joined rooms with history", len(tokens))
	}

	var sawHistorical bool
	for _, want := range tokens {
		tok, err := Parse(want)
		if err != nil {
			t.Errorf("Parse(%q): %v", want, err)
			continue
		}
		if got := tok.String(); got != want {
			t.Errorf("round trip:\n got %q\nwant %q", got, want)
		}
		if tok.Room.IsHistorical() {
			sawHistorical = true
		}
	}
	t.Logf("round-tripped %d live tokens (historical form seen: %v)", len(tokens), sawHistorical)
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + path[1:]
		}
	}
	return path
}
