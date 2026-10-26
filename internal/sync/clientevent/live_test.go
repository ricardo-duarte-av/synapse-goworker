package clientevent

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tidwall/gjson"
)

// TestLiveSerializationMatchesSynapse serialises the events Synapse just
// returned, from their stored JSON, and requires byte-equal structure.
//
// This is the parity check the whole endpoint rests on. If the serialiser is
// wrong, every response is wrong in the same way, and a handler test would only
// confirm that we are consistently wrong.
//
// Two things are pinned so that a difference means a defect rather than the
// passage of time:
//
//   - `time_now`. Synapse stamps `age = time_now - age_ts`. Its `time_now` is
//     unknowable after the fact, but recoverable: `age + origin_server_ts` is
//     exactly the millisecond it used. Feeding that back makes `age` comparable
//     instead of forcing us to ignore it -- and `age` is a field worth
//     checking, since getting it wrong is invisible in casual testing.
//   - The requester. `unsigned.transaction_id` is revealed only to the sending
//     session, so the test uses the same token that sent the events.
//
// Gated on env vars, skipped otherwise:
//
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	GOSYNC_LIVE_REF_SOCKET=/var/sockets/nginx/av-sync-worker-2.sock \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token \
//	  go test ./internal/clientevent -run Live -v
func TestLiveSerializationMatchesSynapse(t *testing.T) {
	dsn := os.Getenv("GOSYNC_TEST_DSN")
	socket := os.Getenv("GOSYNC_LIVE_REF_SOCKET")
	tokenFile := os.Getenv("GOSYNC_LIVE_TOKEN_FILE")
	if dsn == "" || socket == "" || tokenFile == "" {
		t.Skip("GOSYNC_TEST_DSN, GOSYNC_LIVE_REF_SOCKET and GOSYNC_LIVE_TOKEN_FILE not set; skipping live test")
	}
	raw, err := os.ReadFile(expandHome(tokenFile))
	if err != nil {
		t.Skipf("cannot read %s: %v", tokenFile, err)
	}
	accessToken := strings.TrimSpace(string(raw))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to database: %v", err)
	}
	defer pool.Close()

	// Who the requester is decides whether transaction_id is revealed.
	var userID, deviceID string
	var tokenID int64
	if err := pool.QueryRow(ctx,
		`SELECT user_id, COALESCE(device_id, ''), id FROM access_tokens WHERE token = $1`,
		accessToken).Scan(&userID, &deviceID, &tokenID); err != nil {
		t.Fatalf("resolving the test token: %v", err)
	}

	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}

	rooms := liveTestRooms(ctx, t, pool)
	if len(rooms) == 0 {
		t.Skip("no gosync-* test rooms found")
	}

	var compared, mismatched int
	for _, room := range rooms {
		path := "/_matrix/client/r0/rooms/" + urlEscape(room.id) + "/initialSync?limit=10"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := readAllClose(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d: %s", path, resp.StatusCode, body)
		}

		// Timeline events carry MSC4115's unsigned.membership; state events,
		// which Synapse wraps with FilteredEvent.state(), do not.
		for _, want := range gjson.GetBytes(body, "messages.chunk").Array() {
			compared++
			if !compareOne(ctx, t, pool, room, want, "join", userID, deviceID, tokenID) {
				mismatched++
			}
		}
		for _, want := range gjson.GetBytes(body, "state").Array() {
			compared++
			if !compareOne(ctx, t, pool, room, want, "", userID, deviceID, tokenID) {
				mismatched++
			}
		}
	}

	t.Logf("compared %d events across %d rooms; %d mismatched", compared, len(rooms), mismatched)
	if compared == 0 {
		t.Error("no events compared; the test proved nothing")
	}
}

type liveRoom struct {
	id      string
	version string
	name    string
}

func liveTestRooms(ctx context.Context, t *testing.T, pool *pgxpool.Pool) []liveRoom {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT r.room_id, r.room_version, s.name
		  FROM rooms r JOIN room_stats_state s USING (room_id)
		 WHERE s.name LIKE 'gosync-%'
		 ORDER BY s.name`)
	if err != nil {
		t.Fatalf("listing test rooms: %v", err)
	}
	defer rows.Close()
	var out []liveRoom
	for rows.Next() {
		var r liveRoom
		if err := rows.Scan(&r.id, &r.version, &r.name); err != nil {
			t.Fatalf("scanning test rooms: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func compareOne(ctx context.Context, t *testing.T, pool *pgxpool.Pool, room liveRoom,
	want gjson.Result, membership, userID, deviceID string, tokenID int64) bool {
	t.Helper()

	eventID := want.Get("event_id").String()

	var storedJSON, internalMetadata []byte
	var eventType string
	err := pool.QueryRow(ctx, `
		SELECT ej.json, ej.internal_metadata, e.type
		  FROM event_json ej JOIN events e USING (event_id)
		 WHERE ej.event_id = $1`, eventID).Scan(&storedJSON, &internalMetadata, &eventType)
	if err != nil {
		t.Errorf("%s: loading stored event: %v", eventID, err)
		return false
	}

	// Recover the exact millisecond Synapse used, so `age` is comparable.
	nowMS := want.Get("origin_server_ts").Int() + want.Get("unsigned.age").Int()

	got, err := Serialize(Stored{
		EventID:          eventID,
		RoomID:           room.id,
		Type:             eventType,
		JSON:             storedJSON,
		InternalMetadata: internalMetadata,
		RoomVersion:      room.version,
		Membership:       membership,
	}, nowMS, Config{
		Format:    FormatV1,
		Requester: Requester{UserID: userID, DeviceID: deviceID, TokenID: tokenID},
	})
	if err != nil {
		t.Errorf("%s: Serialize: %v", eventID, err)
		return false
	}

	var gotAny, wantAny any
	if err := json.Unmarshal(got, &gotAny); err != nil {
		t.Errorf("%s: our output is not JSON: %v", eventID, err)
		return false
	}
	if err := json.Unmarshal([]byte(want.Raw), &wantAny); err != nil {
		t.Errorf("%s: reference output is not JSON: %v", eventID, err)
		return false
	}
	if !reflect.DeepEqual(gotAny, wantAny) {
		g, _ := json.MarshalIndent(gotAny, "", "  ")
		w, _ := json.MarshalIndent(wantAny, "", "  ")
		t.Errorf("%s (%s, room %s v%s) mismatch:\n--- ours ---\n%s\n--- synapse ---\n%s",
			eventID, eventType, room.name, room.version, g, w)
		return false
	}
	return true
}

func readAllClose(resp *http.Response) ([]byte, error) {
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			return buf, nil
		}
	}
}

func urlEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		const hex = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0f])
	}
	return b.String()
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + path[1:]
		}
	}
	return path
}
