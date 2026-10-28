package build

import (
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/id"
)

// TestLiveRebuildsSynapseEvents rebuilds the newest local events in the two
// test rooms from their stored fields and signs them with the server's key.
// Every event ID must come out equal: proof that Build serialises, hashes and
// signs exactly as Synapse does. Read-only.
//
//	SYNAPSE_DSN="host=/var/sockets user=synapse dbname=synapse-db" \
//	SIGNING_KEY=/opt/matrix/synapse/synapse/aguiarvieira.pt.signing.key \
//	GOEXPERIMENT=jsonv2 go test -run TestLive ./internal/build/
func TestLiveRebuildsSynapseEvents(t *testing.T) {
	dsn, keyPath := os.Getenv("SYNAPSE_DSN"), os.Getenv("SIGNING_KEY")
	if dsn == "" || keyPath == "" {
		t.Skip("SYNAPSE_DSN and SIGNING_KEY not set")
	}
	b, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	// Synapse signs with the first key in the file (config/key.py).
	first, _, _ := strings.Cut(strings.TrimSpace(string(b)), "\n")
	key, err := ParseKey("aguiarvieira.pt", strings.TrimSpace(first))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn+" default_transaction_read_only=on")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	// The test rooms' newest events, and local redactions in any room: the
	// test rooms are v11 and later, and before v11 redacts is top-level.
	rows, err := conn.Query(ctx, `(SELECT e.event_id, r.room_version, j.json
		FROM events e JOIN event_json j USING (event_id) JOIN rooms r ON r.room_id = e.room_id
		WHERE e.room_id IN ('!decSfHzLLhrNKJaZid:aguiarvieira.pt', '!nmZrvcKkeQiFGRlJtE:aguiarvieira.pt',
			'!FRudVQysKkIHAjrSzd:aguiarvieira.pt', '!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4')
		  AND e.sender LIKE '%:aguiarvieira.pt' AND NOT e.outlier
		  AND NOT EXISTS (SELECT 1 FROM redactions x WHERE x.redacts = e.event_id)
		ORDER BY e.stream_ordering DESC LIMIT 200)
		UNION ALL
		(SELECT e.event_id, r.room_version, j.json
		FROM events e JOIN event_json j USING (event_id) JOIN rooms r ON r.room_id = e.room_id
		WHERE e.type = 'm.room.redaction' AND e.sender LIKE '%:aguiarvieira.pt' AND NOT e.outlier
		  AND NOT EXISTS (SELECT 1 FROM redactions x WHERE x.redacts = e.event_id)
		ORDER BY e.stream_ordering DESC LIMIT 100)
		UNION ALL
		(SELECT e.event_id, r.room_version, j.json
		FROM events e JOIN event_json j USING (event_id) JOIN rooms r ON r.room_id = e.room_id
		WHERE r.room_version IN ('1', '2', '3') AND e.sender LIKE '%:aguiarvieira.pt' AND NOT e.outlier
		  AND NOT EXISTS (SELECT 1 FROM redactions x WHERE x.redacts = e.event_id)
		ORDER BY e.stream_ordering DESC LIMIT 200)`)
	if err != nil {
		t.Fatal(err)
	}
	// A v1/v2 event lists each prev and auth event with its reference
	// hash: RefHash must find the stored one from the referenced event.
	refJSON := func(eventID string) (string, bool) {
		var raw string
		err := conn.QueryRow(ctx, `SELECT json FROM event_json WHERE event_id = $1`, eventID).Scan(&raw)
		return raw, err == nil
	}
	n, v1 := 0, 0
	var older []string
	for rows.Next() {
		var eventID, rv, raw string
		if err := rows.Scan(&eventID, &rv, &raw); err != nil {
			t.Fatal(err)
		}
		j := gjson.Parse(raw)
		// Signed by a key we hold? Older events may use a rotated key.
		if !j.Get("signatures." + gjson.Escape("aguiarvieira.pt") + "." + gjson.Escape(string(key.ID))).Exists() {
			continue
		}
		p := Proto{
			RoomVersion:    id.RoomVersion(rv),
			RoomID:         j.Get("room_id").Str,
			Sender:         j.Get("sender").Str,
			Type:           j.Get("type").Str,
			Content:        jsontext.Value(j.Get("content").Raw),
			Depth:          j.Get("depth").Int(),
			OriginServerTS: j.Get("origin_server_ts").Int(),
		}
		if sk := j.Get("state_key"); sk.Exists() {
			s := sk.Str
			p.StateKey = &s
		}
		if r := j.Get("redacts"); r.Exists() {
			s := r.Str
			p.Redacts = &s
		}
		if d := j.Get("msc4354_sticky.duration_ms"); d.Exists() {
			n := d.Int()
			p.StickyDurationMS = &n
		}
		// A top-level key Build does not know would change the hash.
		j.ForEach(func(k, _ gjson.Result) bool {
			switch k.Str {
			case "auth_events", "content", "depth", "hashes", "origin_server_ts", "prev_events",
				"room_id", "sender", "signatures", "state_key", "type", "unsigned", "redacts", "msc4354_sticky":
			case "origin":
				p.Origin = jsontext.Value(j.Get("origin").Raw)
			case "event_id":
				if rv != "1" && rv != "2" {
					t.Errorf("%s: event_id in a v%s event", eventID, rv)
				}
			default:
				t.Errorf("%s: top-level key %q not built", eventID, k.Str)
			}
			return true
		})
		if rv == "1" || rv == "2" {
			p.EventID = eventID
			p.RefHashes = map[string][]byte{}
			refs := func(arr []gjson.Result) (ids []string) {
				for _, v := range arr {
					ref := v.Get("0").Str
					ids = append(ids, ref)
					stored := v.Get("1.sha256").Str
					p.RefHashes[ref], _ = base64.RawStdEncoding.DecodeString(stored)
					if raw, ok := refJSON(ref); ok {
						h, err := RefHash([]byte(raw), id.RoomVersion(rv))
						if err != nil {
							t.Errorf("%s: %s: %v", eventID, ref, err)
						} else if got := base64.RawStdEncoding.EncodeToString(h); got != stored {
							t.Errorf("%s: reference hash of %s: %s, stored %s", eventID, ref, got, stored)
						}
					}
				}
				return ids
			}
			p.PrevEvents = refs(j.Get("prev_events").Array())
			p.AuthEvents = refs(j.Get("auth_events").Array())
			v1++
		} else {
			for _, v := range j.Get("prev_events").Array() {
				p.PrevEvents = append(p.PrevEvents, v.Str)
			}
			for _, v := range j.Get("auth_events").Array() {
				p.AuthEvents = append(p.AuthEvents, v.Str)
			}
		}
		if rv == "1" || rv == "2" || rv == "3" {
			older = append(older, rv)
		}
		got, err := Build(p, key)
		if err != nil {
			t.Errorf("%s: %v", eventID, err)
			continue
		}
		n++
		sigPath := "signatures." + gjson.Escape("aguiarvieira.pt") + "." + gjson.Escape(string(key.ID))
		if want, have := j.Get(sigPath).Str, gjson.GetBytes(got.JSON, sigPath).Str; want != have {
			t.Errorf("%s: signature %s, stored %s", eventID, have, want)
		}
		if want, have := j.Get("hashes.sha256").Str, gjson.GetBytes(got.JSON, "hashes.sha256").Str; want != have {
			t.Errorf("%s: content hash %s, stored %s", eventID, have, want)
		}
		if got.ID != eventID {
			t.Errorf("%s (%s): rebuilt as %s\nstored:  %.300s\nrebuilt: %.300s", eventID, p.Type, got.ID, raw, got.JSON)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("no local events signed with this key in the test rooms")
	}
	t.Logf("rebuilt %d events, %d of them in rooms v1-v3 (%d v1/v2)", n, len(older), v1)
}
