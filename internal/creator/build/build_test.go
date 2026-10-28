package build

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

func testKey(t *testing.T) *Key {
	t.Helper()
	k, err := ParseKey("example.org", federation.GenerateSigningKey().SynapseString())
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func proto() Proto {
	return Proto{
		RoomVersion:    id.RoomV11,
		RoomID:         "!r:example.org",
		Sender:         "@u:example.org",
		Type:           "m.room.message",
		Content:        jsontext.Value(`{"msgtype":"m.text","body":"hi"}`),
		PrevEvents:     []string{"$p"},
		AuthEvents:     []string{"$c", "$pl", "$m"},
		Depth:          7,
		OriginServerTS: 1700000000000,
	}
}

// TestIDMatchesGoinboundPort: two independent implementations of the
// reference hash, mautrix's and the Synapse port in package pdu, agree.
func TestIDMatchesGoinboundPort(t *testing.T) {
	sticky := int64(60000)
	for name, p := range map[string]Proto{
		"message": proto(),
		"sticky":  func() Proto { p := proto(); p.StickyDurationMS = &sticky; return p }(),
		"state":   func() Proto { p := proto(); s := ""; p.Type, p.StateKey = "m.room.topic", &s; return p }(),
	} {
		ev, err := Build(p, testKey(t))
		if err != nil {
			t.Fatal(err)
		}
		rv, ok := pdu.LookupRoomVersion("11")
		if !ok {
			t.Fatal("room version 11 unknown to pdu")
		}
		want, err := pdu.CalculateEventID(ev.JSON, rv)
		if err != nil {
			t.Fatal(err)
		}
		if ev.ID != want {
			t.Errorf("%s: mautrix %s, pdu port %s", name, ev.ID, want)
		}
		if ok, err := pdu.CheckContentHash(ev.JSON); err != nil || !ok {
			t.Errorf("%s: content hash does not verify (%v)", name, err)
		}
	}
}

func TestStickyIsHashedButNotSigned(t *testing.T) {
	key := testKey(t)
	plain, err := Build(proto(), key)
	if err != nil {
		t.Fatal(err)
	}
	d := int64(60000)
	p := proto()
	p.StickyDurationMS = &d
	sticky, err := Build(p, key)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(sticky.JSON, "msc4354_sticky.duration_ms").Int() != d {
		t.Fatalf("sticky missing from %s", sticky.JSON)
	}
	if gjson.GetBytes(plain.JSON, "hashes.sha256").Str == gjson.GetBytes(sticky.JSON, "hashes.sha256").Str {
		t.Error("sticky is not covered by the content hash")
	}
}

func TestDepth(t *testing.T) {
	for _, c := range []struct {
		in   []int64
		want int64
	}{
		{nil, 1},
		{[]int64{3, 9, 4}, 10},
		{[]int64{MaxDepth}, MaxDepth},
		{[]int64{MaxDepth - 1}, MaxDepth},
	} {
		if got := Depth(c.in); got != c.want {
			t.Errorf("Depth(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestRedactsPlacement(t *testing.T) {
	target := "$target"
	p := proto()
	p.Type, p.Redacts = "m.room.redaction", &target
	p.Content = jsontext.Value(`{"redacts":"$target"}`)
	v11, err := Build(p, testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(v11.JSON, "redacts").Exists() {
		t.Error("v11 redaction has a top-level redacts (MSC2174 moved it to content)")
	}
	p.RoomVersion = id.RoomV10
	v10, err := Build(p, testKey(t))
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(v10.JSON, "redacts").Str != target {
		t.Error("v10 redaction lacks the top-level redacts")
	}
	// Synapse's redaction drops it, so the signature and the event ID do not
	// cover it (mautrix's keeps it).
	b, err := SigningBytes(v10.PDU, id.RoomV10)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(b, "redacts").Exists() {
		t.Errorf("v10 signing bytes keep the top-level redacts: %s", b)
	}
}

func TestUnsupportedVersions(t *testing.T) {
	for _, rv := range []id.RoomVersion{id.RoomV1, id.RoomV2, "nonsense"} {
		p := proto()
		p.RoomVersion = rv
		if _, err := Build(p, testKey(t)); err == nil {
			t.Errorf("room version %q built", rv)
		}
	}
}
