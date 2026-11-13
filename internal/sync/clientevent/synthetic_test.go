package clientevent

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Vectors from Synapse 1.165.0's make_synthetic_join_event and
// serialize_synthetic_event, run in its image.
func TestSyntheticJoin(t *testing.T) {
	v2 := Config{Format: FormatV2NoRoomID}
	for _, c := range []struct {
		room, user string
		ts         int64
		cfg        Config
		want       string
	}{
		{"!abcdefghijkl:example.org", "@someverylongname:example.org", 1700000000123, v2,
			`{"content":{"membership":"join"},"event_id":"$fake_announcement_join-abcdefgh-somevery--qW73pqr7j3vl6417Qq22Df6OcRjdM4DFujsxfDnapw","origin_server_ts":1700000000123,"sender":"@someverylongname:example.org","state_key":"@someverylongname:example.org","type":"m.room.member"}`},
		{"!a:b", "@\u00e9:b", 1700000000123, v2,
			`{"content":{"membership":"join"},"event_id":"$fake_announcement_join-a:b-\u00e9-fUqlCkVLim14HIDHhH4cVYPnhyHsdBUXS2ys1OhjNfw","origin_server_ts":1700000000123,"sender":"@\u00e9:b","state_key":"@\u00e9:b","type":"m.room.member"}`},
		{"!\u00c0\u00c9\u00ce\u00f5\u00fc\U0001F600abcdef:x", "@\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600:x", 1700000000123, v2,
			`{"content":{"membership":"join"},"event_id":"$fake_announcement_join-\u00c0\u00c9\u00ce\u00f5\u00fc\U0001F600ab-\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600-tKA6i8IaeNMDKHdWRdnaO3MGT-Y0dmJNKmVNcXkirsI","origin_server_ts":1700000000123,"sender":"@\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600:x","state_key":"@\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600\U0001F600:x","type":"m.room.member"}`},
		{"!a:b", "@u:b", 5, Config{Format: FormatV2NoRoomID, EventFields: []string{"type", "content.membership", "sender", "nope.x"}},
			`{"content":{"membership":"join"},"sender":"@u:b","type":"m.room.member"}`},
	} {
		got, err := SerializeSynthetic(SyntheticJoin(c.room, c.user, c.ts), c.cfg)
		if err != nil {
			t.Fatal(err)
		}
		var g, w any
		_ = json.Unmarshal(got, &g)
		// A raw string cannot spell an astral code point; JSON has no \U.
		_ = json.Unmarshal([]byte(strings.ReplaceAll(c.want, `\U0001F600`, "\U0001F600")), &w)
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s %s:\n got %s\nwant %s", c.room, c.user, got, c.want)
		}
	}
}

// The other formats, as the Rust's apply_event_format gives them.
func TestSyntheticJoinFormats(t *testing.T) {
	ev := SyntheticJoin("!abcdefghijkl:example.org", "@someverylongname:example.org", 1234)
	id := `"event_id":"$fake_announcement_join-abcdefgh-somevery--qW73pqr7j3vl6417Qq22Df6OcRjdM4DFujsxfDnapw"`
	for f, want := range map[Format]string{
		FormatV2:  `{"content":{"membership":"join"},` + id + `,"origin_server_ts":1234,"room_id":"!abcdefghijkl:example.org","sender":"@someverylongname:example.org","state_key":"@someverylongname:example.org","type":"m.room.member"}`,
		FormatV1:  `{"content":{"membership":"join"},` + id + `,"origin_server_ts":1234,"room_id":"!abcdefghijkl:example.org","sender":"@someverylongname:example.org","state_key":"@someverylongname:example.org","type":"m.room.member","user_id":"@someverylongname:example.org"}`,
		FormatRaw: `{"content":{"membership":"join"},` + id + `,"origin_server_ts":1234,"room_id":"!abcdefghijkl:example.org","sender":"@someverylongname:example.org","state_key":"@someverylongname:example.org","type":"m.room.member"}`,
	} {
		got, err := SerializeSynthetic(ev, Config{Format: f})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("format %d:\n got %s\nwant %s", f, got, want)
		}
	}
}
