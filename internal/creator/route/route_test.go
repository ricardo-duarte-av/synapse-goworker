package route

import "testing"

func TestClassify(t *testing.T) {
	const r = "!decSfHzLLhrNKJaZid:aguiarvieira.pt"
	cases := []struct {
		method, path string
		want         Request
	}{
		{"PUT", "/_matrix/client/v3/rooms/%21decSfHzLLhrNKJaZid%3Aaguiarvieira.pt/send/m.room.message/m1.2",
			Request{Endpoint: Send, Write: true, Room: r, Action: "m.room.message", TxnID: "m1.2"}},
		{"POST", "/_matrix/client/r0/rooms/" + r + "/send/m.room.message",
			Request{Endpoint: Send, Write: true, Room: r, Action: "m.room.message"}},
		{"PUT", "/_matrix/client/api/v1/rooms/" + r + "/send/m.room.message/t",
			Request{Endpoint: Send, Write: true, Room: r, Action: "m.room.message", TxnID: "t"}},
		{"PUT", "/_matrix/client/v3/rooms/" + r + "/state/m.room.topic/",
			Request{Endpoint: State, Write: true, Room: r, Action: "m.room.topic"}},
		{"PUT", "/_matrix/client/v3/rooms/" + r + "/state/m.room.member/%40u%3Aex",
			Request{Endpoint: State, Write: true, Room: r, Action: "m.room.member", StateKey: "@u:ex"}},
		{"GET", "/_matrix/client/v3/rooms/" + r + "/state/m.room.topic/",
			Request{Endpoint: State, Write: false, Room: r, Action: "m.room.topic"}},
		{"PUT", "/_matrix/client/v3/rooms/" + r + "/redact/%24abc%2Fdef/t",
			Request{Endpoint: Redact, Write: true, Room: r, Action: "$abc/def", TxnID: "t"}},
		{"POST", "/_matrix/client/v3/rooms/" + r + "/kick",
			Request{Endpoint: Membership, Write: true, Room: r, Action: "kick"}},
		{"PUT", "/_matrix/client/v3/rooms/" + r + "/leave/t9",
			Request{Endpoint: Membership, Write: true, Room: r, Action: "leave", TxnID: "t9"}},
		{"POST", "/_matrix/client/v3/join/%23test%3Aex",
			Request{Endpoint: Join, Write: true, Room: "#test:ex"}},
		{"PUT", "/_matrix/client/v3/join/" + r + "/t",
			Request{Endpoint: Join, Write: true, Room: r, TxnID: "t"}},
		{"POST", "/_matrix/client/v3/knock/" + r,
			Request{Endpoint: Knock, Write: true, Room: r}},
		{"PUT", "/_matrix/client/v3/profile/%40u%3Aex/displayname",
			Request{Endpoint: Profile, Write: true, FieldPath: true, User: "@u:ex", Action: "displayname"}},
		{"PUT", "/_matrix/client/r0/profile/%40u%3Aex/avatar_url",
			Request{Endpoint: Profile, Write: true, FieldPath: true, User: "@u:ex", Action: "avatar_url"}},
		{"DELETE", "/_matrix/client/v3/profile/%40u%3Aex/m.tz",
			Request{Endpoint: Profile, Write: true, FieldPath: true, User: "@u:ex", Action: "m.tz"}},
		// A custom field outside v3 is no servlet's.
		{"PUT", "/_matrix/client/r0/profile/%40u%3Aex/m.tz",
			Request{Endpoint: Profile, Write: true}},
		{"PUT", "/_matrix/client/unstable/uk.tcpip.msc4133/profile/%40u%3Aex/m.tz",
			Request{Endpoint: Other}},
		{"GET", "/_matrix/client/v3/profile/%40u%3Aex",
			Request{Endpoint: Profile, Write: false}},

		// Matched by nginx's patterns, but not a servlet behind them.
		{"GET", "/_matrix/client/v3/rooms/" + r + "/send/m.room.message/t", Request{Endpoint: Other}},
		{"POST", "/_matrix/client/v3/rooms/" + r + "/kick/extra", Request{Endpoint: Other}},
		{"PUT", "/_matrix/client/v3/rooms/" + r + "/send/a/b/c", Request{Endpoint: Other}},
		{"PUT", "/_matrix/client/v3/rooms//send/a/b", Request{Endpoint: Other}},
		{"PUT", "/_matrix/client/v4/rooms/" + r + "/send/a/b", Request{Endpoint: Other}},
		{"PUT", "/_matrix/client/api/v2/rooms/" + r + "/send/a/b", Request{Endpoint: Other}},
		{"PUT", "/_matrix/federation/v1/send/t", Request{Endpoint: Other}},
		{"PUT", "/_matrix/client/v3/rooms/%zz/send/a/b", Request{Endpoint: Other}},
	}
	for _, c := range cases {
		if got := Classify(c.method, c.path); got != c.want {
			t.Errorf("%s %s\n got %+v\nwant %+v", c.method, c.path, got, c.want)
		}
	}
}
