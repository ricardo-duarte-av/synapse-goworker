package creator

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/federation/pdu"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/build"
)

// fakePolicy answers /_matrix/policy/v1/sign as the policy server would:
// signed with its key, or with status and body.
type fakePolicy struct {
	priv   ed25519.PrivateKey
	status int
	body   string
	calls  int
	wrong  bool
}

func (f *fakePolicy) PolicySign(_ context.Context, server string, event []byte) (int, []byte, error) {
	f.calls++
	if f.status != 0 {
		return f.status, []byte(f.body), nil
	}
	var ev pdu.PDU
	if err := json.Unmarshal(event, &ev); err != nil {
		return 400, nil, nil
	}
	key := f.priv
	if f.wrong {
		// Signed, but not with the room's key.
		_, key, _ = ed25519.GenerateKey(nil)
	}
	msg, _ := build.SigningBytes(&ev, id.RoomV11)
	sig := base64.RawStdEncoding.EncodeToString(ed25519.Sign(key, msg))
	b, _ := json.Marshal(map[string]any{server: map[string]string{"ed25519:policy_server": sig}})
	return 200, b, nil
}

// withPolicy gives the room a policy server, joined, and returns its signer.
func withPolicy(t *testing.T, h *harness) *fakePolicy {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(nil)
	h.f.addState(t, "m.room.member", "@policy:policy.example", `{"membership":"join"}`)
	h.f.addState(t, "m.room.policy", "", `{"via":"policy.example","public_keys":{"ed25519":"`+base64.RawStdEncoding.EncodeToString(pub)+`"}}`)
	fp := &fakePolicy{priv: priv}
	h.c.o.Policy = fp
	return fp
}

// A signed message is sent with the policy server's signature on it.
func TestPolicySigned(t *testing.T) {
	h := newHarness(t)
	fp := withPolicy(t, h)
	rec, out := h.send(t, http.MethodPut, "p1", `{"msgtype":"m.text","body":"hi"}`)
	if out.Result != "created" || fp.calls != 1 {
		t.Fatalf("%+v %s, %d calls", out, rec.Body, fp.calls)
	}
	if sig := gjson.GetBytes(h.p.entries[0], `event.signatures.policy\.example.ed25519:policy_server`).Str; sig == "" {
		t.Errorf("no policy signature: %s", gjson.GetBytes(h.p.entries[0], "event.signatures").Raw)
	}
	// The event ID is the same as without it.
	if gjson.GetBytes(h.p.entries[0], "event.signatures.example\\.org").Raw == "" {
		t.Error("our own signature is gone")
	}
}

func TestPolicyRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
		wrong  bool
		want   string
		calls  int
	}{
		"refused": {403, `{"errcode":"M_FORBIDDEN","error":"spam","x":1}`, false,
			`403 {"errcode":"M_FORBIDDEN","error":"spam","x":1}`, 1},
		"unauthorised": {401, `{"errcode":"M_UNKNOWN_TOKEN","error":"who"}`, false, `400 {"errcode":"M_UNKNOWN","error":"who"}`, 1},
		"not json":     {403, `nope`, false, `403 {"errcode":"M_UNKNOWN","error":"Forbidden"}`, 1},
		"server error": {500, `{}`, false, `500 {"errcode":"M_UNKNOWN","error":"Internal server error"}`, 1},
		"no signature": {200, `{}`, false,
			`403 {"errcode":"M_FORBIDDEN","error":"This event has been rejected as probable spam by the policy server"}`, 1},
		"bad signature": {0, ``, true, `403 {"errcode":"M_FORBIDDEN","error":"This message has been rejected as probable spam"}`, 2},
	} {
		h := newHarness(t)
		fp := withPolicy(t, h)
		fp.status, fp.body, fp.wrong = tc.status, tc.body, tc.wrong
		rec, out := h.send(t, http.MethodPut, "p1", `{"msgtype":"m.text","body":"hi"}`)
		if got := itoaStatus(rec.Code) + " " + rec.Body.String(); out.Result != "refused" || got != tc.want || fp.calls != tc.calls {
			t.Errorf("%s: %+v %s, %d calls", name, out, got, fp.calls)
		}
		if len(h.p.entries) != 0 {
			t.Errorf("%s: sent", name)
		}
	}
}

// The policy event itself is never asked about; nor are member events.
func TestPolicyNotAsked(t *testing.T) {
	h := newHarness(t)
	fp := withPolicy(t, h)
	fp.status, fp.body = 403, `{"errcode":"M_FORBIDDEN","error":"spam"}`
	if _, out := h.put(t, stateReq("m.room.policy", ""), "/state/m.room.policy/", `{"via":"policy.example","public_keys":{}}`); out.Result != "created" {
		t.Errorf("policy event: %+v", out)
	}
	h.f.addState(t, "m.room.member", other, `{"membership":"join"}`)
	if _, out := h.member(t, memberReq("kick", ""), `{"user_id":"`+other+`"}`); out.Result != "created" || fp.calls != 0 {
		t.Errorf("kick: %+v, %d calls", out, fp.calls)
	}
	// A policy server that is not in the room is no policy server.
	h = newHarness(t)
	pub, _, _ := ed25519.GenerateKey(nil)
	h.f.addState(t, "m.room.policy", "", `{"via":"policy.example","public_keys":{"ed25519":"`+base64.RawStdEncoding.EncodeToString(pub)+`"}}`)
	fp = &fakePolicy{status: 403}
	h.c.o.Policy = fp
	if _, out := h.send(t, http.MethodPut, "p2", `{"msgtype":"m.text","body":"hi"}`); out.Result != "created" || fp.calls != 0 {
		t.Errorf("absent policy server: %+v, %d calls", out, fp.calls)
	}
}
