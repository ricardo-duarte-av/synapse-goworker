package creator

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/id"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/build"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

// Policy servers (MSC4284): RoomPolicyHandler (handlers/room_policy.py,
// v1.163.0), as _create_and_send_nonmember_event_locked uses it: the room's
// policy server is asked to sign the event before it is handled
// (ask_policy_server_to_sign_event, verify=False), then is_event_allowed
// checks the signature, asking once more if it does not verify.

// policyKeyID is POLICY_SERVER_KEY_ID.
const policyKeyID = "ed25519:policy_server"

// PolicySigner is the policy server request: the answer's status and body,
// or err for none at all.
type PolicySigner interface {
	PolicySign(ctx context.Context, server string, event []byte) (status int, body []byte, err error)
}

// policyServer is _get_policy_server, on the state before the event: the
// server and its ed25519 key (unpadded base64), or ok false for none.
func (c *Creator) policyServer(ctx context.Context, before pdu.StateMap) (server, key string, ok bool, err error) {
	load := func(typ string) (gjson.Result, bool, error) {
		evID, ok := before[pdu.StateKey{Type: typ}]
		if !ok {
			return gjson.Result{}, false, nil
		}
		evs, _, err := c.o.Store.GetStoredEvents(ctx, []string{evID})
		if err != nil {
			return gjson.Result{}, false, err
		}
		if se := evs[evID]; se != nil {
			return se.Content, true, nil
		}
		return gjson.Result{}, false, nil
	}
	content, found, err := load("m.room.policy")
	if err != nil {
		return "", "", false, err
	}
	var pk gjson.Result
	if found {
		if keys := pdu.Get(content, "public_keys"); keys.IsObject() {
			pk = pdu.Get(keys, "ed25519")
		}
	} else {
		if content, found, err = load("org.matrix.msc4284.policy"); err != nil || !found {
			return "", "", false, err
		}
		pk = pdu.Get(content, "public_key")
	}
	if pk.Type != gjson.String {
		return "", "", false, nil
	}
	// content.get("via", ""): a string, not ours, a valid server name.
	via := pdu.Get(content, "via")
	if !via.Exists() {
		return "", "", false, nil
	}
	if via.Type != gjson.String || via.Str == c.o.ServerName || !pdu.ValidServerName(via.Str) {
		return "", "", false, nil
	}
	in, err := c.hostInRoom(ctx, before, via.Str)
	if err != nil || !in {
		return "", "", false, err
	}
	return via.Str, pk.Str, true, nil
}

// hostInRoom is is_host_in_room: a member of that server joined.
func (c *Creator) hostInRoom(ctx context.Context, before pdu.StateMap, server string) (bool, error) {
	var ids []string
	for k, v := range before {
		if k.Type == "m.room.member" && strings.HasSuffix(k.StateKey, ":"+server) {
			ids = append(ids, v)
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	evs, _, err := c.o.Store.GetStoredEvents(ctx, ids)
	if err != nil {
		return false, err
	}
	for _, se := range evs {
		if m := pdu.Get(se.Content, "membership"); m.Type == gjson.String && m.Str == "join" {
			return true, nil
		}
	}
	return false, nil
}

// askPolicy is ask_policy_server_to_sign_event: the signatures are added to
// the event; a refusal is the client's answer.
func (c *Creator) askPolicy(ctx context.Context, built *build.Event, pduJSON []byte, server string) *stop {
	if c.o.Policy == nil {
		return fb("policy_server")
	}
	status, body, err := c.o.Policy.PolicySign(ctx, server, pduJSON)
	if d, derr := pyjson.Dedupe(body); derr == nil {
		body = d
	}
	switch {
	case err != nil:
		// RequestSendFailed: not an HttpResponseException, so Synapse's 500.
		c.o.Log.Warn().Err(err).Str("server", server).Msg("Policy server unreachable")
		return internalError("policy_unreachable").stop()
	case status >= 500 || status == 429:
		// Retried no more (there is a timeout), raised as RequestSendFailed.
		return internalError("policy_error").stop()
	case status >= 300:
		return proxiedError(status, body).stop()
	}
	v := gjson.ParseBytes(body)
	if !gjson.ValidBytes(body) {
		return internalError("policy_answer").stop()
	}
	empty := (v.IsObject() && len(v.Map()) == 0) || (v.IsArray() && len(v.Array()) == 0) ||
		v.Type == gjson.Null || (v.Type == gjson.String && v.Str == "") || (v.Type == gjson.Number && v.Num == 0) || v.Type == gjson.False
	if empty {
		return forbidden("policy_refused", "This event has been rejected as probable spam by the policy server").stop()
	}
	if !v.IsObject() {
		return internalError("policy_answer").stop()
	}
	sigs := pdu.Get(v, server)
	if !sigs.Exists() {
		return nil
	}
	if !sigs.IsObject() {
		return internalError("policy_answer").stop()
	}
	var st *stop
	sigs.ForEach(func(k, sig gjson.Result) bool {
		if sig.Type != gjson.String {
			st = internalError("policy_answer").stop()
			return false
		}
		if err := built.AddSignature(server, id.KeyID(k.Str), sig.Str); err != nil {
			st = fb("build")
			return false
		}
		return true
	})
	return st
}

// policySigned is _verify_policy_server_signature: the event carries a
// valid signature by the room's policy key.
func policySigned(built *build.Event, rv id.RoomVersion, server, key string) bool {
	pub, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(key, "="))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	sig, ok := built.PDU.Signatures[server][policyKeyID]
	if !ok {
		return false
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(sig, "="))
	if err != nil {
		return false
	}
	msg, err := build.SigningBytes(built.PDU, rv)
	return err == nil && ed25519.Verify(pub, msg, raw)
}

// proxiedError is HttpResponseException.to_synapse_error: the remote's
// errcode and error, M_UNKNOWN_TOKEN made M_UNKNOWN, 401 made 400, and the
// rest of its fields kept.
func proxiedError(status int, body []byte) *refusal {
	f := &refusal{reason: "policy_refused", status: status, errcode: "M_UNKNOWN", msg: http.StatusText(status), extra: map[string]any{}}
	var j map[string]json.RawMessage
	if json.Unmarshal(body, &j) == nil {
		if raw, ok := j["errcode"]; ok {
			delete(j, "errcode")
			var s string
			if json.Unmarshal(raw, &s) == nil {
				f.errcode = s
			}
		}
		if raw, ok := j["error"]; ok {
			delete(j, "error")
			var s string
			if json.Unmarshal(raw, &s) == nil {
				f.msg = s
			}
		}
		for k, v := range j {
			f.extra[k] = v
		}
	}
	if f.errcode == "M_UNKNOWN_TOKEN" {
		f.errcode = "M_UNKNOWN"
	}
	if f.status == http.StatusUnauthorized {
		f.status = http.StatusBadRequest
	}
	return f
}

// policyCheck is ask_policy_server_to_sign_event(verify=False), then
// is_event_allowed: a signature that does not verify gets the event asked
// for once more, and then it is refused as spam.
func (c *Creator) policyCheck(ctx context.Context, built *build.Event, pduJSON []byte, rv *pdu.RoomVersion, before pdu.StateMap, in *input) *stop {
	if in.stateKey != nil && *in.stateKey == "" && (in.typ == "m.room.policy" || in.typ == "org.matrix.msc4284.policy") {
		// _is_policy_server_state_event: always allowed.
		return nil
	}
	server, key, ok, err := c.policyServer(ctx, before)
	if err != nil {
		return fb("db_error")
	}
	if !ok {
		return nil
	}
	if built.PDU == nil {
		// A v1/v2 event: the policy server path here signs only the v3+
		// type. A policy server in a v1/v2 room is left to Synapse.
		return fb("policy_v1")
	}
	if st := c.askPolicy(ctx, built, pduJSON, server); st != nil {
		return st
	}
	if policySigned(built, id.RoomVersion(rv.ID), server, key) {
		return nil
	}
	spam := forbidden("policy_refused", "This message has been rejected as probable spam").stop()
	if st := c.askPolicy(ctx, built, pduJSON, server); st != nil && st.fallback != "" {
		return st
	} else if st != nil {
		return spam
	}
	if !policySigned(built, id.RoomVersion(rv.ID), server, key) {
		return spam
	}
	return nil
}
