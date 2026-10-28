package creator

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/appservice"
)

// withBridge registers an appservice whose sender is @bot and whose users
// are @u (non-exclusively, like a double-puppeting registration) and
// @bridge_*.
func withBridge(t *testing.T, h *harness, rateLimited bool) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "reg.yaml")
	limited := "false"
	if rateLimited {
		limited = "true"
	}
	_ = os.WriteFile(f, []byte("id: bridge\nas_token: astoken\nhs_token: h\nsender_localpart: bot\nrate_limited: "+limited+`
namespaces:
  users:
    - {exclusive: false, regex: '@u:example\.org'}
    - {exclusive: true, regex: '@bridge_.*:example\.org'}
`), 0o600)
	svcs, err := appservice.Load([]string{f}, "example.org")
	if err != nil {
		t.Fatal(err)
	}
	h.c.o.AppServices = appservice.NewRegistry(svcs)
}

func (h *harness) asSend(t *testing.T, query, txn, body string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.room.message/"+txn+query, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer astoken")
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, route.Request{Endpoint: route.Send, Write: true, Room: room, Action: "m.room.message", TxnID: txn})
}

// A double-puppeted message: the appservice acts as @u, with its ts. The
// requester and the context name the appservice; a retry answers from the
// transaction cache, there being no device to look the transaction up by.
func TestAppserviceMasquerade(t *testing.T) {
	h := newHarness(t)
	withBridge(t, h, false)
	rec, out := h.asSend(t, "?user_id=@u:example.org&ts=1234", "a1", `{"msgtype":"m.text","body":"bridged"}`)
	if out.Result != "created" {
		t.Fatalf("%+v %d %s", out, rec.Code, rec.Body)
	}
	e := gjson.ParseBytes(h.p.entries[0])
	for path, want := range map[string]string{
		"event.sender":                   `"@u:example.org"`,
		"event.origin_server_ts":         "1234",
		"requester.app_server_id":        `"bridge"`,
		"requester.access_token_id":      "null",
		"requester.device_id":            "null",
		"requester.authenticated_entity": `"@u:example.org"`,
		"context.app_service_id":         `"bridge"`,
		"internal_metadata.txn_id":       `"a1"`,
	} {
		if got := e.Get(path).Raw; got != want {
			t.Errorf("%s = %s, want %s", path, got, want)
		}
	}
	if e.Get("internal_metadata.device_id").Exists() {
		t.Error("a device for an appservice without one")
	}
	id := gjson.Get(rec.Body.String(), "event_id").Str
	rec, out = h.asSend(t, "?user_id=@u:example.org&ts=1234", "a1", `{"msgtype":"m.text","body":"bridged"}`)
	if out.Result != "txn_cached" || gjson.Get(rec.Body.String(), "event_id").Str != id || len(h.p.entries) != 1 {
		t.Errorf("retry: %+v %s", out, rec.Body)
	}
}

func TestAppserviceRefusals(t *testing.T) {
	for query, want := range map[string]string{
		"?user_id=@other:elsewhere.org":          `403 {"errcode":"M_FORBIDDEN","error":"Application service cannot masquerade as this user (@other:elsewhere.org)."}`,
		"?user_id=@bridge_9:example.org":         `403 {"errcode":"M_FORBIDDEN","error":"Application service has not registered this user (@bridge_9:example.org)"}`,
		"?user_id=@u:example.org&device_id=NOPE": `400 {"errcode":"M_UNKNOWN_DEVICE","error":"Application service trying to use a device that doesn't exist ('NOPE' for @u:example.org)"}`,
		"?ts=-5":                                 `400 {"errcode":"M_INVALID_PARAM","error":"Query parameter ts must be a positive integer."}`,
		"?ts=soon":                               `400 {"errcode":"M_INVALID_PARAM","error":"Query parameter ts must be an integer"}`,
	} {
		h := newHarness(t)
		withBridge(t, h, false)
		rec, out := h.asSend(t, query, "a1", `{"msgtype":"m.text","body":"x"}`)
		if got := rec.Body.String(); out.Result != "refused" || strings.TrimSpace(strings.Join([]string{itoaStatus(rec.Code), got}, " ")) != want {
			t.Errorf("%s: %+v %d %s", query, out, rec.Code, got)
		}
	}
	// The appservice's own sender, and a device it may use.
	h := newHarness(t)
	withBridge(t, h, false)
	if _, out := h.asSend(t, "?user_id=@u:example.org&device_id=BRIDGEDEV", "a2", `{"msgtype":"m.text","body":"x"}`); out.Result != "created" {
		t.Errorf("device masquerade: %+v", out)
	}
	if got := gjson.GetBytes(h.p.entries[0], "internal_metadata.device_id").Str; got != "BRIDGEDEV" {
		t.Errorf("device %q", got)
	}
}

// An appservice registered with rate_limited: false is not ratelimited.
func TestAppserviceInvitesUnlimited(t *testing.T) {
	h := newHarness(t)
	withBridge(t, h, false)
	h.c.o.Membership.InvitesPerIssuer.Burst = 1
	for i, target := range []string{other, "@w:example.org"} {
		r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/rooms/"+room+"/invite?user_id=@u:example.org", strings.NewReader(`{"user_id":"`+target+`"}`))
		r.Header.Set("Authorization", "Bearer astoken")
		rec := httptest.NewRecorder()
		if out := h.c.Serve(rec, r, memberReq("invite", "")); out.Result != "created" {
			t.Fatalf("invite %d: %+v %s", i, out, rec.Body)
		}
	}
}

func itoaStatus(n int) string {
	return string([]byte{byte('0' + n/100), byte('0' + n/10%10), byte('0' + n%10)})
}
