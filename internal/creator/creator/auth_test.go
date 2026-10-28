package creator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/route"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/appservice"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
)

// fakeAuth answers as clientauth does, from the harness's token ("tok") and
// appservice registrations. clientauth's own tests hold its rules; this is
// what the creator does with each kind of verdict.
type fakeAuth struct {
	h *harness
	// verdict, when set, is every answer.
	verdict *clientauth.Verdict
	err     error
}

func (a *fakeAuth) Authenticate(ctx context.Context, r *http.Request) (clientauth.Verdict, error) {
	if a.verdict != nil || a.err != nil {
		var v clientauth.Verdict
		if a.verdict != nil {
			v = *a.verdict
		}
		return v, a.err
	}
	token, rej := clientauth.TokenFromRequest(r)
	if rej != nil {
		return clientauth.Verdict{Rejection: rej, Source: clientauth.SourceNative}, nil
	}
	if reg, ok := a.h.c.o.AppServices.(*appservice.Registry); ok {
		if as := reg.ByToken(token); as != nil {
			return a.appservice(ctx, r, as)
		}
	}
	t := a.h.f.token
	if token != "tok" || t == nil {
		return clientauth.Verdict{Source: clientauth.SourceWhoami, Rejection: &clientauth.Rejection{Status: 401,
			ErrCode: "M_UNKNOWN_TOKEN", Body: []byte(`{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed.","soft_logout":false}`)}}, nil
	}
	entity := t.Owner
	if entity == "" {
		entity = t.UserID
	}
	return clientauth.Verdict{Valid: true, UserID: t.UserID, DeviceID: t.DeviceID, IsGuest: t.Guest, TokenID: t.ID,
		Entity: entity, Source: clientauth.SourceNative}, nil
}

func (a *fakeAuth) appservice(ctx context.Context, r *http.Request, as *appservice.Service) (clientauth.Verdict, error) {
	forbidden := func(status int, code, msg string) (clientauth.Verdict, error) {
		return clientauth.Verdict{Source: clientauth.SourceNative, Rejection: &clientauth.Rejection{Status: status, ErrCode: code,
			Body: []byte(fmt.Sprintf(`{"errcode":%q,"error":%q}`, code, msg))}}, nil
	}
	if as.IPRanges {
		return clientauth.Verdict{Valid: true, UserID: as.Sender, Source: clientauth.SourceWhoami}, nil
	}
	q := r.URL.Query()
	user := as.Sender
	if v, ok := q["user_id"]; ok && v[0] != as.Sender {
		user = v[0]
		in, known := as.InUserNamespace(user)
		switch {
		case !known:
			return clientauth.Verdict{Valid: true, UserID: user, Source: clientauth.SourceWhoami}, nil
		case !in:
			return forbidden(403, "M_FORBIDDEN", fmt.Sprintf("Application service cannot masquerade as this user (%s).", user))
		}
		if exists, _, _, _ := a.h.f.UserFlags(ctx, user); !exists {
			return forbidden(403, "M_FORBIDDEN", fmt.Sprintf("Application service has not registered this user (%s)", user))
		}
	}
	device := ""
	if v := q["device_id"]; len(v) > 0 {
		device = v[0]
		if ok, _ := a.h.f.DeviceExists(ctx, user, device); !ok {
			return forbidden(400, "M_UNKNOWN_DEVICE", fmt.Sprintf(
				"Application service trying to use a device that doesn't exist ('%s' for %s)", device, user))
		}
	}
	return clientauth.Verdict{Valid: true, UserID: user, DeviceID: device, AppServiceID: as.ID, Entity: user,
		Source: clientauth.SourceNative}, nil
}

func (h *harness) sendAs(t *testing.T, auth string) (*httptest.ResponseRecorder, Outcome) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/_matrix/client/v3/rooms/"+room+"/send/m.room.message/t",
		strings.NewReader(`{"msgtype":"m.text","body":"hi"}`))
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	return rec, h.c.Serve(rec, r, route.Request{Endpoint: route.Send, Room: room, Action: "m.room.message", TxnID: "t"})
}

// clientauth's refusals are Synapse's: relayed byte for byte, whether its
// own or /whoami's, and nothing is written.
func TestAuthRefusalsRelayed(t *testing.T) {
	for name, tc := range map[string]struct {
		auth, reason, body string
	}{
		"no token": {"", "missing_token", `{"errcode":"M_MISSING_TOKEN","error":"Missing access token"}`},
		"unknown":  {"Bearer nope", "unknown_token", `{"errcode":"M_UNKNOWN_TOKEN","error":"Invalid access token passed.","soft_logout":false}`},
	} {
		h := newHarness(t)
		rec, out := h.sendAs(t, tc.auth)
		if out.Result != "refused" || out.Refused != tc.reason || rec.Code != 401 || rec.Body.String() != tc.body {
			t.Errorf("%s: %+v %d %s", name, out, rec.Code, rec.Body)
		}
		if len(h.w.calls) != 0 {
			t.Errorf("%s: wrote %v", name, h.w.calls)
		}
	}
}

// What clientauth leaves to /whoami, or cannot answer, is main's.
func TestRequestsLeftToSynapse(t *testing.T) {
	for name, tc := range map[string]struct {
		auth string
		fake fakeAuth
		want string
	}{
		"whoami's verdict": {"Bearer x", fakeAuth{verdict: &clientauth.Verdict{Valid: true, UserID: user, Source: clientauth.SourceWhoami}}, "auth_whoami"},
		"no answer":        {"Bearer x", fakeAuth{err: fmt.Errorf("database down")}, "db_error"},
		"not ascii":        {"Bearer tök", fakeAuth{}, "token_encoding"},
		"ip whitelist":     {"Bearer as-iprange", fakeAuth{}, "auth_whoami"},
		"unknown service":  {"Bearer x", fakeAuth{verdict: &clientauth.Verdict{Valid: true, UserID: user, AppServiceID: "gone", Source: clientauth.SourceNative}}, "appservice_unknown"},
	} {
		h := newHarness(t)
		h.c.o.AppServices = appservice.NewRegistry(map[string]*appservice.Service{
			"as-iprange": {ID: "x", Token: "as-iprange", Sender: "@x:example.org", IPRanges: true}})
		fake := tc.fake
		fake.h = h
		h.c.o.Auth = &fake
		if _, out := h.sendAs(t, tc.auth); out.Fallback != tc.want {
			t.Errorf("%s: %+v, want fallback %q", name, out, tc.want)
		}
	}
}
