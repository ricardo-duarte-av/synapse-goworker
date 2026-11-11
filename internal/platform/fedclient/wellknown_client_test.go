package fedclient

import (
	"net/http"
	"testing"
)

// A .well-known lookup is one request per resolution: its connection must
// not be kept (nor be HTTP/2, which no idle limit bounds).
func TestWellKnownClientKeepsNoConnection(t *testing.T) {
	tr, ok := wellKnownClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport %T", wellKnownClient.Transport)
	}
	if !tr.DisableKeepAlives || tr.ForceAttemptHTTP2 {
		t.Errorf("keep-alives disabled %v, HTTP/2 forced %v", tr.DisableKeepAlives, tr.ForceAttemptHTTP2)
	}
	if tr.DialContext == nil {
		t.Error("no filtering dialer")
	}
}
