package fedauth

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"go.mau.fi/util/exhttp"
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/federation"
)

// signedSend produces a genuinely signed PUT /send carrying a transaction body,
// and that body.
func signedSend(t *testing.T, remote, destination string) (*http.Request, []byte, *federation.SigningKey) {
	t.Helper()
	key := federation.GenerateSigningKey()
	client := federation.NewClient(remote, key, federation.NewInMemoryCache(), exhttp.SensibleClientSettings)
	ct := &captureTransport{}
	client.HTTP = &http.Client{Transport: ct}

	_, _ = client.SendTransaction(context.Background(), &federation.ReqSendTransaction{
		Destination:    destination,
		TxnID:          "txn1",
		Origin:         remote,
		OriginServerTS: jsontime.UnixMilliNow(),
		PDUs:           []federation.PDU{},
		EDUs:           []federation.EDU{federation.EDU(`{"edu_type":"m.typing","content":{"room_id":"!r:x","typing":true}}`)},
	})
	if ct.req == nil || ct.req.GetBody == nil {
		t.Fatal("no replayable request was produced")
	}
	rc, err := ct.req.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return ct.req, body, key
}

func withBody(r *http.Request, body []byte) *http.Request {
	r2 := r.Clone(context.Background())
	r2.Body = io.NopCloser(bytes.NewReader(body))
	r2.ContentLength = int64(len(body))
	return r2
}

// The signature covers the transaction body, so a transaction is only accepted
// with exactly the EDUs the origin signed.
func TestVerifySignedTransaction(t *testing.T) {
	v := New("example.com", Options{})
	req, body, key := signedSend(t, "remote.example", "example.com")
	trust(t, v, "remote.example", key)

	if res := v.Verify(withBody(req, body)); !res.OK() || res.Origin != "remote.example" {
		t.Fatalf("a correctly signed transaction was rejected: %+v", res.Err)
	}

	tampered := bytes.Replace(body, []byte(`"typing":true`), []byte(`"typing":false`), 1)
	if bytes.Equal(tampered, body) {
		t.Fatal("test did not alter the body")
	}
	if res := v.Verify(withBody(req, tampered)); res.OK() {
		t.Error("a transaction with altered EDUs verified")
	}
}

// A transaction body is far larger than the 64KiB gopro allows its GET
// endpoints; the limit must be configurable and enforced.
func TestVerifyBodyLimit(t *testing.T) {
	v := New("example.com", Options{MaxBodySize: 16})
	req, body, key := signedSend(t, "remote.example", "example.com")
	trust(t, v, "remote.example", key)
	if res := v.Verify(withBody(req, body)); res.OK() {
		t.Error("a body over MaxBodySize verified")
	}
}

// An empty-object body: Synapse signs "content": {} with it, which mautrix
// cannot check (omitempty drops {} under jsonv2), so Verify does.
func TestVerifyEmptyObjectBody(t *testing.T) {
	v := New("example.com", Options{})
	key := federation.GenerateSigningKey()
	trust(t, v, "remote.example", key)
	sign := func(withContent bool) *http.Request {
		msg := map[string]any{"method": "POST", "uri": "/_matrix/federation/v1/user/keys/query",
			"origin": "remote.example", "destination": "example.com"}
		if withContent {
			msg["content"] = map[string]any{}
		}
		sig, err := key.SignJSON(msg)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := http.NewRequest(http.MethodPost, "http://example.com/_matrix/federation/v1/user/keys/query", bytes.NewReader([]byte(`{}`)))
		r.ContentLength = 2
		r.Header.Set("Authorization", `X-Matrix origin="remote.example",destination="example.com",key="`+string(key.ID)+`",sig="`+sig+`"`)
		return r
	}
	r := sign(true)
	if res := v.Verify(r); !res.OK() || res.Origin != "remote.example" {
		t.Fatalf("Synapse's signature over content {} was rejected: %+v", res.Err)
	}
	if b, _ := io.ReadAll(r.Body); string(b) != `{}` {
		t.Errorf("body left %q", b)
	}
	if res := v.Verify(sign(false)); res.OK() {
		t.Error("a signature without the content verified")
	}
}
