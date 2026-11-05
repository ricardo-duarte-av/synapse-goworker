package fedclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

var tlsSettings atomic.Pointer[func() synapsecfg.FederationTLS]

// SetTLS installs where federation_verify_certificates and its whitelist
// come from: homeserver.yaml's current copy, read on every connection.
func SetTLS(f func() synapsecfg.FederationTLS) { tlsSettings.Store(&f) }

func verifyFor(host string) bool {
	if f := tlsSettings.Load(); f != nil {
		return (*f)().VerifyFor(host)
	}
	return true
}

// VerifyAsConfigured makes t check certificates as Synapse's
// ClientTLSOptionsFactory does: against the system roots for the host,
// unless federation_verify_certificates is false or the host matches
// federation_certificate_verification_whitelist. Go's own check is turned
// off so that this one, which knows the host, decides.
func VerifyAsConfigured(t *http.Transport) {
	if t == nil {
		return
	}
	cfg := t.TLSClientConfig
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	cfg.InsecureSkipVerify = true
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if !verifyFor(cs.ServerName) {
			return nil
		}
		if len(cs.PeerCertificates) == 0 {
			return errors.New("fedclient: no certificate")
		}
		inter := x509.NewCertPool()
		for _, c := range cs.PeerCertificates[1:] {
			inter.AddCert(c)
		}
		_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{DNSName: cs.ServerName, Intermediates: inter})
		return err
	}
	t.TLSClientConfig = cfg
	t.ForceAttemptHTTP2 = true
}
