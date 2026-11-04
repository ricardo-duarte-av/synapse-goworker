package fedclient

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"

	"maunium.net/go/mautrix/federation"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// The process's one federation IP filter: Synapse's
// federation_ip_range_blocklist and allow list (config/server.py), which
// MatrixFederationHttpClient applies to every outbound federation request,
// key fetches included. mautrix's DefaultAllowIP is not that list: it lets
// through 100.64.0.0/10 and the other reserved ranges Synapse blocks, and
// knows no allow list.
//
// Read on every connection from the function SetIPRanges installs, so a
// reloaded homeserver.yaml applies at once. Until one is installed,
// Synapse's default blocklist applies.

var ipRanges atomic.Pointer[func() synapsecfg.IPRanges]

// SetIPRanges installs where the ranges come from: homeserver.yaml's
// current copy.
func SetIPRanges(f func() synapsecfg.IPRanges) { ipRanges.Store(&f) }

func currentRanges() synapsecfg.IPRanges {
	if f := ipRanges.Load(); f != nil {
		return (*f)()
	}
	return synapsecfg.DefaultFederationIPRanges()
}

// AllowIP is a federation.Client's AllowIP: whether we may connect to ip.
func AllowIP(ip net.IP) bool {
	a, ok := netip.AddrFromSlice(ip)
	return ok && !currentRanges().Blocked(a)
}

// DialControl is a net.Dialer's Control, for a federation transport not
// built by mautrix (the sender's): the filter, on the address actually
// dialled.
func DialControl(network, address string, _ syscall.RawConn) error {
	switch network {
	case "tcp4", "tcp6":
	default:
		return fmt.Errorf("fedclient: unsupported network %s", network)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("fedclient: address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !AllowIP(ip) {
		return fmt.Errorf("fedclient: refusing to connect to %s: in federation_ip_range_blocklist", host)
	}
	return nil
}

// wellKnownClient is what server discovery fetches .well-known/matrix/server
// with: Synapse's WellKnownResolver connects through the same blocklisting
// agent as federation requests. mautrix would otherwise use
// http.DefaultClient, which connects anywhere.
var wellKnownClient = func() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: DialControl}
	t.DialContext = d.DialContext
	return &http.Client{Transport: t}
}()

// FilterWellKnown makes a resolving transport fetch .well-known through the
// filter. Call it before useragent.Resolving, which wraps that client.
func FilterWellKnown(srt *federation.ServerResolvingTransport) {
	opts := &federation.ResolveServerNameOpts{}
	if srt.ResolveOpts != nil {
		*opts = *srt.ResolveOpts
	}
	opts.HTTPClient = wellKnownClient
	srt.ResolveOpts = opts
}
