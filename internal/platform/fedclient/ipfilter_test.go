package fedclient

import (
	"net"
	"net/netip"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

func TestAllowIP(t *testing.T) {
	ipRanges.Store(nil)
	for ip, want := range map[string]bool{
		"8.8.8.8": true, "2001:4860::8888": true,
		"10.1.2.3": false, "100.64.0.1": false, "127.0.0.1": false, "169.254.1.1": false,
		"0.0.0.0": false, "::": false, "::ffff:192.168.1.1": false, "fc00::1": false,
	} {
		if got := AllowIP(net.ParseIP(ip)); got != want {
			t.Errorf("default %s: %v", ip, got)
		}
	}
	// An allow list lets a blocked range through; the installed function is
	// read on every check.
	r := synapsecfg.DefaultFederationIPRanges()
	r.Allow = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/24")}
	SetIPRanges(func() synapsecfg.IPRanges { return r })
	defer ipRanges.Store(nil)
	if !AllowIP(net.ParseIP("10.0.0.5")) || AllowIP(net.ParseIP("10.0.1.5")) {
		t.Error("allow list")
	}
	if err := DialControl("tcp4", "10.0.1.5:8448", nil); err == nil {
		t.Error("dialled a blocked address")
	}
	if err := DialControl("tcp4", "10.0.0.5:8448", nil); err != nil {
		t.Error(err)
	}
	if err := DialControl("udp", "8.8.8.8:53", nil); err == nil {
		t.Error("dialled udp")
	}
}
