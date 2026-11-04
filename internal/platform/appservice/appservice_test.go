package appservice

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

func TestExclusiveUsers(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "reg.yaml")
	_ = os.WriteFile(f, []byte(`
namespaces:
  users:
    - exclusive: true
      regex: '@telegram_.*:example\.org'
    - exclusive: false
      regex: '@.*:example\.org'
`), 0o600)
	re, err := ExclusiveUsers([]string{f})
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]bool{"@telegram_123:example.org": true, "@alice:example.org": false} {
		if got := re.MatchString(id); got != want {
			t.Errorf("%s: %v, want %v (non-exclusive namespaces do not count)", id, got, want)
		}
	}
}

// The deployment's registrations must load and compile.
//
//	HOMESERVER_YAML=/opt/matrix/synapse/synapse/homeserver.yaml go test ./internal/appservice/
func TestLiveRegistrations(t *testing.T) {
	path := os.Getenv("HOMESERVER_YAML")
	if path == "" {
		t.Skip("HOMESERVER_YAML not set")
	}
	hs, err := synapsecfg.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	re, err := ExclusiveUsers(hs.AppServiceFiles)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d registrations; exclusive regex: %v", len(hs.AppServiceFiles), re)
	svcs, err := Load(hs.AppServiceFiles, hs.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range svcs {
		t.Logf("%s: unsupported %q", s.ID, s.Unsupported)
	}
}

func TestLoad(t *testing.T) {
	f := filepath.Join(t.TempDir(), "reg.yaml")
	_ = os.WriteFile(f, []byte(`
id: bridge
as_token: secret
hs_token: other
sender_localpart: bot
rate_limited: false
namespaces:
  users:
    - exclusive: true
      regex: '@bridge_.*:example\.org'
`), 0o600)
	svcs, err := Load([]string{f}, "example.org")
	if err != nil {
		t.Fatal(err)
	}
	s := svcs["secret"]
	if s == nil || s.ID != "bridge" || s.Sender != "@bot:example.org" || s.RateLimited || s.IPRanges {
		t.Fatalf("%+v", s)
	}
	for id, want := range map[string]bool{"@bridge_1:example.org": true, "@bridge_1:example.org.evil": true, "@x:example.org": false, "x@bridge_1:example.org": false} {
		if in, ok := s.InUserNamespace(id); !ok || in != want {
			t.Errorf("%s: %v %v", id, in, ok)
		}
	}
}

func testRegistration(extra string) []byte {
	return []byte(`
id: bridge
url: http://bridge
as_token: AS_TOKEN
hs_token: HS_TOKEN
sender_localpart: bridgebot
namespaces:
  users:
    - exclusive: true
      regex: '@bridge_.*:example\.org'
    - exclusive: false
      regex: '@shared'
` + extra)
}

func TestParse(t *testing.T) {
	as, err := parse("example.org", testRegistration(""))
	if err != nil || as.Unsupported != "" {
		t.Fatal(err, as)
	}
	for user, want := range map[string]bool{
		"@bridgebot:example.org":  true, // the sender
		"@bridge_1:example.org":   true,
		"@bridge_1:example.orgX":  true, // re.match: a prefix suffices
		"@sharedwhatever:else":    true, // a non-exclusive namespace counts
		"@alice:example.org":      false,
		"x@bridge_1:example.org":  false, // anchored at the start
		"@bridgebot:example.org2": false,
	} {
		if got := as.InterestedInUser(user); got != want {
			t.Errorf("%s: got %v, want %v", user, got, want)
		}
	}

	la, err := parse("example.org", []byte(strings.Replace(string(testRegistration("")),
		`'@shared'`, `'@(?!admin)'`, 1)))
	if err != nil || la.Unsupported == "" {
		t.Fatalf("a lookahead should be valid but not native: %v %+v", err, la)
	}
	if _, ok := la.InUserNamespace("@x"); ok {
		t.Error("a namespace RE2 cannot compile answers")
	}

	for _, bad := range []string{
		strings.Replace(string(testRegistration("")), "as_token: AS_TOKEN\n", "", 1),
		strings.Replace(string(testRegistration("")), "sender_localpart: bridgebot", "sender_localpart: 'bridge bot'", 1),
		strings.Replace(string(testRegistration("")), "exclusive: false\n", "", 1),
	} {
		if _, err := parse("example.org", []byte(bad)); err == nil {
			t.Errorf("accepted:\n%s", bad)
		}
	}

	as, err = parse("example.org", testRegistration("ip_range_whitelist: ['10.0.0.0/8', '192.0.2.7']\n"))
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]bool{"10.1.2.3": true, "192.0.2.7": true, "192.0.2.8": false, "::ffff:10.0.0.1": true, "nonsense": false} {
		if got := as.AllowsIP(ip); got != want {
			t.Errorf("%s: got %v, want %v", ip, got, want)
		}
	}
}
