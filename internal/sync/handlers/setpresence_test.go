package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/presence"
)

// Synapse parses this with allowed_values, so an unrecognised state is a 400
// rather than something quietly ignored.
func TestParseSetPresence(t *testing.T) {
	cases := []struct {
		query   string
		want    string
		wantErr bool
	}{
		{"", presence.StateOnline, false}, // Synapse's default
		{"?set_presence=online", presence.StateOnline, false},
		{"?set_presence=offline", presence.StateOffline, false},
		{"?set_presence=unavailable", presence.StateUnavailable, false},
		// Accepted by the presence writer under MSC3026, but NOT by the /sync
		// servlet, which is what we are matching.
		{"?set_presence=busy", "", true},
		{"?set_presence=Online", "", true},
		{"?set_presence=away", "", true},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync"+tc.query, nil)
		got, err := parseSetPresence(r)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q was accepted, want 400", tc.query)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tc.query, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q -> %q, want %q", tc.query, got, tc.want)
		}
	}
}

// `set_presence=offline` means "leave my presence alone", not "set me
// offline". Synapse computes affect_presence = set_presence != offline and
// relays nothing when it is false. Relaying "offline" instead would actively
// push users offline -- the opposite of leaving them alone, and worse than the
// bug this whole feature exists to fix.
func TestOfflineRelaysNothing(t *testing.T) {
	if !allowedPresence[presence.StateOffline] {
		t.Fatal("offline must still be an accepted parameter value")
	}
	// relayPresence returns before touching the client, so a nil client with
	// a non-offline state would panic if the guard were the other way round.
	relayPresence(context.Background(), Deps{},
		clientauth.Verdict{Valid: true, UserID: "@a:e.com", DeviceID: "DEV"},
		presence.StateOffline)
}
