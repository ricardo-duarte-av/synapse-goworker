package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// The User-Agent is the first thing in the request log chosen by the caller
// rather than by us, so the tests here are about what a hostile or broken
// client can put in it, not about the happy path.
func TestUserAgentIsSafeToLog(t *testing.T) {
	cases := []struct {
		name string
		ua   string
		want string
	}{
		{
			name: "a real client passes through untouched",
			ua:   "Element X Android 26.08.3 (Pixel 8; Android 15)",
			want: "Element X Android 26.08.3 (Pixel 8; Android 15)",
		},
		{"absent", "", ""},
		{
			// A newline would let a caller write what looks like a second log
			// line into anything tailing the file.
			name: "control characters are dropped",
			ua:   "evil\nlevel=error message=\"fake\"\rsc\x00hildi",
			want: "evillevel=error message=\"fake\"schildi",
		},
		{
			// Non-ASCII is legitimate and must survive.
			name: "valid multi-byte text survives",
			ua:   "gomuks/0.4 (Ünicode ✓)",
			want: "gomuks/0.4 (Ünicode ✓)",
		},
		{
			name: "invalid UTF-8 is stripped, not passed on",
			ua:   "curl/8.5\xff\xfe",
			want: "curl/8.5",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.ua != "" {
				r.Header.Set("User-Agent", tc.ua)
			}
			if got := userAgent(r); got != tc.want {
				t.Errorf("userAgent() = %q, want %q", got, tc.want)
			}
		})
	}
}

// A client can send a header far longer than any real one, and without a cap
// every log line it produces carries the whole thing.
func TestUserAgentIsCapped(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("User-Agent", strings.Repeat("A", 4096))

	got := userAgent(r)
	if len(got) > maxUserAgent+len("…") {
		t.Errorf("length %d exceeds the cap of %d", len(got), maxUserAgent)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("a truncated value must say so: %q", got)
	}
}

// Truncation must not cut a rune in half: half a rune is invalid UTF-8, which
// is the thing the sanitiser exists to keep out of the log.
func TestUserAgentTruncatesOnARuneBoundary(t *testing.T) {
	// Three-byte runes do not divide the cap evenly, so some offset in this
	// sweep lands mid-rune unless the cut is aligned.
	for extra := 0; extra < 8; extra++ {
		ua := strings.Repeat("A", extra) + strings.Repeat("✓", maxUserAgent)
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("User-Agent", ua)

		got := userAgent(r)
		if !utf8.ValidString(got) {
			t.Fatalf("padding %d produced invalid UTF-8: %q", extra, got)
		}
	}
}
