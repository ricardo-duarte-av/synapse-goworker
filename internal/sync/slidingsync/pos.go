package slidingsync

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/streamtoken"
)

// Pos is a sliding sync position: `<connection position>/<stream token>`.
//
// The right half is exactly the 14-field token classic sync uses, which is why
// internal/streamtoken serves both endpoints. The left half is an opaque
// server-side key into the connection store, and a zero there is the sentinel
// for "I have no connection state" -- distinct from sending no `pos` at all,
// because the stream token still bounds the response. 9.3% of live requests
// carrying a `pos` use it.
type Pos struct {
	ConnectionPosition int64
	StreamToken        streamtoken.Token
}

// ParsePos reads a `pos`.
func ParsePos(s string) (Pos, error) {
	slash := strings.IndexByte(s, '/')
	if slash < 0 {
		return Pos{}, fmt.Errorf("sliding sync pos %q has no connection position", s)
	}
	conn, err := strconv.ParseInt(s[:slash], 10, 64)
	if err != nil {
		return Pos{}, fmt.Errorf("sliding sync pos %q: %w", s, err)
	}
	if conn < 0 {
		return Pos{}, fmt.Errorf("sliding sync pos %q has a negative connection position", s)
	}
	tok, err := streamtoken.Parse(s[slash+1:])
	if err != nil {
		return Pos{}, fmt.Errorf("sliding sync pos %q: %w", s, err)
	}
	return Pos{ConnectionPosition: conn, StreamToken: tok}, nil
}

// String renders a `pos`.
func (p Pos) String() string {
	return strconv.FormatInt(p.ConnectionPosition, 10) + "/" + p.StreamToken.String()
}
