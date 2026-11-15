package inbound

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/backoff"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/fedfetch"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
)

type fakeBackfill struct {
	errs  map[string]error
	asked []string
}

func (f *fakeBackfill) Backfill(_ context.Context, dest, _ string, _ *pdu.RoomVersion, _ []string, _ int) ([]*fedfetch.Pulled, error) {
	f.asked = append(f.asked, dest)
	return nil, f.errs[dest]
}

func status(code int) error {
	return mautrix.HTTPError{Response: &http.Response{StatusCode: code}}
}

// try_backfill: our own server skipped, servers in backoff, 5xx and
// broken answers passed over, an empty answer a success, and five 4xx
// refusals the end of it.
func TestTryBackfill(t *testing.T) {
	rv, _ := pdu.LookupRoomVersion("10")
	run := func(errs map[string]error, domains ...string) (*fakeBackfill, bool, error) {
		f := &fakeBackfill{errs: errs}
		p := &Processor{server: "us", log: zerolog.Nop(), backfill: &BackfillDeps{Fetch: f}}
		did, err := p.tryBackfill(context.Background(), "!r", rv, domains, []string{"$x"})
		return f, did, err
	}

	f, did, err := run(map[string]error{
		"a": &backoff.NotRetrying{Dest: "a"}, "b": status(502), "c": fedfetch.ErrBackfillFailed,
	}, "us", "a", "b", "c", "d")
	if err != nil || !did || len(f.asked) != 4 || f.asked[0] != "a" {
		t.Fatalf("got did=%v err=%v asked=%v", did, err, f.asked)
	}

	errs := map[string]error{}
	var doms []string
	for _, d := range []string{"a", "b", "c", "d", "e", "f"} {
		errs[d] = status(403)
		doms = append(doms, d)
	}
	f, did, err = run(errs, doms...)
	if err != nil || did || len(f.asked) != maxBackfillDenied {
		t.Fatalf("denied: got did=%v err=%v asked=%v", did, err, f.asked)
	}

	_, did, err = run(map[string]error{"a": fedfetch.ErrUncertain}, "a", "b")
	if did || !errors.Is(err, ErrBackfillNotReproduced) {
		t.Fatalf("uncertain: got did=%v err=%v", did, err)
	}
}
