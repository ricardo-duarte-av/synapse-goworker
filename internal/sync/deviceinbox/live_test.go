package deviceinbox

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// TestLiveDeleterAgainstRealDatabase checks the one writing connection this
// worker has: that the role is what deploy/device-inbox-role.sql describes, and
// that both statements of the deletion loop actually run.
//
// It deletes for a user id that cannot exist, so it exercises the SQL end to
// end while being incapable of removing a real message. Nothing here inserts:
// the role deliberately cannot, and a test that could put rows into a
// production device_inbox would be a worse thing to own than an untested
// delete path.
//
// Gated on env vars and skipped otherwise, so it never fails CI:
//
//	GOSYNC_TEST_TODEVICE_DSN="host=/var/sockets user=gosync_inbox dbname=synapse-db" \
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	  go test ./internal/deviceinbox -run Live -v
func TestLiveDeleterAgainstRealDatabase(t *testing.T) {
	dsn := os.Getenv("GOSYNC_TEST_TODEVICE_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_TODEVICE_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open itself is most of the test: it refuses a role that is read-only,
	// cannot delete from device_inbox, or can reach beyond it.
	d, err := Open(ctx, Config{DSN: dsn, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	const nobody = "@gosync-test-nobody:invalid"
	n, err := d.DeleteUpTo(ctx, nobody, "NOSUCHDEVICE", 1)
	if err != nil {
		t.Fatalf("DeleteUpTo: %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted %d rows for a user that cannot exist", n)
	}

	// Second call is served from the cache and must still report nothing.
	if n, err := d.DeleteUpTo(ctx, nobody, "NOSUCHDEVICE", 1); err != nil || n != 0 {
		t.Fatalf("DeleteUpTo (cached) = %d, %v", n, err)
	}
}

// TestLiveReadRejectsBroadRole is the negative half: pointed at the read-only
// role, Open must refuse rather than start and fail later on the sync path.
func TestLiveReadRejectsBroadRole(t *testing.T) {
	dsn := os.Getenv("GOSYNC_TEST_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d, err := Open(ctx, Config{DSN: dsn, ConnectTimeout: 10 * time.Second})
	if err == nil {
		d.Close()
		t.Fatal("Open accepted the read-only role; it must refuse a role that cannot delete")
	}
	if !strings.Contains(err.Error(), "read_only") && !strings.Contains(err.Error(), "DELETE") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}

// TestLiveMessagesForDeviceQuery checks the read side runs against the real
// schema. A device that does not exist has no messages, which is all this can
// assert without a device of its own -- the shape of the query is what is under
// test, not the rows.
func TestLiveMessagesForDeviceQuery(t *testing.T) {
	dsn := os.Getenv("GOSYNC_TEST_DSN")
	if dsn == "" {
		t.Skip("GOSYNC_TEST_DSN not set; skipping live test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := store.Open(ctx, store.Config{DSN: dsn, ConnectTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	msgs, next, err := s.MessagesForDevice(ctx, "@gosync-test-nobody:invalid",
		"NOSUCHDEVICE", 0, 1<<40, store.ToDeviceLimit)
	if err != nil {
		t.Fatalf("MessagesForDevice: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("got %d messages for a device that cannot exist", len(msgs))
	}
	// Under the limit, so the caller resumes from the window's upper bound.
	if next != 1<<40 {
		t.Errorf("next = %d, want the requested upper bound", next)
	}
}
