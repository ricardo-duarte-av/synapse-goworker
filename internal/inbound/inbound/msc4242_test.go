package inbound

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
)

// A PDU in an MSC4242 room is ignored while Synapse does not know the room
// version, and is Synapse's once msc4242_enabled makes it known.
func TestPlanPDUsMSC4242Room(t *testing.T) {
	admin := os.Getenv("INBOUND_TEST_ADMIN_DSN")
	if admin == "" {
		t.Skip("INBOUND_TEST_ADMIN_DSN not set")
	}
	if strings.Contains(admin, "/var/sockets") {
		t.Fatal("INBOUND_TEST_ADMIN_DSN must be a throwaway database, not the live one")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	room := fmt.Sprintf("!msc4242-%d:remote.test", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, `INSERT INTO rooms (room_id, room_version) VALUES ($1, 'org.matrix.msc4242.12')`, room); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), `DELETE FROM rooms WHERE room_id = $1`, room) })

	pst, err := pdustore.Open(ctx, admin, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pst.Close()
	raw := json.RawMessage(`{"room_id":"` + room + `","type":"m.room.message","sender":"@a:remote.test","content":{}}`)

	plan, why := NewFrontDoor(FrontDoorDeps{Store: pst}).PlanPDUs(ctx, "remote.test", []json.RawMessage{raw}, time.Now())
	if why != "" || plan == nil || len(plan.rooms) != 0 {
		t.Fatalf("disabled: %q %+v; want the PDU ignored", why, plan)
	}
	if _, why := NewFrontDoor(FrontDoorDeps{Store: pst, MSC4242: true}).PlanPDUs(ctx, "remote.test", []json.RawMessage{raw}, time.Now()); why != "pdu_msc4242_room" {
		t.Fatalf("enabled: %q; want the transaction Synapse's", why)
	}
}
