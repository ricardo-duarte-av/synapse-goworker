package handlers

import (
	"encoding/json"
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

func rows() []store.ReceiptRow {
	return []store.ReceiptRow{
		{ReceiptType: "m.read", UserID: "@a:e", EventID: "$1", Data: json.RawMessage(`{"ts":1}`)},
		{ReceiptType: "m.read", UserID: "@b:e", EventID: "$2", ThreadID: "main", Data: json.RawMessage(`{"ts":2}`)},
	}
}

func content(t *testing.T, body json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	return m["content"].(map[string]any)
}

// The plural query selects thread_id and merges it into the receipt data; the
// singular one does not select it at all. Same receipt, two shapes, depending
// on the endpoint.
func TestReceiptThreadIDDependsOnEndpoint(t *testing.T) {
	withThreads, err := receiptEvent("!r:e", rows(), "@me:e", true)
	if err != nil {
		t.Fatal(err)
	}
	c := content(t, withThreads)
	threaded := c["$2"].(map[string]any)["m.read"].(map[string]any)["@b:e"].(map[string]any)
	if threaded["thread_id"] != "main" {
		t.Errorf("plural endpoint should stamp thread_id, got %v", threaded)
	}

	without, err := receiptEvent("!r:e", rows(), "@me:e", false)
	if err != nil {
		t.Fatal(err)
	}
	c = content(t, without)
	plain := c["$2"].(map[string]any)["m.read"].(map[string]any)["@b:e"].(map[string]any)
	if _, ok := plain["thread_id"]; ok {
		t.Errorf("singular endpoint should not stamp thread_id, got %v", plain)
	}
}

// MSC4102: an unthreaded receipt replaces a threaded one for the same user and
// event. The MSC is explicit that only semantically meaningless receipts are
// dropped.
func TestUnthreadedReceiptWinsOverThreaded(t *testing.T) {
	in := []store.ReceiptRow{
		{ReceiptType: "m.read", UserID: "@a:e", EventID: "$1", ThreadID: "main", Data: json.RawMessage(`{"ts":1}`)},
		{ReceiptType: "m.read", UserID: "@a:e", EventID: "$1", Data: json.RawMessage(`{"ts":2}`)},
	}
	body, err := receiptEvent("!r:e", in, "@me:e", true)
	if err != nil {
		t.Fatal(err)
	}
	got := content(t, body)["$1"].(map[string]any)["m.read"].(map[string]any)["@a:e"].(map[string]any)
	if _, ok := got["thread_id"]; ok {
		t.Errorf("the unthreaded receipt should have won, got %v", got)
	}
	if got["ts"] != float64(2) {
		t.Errorf("ts = %v, want the unthreaded receipt's 2", got["ts"])
	}
}

// m.read.private is exactly the receipt type that must not be published to
// anyone but its owner.
func TestPrivateReceiptsOfOthersAreRemoved(t *testing.T) {
	in := []store.ReceiptRow{
		{ReceiptType: "m.read.private", UserID: "@me:e", EventID: "$1", Data: json.RawMessage(`{"ts":1}`)},
		{ReceiptType: "m.read.private", UserID: "@other:e", EventID: "$1", Data: json.RawMessage(`{"ts":2}`)},
		{ReceiptType: "m.read", UserID: "@other:e", EventID: "$1", Data: json.RawMessage(`{"ts":3}`)},
	}
	body, err := receiptEvent("!r:e", in, "@me:e", false)
	if err != nil {
		t.Fatal(err)
	}
	byType := content(t, body)["$1"].(map[string]any)
	private := byType["m.read.private"].(map[string]any)
	if _, ok := private["@me:e"]; !ok {
		t.Error("the caller's own private receipt should survive")
	}
	if _, ok := private["@other:e"]; ok {
		t.Error("another user's private receipt must be removed")
	}
	if _, ok := byType["m.read"].(map[string]any)["@other:e"]; !ok {
		t.Error("another user's public receipt should survive")
	}
}

func TestNoReceiptsYieldsNothing(t *testing.T) {
	body, err := receiptEvent("!r:e", nil, "@me:e", true)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		t.Errorf("expected nil for a room with no receipts, got %s", body)
	}
}
