package replication

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var receiptsRetries = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goworker_receipts_replication_retries_total",
	Help: "Replication requests the receipts writer retried, by instance and reason.",
}, []string{"writer", "reason"})

// StreamPositions is _INT_STREAM_POS: in a request, which instance wrote
// what; in a response, how far the answering instance's streams are.
type StreamPositions struct {
	InstanceName string           `json:"instance_name"`
	Streams      map[string]int64 `json:"streams"`
}

// AddRoomAccountData is ReplicationAddRoomAccountDataRestServlet
// (replication/http/account_data.py), which a process that does not write
// account data calls on one that does: content stored as the user's
// account data of that type in that room. ours are the positions of the
// streams this process writes, for the writer to wait on; the writer's
// answer carries its own positions (theirs), for us to wait on, and its
// max_stream_id. CACHE is False: no txn_id.
func (c *Client) AddRoomAccountData(ctx context.Context, instance, userID, roomID, dataType string,
	content json.RawMessage, ours StreamPositions) (maxStreamID int64, theirs map[string]int64, err error) {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return 0, nil, fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	body, err := json.Marshal(map[string]any{"content": content, "_INT_STREAM_POS": ours})
	if err != nil {
		return 0, nil, fmt.Errorf("replication: encode account data: %w", err)
	}
	url := t.base + "/_synapse/replication/add_room_account_data/" + quote(userID) + "/" + quote(roomID) + "/" + quote(dataType)
	resp, err := c.call(ctx, t, "add_room_account_data", url, body)
	if err != nil {
		return 0, nil, err
	}
	var out struct {
		MaxStreamID int64            `json:"max_stream_id"`
		Positions   map[string]int64 `json:"_INT_STREAM_POS"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return 0, nil, fmt.Errorf("replication: add_room_account_data answer: %w", err)
	}
	return out.MaxStreamID, out.Positions, nil
}
