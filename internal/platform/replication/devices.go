package replication

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

var deviceListsRetries = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "goworker_device_lists_replication_retries_total",
	Help: "Replication requests the device-lists writer retried, by instance and reason.",
}, []string{"writer", "reason"})

// RemoveUserAccountData is ReplicationRemoveUserAccountDataRestServlet,
// which a process that does not write account data calls on one that
// does (MSC3391's delete): the user's account data of that type removed.
// CACHE is False: no txn_id. theirs is the writer's stream positions.
func (c *Client) RemoveUserAccountData(ctx context.Context, instance, userID, dataType string, ours StreamPositions) (map[string]int64, error) {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return nil, fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	body, err := json.Marshal(map[string]any{"_INT_STREAM_POS": ours})
	if err != nil {
		return nil, err
	}
	url := t.base + "/_synapse/replication/remove_user_account_data/" + quote(userID) + "/" + quote(dataType)
	resp, err := c.call(ctx, t, "remove_user_account_data", url, body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Positions map[string]int64 `json:"_INT_STREAM_POS"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, fmt.Errorf("replication: remove_user_account_data answer: %w", err)
	}
	return out.Positions, nil
}

// RemovePusher is ReplicationRemovePusherRestServlet: main, the one
// process that deletes pushers, removes the user's pusher of that app ID
// and pushkey. CACHE is False: no txn_id.
func (c *Client) RemovePusher(ctx context.Context, userID, appID, pushKey string, ours StreamPositions) error {
	t, ok := c.targets[synapsecfg.MainProcessInstanceName]
	if !ok {
		return fmt.Errorf("replication: main is not in instance_map")
	}
	body, err := json.Marshal(map[string]any{"app_id": appID, "pushkey": pushKey, "_INT_STREAM_POS": ours})
	if err != nil {
		return err
	}
	_, err = c.call(ctx, t, "remove_pusher", t.base+"/_synapse/replication/remove_pusher/"+quote(userID), body)
	return err
}
