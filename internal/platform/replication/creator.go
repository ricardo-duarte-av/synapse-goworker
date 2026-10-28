package replication

// The event creator's endpoints: send_events, and main's membership_change
// and added_delayed_event, and the profile writer's profile_set_field and
// profile_delete_field; and their goevent_ metrics.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// SendClientEvents persists events built here through instance, as
// ReplicationSendEventsRestServlet expects them: {"events": [...]}, each
// entry already serialized (_serialize_payload). It returns the stream ID and
// event ID the writer answers with; an event ID other than the last one sent
// means the writer deduplicated it, by transaction.
//
// The stream positions Synapse's own client includes (_INT_STREAM_POS) are
// omitted: they tell the writer to wait for the sender's writes, and the only
// streams this process could have written to are none.
//
// A non-2xx answer is returned as *HTTPError with the writer's status and
// body, which Synapse relays to the client as they are (to_synapse_error):
// a ratelimit, a forbidden remote invite.
func (c *Client) SendClientEvents(ctx context.Context, instance string, entries []json.RawMessage) (streamID int64, eventID string, err error) {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return 0, "", fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	body, err := json.Marshal(struct {
		Events []json.RawMessage `json:"events"`
	}{entries})
	if err != nil {
		return 0, "", fmt.Errorf("replication: encode events: %w", err)
	}
	url := t.base + "/_synapse/replication/send_events/" + txnID()

	start := time.Now()
	b, err := c.call(ctx, t, "send_events", url, body)
	if err != nil {
		var he *HTTPError
		result := "error"
		switch {
		case ctx.Err() != nil:
			result = "timeout"
		case errors.As(err, &he) && he.Status == http.StatusConflict:
			observeCreatorEvents(t.name, "conflict", start)
			return 0, "", ErrPartialStateConflict
		case errors.As(err, &he):
			result = "status_" + fmt.Sprint(he.Status)
		}
		observeCreatorEvents(t.name, result, start)
		return 0, "", err
	}
	observeCreatorEvents(t.name, "ok", start)
	var resp struct {
		StreamID int64  `json:"stream_id"`
		EventID  string `json:"event_id"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return 0, "", fmt.Errorf("replication: send_events answer: %w", err)
	}
	return resp.StreamID, resp.EventID, nil
}

func observeCreatorEvents(instance, result string, start time.Time) {
	creatorSendEvents.WithLabelValues(instance, result).Observe(time.Since(start).Seconds())
}

var creatorSendEvents = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "goevent_replication_send_events_duration_seconds",
	Help:    "Time to persist events through send_events, including retries, by outcome.",
	Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
}, []string{"writer", "result"})

var (
	creatorRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goevent_replication_retries_total",
		Help: "Replication requests retried, by instance and reason.",
	}, []string{"writer", "reason"})
)

// UserLeftRoom is ReplicationUserJoinedLeftRoomRestServlet, which a worker's
// _user_left_room calls on main once a local user's leave from a room they
// were joined to is persisted: main's distributor fires user_left_room (the
// typing handler drops their typing notification). CACHE is False for it, so
// the path carries no txn_id.
func (c *Client) UserLeftRoom(ctx context.Context, roomID, userID string) error {
	t, ok := c.targets[synapsecfg.MainProcessInstanceName]
	if !ok {
		return fmt.Errorf("replication: main is not in instance_map")
	}
	url := t.base + "/_synapse/replication/membership_change/" + quote(roomID) + "/" + quote(userID) + "/left"
	_, err := c.call(ctx, t, "membership_change", url, []byte("{}"))
	return err
}

// DelayedEventAdded is ReplicationAddedDelayedEventRestServlet: main, which
// sends delayed events, is told the send time of the next one due.
func (c *Client) DelayedEventAdded(ctx context.Context, nextSendTS int64) error {
	t, ok := c.targets[synapsecfg.MainProcessInstanceName]
	if !ok {
		return fmt.Errorf("replication: main is not in instance_map")
	}
	body, _ := json.Marshal(map[string]int64{"next_send_ts": nextSendTS})
	_, err := c.call(ctx, t, "added_delayed_event", t.base+"/_synapse/replication/added_delayed_event/", body)
	return err
}

// ProfileSetField is ReplicationProfileSetField, which a worker that is not
// the profile updates writer (the first events writer) calls to set a
// profile field: the writer stores it, updates the user directory and, with
// propagate, schedules the join events into the user's rooms. requester is
// the requesting user's ID, the only part of the requester sent. CACHE is
// False: no txn_id.
func (c *Client) ProfileSetField(ctx context.Context, instance, userID, requester, field string, value json.RawMessage, byAdmin, propagate bool) error {
	body, err := json.Marshal(map[string]any{
		"requester": requester, "field_name": field, "new_value": value,
		"by_admin": byAdmin, "propagate": propagate,
	})
	if err != nil {
		return fmt.Errorf("replication: encode profile field: %w", err)
	}
	return c.profileCall(ctx, instance, "profile_set_field", userID, body)
}

// ProfileDeleteField is ReplicationProfileDeleteField: a custom field
// removed. CACHE is False: no txn_id.
func (c *Client) ProfileDeleteField(ctx context.Context, instance, userID, requester, field string, byAdmin bool) error {
	body, err := json.Marshal(map[string]any{"requester": requester, "field_name": field, "by_admin": byAdmin})
	if err != nil {
		return fmt.Errorf("replication: encode profile field: %w", err)
	}
	return c.profileCall(ctx, instance, "profile_delete_field", userID, body)
}

func (c *Client) profileCall(ctx context.Context, instance, name, userID string, body []byte) error {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	_, err := c.call(ctx, t, name, t.base+"/_synapse/replication/"+name+"/"+quote(userID), body)
	return err
}
