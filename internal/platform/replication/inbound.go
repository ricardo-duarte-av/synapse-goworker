package replication

// The inbound path's endpoints: fed_send_edu, fed_send_events and
// multi_user_device_resync, and their goinbound_ metrics.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/edutype"
)

// SendEDU forwards one EDU to instance, as ReplicationFederationSendEduRestServlet
// expects it: {"origin": ..., "content": ...}.
//
// Retries follow _base.py: a timeout is retried after a second, as often as the
// context allows, and a connection failure up to connectRetries times with
// exponential backoff. Both are safe because every attempt carries the same
// txn_id. Any other failure is returned at once.
//
// The stream positions Synapse's own client includes in the body
// (_INT_STREAM_POS) are omitted. They tell the writer to wait until it has
// seen the sender's writes, and this process writes nothing; the server treats
// the key as optional (_base.py:431).
func (c *Client) SendEDU(ctx context.Context, instance, eduType, origin string, content json.RawMessage) error {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}

	body, err := json.Marshal(struct {
		Origin  string          `json:"origin"`
		Content json.RawMessage `json:"content"`
	}{origin, content})
	if err != nil {
		return fmt.Errorf("replication: encode edu: %w", err)
	}

	url := t.base + "/_synapse/replication/fed_send_edu/" + quote(eduType) + "/" + txnID()

	start := time.Now()
	connectFailures := 0
	for {
		_, err = c.post(ctx, t, url, body)
		switch {
		case err == nil:
			observe(t.name, eduType, "ok", start)
			return nil
		case ctx.Err() != nil:
			observe(t.name, eduType, "timeout", start)
			return fmt.Errorf("replication: send %s edu to %s: %w", eduType, t.name, ctx.Err())
		case isTimeout(err):
			c.retries.WithLabelValues(t.name, "timeout").Inc()
			if serr := c.sleep(ctx, time.Second); serr != nil {
				continue // reported by the ctx.Err() case
			}
		case isConnectError(err) && connectFailures < connectRetries:
			c.retries.WithLabelValues(t.name, "connect").Inc()
			delay := time.Second << connectFailures
			connectFailures++
			if serr := c.sleep(ctx, delay); serr != nil {
				continue
			}
		default:
			observe(t.name, eduType, "error", start)
			return fmt.Errorf("replication: send %s edu to %s: %w", eduType, t.name, err)
		}
	}
}

// SendEvents persists events through instance, as
// ReplicationFederationSendEventsRestServlet expects them: {"events": [...],
// "backfilled": ..., "room_id": ...}, each entry already serialized. It
// returns the max_stream_id the persister answers with. Retries are
// SendEDU's: the same txn_id on every attempt, which the persister answers
// from its response cache.
func (c *Client) SendEvents(ctx context.Context, instance, roomID string, entries []json.RawMessage, backfilled bool) (int64, error) {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return 0, fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	body, err := json.Marshal(struct {
		Events     []json.RawMessage `json:"events"`
		Backfilled bool              `json:"backfilled"`
		RoomID     string            `json:"room_id"`
	}{entries, backfilled, roomID})
	if err != nil {
		return 0, fmt.Errorf("replication: encode events: %w", err)
	}
	url := t.base + "/_synapse/replication/fed_send_events/" + txnID()

	start := time.Now()
	connectFailures := 0
	for {
		b, err := c.post(ctx, t, url, body)
		var he *HTTPError
		switch {
		case err == nil:
			observeInboundEvents(t.name, "ok", start)
			var resp struct {
				MaxStreamID int64 `json:"max_stream_id"`
			}
			if err := json.Unmarshal(b, &resp); err != nil {
				return 0, fmt.Errorf("replication: fed_send_events answer: %w", err)
			}
			return resp.MaxStreamID, nil
		case ctx.Err() != nil:
			observeInboundEvents(t.name, "timeout", start)
			return 0, fmt.Errorf("replication: fed_send_events to %s: %w", t.name, ctx.Err())
		case errors.As(err, &he) && he.Status == http.StatusConflict:
			observeInboundEvents(t.name, "conflict", start)
			return 0, ErrPartialStateConflict
		case isTimeout(err):
			c.retries.WithLabelValues(t.name, "timeout").Inc()
			_ = c.sleep(ctx, time.Second)
		case isConnectError(err) && connectFailures < connectRetries:
			c.retries.WithLabelValues(t.name, "connect").Inc()
			delay := time.Second << connectFailures
			connectFailures++
			_ = c.sleep(ctx, delay)
		default:
			observeInboundEvents(t.name, "error", start)
			return 0, fmt.Errorf("replication: fed_send_events to %s: %w", t.name, err)
		}
	}
}

func observeInboundEvents(instance, result string, start time.Time) {
	inboundSendEvents.WithLabelValues(instance, result).Observe(time.Since(start).Seconds())
}

var inboundSendEvents = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "goinbound_replication_send_events_duration_seconds",
	Help:    "Time to persist events through fed_send_events, including retries, by outcome.",
	Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
}, []string{"writer", "result"})

func observe(instance, eduType, result string, start time.Time) {
	sendDuration.WithLabelValues(instance, edutype.Label(eduType), result).Observe(time.Since(start).Seconds())
}

var (
	sendDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goinbound_replication_send_edu_duration_seconds",
		Help:    "Time to forward one EDU to its stream writer, including retries, by outcome.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"writer", "edu_type", "result"})
	inboundRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goinbound_replication_retries_total",
		Help: "Replication requests retried, by instance and reason.",
	}, []string{"writer", "reason"})
)

// MultiUserDeviceResync asks instance (the first device_lists writer) to
// resync remote users' devices: ReplicationMultiUserDevicesResyncRestServlet,
// as DeviceListWorkerUpdater.multi_user_device_resync calls it.
func (c *Client) MultiUserDeviceResync(ctx context.Context, instance string, userIDs []string) error {
	t, ok := c.targets[canonical(instance)]
	if !ok {
		return fmt.Errorf("replication: instance %q is not in instance_map", instance)
	}
	body, err := json.Marshal(struct {
		UserIDs []string `json:"user_ids"`
	}{userIDs})
	if err != nil {
		return err
	}
	_, err = c.post(ctx, t, t.base+"/_synapse/replication/multi_user_device_resync/"+txnID(), body)
	if err != nil {
		return fmt.Errorf("replication: multi_user_device_resync to %s: %w", t.name, err)
	}
	return nil
}
