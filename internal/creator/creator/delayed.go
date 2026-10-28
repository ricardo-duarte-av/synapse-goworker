package creator

import (
	stdbytes "bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/ratelimit"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/creator/writer"
)

// Delayed events (MSC4140): DelayedEventsHandler.add (handlers/
// delayed_events.py, v1.163.0) on a worker. The row is written here; main,
// which schedules delayed events, is told the next send time and sends the
// event when it is due.

// addDelayed is add, answered with {"delay_id"}.
func (c *Creator) addDelayed(ctx context.Context, w http.ResponseWriter, rq *requester, in *input, delayMS int64, txnKey string) Outcome {
	// "Use standard request limiter for scheduling new delayed events."
	if out, limited := c.messageRatelimit(ctx, w, rq); limited {
		return out
	}
	d := c.o.Delayed
	if !d.Enabled() {
		return c.refuse(w, forbidden("delayed_disabled", "Sending delayed events has been disallowed"))
	}
	if delayMS > d.MaxDelayMS {
		return c.refuse(w, badRequest("delay_too_large", fmt.Sprintf(
			"The requested delay (%dms) exceeds the allowed maximum (%dms)", delayMS, d.MaxDelayMS), "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE"))
	}
	if st := validateBuilder(in, gjson.ParseBytes(in.content), in.room); st != nil {
		return c.end(w, st)
	}
	var content stdbytes.Buffer
	if err := json.Compact(&content, in.content); err != nil {
		return fallback("not_object")
	}
	now := c.o.Now().UnixMilli()
	row := writer.DelayedEvent{
		DelayID:          delayID(),
		UserLocalpart:    localpart(rq.UserID),
		DelayMS:          delayMS,
		SendTS:           now + delayMS,
		RoomID:           in.room,
		EventType:        in.typ,
		StateKey:         in.stateKey,
		OriginServerTS:   in.originTS,
		Content:          content.String(),
		StickyDurationMS: in.sticky,
	}
	if rq.DeviceID != "" {
		dev := rq.DeviceID
		row.DeviceID = &dev
	}
	next, err := c.o.Writer.AddDelayedEvent(ctx, row, d.PerUser)
	var le *writer.DelayedLimitError
	switch {
	case errors.As(err, &le):
		// LimitExceededError, its message replaced; no pause.
		f := &refusal{reason: "delayed_limit", status: http.StatusTooManyRequests, errcode: "M_LIMIT_EXCEEDED",
			msg: "The maximum number of delayed events has been reached.", extra: map[string]any{"retry_after_ms": nil}}
		if le.RetryAfterMS != nil {
			f.extra["retry_after_ms"] = *le.RetryAfterMS
			f.headers = map[string]string{"Retry-After": strconv.FormatInt(ratelimit.RetryAfterHeader(*le.RetryAfterMS), 10)}
		}
		return c.refuse(w, f)
	case err != nil:
		c.o.Log.Error().Err(err).Msg("Could not add a delayed event")
		return c.refuse(w, internalError("db_error"))
	}
	if c.o.Main != nil {
		if err := c.o.Main.DelayedEventAdded(context.WithoutCancel(ctx), next); err != nil {
			// The row stays; main finds it with its next delayed event.
			c.o.Log.Warn().Err(err).Msg("Could not tell main of a delayed event")
			return c.refuse(w, internalError("delayed_notify"))
		}
	}
	body, _ := json.Marshal(map[string]string{"delay_id": row.DelayID})
	writeJSON(w, body)
	c.answers.add(txnKey, string(body), c.o.Now())
	return Outcome{Result: "delayed"}
}

// messageRatelimit is RequestRatelimiter.ratelimit with update: rc_message,
// or the user's override; an appservice's sender, an exempt user and an
// unlimited appservice are not limited.
func (c *Creator) messageRatelimit(ctx context.Context, w http.ResponseWriter, rq *requester) (Outcome, bool) {
	if c.o.AppServices != nil {
		for _, as := range c.o.AppServices.All() {
			if as.Sender == rq.UserID {
				return Outcome{}, false
			}
		}
	}
	rate, burst := c.o.Delayed.RcMessage.PerSecond, c.o.Delayed.RcMessage.Burst
	found, mps, bc, err := c.o.Store.RatelimitOverride(ctx, rq.UserID)
	if err != nil {
		return fallback("db_error"), true
	}
	if found {
		if mps == nil || *mps == 0 {
			return Outcome{}, false
		}
		rate, burst = float64(*mps), 0
		if bc != nil {
			burst = int(*bc)
		}
	}
	// can_do_action: an unlimited appservice, or the authenticated
	// entity's own override.
	if rq.AppService != nil && !rq.AppService.RateLimited {
		return Outcome{}, false
	}
	entity := rq.Entity
	if entity == "" {
		entity = rq.UserID
	}
	if exempt, err := c.o.Store.RatelimitExempt(ctx, entity); err != nil {
		return fallback("db_error"), true
	} else if exempt {
		return Outcome{}, false
	}
	c.msgOnce.Do(func() { c.msgLimiter = ratelimit.New(c.o.Delayed.RcMessage, c.o.Now) })
	if ok, retry := c.msgLimiter.CanDoRate(rq.UserID, rate, burst, true); !ok {
		return c.limitExceeded(ctx, w, c.o.Delayed.RcMessage.Key, retry), true
	}
	return Outcome{}, false
}

// delayID is _generate_delay_id: "syd_" and 20 random letters.
func delayID() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := []byte("syd_")
	for range 20 {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		b = append(b, letters[n.Int64()])
	}
	return string(b)
}

func localpart(userID string) string {
	if i := strings.IndexByte(userID, ':'); i > 0 {
		return userID[1:i]
	}
	return strings.TrimPrefix(userID, "@")
}

func writeJSON(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
