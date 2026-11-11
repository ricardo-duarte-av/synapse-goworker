package presencewriter

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// kv and orderedJSON are a dict in insertion order, as json_encoder writes
// one.
type kv struct {
	k string
	v any
}

type orderedJSON []kv

func (o orderedJSON) encode() []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, e := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(e.k)
		v, _ := json.Marshal(e.v)
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	if enc, err := pyjson.Encode(b.Bytes()); err == nil {
		return []byte(enc)
	}
	return b.Bytes()
}

// Tracking reports whether this process writes presence and tracks it
// (presence enabled, not "untracked").
func (a *App) Tracking() bool { return a.tr != nil && a.tr.track }

// device is the requester's device as Synapse holds it: None for none.
func devOf(deviceID string) devKey {
	if deviceID == "" {
		return devKey{}
	}
	return devKey{id: deviceID, isSet: true}
}

// UserSyncing is the sync servlet's presence half on the writer's process:
// rc_presence.per_user when ratelimited (a refused sync leaves presence
// alone; /events has no limiter), then PresenceHandler.user_syncing. The function it returns ends the sync,
// and is never nil.
//
// An error is logged and the sync goes on, as gosync's relay does: the
// cost of failing it is a client that stops working.
func (a *App) UserSyncing(ctx context.Context, v clientauth.Verdict, setPresence string, ratelimited bool) func() {
	t := a.tr
	if t == nil || !t.track || setPresence == stateOffline {
		return func() {}
	}
	if ratelimited {
		if ok, err := a.allowedSync(ctx, v); err != nil || !ok {
			if err != nil {
				a.Log.Error().Err(err).Msg("presence: ratelimit_override")
			}
			return func() {}
		}
	}
	k := userDevice{v.UserID, devOf(v.DeviceID)}
	sources.WithLabelValues("sync_start").Inc()
	t.op.Lock()
	t.mu.Lock()
	t.syncs[k]++
	t.mu.Unlock()
	err := t.setState(ctx, v.UserID, k.dev, setPresence, nil, false, true)
	t.op.Unlock()
	if err != nil {
		failures.WithLabelValues("persist").Inc()
		a.Log.Error().Err(err).Str("user_id", v.UserID).Msg("presence: a sync's state")
	}
	return func() {
		// run_in_background: the response is not held for it.
		go func() {
			sources.WithLabelValues("sync_end").Inc()
			ctx := context.WithoutCancel(ctx)
			t.op.Lock()
			err := t.syncEnd(ctx, k)
			t.op.Unlock()
			if err != nil {
				failures.WithLabelValues("persist").Inc()
				a.Log.Error().Err(err).Str("user_id", v.UserID).Msg("Error updating presence after sync")
			}
		}()
	}
}

// allowedSync is the sync servlet's can_do_action. The bucket is asked
// first and ratelimit_override only when it refuses: an exempt user is
// then let by, and only those requests read the table.
func (a *App) allowedSync(ctx context.Context, v clientauth.Verdict) (bool, error) {
	if v.AppServiceID != "" && v.AppServiceUnlimited {
		return true, nil
	}
	if ok, _ := a.syncLimiter.CanDo(v.UserID, true); ok {
		return true, nil
	}
	entity := v.Entity
	if entity == "" {
		entity = v.UserID
	}
	return a.Store.RatelimitExempt(ctx, entity)
}

// Bump is bump_presence_active_time on the writer, for this process's
// typing, receipts and read markers. Nothing while presence is not tracked.
func (a *App) Bump(ctx context.Context, userID, deviceID string) error {
	t := a.tr
	if t == nil || !t.track {
		return nil
	}
	sources.WithLabelValues("bump").Inc()
	t.op.Lock()
	defer t.op.Unlock()
	return t.bump(ctx, userID, devOf(deviceID))
}

// presenceEDU is incoming_presence, under op.
func (a *App) presenceEDU(ctx context.Context, origin string, content json.RawMessage) error {
	t := a.tr
	t.op.Lock()
	defer t.op.Unlock()
	err := t.incoming(ctx, origin, content)
	if err != nil {
		failures.WithLabelValues("edu").Inc()
	}
	return err
}
