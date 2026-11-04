package pduverify

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pushcheck"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// The push check is platform/pushcheck's, the creator's too; this is its
// framing for received events: the snapshot of the room as Synapse found it.

// PushConfig is what push evaluation takes from homeserver.yaml.
type PushConfig = pushcheck.Config

// PushConfigFor reads it, with Synapse 1.164's defaults.
func PushConfigFor(hs *synapsecfg.Config) (PushConfig, error) { return pushcheck.ConfigFor(hs) }

// EnablePush turns on the push check.
func (s *Verifier) EnablePush(c PushConfig) { s.push = pushcheck.New(s.st, c) }

// Push reproduces BulkPushRuleEvaluator._action_for_event_by_user
// (bulk_push_rule_evaluator.py) for a received event, and compares the rows
// with those Synapse stored in event_push_actions.
//
// before is the state before the event (Replay computes it); nil falls back
// to the event's own state group, which is the state before a non-state
// event. snap holds the room as Synapse found it, when the caller has one;
// nil in the sweep.
func (s *Verifier) Push(ctx context.Context, id string, before pdu.StateMap, snap *pdustore.Snapshot) *Result {
	return s.pushCheck(ctx, id, before, snap, nil)
}

// pushCheck is Push, comparing given rows when not nil instead of computing
// them: the decide check passes write mode's.
func (s *Verifier) pushCheck(ctx context.Context, id string, before pdu.StateMap, snap *pdustore.Snapshot,
	given map[string]pushcheck.Row) *Result {

	res := &Result{EventID: id}
	if s.push == nil {
		return res.fail(OutcomeSkipped, "push not configured")
	}
	o := pushcheck.CheckOptions{
		Given: given,
		LoadGroup: func(ctx context.Context, g int64) (pdu.StateMap, error) {
			return s.groups.get(ctx, s.st, g)
		},
	}
	if snap != nil {
		o.MaxStream, o.EventPresent, o.Current = snap.MaxStream, snap.EventPresent, snapshotCurrent(snap)
	}
	v := s.push.Check(ctx, id, before, o)
	res.Outcome, res.Reason = v.Outcome, v.Reason
	res.RoomID, res.Type, res.Version = v.RoomID, v.Type, v.Version
	res.PushRows, res.PushNotify, res.PushDiff = v.Rows, v.Notify, v.Diff
	if res.Outcome == OutcomeMismatch {
		res.Reason = "push"
	}
	return res
}

// pushRows is the rows Synapse would stage, an invite target's prior
// membership read from the snapshot when there is one.
func (s *Verifier) pushRows(ctx context.Context, ev *pdu.Event, before pdu.StateMap, snap *pdustore.Snapshot) (map[string]pushcheck.Row, error) {
	var current pdu.StateMap
	if snap != nil {
		current = snapshotCurrent(snap)
	}
	return s.push.RowsAt(ctx, ev, before, current)
}

// snapshotCurrent is a snapshot's current state, never nil: a snapshot
// without one is read as empty, not as the state before the event.
func snapshotCurrent(snap *pdustore.Snapshot) pdu.StateMap {
	if snap.Current == nil {
		return pdu.StateMap{}
	}
	return snap.Current
}
