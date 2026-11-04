// Package pushcheck computes the event_push_actions rows Synapse writes for
// an event, and checks them against what Synapse stored: the one port of
// BulkPushRuleEvaluator._action_for_event_by_user
// (bulk_push_rule_evaluator.py), for the events inbound receives and those
// the creator builds alike. Synapse runs the same function for both
// (_run_push_actions_and_persist_event, EventCreationHandler._persist_events);
// what differs is the framing, which CheckOptions carries.
package pushcheck

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdu"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pdustore"
)

// Outcomes of a check.
const (
	OutcomeMatch    = "match"
	OutcomeMismatch = "mismatch"
	OutcomeSkipped  = "skipped"
	OutcomeError    = "error"
	// OutcomeRaise is a Synapse exception: the evaluation itself fails in
	// Synapse, so no rows would be written.
	OutcomeRaise = "raise"
)

// Verdict is the result of a check.
type Verdict struct {
	Outcome string
	Reason  string
	// RoomID, Type and Version describe the event, once it was read.
	RoomID, Type, Version string
	// Rows is how many rows Synapse stored, Notify how many notify.
	Rows, Notify int
	// Diff lists the users whose rows differ.
	Diff []string
}

// CheckOptions are how a caller frames a check.
type CheckOptions struct {
	// MaxStream is the room's stream position read before the event was
	// made (the creator) or as Synapse found the room (inbound's snapshot);
	// 0 for unknown. Who is in the room, and their display names, are read
	// from current state, so the check is skipped when a member event has
	// been persisted since.
	MaxStream int64
	// EventPresent is a snapshot that already held the event: taken too
	// late to be what Synapse saw.
	EventPresent bool
	// Current is the room's current state in the snapshot, nil for none:
	// where an invite target's prior membership is read from.
	Current pdu.StateMap
	// Given, when not nil, are rows to compare instead of computing them:
	// write mode's own.
	Given map[string]Row
	// LoadGroup reads a state group's full state; nil is the store's
	// CachedStateForGroup.
	LoadGroup func(ctx context.Context, group int64) (pdu.StateMap, error)
}

func (v *Verdict) fail(outcome, reason string) Verdict {
	v.Outcome, v.Reason = outcome, reason
	return *v
}

// Evaluator evaluates push rules.
type Evaluator struct {
	st  *pdustore.Store
	cfg *Config
}

// New builds an Evaluator.
func New(st *pdustore.Store, cfg Config) *Evaluator {
	return &Evaluator{st: st, cfg: &cfg}
}

// Row is one user's event_push_actions_staging row.
type Row struct {
	UserID string
	// Actions is the actions column, as Synapse serialises it
	// (_serialize_action): "" for the two default action lists.
	Actions   string
	Notif     bool
	Highlight bool
	Unread    bool
	ThreadID  string
}

// Rows computes the rows Synapse would stage for ev, given the state
// before it.
func (s *Evaluator) Rows(ctx context.Context, ev *pdu.Event, before pdu.StateMap) (map[string]Row, error) {
	return s.RowsAt(ctx, ev, before, nil)
}

// RowsAt is Rows, an invite target's prior membership read from current
// (a snapshot's current state) rather than from before when not nil.
func (s *Evaluator) RowsAt(ctx context.Context, ev *pdu.Event, before, current pdu.StateMap) (map[string]Row, error) {
	prior := before
	if current != nil {
		prior = current
	}
	rows, err := s.pushRows(ctx, ev, before, prior)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Row, len(rows))
	for u, r := range rows {
		out[u] = Row{UserID: u, Actions: r.column, Notif: r.notif, Highlight: r.highlight, Unread: r.unread, ThreadID: r.thread}
	}
	return out, nil
}

// toPushRows turns given rows back into the internal form, for Check.
func toPushRows(rows map[string]Row) map[string]pushRow {
	out := make(map[string]pushRow, len(rows))
	for u, r := range rows {
		var actions any
		if r.Actions == "" {
			actions = jsonNormal(defaultNotifAction)
			if r.Highlight {
				actions = jsonNormal(defaultHighlightAction)
			}
		} else {
			_ = json.Unmarshal([]byte(r.Actions), &actions)
		}
		out[u] = pushRow{actions: actions, column: r.Actions, notif: r.Notif, highlight: r.Highlight, unread: r.Unread, thread: r.ThreadID}
	}
	return out
}

// domain is a user ID's server name.
func domain(id string) string {
	if i := strings.IndexByte(id, ':'); i >= 0 {
		return id[i+1:]
	}
	return id
}
