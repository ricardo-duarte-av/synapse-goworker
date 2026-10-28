// Package pushcalc computes the event_push_actions rows Synapse writes for an
// event, and checks them against what Synapse stored.
//
// The evaluation (push.go, invite.go) is goinbound's port of
// BulkPushRuleEvaluator._action_for_event_by_user, checked there against
// every PDU the homeserver received. Locally created events go through the
// same function in Synapse (EventCreationHandler._persist_events), so the
// port carries over unchanged; only its wiring here is new.
package pushcalc

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
	// Rows is how many rows Synapse stored.
	Rows int
	// Diff lists the users whose rows differ.
	Diff []string
}

func (v *Verdict) fail(outcome, reason string) Verdict {
	v.Outcome, v.Reason = outcome, reason
	return *v
}

// Calculator evaluates push rules.
type Calculator struct {
	st  *pdustore.Store
	cfg *Config
}

// New builds a Calculator.
func New(st *pdustore.Store, cfg Config) *Calculator {
	return &Calculator{st: st, cfg: &cfg}
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
func (s *Calculator) Rows(ctx context.Context, ev *pdu.Event, before pdu.StateMap) (map[string]Row, error) {
	rows, err := s.pushRows(ctx, ev, before)
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
