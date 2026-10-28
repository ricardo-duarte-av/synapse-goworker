package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

// Redaction describes the redaction event that applies to an event.
type Redaction struct {
	// EventID is the redaction event.
	EventID string
	// Event is the redaction event itself, ready to be serialised as
	// `unsigned.redacted_because`.
	Event clientEventSource
}

// clientEventSource is the little of a redaction event the serialiser needs.
type clientEventSource struct {
	EventID          string
	RoomID           string
	Type             string
	JSON             []byte
	InternalMetadata []byte
}

// Redactions finds which of the given events have been redacted.
//
// Redaction is applied on READ, not only when the redaction arrives: a redacted
// event keeps its original body in event_json until a background job censors
// it, and Synapse censors in place only after redaction_retention_period. So
// this query is not an optimisation -- skipping it publishes content that was
// redacted, possibly years ago.
//
// _get_events_from_db and _maybe_redact_event_row (events_worker.py), in
// their order:
//
//   - redactions of m.room.create are ignored outright;
//   - the redactions rows of each target, in the order the unordered
//     SELECT returns them (physical order: ORDER BY ctid), split by their
//     recheck column. The first confirmed one wins with no further check;
//   - else the unconfirmed ones, then MSC4293's room_ban_redactions for the
//     target's room and sender (unless the target came at or after the
//     redaction's end): the first that exists, is not rejected, is in the
//     target's room and, if its internal metadata asks for a recheck,
//     shares a domain with the target's sender.
//
// Ordering by stream_ordering instead picked a different redaction than
// Synapse for an event redacted twice; leaving room_ban_redactions out
// served events Synapse shows redacted (9,808 rows on aguiarvieira.pt).
func (s *Store) Redactions(ctx context.Context, eventIDs []string) (map[string]Redaction, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}

	type target struct {
		roomID, sender string
		stream         int64
		confirmed      []string
		unconfirmed    []string
	}
	targets := map[string]*target{}
	rows, err := s.query(ctx, "RedactionTargets", `
		SELECT event_id, room_id, COALESCE(sender, ''), COALESCE(stream_ordering, 0)
		  FROM events WHERE event_id = ANY($1) AND type <> 'm.room.create'`, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: redaction targets: %w", err)
	}
	for rows.Next() {
		var id string
		t := &target{}
		if err := rows.Scan(&id, &t.roomID, &t.sender, &t.stream); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: redaction targets: %w", err)
		}
		targets[id] = t
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: redaction targets: %w", err)
	}
	if len(targets) == 0 {
		return nil, nil
	}

	rows, err = s.query(ctx, "Redactions", `
		SELECT event_id, redacts, recheck FROM redactions
		 WHERE redacts = ANY($1) ORDER BY ctid`, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: redactions: %w", err)
	}
	for rows.Next() {
		var redacter, redacted string
		var recheck bool
		if err := rows.Scan(&redacter, &redacted, &recheck); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: redactions: %w", err)
		}
		if t := targets[redacted]; t != nil {
			if recheck {
				t.unconfirmed = append(t.unconfirmed, redacter)
			} else {
				t.confirmed = append(t.confirmed, redacter)
			}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: redactions: %w", err)
	}

	pairRooms, pairUsers := make([]string, 0, len(targets)), make([]string, 0, len(targets))
	seen := map[[2]string]bool{}
	for _, t := range targets {
		if k := [2]string{t.roomID, t.sender}; !seen[k] {
			seen[k] = true
			pairRooms, pairUsers = append(pairRooms, t.roomID), append(pairUsers, t.sender)
		}
	}
	rows, err = s.query(ctx, "RoomBanRedactions", `
		SELECT room_id, user_id, redacting_event_id, redact_end_ordering FROM room_ban_redactions
		 WHERE (room_id, user_id) IN (SELECT * FROM unnest($1::text[], $2::text[]))
		 ORDER BY ctid`, pairRooms, pairUsers)
	if err != nil {
		return nil, fmt.Errorf("store: room ban redactions: %w", err)
	}
	for rows.Next() {
		var room, user, redacter string
		var end *int64
		if err := rows.Scan(&room, &user, &redacter, &end); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: room ban redactions: %w", err)
		}
		for _, t := range targets {
			if t.roomID != room || t.sender != user {
				continue
			}
			if end != nil && *end != 0 && t.stream >= *end {
				continue
			}
			t.unconfirmed = append(t.unconfirmed, redacter)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: room ban redactions: %w", err)
	}

	var candidates []string
	for _, t := range targets {
		candidates = append(candidates, t.confirmed...)
		candidates = append(candidates, t.unconfirmed...)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	type redactionRow struct {
		src      clientEventSource
		sender   string
		rejected bool
		recheck  bool
	}
	found := map[string]redactionRow{}
	rows, err = s.query(ctx, "RedactionEvents", `
		SELECT e.event_id, e.room_id, COALESCE(e.sender, ''), e.type, ej.json, ej.internal_metadata,
		       (e.rejection_reason IS NOT NULL OR rj.event_id IS NOT NULL)
		  FROM events e JOIN event_json ej USING (event_id)
		  LEFT JOIN rejections rj USING (event_id)
		 WHERE e.event_id = ANY($1)`, candidates)
	if err != nil {
		return nil, fmt.Errorf("store: redaction events: %w", err)
	}
	for rows.Next() {
		var r redactionRow
		if err := rows.Scan(&r.src.EventID, &r.src.RoomID, &r.sender, &r.src.Type,
			&r.src.JSON, &r.src.InternalMetadata, &r.rejected); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: redaction events: %w", err)
		}
		r.recheck = gjson.GetBytes(r.src.InternalMetadata, "recheck_redaction").Bool()
		found[r.src.EventID] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: redaction events: %w", err)
	}

	out := map[string]Redaction{}
	for id, t := range targets {
		if len(t.confirmed) > 0 {
			// Redacted by it whatever it is; redacted_because only if
			// the serialiser can load it (get_event, not rejected).
			r := Redaction{EventID: t.confirmed[0]}
			if e, ok := found[t.confirmed[0]]; ok && !e.rejected {
				r.Event = e.src
			}
			out[id] = r
			continue
		}
		for _, cand := range t.unconfirmed {
			e, ok := found[cand]
			if !ok || e.rejected || e.src.RoomID != t.roomID {
				continue
			}
			if e.recheck && domainOf(e.sender) != domainOf(t.sender) {
				continue
			}
			out[id] = Redaction{EventID: cand, Event: e.src}
			break
		}
	}
	return out, nil
}

// RedactionEvent exposes the redaction event for serialisation.
func (r Redaction) RedactionEvent() (eventID, roomID, eventType string, body, meta []byte) {
	return r.Event.EventID, r.Event.RoomID, r.Event.Type, r.Event.JSON, r.Event.InternalMetadata
}

func domainOf(userID string) string {
	if i := strings.IndexByte(userID, ':'); i >= 0 {
		return userID[i+1:]
	}
	return ""
}
