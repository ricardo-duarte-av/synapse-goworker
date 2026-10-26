package store

import (
	"context"
	"fmt"
	"strings"
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
// Mirrors _maybe_redact_event_row:
//
//   - redactions of m.room.create are ignored outright;
//   - a rejected redaction does not count;
//   - the redaction must be in the same room as its target;
//   - from room version 3 a redaction whose target was unknown at the time is
//     rechecked on read, and only counts if its sender shares a domain with the
//     target's sender.
func (s *Store) Redactions(ctx context.Context, eventIDs []string) (map[string]Redaction, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT r.redacts, r.event_id, re.sender, target.sender, re.room_id, target.room_id,
		       COALESCE(rej.internal_metadata::jsonb ->> 'recheck_redaction', 'false'),
		       rej.json, rej.internal_metadata, re.type
		  FROM redactions r
		  JOIN events re ON re.event_id = r.event_id
		  JOIN events target ON target.event_id = r.redacts
		  JOIN event_json rej ON rej.event_id = r.event_id
		 WHERE r.redacts = ANY($1)
		   AND re.rejection_reason IS NULL
		   AND target.type <> 'm.room.create'
		 ORDER BY re.stream_ordering`
	rows, err := s.query(ctx, "Redactions", q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: redactions: %w", err)
	}
	defer rows.Close()

	out := map[string]Redaction{}
	for rows.Next() {
		var (
			redacts, redactionID          string
			redactionSender, targetSender string
			redactionRoom, targetRoom     string
			recheck                       string
			body, meta                    []byte
			redactionType                 string
		)
		if err := rows.Scan(&redacts, &redactionID, &redactionSender, &targetSender,
			&redactionRoom, &targetRoom, &recheck, &body, &meta, &redactionType); err != nil {
			return nil, fmt.Errorf("store: redactions: %w", err)
		}
		if redactionRoom != targetRoom {
			// A redaction can only redact within its own room.
			continue
		}
		if recheck == "true" && domainOf(redactionSender) != domainOf(targetSender) {
			// Room version 3+ rechecks a redaction that arrived before its
			// target: only the target sender's own server may redact it.
			continue
		}
		if _, already := out[redacts]; already {
			// The first redaction wins; ordering by stream_ordering makes that
			// the earliest one, which is what Synapse's iteration order gives.
			continue
		}
		out[redacts] = Redaction{
			EventID: redactionID,
			Event: clientEventSource{
				EventID: redactionID, RoomID: redactionRoom, Type: redactionType,
				JSON: body, InternalMetadata: meta,
			},
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: redactions: %w", err)
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
