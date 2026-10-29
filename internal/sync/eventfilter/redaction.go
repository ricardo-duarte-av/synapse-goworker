package eventfilter

import (
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// AttachRedaction marks an event as redacted and renders the redaction event
// that explains it.
//
// `redacted_because` is a fully serialised client event, not an id, so it goes
// through the same serialiser -- which is why this cannot live in the store.
//
// Shared by both sync endpoints for the same reason the visibility filter is:
// sliding sync originally skipped this and served the ORIGINAL CONTENT of a
// redacted event. That is not a cosmetic difference. It was invisible until a
// comparison happened to include a room with a redaction in its recent
// timeline.
func AttachRedaction(stored *clientevent.Stored, redactions map[string]store.Redaction,
	timeNow int64, cfg clientevent.Config) error {

	r, ok := redactions[stored.EventID]
	if !ok {
		return nil
	}
	eventID, roomID, eventType, body, meta, receivedTS := r.RedactionEvent()
	stored.RedactedBy = r.EventID
	if len(body) == 0 {
		// Redacted, by an event the serialiser could not load: no
		// redacted_because, as Synapse has none to give.
		return nil
	}
	because, err := clientevent.Serialize(clientevent.Stored{
		EventID: eventID, RoomID: roomID, Type: eventType,
		JSON: body, InternalMetadata: meta, ReceivedTS: receivedTS, RoomVersion: stored.RoomVersion,
	}, timeNow, cfg)
	if err != nil {
		return err
	}
	stored.RedactedBy = eventID
	stored.RedactedBecause = because
	return nil
}
