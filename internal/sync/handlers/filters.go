package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/filter"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/matrixerr"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// resolveFilter turns the `filter` query parameter into a filter collection.
//
// Two forms, distinguished by a leading brace, exactly as Synapse does it:
// a JSON document inline in the query string, or the ID of a filter the client
// uploaded earlier with PUT /user/{userId}/filter.
//
// Only the inline form is capped by filter_timeline_limit. That asymmetry is
// Synapse's, not ours (rest/client/sync.py calls set_timeline_upper_limit on
// one branch and not the other), and it means a client can exceed the cap by
// uploading the filter first -- which is worth knowing before treating the cap
// as a limit on anything.
func resolveFilter(ctx context.Context, d Deps, userID, param string) (*filter.Collection, *matrixerr.Error) {
	if param == "" {
		return filter.DefaultWithFeatures(d.filterFeatures()), nil
	}

	if strings.HasPrefix(param, "{") {
		if !gjson.Valid(param) {
			return nil, &matrixerr.Error{ErrCode: matrixerr.CodeNotJSON, Error: "Invalid filter JSON"}
		}
		doc := json.RawMessage(param)
		capped, err := capTimelineLimit(doc, d.FilterTimelineLimit)
		if err != nil {
			return nil, &matrixerr.Error{ErrCode: matrixerr.CodeUnknown, Error: err.Error()}
		}
		// Inline filters ARE validated, as Synapse's check_valid_filter does
		// on this branch and only this branch.
		c, err := filter.NewInline(capped, d.filterFeatures())
		if err != nil {
			return nil, &matrixerr.Error{ErrCode: matrixerr.CodeUnknown, Error: err.Error()}
		}
		return c, nil
	}

	// `filter_id` is a BIGINT column, so a non-numeric ID is a bad request
	// rather than a lookup that happens to miss.
	id, convErr := strconv.ParseInt(param, 10, 64)
	if convErr != nil {
		return nil, &matrixerr.Error{ErrCode: matrixerr.CodeInvalidParam, Error: "Invalid filter ID"}
	}
	raw, err := d.Store.UserFilter(ctx, userID, id)
	if errors.Is(err, store.ErrNoSuchFilter) {
		return nil, &matrixerr.Error{ErrCode: matrixerr.CodeInvalidParam, Error: "No such filter"}
	}
	if err != nil {
		return nil, internalError(d, "user filter", err)
	}
	// NOT validated: Synapse's get_user_filter hands the stored JSON straight
	// to FilterCollection with no schema check. Whatever is in the table is
	// what the client gets, and a filter we refuse to read is a client that
	// cannot sync at all.
	c, err := filter.NewWithFeatures(raw, d.filterFeatures())
	if err != nil {
		// Only a structurally broken document reaches here -- not JSON, or a
		// field of the wrong type. That is our bug or a corrupt row, not the
		// client's fault.
		return nil, internalError(d, "parse stored filter", err)
	}
	return c, nil
}

// capTimelineLimit applies Synapse's set_timeline_upper_limit.
//
// It lowers an existing `room.timeline.limit`; it never inserts one. A filter
// that says nothing about the limit keeps the default of 10, not the cap.
func capTimelineLimit(doc json.RawMessage, cap int) (json.RawMessage, error) {
	if cap < 0 {
		return doc, nil
	}
	cur := gjson.GetBytes(doc, "room.timeline.limit")
	if !cur.Exists() || cur.Type != gjson.Number || cur.Int() <= int64(cap) {
		return doc, nil
	}
	return sjson.SetBytes(doc, "room.timeline.limit", cap)
}

func (d Deps) filterFeatures() filter.Features {
	return filter.Features{
		MSC3773: d.MSC3773Enabled,
		MSC3874: d.MSC3874Enabled,
	}
}

// filterQueryParam reads the `filter` parameter, which /sync takes and the
// legacy endpoints do not.
func filterQueryParam(r *http.Request) string {
	return r.URL.Query().Get("filter")
}

// The adapters below present our stored rows as the events a filter inspects.
//
// Filters run on the PDU, before serialisation, because that is what Synapse
// filters: an EventBase, not a client event. The difference matters for
// `contains_url` and labels, which live in `content` and survive serialisation,
// and for `room_id`, which /sync strips from the rendering but the filter still
// sees.

func timelineFilterEvent(roomID string, ev store.TimelineEvent) filter.Event {
	return filter.Event{
		Type:    ev.Type,
		Sender:  ev.Sender,
		RoomID:  roomID,
		Content: gjson.GetBytes(ev.JSON, "content"),
		EventID: ev.EventID,
		IsPDU:   true,
	}
}

func stateFilterEvent(roomID string, ev store.StateEvent) filter.Event {
	return filter.Event{
		Type:    ev.Type,
		Sender:  ev.Sender,
		RoomID:  roomID,
		Content: gjson.GetBytes(ev.JSON, "content"),
		EventID: ev.EventID,
		IsPDU:   true,
	}
}

// accountDataFilterEvent presents an account data entry the way Synapse does:
// a bare `{"type": ..., "content": ...}` dict with no sender and no room, so a
// filter naming senders excludes all of it.
func accountDataFilterEvent(roomID string, e store.AccountDataEntry) filter.Event {
	return filter.Event{
		Type:    e.Type,
		RoomID:  roomID,
		Content: gjson.ParseBytes(e.Content),
	}
}

// ephemeralFilterEvent reads back an already-rendered ephemeral event.
//
// Receipts and typing are synthesised rather than stored, so there is no PDU to
// consult; Synapse filters the same dict it is about to send. Note the room_id
// is still present at this point -- it is stripped afterwards, per room.
func ephemeralFilterEvent(roomID string, body json.RawMessage) filter.Event {
	return filter.Event{
		Type:    gjson.GetBytes(body, "type").String(),
		RoomID:  roomID,
		Content: gjson.GetBytes(body, "content"),
	}
}

func presenceFilterEvent(userID string) filter.Event {
	return filter.Event{Type: "m.presence", Sender: userID, IsPresence: true}
}

// eventFormat maps a filter's `event_format` onto our serialiser.
//
// `federation` is not a richer client format: it is the stored PDU with no
// transform at all, which is why it keeps room_id even in /sync where every
// other event has it stripped.
func eventFormat(c *filter.Collection, clientFormat clientevent.Format) clientevent.Format {
	if c.EventFormat == "federation" {
		return clientevent.FormatRaw
	}
	return clientFormat
}

// applyRelationFilter drops events that lack a relation the filter demands.
//
// `related_by_senders` and `related_by_rel_types` cannot be answered from the
// event itself -- they ask about OTHER events pointing at it -- so this is the
// one filter clause that needs the database.
func applyRelationFilter(ctx context.Context, d Deps, f *filter.Filter,
	events []store.TimelineEvent) ([]store.TimelineEvent, error) {

	if !f.HasRelationConstraint() || len(events) == 0 {
		return events, nil
	}
	ids := make([]string, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.EventID)
	}
	keep, err := d.Store.EventsWithRelations(ctx, ids, f.RelatedBySenders, f.RelatedByRelTypes)
	if err != nil {
		return nil, err
	}
	out := events[:0:0]
	for _, ev := range events {
		if keep[ev.EventID] {
			out = append(out, ev)
		}
	}
	return out, nil
}
