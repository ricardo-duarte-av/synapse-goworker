package eventfilter

import (
	"context"
	"encoding/json"

	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/clientevent"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"
)

// Aggregation is the bundle attached to one event's `unsigned.m.relations`.
type Aggregation struct {
	thread     *threadAggregation
	replaceID  string
	references []string
}

type threadAggregation struct {
	count         int
	latestEventID string
	participated  bool
}

// BundleAggregations works out the relations to bundle into a timeline.
//
// Synapse only does this for a `limited` timeline (an initial sync always is):
// when a client receives the whole history it can aggregate for itself, but
// when it receives a window it cannot see the replies that fall outside it.
//
// A port of RelationsHandler.get_bundled_aggregations. Two rules are easy to
// miss:
//
//   - An event that is ITSELF an edit or an annotation gets no bundle. A thread
//     reply does, which is why the relation type has to be known rather than
//     just its presence.
//   - Threads are only computed for events that are not themselves a relation,
//     so a reply inside a thread does not sprout a nested thread summary.
func BundleAggregations(ctx context.Context, db *store.Store, userID string,
	messages []store.TimelineEvent, ignored map[string]bool) (map[string]Aggregation, []string, error) {

	candidates := make([]string, 0, len(messages))
	for _, ev := range messages {
		if ev.IsState {
			continue
		}
		candidates = append(candidates, ev.EventID)
	}
	if len(candidates) == 0 {
		return nil, nil, nil
	}

	relTypes, err := db.RelationTypesOf(ctx, candidates)
	if err != nil {
		return nil, nil, err
	}

	// Events that are themselves an edit or annotation carry no aggregations.
	bundled := make([]string, 0, len(candidates))
	threadRoots := make([]string, 0, len(candidates))
	for _, id := range candidates {
		switch relTypes[id] {
		case "m.replace", "m.annotation":
			continue
		case "":
			// Not a relation at all, so it may root a thread.
			threadRoots = append(threadRoots, id)
		}
		bundled = append(bundled, id)
	}
	if len(bundled) == 0 {
		return nil, nil, nil
	}

	out := map[string]Aggregation{}

	summaries, err := db.ThreadSummaries(ctx, threadRoots)
	if err != nil {
		return nil, nil, err
	}

	// The latest reply in a thread is serialised inside the bundle, so it has
	// to be fetched, and it gets aggregations of its own.
	var extra []string
	if len(summaries) > 0 {
		roots := make([]string, 0, len(summaries))
		for id := range summaries {
			roots = append(roots, id)
		}
		participated, err := db.ThreadsParticipated(ctx, roots, userID)
		if err != nil {
			return nil, nil, err
		}

		var ignoredCounts map[string]int
		if len(ignored) > 0 {
			senders := make([]string, 0, len(ignored))
			for u := range ignored {
				senders = append(senders, u)
			}
			ignoredCounts, err = db.ThreadRepliesBySender(ctx, roots, senders)
			if err != nil {
				return nil, nil, err
			}
		}

		bySender := map[string]string{}
		for _, ev := range messages {
			bySender[ev.EventID] = ev.Sender
		}

		for id, summary := range summaries {
			count := summary.Count - ignoredCounts[id]
			if count <= 0 {
				continue
			}
			agg := out[id]
			agg.thread = &threadAggregation{
				count:         count,
				latestEventID: summary.LatestEventID,
				// The root's own sender always counts as a participant, which
				// saves a query for the commonest case.
				participated: bySender[id] == userID || participated[id],
			}
			out[id] = agg
			extra = append(extra, summary.LatestEventID)
		}
	}

	// References and edits are computed for the bundled events AND for any
	// latest-thread-event pulled in above, so a thread's latest reply shows its
	// own edits.
	withExtra := append(append([]string(nil), bundled...), extra...)

	edits, err := db.ApplicableEdits(ctx, withExtra)
	if err != nil {
		return nil, nil, err
	}
	for id, edit := range edits {
		agg := out[id]
		agg.replaceID = edit
		out[id] = agg
	}

	refs, err := db.References(ctx, withExtra, ignored)
	if err != nil {
		return nil, nil, err
	}
	for id, list := range refs {
		agg := out[id]
		agg.references = list
		out[id] = agg
	}

	if len(out) == 0 {
		return nil, nil, nil
	}
	return out, extra, nil
}

// ReplaceID is the event ID of the latest edit, or empty if there is none.
//
// Exported alone rather than the whole struct: the only thing a caller outside
// this package needs is which extra events to load, and the rest of the bundle
// is assembled by AttachAggregations.
func (a Aggregation) ReplaceID() string { return a.replaceID }

// AttachAggregations writes `unsigned.m.relations` onto a serialised event.
//
// The nested events -- a thread's latest reply and an edit -- are full client
// events, serialised with the same config, so this runs after the outer event
// has been rendered rather than inside the serialiser.
func AttachAggregations(body json.RawMessage, agg Aggregation, extra map[string]Aggregation,
	events map[string]store.StateEvent, timeNow int64, cfg clientevent.Config) (json.RawMessage, error) {

	relations := map[string]any{}

	if len(agg.references) > 0 {
		chunk := make([]map[string]string, 0, len(agg.references))
		for _, id := range agg.references {
			chunk = append(chunk, map[string]string{"event_id": id})
		}
		relations["m.reference"] = map[string]any{"chunk": chunk}
	}

	if agg.replaceID != "" {
		if ev, ok := events[agg.replaceID]; ok {
			serialised, err := clientevent.Serialize(ev.Stored, timeNow, cfg)
			if err != nil {
				return nil, err
			}
			relations["m.replace"] = json.RawMessage(serialised)
		}
	}

	if agg.thread != nil {
		if ev, ok := events[agg.thread.latestEventID]; ok {
			serialised, err := clientevent.Serialize(ev.Stored, timeNow, cfg)
			if err != nil {
				return nil, err
			}
			// The latest reply carries its own aggregations, one level deep.
			if nested, ok := extra[agg.thread.latestEventID]; ok {
				serialised, err = AttachAggregations(serialised, nested, nil, events, timeNow, cfg)
				if err != nil {
					return nil, err
				}
			}
			relations["m.thread"] = map[string]any{
				"latest_event":              json.RawMessage(serialised),
				"count":                     agg.thread.count,
				"current_user_participated": agg.thread.participated,
			}
		}
	}

	if len(relations) == 0 {
		return body, nil
	}
	encoded, err := json.Marshal(relations)
	if err != nil {
		return nil, err
	}
	return sjson.SetRawBytes(body, `unsigned.m\.relations`, encoded)
}
