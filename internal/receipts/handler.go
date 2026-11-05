package receipts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/userid"
)

// Receipt types (ReceiptTypes).
const (
	TypeRead        = "m.read"
	TypeReadPrivate = "m.read.private"
	TypeFullyRead   = "m.fully_read"
)

// ErrNotWriter is a receipt for a process that does not write receipts.
var ErrNotWriter = errors.New("receipts: this process is not the receipts writer")

// errGraphToLinear is _graph_to_linear raising: its SQL selects event_id
// WHERE ... with no FROM, so in Synapse a receipt naming more than one
// event fails, every time, before a stream ID is taken. Reproduced, not
// fixed: storing such a receipt would be a receipt Synapse never stores.
var errGraphToLinear = errors.New(`receipts: _graph_to_linear: column "event_id" does not exist`)

// insert is insert_receipt; isNew is its result being a position (not
// None): a receipt to notify and federate.
func (a *App) insert(ctx context.Context, r Receipt, origin string) (bool, error) {
	if !a.writing {
		return false, ErrNotWriter
	}
	if len(r.EventIDs) == 0 {
		return false, nil
	}
	if len(r.EventIDs) > 1 {
		stored.WithLabelValues(origin, "error").Inc()
		return false, errGraphToLinear
	}
	r.EventID = r.EventIDs[0]

	id, err := a.ids.Next(ctx)
	if err != nil {
		stored.WithLabelValues(origin, "error").Inc()
		return false, err
	}
	_, isNew, err := a.Store.InsertLinearized(ctx, r, id.Value, a.Instance)
	if ferr := id.Finish(ctx, err == nil); err == nil && ferr != nil {
		err = ferr
	}
	if err != nil {
		stored.WithLabelValues(origin, "error").Inc()
		return false, err
	}
	if !isNew {
		stored.WithLabelValues(origin, "old").Inc()
		return false, nil
	}
	if err := a.Store.InsertGraph(ctx, r); err != nil {
		stored.WithLabelValues(origin, "error").Inc()
		return false, err
	}
	stored.WithLabelValues(origin, "new").Inc()
	if a.OnReceipt != nil {
		row := Row{RoomID: r.RoomID, ReceiptType: r.ReceiptType, UserID: r.UserID,
			EventID: r.EventID, Data: json.RawMessage(r.Data)}
		if r.ThreadID != nil {
			row.ThreadID = *r.ThreadID
		}
		a.OnReceipt(row)
	}
	return true, nil
}

// errForbiddenEvent is get_event's AuthError for an event the user may not
// see.
var errForbiddenEvent = &clientError{status: 403, body: map[string]any{
	"errcode": "M_FORBIDDEN", "error": "You don't have permission to access that event."}}

// clientReceipt is received_client_receipt.
func (a *App) clientReceipt(ctx context.Context, roomID, receiptType, userID, eventID string, threadID *string) (string, error) {
	access, err := a.Events.Access(ctx, userID, roomID, eventID)
	if err != nil {
		return "", err
	}
	switch access {
	case Missing:
		// get_event returned None: nothing is stored, and the answer is 200.
		return "event_missing", nil
	case Forbidden:
		return "", errForbiddenEvent
	}
	data := `{"ts":` + strconv.FormatInt(time.Now().UnixMilli(), 10) + `}`
	isNew, err := a.insert(ctx, Receipt{
		RoomID: roomID, ReceiptType: receiptType, UserID: userID,
		EventIDs: []string{eventID}, ThreadID: threadID, Data: data,
	}, "local")
	if err != nil {
		return "", err
	}
	if !isNew {
		return "old", nil
	}
	return "stored", nil
}

// readMarker is received_client_read_marker: the read marker moves only
// forward, unless allowBackward, and is room account data on its writer.
func (a *App) readMarker(ctx context.Context, roomID, userID, eventID string, allowBackward bool) error {
	lock, _ := a.markerLocks.LoadOrStore(roomID+"\x00"+userID, &sync.Mutex{})
	mu := lock.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	existing, err := a.Store.RoomAccountData(ctx, userID, roomID, TypeFullyRead)
	if err != nil {
		markers.WithLabelValues("error").Inc()
		return err
	}
	// Also proves we know the event: a SynapseError(404) otherwise.
	ordering, err := a.Store.EventOrdering(ctx, eventID, roomID)
	if err != nil {
		markers.WithLabelValues("error").Inc()
		return err
	}
	shouldUpdate := true
	if existing != nil && truthyObject(existing) && !allowBackward {
		old := gjson.GetBytes(existing, "event_id")
		if !old.Exists() {
			// existing_read_marker["event_id"]: KeyError.
			markers.WithLabelValues("error").Inc()
			return fmt.Errorf("receipts: m.fully_read of %s in %s has no event_id", userID, roomID)
		}
		if old.Type != gjson.String {
			// A non-string reaches the events query, which raises.
			markers.WithLabelValues("error").Inc()
			return fmt.Errorf("receipts: m.fully_read of %s in %s: event_id is not a string", userID, roomID)
		}
		oldOrdering, err := a.Store.EventOrdering(ctx, old.Str, roomID)
		var missing *ErrNoEvent
		switch {
		case errors.As(err, &missing):
			// The old event is gone (retention): the new one is ahead.
		case err != nil:
			markers.WithLabelValues("error").Inc()
			return err
		default:
			shouldUpdate = ordering.After(oldOrdering)
		}
	}
	if !shouldUpdate {
		markers.WithLabelValues("behind").Inc()
		return nil
	}
	if a.AccountData == nil {
		markers.WithLabelValues("error").Inc()
		return errors.New("receipts: no account-data writer to store m.fully_read on")
	}
	content, _ := json.Marshal(map[string]string{"event_id": eventID})
	if err := a.AccountData.AddRoomAccountData(ctx, userID, roomID, TypeFullyRead, content); err != nil {
		markers.WithLabelValues("error").Inc()
		return err
	}
	markers.WithLabelValues("moved").Inc()
	return nil
}

// truthyObject is a stored account data value's truth: {} is false.
func truthyObject(raw json.RawMessage) bool {
	v := gjson.ParseBytes(raw)
	if v.IsObject() {
		n := 0
		v.ForEach(func(_, _ gjson.Result) bool { n++; return false })
		return n > 0
	}
	return v.Exists() && v.Type != gjson.Null
}

// ReceiveEDU is _received_remote_receipt then _handle_new_receipts: each
// room we are in, and the origin is in, and each of its users that is the
// origin's and compliant, stored in turn. An error is what Synapse raises
// (a missing key, a value of the wrong kind, a receipt naming several
// events) and leaves the receipts before it stored.
func (a *App) ReceiveEDU(ctx context.Context, origin string, content json.RawMessage) error {
	if !a.writing {
		edus.WithLabelValues("not_writer").Inc()
		return ErrNotWriter
	}
	// As json.loads holds it: a repeated key once, with its last value.
	content, err := pyjson.Dedupe(content)
	if err != nil {
		edus.WithLabelValues("error").Inc()
		return err
	}
	root := gjson.ParseBytes(content)
	if !root.IsObject() {
		edus.WithLabelValues("error").Inc()
		return errors.New("receipts: EDU content is not an object")
	}
	var receipts []Receipt
	var ferr error
	root.ForEach(func(roomKey, roomValues gjson.Result) bool {
		roomID := roomKey.Str
		in, err := a.Store.HostJoined(ctx, roomID, a.serverName)
		if err != nil {
			ferr = err
			return false
		}
		if !in {
			dropped.WithLabelValues("not_in_room").Inc()
			a.Log.Info().Str("room", roomID).Str("origin", origin).
				Msg("Ignoring receipt: we are not in the room")
			return true
		}
		hosts, err := a.Store.HostsApprox(ctx, roomID)
		if err != nil {
			ferr = err
			return false
		}
		if _, ok := hosts[origin]; !ok {
			dropped.WithLabelValues("origin_not_in_room").Inc()
			a.Log.Info().Str("room", roomID).Str("origin", origin).
				Msg("Ignoring receipt: the origin is not in the room")
			return true
		}
		if !roomValues.IsObject() {
			ferr = fmt.Errorf("receipts: EDU room %s is not an object", roomID)
			return false
		}
		roomValues.ForEach(func(typeKey, users gjson.Result) bool {
			if !users.IsObject() {
				ferr = fmt.Errorf("receipts: EDU %s/%s is not an object", roomID, typeKey.Str)
				return false
			}
			users.ForEach(func(userKey, values gjson.Result) bool {
				user := userKey.Str
				if userid.Domain(user) != origin {
					dropped.WithLabelValues("wrong_origin").Inc()
					a.Log.Info().Str("user", user).Str("origin", origin).Msg("Received receipt for user from another server, ignoring")
					return true
				}
				if !userid.Compliant(user) {
					dropped.WithLabelValues("non_compliant").Inc()
					a.Log.Info().Str("user", user).Str("origin", origin).Msg("Received receipt for non-compliant user ID, ignoring")
					return true
				}
				if !values.IsObject() {
					ferr = fmt.Errorf("receipts: EDU values for %s are not an object", user)
					return false
				}
				dataRaw := `{}`
				if d := values.Get("data"); d.Exists() {
					dataRaw = d.Raw
				}
				data := gjson.Parse(dataRaw)
				if !data.IsObject() {
					// data.get("thread_id"): AttributeError.
					ferr = fmt.Errorf("receipts: EDU data for %s is not an object", user)
					return false
				}
				var threadID *string
				if t := data.Get("thread_id"); t.Type == gjson.String {
					s := t.Str
					threadID = &s
				}
				ids := values.Get("event_ids")
				if !ids.Exists() {
					ferr = fmt.Errorf("receipts: EDU for %s has no event_ids", user) // KeyError
					return false
				}
				if !ids.IsArray() {
					ferr = fmt.Errorf("receipts: EDU event_ids for %s is not a list", user)
					return false
				}
				var eventIDs []string
				for _, e := range ids.Array() {
					if e.Type != gjson.String {
						ferr = fmt.Errorf("receipts: EDU event_ids for %s holds a non-string", user)
						return false
					}
					eventIDs = append(eventIDs, e.Str)
				}
				enc, err := pyjson.Encode([]byte(dataRaw))
				if err != nil {
					ferr = err
					return false
				}
				receipts = append(receipts, Receipt{
					RoomID: roomID, ReceiptType: typeKey.Str, UserID: user,
					EventIDs: eventIDs, ThreadID: threadID, Data: enc,
				})
				return true
			})
			return ferr == nil
		})
		return ferr == nil
	})
	if ferr != nil {
		edus.WithLabelValues("error").Inc()
		return ferr
	}
	for _, r := range receipts {
		if _, err := a.insert(ctx, r, "remote"); err != nil {
			edus.WithLabelValues("error").Inc()
			return err
		}
	}
	edus.WithLabelValues("handled").Inc()
	return nil
}
