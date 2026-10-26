package clientevent

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Format selects the client event shape.
type Format int

const (
	// FormatV1 is the legacy `/events` and `/initialSync` shape: it keeps
	// `room_id`, adds `user_id` as a duplicate of `sender`, and copies six
	// fields out of `unsigned` to the top level.
	FormatV1 Format = iota
	// FormatV2 is the modern shape, keeping `room_id`.
	FormatV2
	// FormatV2NoRoomID is what `/sync` emits: v2 with `room_id` stripped, since
	// the room is already the key in the response.
	FormatV2NoRoomID
	// FormatRaw is the PDU as stored, with no client transform at all. It is
	// what a filter's `event_format: "federation"` asks for, and it keeps the
	// fields every other format drops -- prev_events, auth_events, hashes,
	// signatures, depth. Synapse reaches it by setting `as_client_event` to
	// false, which skips the format step rather than selecting a variant of it.
	FormatRaw
)

// v2DropKeys are the federation-only fields no client format carries.
// rust/src/events/serialize.rs:61.
var v2DropKeys = [...]string{
	"auth_events", "prev_events", "hashes", "signatures", "depth", "origin", "prev_state",
}

// v1CopyKeys are lifted from `unsigned` to the top level by the v1 format.
// rust/src/events/serialize.rs:72. The order matters only for readability;
// JSON objects are compared by key.
var v1CopyKeys = [...]string{
	"age", "redacted_because", "replaces_state", "prev_content",
	"invite_room_state", "knock_room_state",
}

// Requester is the caller an event is being serialised for.
//
// It decides one thing: whether `unsigned.transaction_id` is revealed. A
// transaction ID is the sender's own idempotency key, so it is shown back only
// to the session that produced it -- another client of the same user must not
// see it.
type Requester struct {
	UserID   string
	DeviceID string
	// TokenID is `access_tokens.id`, used only for events stored without a
	// device_id: old events, and those from appservices, guests or admin-API
	// tokens. Zero means unknown, in which case the fallback is skipped.
	TokenID int64
	IsGuest bool
	// AppServiceID is non-empty when the caller is an appservice. Synapse
	// cannot check which session an appservice used, so it assumes the same
	// one.
	AppServiceID string
}

// Config controls one serialisation.
type Config struct {
	Format    Format
	Requester Requester
	// IncludeStrippedRoomState keeps `unsigned.invite_room_state` and
	// `knock_room_state`. Off everywhere except the invite section of /sync.
	IncludeStrippedRoomState bool
	// MSC4354Enabled mirrors Synapse's experimental.msc4354_enabled. When set,
	// a sticky event carries the time it has left to live.
	MSC4354Enabled bool
	// IncludeAdminMetadata adds the server-admin-only `unsigned` fields
	// `io.element.synapse.soft_failed` and
	// `io.element.synapse.policy_server_spammy`
	// (rust/src/events/serialize.rs, `include_admin_metadata`).
	//
	// Set only for a server admin whose
	// `io.element.synapse.admin_client_config` asks for such events. It goes
	// together with the visibility filter letting them through: showing an
	// admin a soft-failed event without saying so would give them no way to
	// tell it apart from an ordinary one.
	IncludeAdminMetadata bool
	// EventFields is a filter's `event_fields`: an allowlist of (possibly
	// dotted) paths, applied last of all. Nil or empty means no pruning.
	//
	// It runs after every other transform and before bundled aggregations,
	// which is why it can prune `unsigned.membership` but never an
	// `m.relations` bundle: Synapse returns aggregations regardless of what
	// the client asked for.
	EventFields []string
}

// stickyMaxDurationMS caps how long an event may claim to be sticky.
// rust/src/events/mod.rs:403.
const stickyMaxDurationMS = int64(60 * 60 * 1000)

// applyStickyTTL adds `unsigned.msc4354_sticky_duration_ttl_ms` (MSC4354).
//
// The remaining lifetime, not the configured duration: a client needs to know
// how long the event stays pinned from now, so the value shrinks as the event
// ages and the field disappears once it has expired.
//
// The `min(origin_server_ts, time_now)` is not redundant. It stops a remote
// server claiming a timestamp in the future to make its event stick for longer
// than the one-hour cap allows.
func applyStickyTTL(out []byte, nowMS int64) ([]byte, error) {
	sticky := gjson.GetBytes(out, "msc4354_sticky")
	if !sticky.IsObject() {
		return out, nil
	}
	duration := sticky.Get("duration_ms")
	// The MSC requires a non-negative integer; anything else means not sticky.
	if !duration.Exists() || duration.Type != gjson.Number || duration.Int() < 0 {
		return out, nil
	}
	ms := duration.Int()
	if ms > stickyMaxDurationMS {
		ms = stickyMaxDurationMS
	}
	originTS := gjson.GetBytes(out, "origin_server_ts").Int()
	if originTS > nowMS {
		originTS = nowMS
	}
	expiresAt := originTS + ms
	if expiresAt <= nowMS {
		return out, nil
	}
	return sjson.SetBytes(out, "unsigned.msc4354_sticky_duration_ttl_ms", expiresAt-nowMS)
}

// persistedUnsignedFields is the complete set of `unsigned` keys Synapse keeps
// when loading an event. rust/src/events/unsigned.rs.
var persistedUnsignedFields = [...]string{
	"age_ts", "replaces_state", "invite_room_state", "knock_room_state",
	"prev_content", "prev_sender",
}

// filterUnsigned returns the stored `unsigned` reduced to the allowlist.
func filterUnsigned(stored []byte) []byte {
	out := []byte(`{}`)
	unsigned := gjson.GetBytes(stored, "unsigned")
	if !unsigned.Exists() {
		return out
	}
	for _, key := range persistedUnsignedFields {
		v := unsigned.Get(key)
		if !v.Exists() {
			continue
		}
		// Keys are known constants, so SetRawBytes cannot fail on the path;
		// a malformed value would already have failed the gjson lookup.
		next, err := sjson.SetRawBytes(out, key, []byte(v.Raw))
		if err != nil {
			continue
		}
		out = next
	}
	return out
}

// Stored is an event as it comes out of the database.
type Stored struct {
	EventID string
	RoomID  string
	Type    string
	// JSON is `event_json.json` verbatim: the PDU as stored.
	JSON []byte
	// InternalMetadata is `event_json.internal_metadata` verbatim.
	InternalMetadata []byte
	RoomVersion      string
	// Membership is the sender's membership at this event, for MSC4115's
	// `unsigned.membership`. Empty to omit.
	Membership string

	// RedactedBy is the id of the redaction event that applies, empty if none.
	// When set the event is pruned before anything else happens.
	RedactedBy string
	// RedactedBecause is the already-serialised redaction event, attached to
	// the pruned event so a client can see who redacted it and why.
	RedactedBecause []byte
}

// Serialize renders a stored event for a client.
//
// It follows rust/src/events/serialize.rs, whose ordering is load-bearing:
// `unsigned.age` is computed before the format transform, so the v1 format
// copies the computed age rather than the stored `age_ts`.
func Serialize(ev Stored, nowMS int64, cfg Config) ([]byte, error) {
	out := ev.JSON
	var err error

	rv := LookupRoomVersion(ev.RoomVersion)

	// Redaction first: everything below operates on the pruned event, which is
	// what Synapse's serialiser sees too -- it redacts in the storage layer,
	// not here.
	if ev.RedactedBy != "" {
		if out, err = Redact(out, rv); err != nil {
			return nil, err
		}
	}

	// Room version v3+ PDUs do not carry their own event_id -- it is a hash of
	// the event. Clients still expect the field, so it is always inserted.
	if out, err = sjson.SetBytes(out, "event_id", ev.EventID); err != nil {
		return nil, err
	}

	// Rebuild `unsigned` from the allowlist rather than passing the stored
	// object through.
	//
	// Synapse models unsigned as a typed struct of exactly six fields
	// (rust/src/events/unsigned.rs), so anything else in storage is dropped
	// when the event is loaded. That is not a formality: remote servers send
	// `age` and Synapse *stores* it, so a passthrough emits a stale age --
	// baked in at receive time, years out of date -- on events Synapse gives no
	// age at all. Found by comparing federated rooms; every locally created
	// event agreed, because those store `age_ts` instead.
	//
	// `unsigned` is always present in the output, empty as {}: the field is
	// serialised unconditionally, and the v1 format reads it without checking.
	if out, err = sjson.SetRawBytes(out, "unsigned", filterUnsigned(ev.JSON)); err != nil {
		return nil, err
	}

	// age_ts becomes a wall-clock age. It is generated by us and so should be
	// an integer, but an out-of-range value is dropped rather than erroring: a
	// once-valid event must not start failing to serialise.
	if ageTS := gjson.GetBytes(out, "unsigned.age_ts"); ageTS.Exists() && ageTS.Type == gjson.Number {
		if out, err = sjson.SetBytes(out, "unsigned.age", nowMS-ageTS.Int()); err != nil {
			return nil, err
		}
		if out, err = sjson.DeleteBytes(out, "unsigned.age_ts"); err != nil {
			return nil, err
		}
	}

	if out, err = applyTransactionID(out, ev, cfg.Requester); err != nil {
		return nil, err
	}

	if !cfg.IncludeStrippedRoomState {
		if out, err = sjson.DeleteBytes(out, "unsigned.invite_room_state"); err != nil {
			return nil, err
		}
		if out, err = sjson.DeleteBytes(out, "unsigned.knock_room_state"); err != nil {
			return nil, err
		}
	}

	// Before the format transform: `redacted_because` is one of the six keys
	// the v1 format lifts out of unsigned to the top level, so adding it
	// afterwards would emit the unsigned copy with no top-level twin.
	if ev.RedactedBy != "" {
		if out, err = sjson.SetBytes(out, "unsigned.redacted_by", ev.RedactedBy); err != nil {
			return nil, err
		}
		if len(ev.RedactedBecause) > 0 {
			if out, err = sjson.SetRawBytes(out, "unsigned.redacted_because", ev.RedactedBecause); err != nil {
				return nil, err
			}
		}
	}

	if out, err = applyFormat(out, cfg.Format); err != nil {
		return nil, err
	}

	// An MSC4291 room derives its ID from the create event's hash, so that
	// event has no room_id of its own. Put it back, or a client cannot tell
	// which room the create event belongs to.
	//
	// Note this runs *after* the format transform, so it also restores the
	// field on /sync's v2-without-room-id -- which is what Synapse does.
	if ev.Type == "m.room.create" && rv.RoomIDsAsHashes {
		if out, err = sjson.SetBytes(out, "room_id", ev.RoomID); err != nil {
			return nil, err
		}
	}

	// A redaction names its target in a different place per room version. It is
	// already in the version-correct one; copy it to the other as well, for
	// clients written against either.
	if ev.Type == "m.room.redaction" {
		if out, err = mirrorRedacts(out, rv); err != nil {
			return nil, err
		}
	}

	// Server-admin-only metadata, written from internal_metadata just as
	// rust/src/events/serialize.rs does. Only reachable when the caller is an
	// admin who asked for such events; see Config.IncludeAdminMetadata.
	if cfg.IncludeAdminMetadata {
		if gjson.GetBytes(ev.InternalMetadata, "soft_failed").Bool() {
			if out, err = sjson.SetBytes(out, "unsigned.io\\.element\\.synapse\\.soft_failed", true); err != nil {
				return nil, err
			}
		}
		if gjson.GetBytes(ev.InternalMetadata, "policy_server_spammy").Bool() {
			if out, err = sjson.SetBytes(out, "unsigned.io\\.element\\.synapse\\.policy_server_spammy", true); err != nil {
				return nil, err
			}
		}
	}

	if cfg.MSC4354Enabled {
		if out, err = applyStickyTTL(out, nowMS); err != nil {
			return nil, err
		}
	}

	// MSC4115: the sender's membership at the point of this event, so a client
	// can render history without resolving state itself.
	if ev.Membership != "" {
		if out, err = sjson.SetBytes(out, "unsigned.membership", ev.Membership); err != nil {
			return nil, err
		}
	}

	if len(cfg.EventFields) > 0 {
		if out, err = OnlyFields(out, cfg.EventFields); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func applyFormat(out []byte, format Format) ([]byte, error) {
	var err error
	// The federation format is not a transform that keeps more fields -- it is
	// no transform at all, so it must return before the shared drop list.
	if format == FormatRaw {
		return out, nil
	}
	for _, key := range v2DropKeys {
		if out, err = sjson.DeleteBytes(out, key); err != nil {
			return nil, err
		}
	}
	switch format {
	case FormatV2:
		return out, nil

	case FormatV2NoRoomID:
		return sjson.DeleteBytes(out, "room_id")

	case FormatV1:
		// `user_id` duplicates `sender`. A present-but-null sender is left
		// alone, matching the Rust's `filter(|v| !v.is_null())`.
		if sender := gjson.GetBytes(out, "sender"); sender.Exists() && sender.Type != gjson.Null {
			if out, err = sjson.SetBytes(out, "user_id", sender.Value()); err != nil {
				return nil, err
			}
		}
		for _, key := range v1CopyKeys {
			v := gjson.GetBytes(out, "unsigned."+key)
			if !v.Exists() {
				continue
			}
			if out, err = sjson.SetRawBytes(out, key, []byte(v.Raw)); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return out, nil
}

func mirrorRedacts(out []byte, rv RoomVersion) ([]byte, error) {
	// MSC2174 moved `redacts` INTO content from room version 11, so the
	// canonical place depends on the version -- and it is the opposite of what
	// it looks like. Event.redacts() reads `content.redacts` when
	// updated_redaction_rules is set and top-level `redacts` otherwise
	// (rust/src/events/mod.rs:623); the serialiser then writes it to the OTHER
	// place, for clients written against either version.
	//
	// Getting this backwards drops the field entirely rather than duplicating
	// it, because the read finds nothing.
	from, to := "redacts", "content.redacts"
	if rv.UpdatedRedactionRules {
		from, to = "content.redacts", "redacts"
	}
	v := gjson.GetBytes(out, from)
	if !v.Exists() || v.Type == gjson.Null {
		return out, nil
	}
	return sjson.SetRawBytes(out, to, []byte(v.Raw))
}

// applyTransactionID reveals `unsigned.transaction_id` only to the session that
// sent the event.
//
// The transaction ID is the sender's own idempotency key. Showing it to another
// of the user's sessions would leak which client sent what, so Synapse matches
// on device first and falls back to the access token for events stored before
// device IDs were recorded.
func applyTransactionID(out []byte, ev Stored, req Requester) ([]byte, error) {
	if len(ev.InternalMetadata) == 0 || req.UserID == "" {
		return out, nil
	}
	if gjson.GetBytes(out, "sender").String() != req.UserID {
		return out, nil
	}
	meta := gjson.ParseBytes(ev.InternalMetadata)

	txnID := meta.Get("txn_id")
	if txnID.Exists() && txnID.Type == gjson.String {
		reveal := false
		if eventDevice := meta.Get("device_id"); eventDevice.Exists() {
			reveal = eventDevice.String() == req.DeviceID && req.DeviceID != ""
		} else {
			// No device is stored for old events and for those created by
			// appservices, guests or admin-API tokens. Fall back to the access
			// token; for guests and appservices, which cannot be checked,
			// Synapse assumes the same session.
			eventToken := meta.Get("token_id")
			tokenMatches := eventToken.Exists() && req.TokenID != 0 && eventToken.Int() == req.TokenID
			reveal = tokenMatches || req.IsGuest || req.AppServiceID != ""
		}
		if reveal {
			var err error
			if out, err = sjson.SetBytes(out, "unsigned.transaction_id", txnID.String()); err != nil {
				return nil, err
			}
		}
	}

	if delayID := meta.Get("delay_id"); delayID.Exists() && delayID.Type == gjson.String {
		var err error
		// The unsigned key is the MSC-prefixed name, not a bare "delay_id"
		// (rust/src/events/constants.rs, unsigned_field::DELAY_ID).
		if out, err = sjson.SetBytes(out, "unsigned.org\\.matrix\\.msc4140\\.delay_id", delayID.String()); err != nil {
			return nil, err
		}
	}
	return out, nil
}
