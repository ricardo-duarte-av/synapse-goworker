// Package build turns a prototype event into a signed, hashed PDU, as
// Synapse's EventBuilder.build and create_local_event_from_event_dict do
// (events/builder.py, v1.163.0).
//
// The hashing, redaction for signing, signing and event ID are mautrix's
// (federation/pdu). What goes into the event -- which fields, the depth rule,
// the auth events -- is Synapse's, and is decided by the caller with the
// helpers here.
//
// Ed25519 signatures are deterministic, so the same prototype signed with the
// same key gives the same event ID. That is what makes the shadow check
// possible: rebuild what Synapse built, and the event IDs must be equal.
package build

import (
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync/atomic"

	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/crypto/canonicaljson"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/federation/pdu"
	"maunium.net/go/mautrix/id"
)

// MaxDepth is MAX_DEPTH (api/constants.py): generated depths are capped
// there, so other servers accept them and PostgreSQL can store them.
const MaxDepth int64 = math.MaxInt64

// Proto is everything an event is made of. Synapse's event_dict, before
// hashing and signing.
type Proto struct {
	RoomVersion id.RoomVersion
	RoomID      string
	Sender      string
	Type        string
	// StateKey is nil for a message event.
	StateKey *string
	Content  jsontext.Value
	// Redacts is the top-level redacts. Synapse sets it only in room
	// versions without updated redaction rules (MSC2174 moved it into
	// content); TopLevelRedacts says which.
	Redacts        *string
	PrevEvents     []string
	AuthEvents     []string
	Depth          int64
	OriginServerTS int64
	// StickyDurationMS is MSC4354's msc4354_sticky.duration_ms, from the
	// client's sticky query parameter (rest/client/room.py), nil for none.
	StickyDurationMS *int64

	// EventID is the event's ID in room versions 1 and 2, where the origin
	// chooses it (NewV1EventID). Other versions derive it from the event.
	EventID string
	// RefHashes are the prev and auth events' reference hashes (sha256),
	// which rooms v1 and v2 list beside each (add_event_hashes; RefHash).
	RefHashes map[string][]byte
	// Origin is the top-level origin older Synapses added (before 2025 here).
	// v1.163.0 adds none: only for rebuilding stored events.
	Origin jsontext.Value
}

// Key is the server's signing key.
type Key struct {
	ServerName string
	*federation.SigningKey
}

// ParseKey reads a Synapse signing key line ("ed25519 <id> <seed>").
func ParseKey(serverName, line string) (*Key, error) {
	sk, err := federation.ParseSynapseKey(line)
	if err != nil {
		return nil, fmt.Errorf("build: signing key: %w", err)
	}
	return &Key{ServerName: serverName, SigningKey: sk}, nil
}

// Event is a built event. PDU is nil in room versions 1 and 2, whose
// events are V1.
type Event struct {
	ID   string
	PDU  *pdu.PDU
	V1   *pdu.RoomV1PDU
	JSON []byte
}

// ErrUnsupportedRoomVersion is a room version whose event format this
// package does not build.
var ErrUnsupportedRoomVersion = errors.New("build: unsupported room version")

// ErrNoRefHash is a v1/v2 prev or auth event without a reference hash.
// Synapse would list it with {} (an event it does not hold); every event
// we reference is one we hold, so this is a bug or a race, not Synapse's.
var ErrNoRefHash = errors.New("build: no reference hash")

// Depth is the builder's rule: one more than the deepest prev event, capped.
func Depth(prevDepths []int64) int64 {
	var deepest int64
	for _, d := range prevDepths {
		deepest = max(deepest, d)
	}
	if deepest >= MaxDepth {
		return MaxDepth
	}
	return deepest + 1
}

// TopLevelRedacts reports whether a redaction carries redacts at the top
// level in this room version (builder.py: not updated_redaction_rules).
func TopLevelRedacts(rv id.RoomVersion) bool { return !rv.UpdatedRedactionRules() }

// Build hashes and signs p.
func Build(p Proto, key *Key) (*Event, error) {
	if !p.RoomVersion.IsKnown() {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedRoomVersion, p.RoomVersion)
	}
	if p.Content == nil {
		p.Content = jsontext.Value("{}")
	}
	switch p.RoomVersion {
	case id.RoomV1, id.RoomV2:
		return buildV1(p, key)
	}

	auth := slices.Clone(p.AuthEvents)
	if p.RoomVersion.RoomIDIsCreateEventID() && p.RoomID != "" {
		// MSC4291: the room ID is the create event, which is not listed.
		create := "$" + p.RoomID[1:]
		auth = slices.DeleteFunc(auth, func(s string) bool { return s == create })
	}

	ev := &pdu.PDU{
		AuthEvents:     toEventIDs(auth),
		Content:        p.Content,
		Depth:          p.Depth,
		OriginServerTS: p.OriginServerTS,
		PrevEvents:     toEventIDs(p.PrevEvents),
		RoomID:         id.RoomID(p.RoomID),
		Sender:         id.UserID(p.Sender),
		StateKey:       p.StateKey,
		Type:           p.Type,
	}
	ev.DeprecatedOrigin = p.Origin
	if p.Redacts != nil && TopLevelRedacts(p.RoomVersion) {
		r := id.EventID(*p.Redacts)
		ev.Redacts = &r
	}

	if p.StickyDurationMS != nil {
		ev.Sticky = &event.Sticky{Duration: jsontime.MSInt(*p.StickyDurationMS)}
	}

	// The content hash covers msc4354_sticky and a top-level redacts; the
	// signature and the event ID, which are over the redacted event, do not:
	// Synapse's redaction keeps only its allowlist of top-level keys
	// (rust/src/events/utils.rs, redact), and neither is on it. mautrix's
	// Redact keeps the typed Sticky field (v0.30.0), and Redacts for a
	// redaction before v11, so both are taken off for signing and the ID,
	// after the hash is filled, and put back.
	if err := ev.FillContentHash(); err != nil {
		return nil, fmt.Errorf("build: content hash: %w", err)
	}
	sticky, redacts := ev.Sticky, ev.Redacts
	ev.Sticky, ev.Redacts = nil, nil
	if err := ev.Sign(p.RoomVersion, key.ServerName, key.ID, key.Priv); err != nil {
		return nil, fmt.Errorf("build: sign: %w", err)
	}
	eventID, err := ev.GetEventID(p.RoomVersion)
	if err != nil {
		return nil, fmt.Errorf("build: event id: %w", err)
	}
	ev.Sticky, ev.Redacts = sticky, redacts
	raw, err := jsonMarshal(ev)
	if err != nil {
		return nil, fmt.Errorf("build: marshal: %w", err)
	}
	return &Event{ID: string(eventID), PDU: ev, JSON: raw}, nil
}

// buildV1 is Build for room versions 1 and 2: the event ID is chosen, not
// derived, and each prev and auth event is listed with its reference hash.
func buildV1(p Proto, key *Key) (*Event, error) {
	if p.EventID == "" {
		return nil, fmt.Errorf("build: a v1 event needs an event ID")
	}
	refs := func(ids []string) ([]pdu.V1EventReference, error) {
		out := make([]pdu.V1EventReference, len(ids))
		for i, e := range ids {
			h, ok := p.RefHashes[e]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrNoRefHash, e)
			}
			out[i] = pdu.V1EventReference{ID: id.EventID(e), Hashes: pdu.Hashes{SHA256: h}}
		}
		return out, nil
	}
	prev, err := refs(p.PrevEvents)
	if err != nil {
		return nil, err
	}
	auth, err := refs(p.AuthEvents)
	if err != nil {
		return nil, err
	}
	ev := &pdu.RoomV1PDU{
		AuthEvents:     auth,
		Content:        p.Content,
		Depth:          p.Depth,
		EventID:        id.EventID(p.EventID),
		OriginServerTS: p.OriginServerTS,
		PrevEvents:     prev,
		RoomID:         id.RoomID(p.RoomID),
		Sender:         id.UserID(p.Sender),
		StateKey:       p.StateKey,
		Type:           p.Type,

		DeprecatedOrigin: p.Origin,
	}
	if p.Redacts != nil {
		r := id.EventID(*p.Redacts)
		ev.Redacts = &r
	}
	if p.StickyDurationMS != nil {
		ev.Sticky = &event.Sticky{Duration: jsontime.MSInt(*p.StickyDurationMS)}
	}
	// As in Build: the hash covers both, the signature neither.
	if err := ev.FillContentHash(); err != nil {
		return nil, fmt.Errorf("build: content hash: %w", err)
	}
	sticky, redacts := ev.Sticky, ev.Redacts
	ev.Sticky, ev.Redacts = nil, nil
	if err := ev.Sign(p.RoomVersion, key.ServerName, key.ID, key.Priv); err != nil {
		return nil, fmt.Errorf("build: sign: %w", err)
	}
	ev.Sticky, ev.Redacts = sticky, redacts
	raw, err := jsonMarshal(ev)
	if err != nil {
		return nil, fmt.Errorf("build: marshal: %w", err)
	}
	return &Event{ID: p.EventID, V1: ev, JSON: raw}, nil
}

// RefHash is compute_event_reference_hash for a v1/v2 event's stored JSON:
// sha256 of the event as Synapse prunes it, which keeps neither a
// redaction's top-level redacts nor msc4354_sticky (mautrix keeps both).
func RefHash(raw []byte, rv id.RoomVersion) ([]byte, error) {
	var ev pdu.RoomV1PDU
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("build: reference hash: %w", err)
	}
	ev.Sticky, ev.Redacts = nil, nil
	h, err := ev.GetReferenceHash(rv)
	if err != nil {
		return nil, fmt.Errorf("build: reference hash: %w", err)
	}
	return h[:], nil
}

// v1Counter is _event_id_counter: per process, from 0.
var v1Counter atomic.Uint64

// NewV1EventID is _create_event_id: "$", the time in whole seconds, the
// process's counter, five random letters, ":" and the server name.
func NewV1EventID(nowMS int64, serverName string) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 5)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	n := v1Counter.Add(1) - 1
	return "$" + strconv.FormatInt(nowMS/1000, 10) + strconv.FormatUint(n, 10) + string(b) + ":" + serverName
}

func toEventIDs(ss []string) []id.EventID {
	out := make([]id.EventID, len(ss))
	for i, s := range ss {
		out[i] = id.EventID(s)
	}
	return out
}

// SigningBytes is what a signature on the event covers: the event as
// Synapse redacts it (msc4354_sticky and a top-level redacts dropped),
// without signatures and unsigned, in canonical JSON.
func SigningBytes(ev *pdu.PDU, rv id.RoomVersion) ([]byte, error) {
	c := ev.Clone()
	c.Sticky, c.Redacts = nil, nil
	return canonicaljson.Marshal(c.RedactForSignature(rv))
}

// AddSignature adds another server's signature to the event, as
// add_signature does, and re-renders its JSON. The event ID does not cover
// signatures.
func (e *Event) AddSignature(server string, keyID id.KeyID, sig string) error {
	e.PDU.AddSignature(server, keyID, sig)
	raw, err := jsonMarshal(e.PDU)
	if err != nil {
		return fmt.Errorf("build: marshal: %w", err)
	}
	e.JSON = raw
	return nil
}
