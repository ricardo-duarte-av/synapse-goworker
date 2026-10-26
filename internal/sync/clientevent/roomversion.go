// Package clientevent renders stored Synapse events into the shapes the client
// API sends.
//
// Responses are built by splicing the stored `event_json.json` with sjson
// rather than unmarshalling into a struct and re-encoding. 14,654 events on
// this server contain escaped NUL characters that PostgreSQL `jsonb` cannot
// even cast; a round trip through a Go map would also reorder keys and
// normalise numbers. Splicing keeps every byte we are not deliberately
// changing.
package clientevent

// RoomVersion carries the two flags event serialisation branches on.
//
// Synapse's table is much larger (rust/src/room_versions.rs), but the rest of
// it governs auth rules and redaction algorithms, which this package does not
// implement.
type RoomVersion struct {
	// UpdatedRedactionRules is MSC3820, from room version 11. It moves a
	// redaction's target into `content.redacts`, and changes which fields
	// survive redaction.
	UpdatedRedactionRules bool
	// RoomIDsAsHashes is MSC4291, from room version 12. The room ID is derived
	// from the hash of the create event, so **the create event carries no
	// `room_id`** while every other event in the room does. Serialisation has
	// to put it back for clients.
	RoomIDsAsHashes bool
	// SpecialCaseAliasesAuth keeps `content.aliases` through a redaction.
	// Room versions 1 to 5 only.
	SpecialCaseAliasesAuth bool
	// ImplicitRoomCreator drops `content.creator` from a redacted create
	// event, from room version 11.
	ImplicitRoomCreator bool
	// RestrictedJoinRule keeps `content.allow` on join rules, from v8.
	RestrictedJoinRule bool
	// RestrictedJoinRuleFix keeps `join_authorised_via_users_server` on
	// membership, from v9.
	RestrictedJoinRuleFix bool
	// MSC4242StateDags replaces `auth_events` with `prev_state_events`.
	MSC4242StateDags bool
	// MSC3389RelationRedactions keeps a trimmed `m.relates_to` through a
	// redaction.
	MSC3389RelationRedactions bool
}

// roomVersions mirrors rust/src/room_versions.rs for the two flags above.
// Unknown versions get the conservative pre-v11 behaviour.
// roomVersions mirrors rust/src/room_versions.rs. Each version there inherits
// from the previous with struct-update syntax, so the flags are cumulative:
// v9's restricted_join_rule_fix does not replace v8's restricted_join_rule, it
// joins it. Flattened here, because a Go map has no inheritance and an
// inherited flag silently dropped is exactly the kind of difference that only
// shows up on one room, years later.
var roomVersions = map[string]RoomVersion{
	// 1-5: aliases are special-cased in auth, and survive redaction.
	"1": {SpecialCaseAliasesAuth: true},
	"2": {SpecialCaseAliasesAuth: true},
	"3": {SpecialCaseAliasesAuth: true},
	"4": {SpecialCaseAliasesAuth: true},
	"5": {SpecialCaseAliasesAuth: true},
	// 6-7: the aliases special case is gone.
	"6": {},
	"7": {},
	// 8: restricted join rules (MSC3083).
	"8": {RestrictedJoinRule: true},
	// 9-10: the restricted join rule fix (MSC3375).
	"9":                     {RestrictedJoinRule: true, RestrictedJoinRuleFix: true},
	"10":                    {RestrictedJoinRule: true, RestrictedJoinRuleFix: true},
	"org.matrix.msc1767.10": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true},
	"org.matrix.msc3757.10": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true},
	"org.matrix.msc3389.10": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true,
		MSC3389RelationRedactions: true},
	// 11: updated redaction rules and an implicit creator (MSC3820).
	"11": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true,
		UpdatedRedactionRules: true, ImplicitRoomCreator: true},
	"org.matrix.msc3757.11": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true,
		UpdatedRedactionRules: true, ImplicitRoomCreator: true},
	"org.matrix.hydra.11": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true,
		UpdatedRedactionRules: true, ImplicitRoomCreator: true, RoomIDsAsHashes: true},
	// 12: room IDs as hashes (MSC4291).
	"12": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true,
		UpdatedRedactionRules: true, ImplicitRoomCreator: true, RoomIDsAsHashes: true},
	"org.matrix.msc4242.12": {RestrictedJoinRule: true, RestrictedJoinRuleFix: true,
		UpdatedRedactionRules: true, ImplicitRoomCreator: true, RoomIDsAsHashes: true,
		MSC4242StateDags: true},
}

// LookupRoomVersion returns the flags for a room version identifier.
//
// An unknown version is not an error: Synapse may add one before we do, and
// refusing to serialise every event in such a room would be a far worse
// failure than serialising it with pre-v11 rules.
func LookupRoomVersion(id string) RoomVersion {
	return roomVersions[id]
}

// IsKnownRoomVersion reports whether a room version is one we have rules for.
//
// Distinct from LookupRoomVersion, which deliberately answers for anything:
// serialising an event in an unfamiliar room with pre-v11 rules is far better
// than refusing to serialise it. Sliding sync needs the other question, because
// it filters such rooms out of a room list entirely
// (get_sliding_sync_rooms_for_user_from_membership_snapshots: "their metadata
// may be in a broken state"), and "not in the list" is a much cheaper failure
// than "every request 500s".
func IsKnownRoomVersion(id string) bool {
	_, ok := roomVersions[id]
	return ok
}
