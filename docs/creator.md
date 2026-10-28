# The event creator

`internal/creator`, merged from
[synapse-goevent-creator](https://github.com/ricardo-duarte-av/synapse-goevent-creator)
in phase 7 (2026-10-28): the client endpoints that create room events, in
place of Synapse's `event_creator` worker. These are its working notes,
carried over; the archive repo has the history. Ported against Synapse
v1.164.1.

What changed in the merge: no shadow mode, no room allowlist (every room is
served), no per-endpoint modes (nginx decides what arrives), no role of its
own (pgcat, homeserver.yaml's user), requesters from `platform/clientauth`,
and `pdu`, `pdustore`, `stateres`, `synapsecfg`, `fedclient`,
`replication`, `appservice` shared with the rest of goworker. Mentions of
shadow below are history: it is what caught several of the bugs.

## Decisions

- **The goal is retiring `av-client-events-1` entirely** (operator,
  2026-10-16). The fallback is **main**, not the Python event_creator:
  nothing may come to depend on av-client-events-1. Main is busy and
  single-threaded, so the fallback share should shrink, not grow.
- **The e2e suite runs only in its test rooms** (`cmd/goevent-e2e`,
  `testRooms`): TEST6TEST (`!decSfHzLLhrNKJaZid`), TEST7TESTE
  (`!nmZrvcKkeQiFGRlJtE`), the policy room (`!FRudVQysKkIHAjrSzd`) and the
  v12 room (`!goBCqT-...`). It bans, kicks and sends refusals on purpose:
  never in a real room. The fake policy server (`cmd/goevent-policy`) stays
  in the archive repo.
- **Profile writes are native, reads go to main** (operator, 2026-10-28:
  "time to handle /profile/ properly"; until then all of `/profile/` was
  proxied). In v1.163 and v1.164 a worker that is not the
  first events writer hands every profile write to it
  (`ReplicationProfileSetField`/`DeleteField`), which stores it, updates
  the user directory and, to propagate, schedules `update_join_states`,
  which **main** runs: one join event per room the user is in (670 for
  `@daedric`). Here (`creator/profile.go`) the servlet's checks are ours,
  in its order and words; the writer still stores the field, asked with
  `propagate: false` (its refusals relayed); then `rc_message` on the target
  (the writer's `_update_join_states` ratelimit: a 429 *after* the field is
  stored), and the join events are made natively in the background,
  `update_membership(join, ratelimit=False)` with `create_requester`'s
  requester (no token, no device, entity the requesting user), rooms in ID
  order, a newer change cancelling an older one, failures logged
  (`goevent_profile_propagation_total{result}`). The requester crosses
  replication as a bare user ID, so Synapse here propagates even for a
  shadow-banned user; so do we.
  A propagation survives restarts as Synapse's scheduled task does: it is
  saved before the answer in **our own table in Synapse's database**,
  `goevent_profile_propagations` (user, job ID, entity, `last_room_id`),
  checkpointed after every room, deleted when done, and resumed at start
  after its last room (`ResumeProfiles`, `bisect_right` on the sorted
  rooms). Not `scheduled_tasks`: main runs every ACTIVE or due task there
  itself, every minute. The table is created at start
  (`writer.Setup`, `CREATE TABLE IF NOT EXISTS`, the only DDL it makes;
  without it profile writes are main's); Synapse ignores it, and dropping it loses only
  unfinished propagations. A room done but not yet checkpointed is redone
  after a crash, and the duplicate rule makes that a no-op. Custom
  fields (MSC4133, v3 only) never propagate; the unstable MSC4133 path is
  proxied.
- **Our own tables in Synapse's database are `goevent_`-named**, created by
  `writer.Setup` (operator, 2026-10-28, for profile propagations). Nothing
  of Synapse's schema is ever altered. A propagation is resumed by whichever
  process starts with the creator: stop goevent-creator before goworker's
  creator starts (docs/cutover.md).
- **Refusals are encoded as Synapse encodes them**: `<`, `>` and `&` as they
  are (`synapseJSON`). encoding/json wrote `\u003c`, so the common
  `user_level (0) < send_level (50)` differed byte for byte until
  2026-10-28.

## Facts checked against the live system

- `av-client-events-1` answers **500 M_UNKNOWN to an unauthenticated request
  without `X-Forwarded-For`** (2026-10-16); authenticated ones (whoami with
  a token, valid or not) are fine without it. ReverseProxy's Rewrite mode
  strips the header, so `proxy.rewrite` puts nginx's headers back. Test by
  hand with `-H 'X-Forwarded-For: 127.0.0.1'`.
- **The build reproduces Synapse byte for byte.** `TestLiveRebuildsSynapseEvents`
  rebuilds the newest 200 local events in the two rooms from their stored
  fields: every event ID and every signature equal (2026-10-16). Ed25519 is
  deterministic, which is what makes the shadow check's "rebuild" exact.
- **mautrix keeps `msc4354_sticky` when redacting; Synapse drops it** (its
  redaction keeps only an allowlist of top-level keys,
  `rust/src/events/utils.rs`, `redact`). The sticky field is in the content
  hash but not in the signature or the event ID, so `build.Build` takes it off
  for those two. Without this every sticky event (most MatrixRTC traffic here)
  gets a wrong ID. Worth reporting upstream.
- **The same for a top-level `redacts`** (rooms before v11): Synapse's
  redaction drops it, mautrix's keeps it, so every v3–v10 redaction got a
  wrong signature and ID until 2026-10-22 (shadow caught one in a v9 room;
  the test rooms are all v11+). `TestLiveRebuildsSynapseEvents` now also
  rebuilds local redactions from any room.
- **Rooms v1 to v3 are native too** (2026-10-28). Until then any room
  before v4 fell back (`EventFormat < 3`, v3 included). In v1/v2 the
  origin picks the ID, `_create_event_id`'s `$<unix seconds><process
  counter><5 letters>:<server>`, and each prev and auth event is listed as
  `[id, {"sha256": reference hash}]` (`add_event_hashes`; `build.RefHash`,
  which drops `redacts` and `msc4354_sticky` as Synapse's prune does).
  Events before 2025 here also carry a top-level `origin`, which v1.163
  no longer adds (`Proto.Origin` exists only to rebuild them).
  `TestLiveRebuildsSynapseEvents` rebuilds the newest 200 local events in
  v1-v3 rooms (189 v1) with every signature, content hash and stored
  reference hash equal. Shadow cannot compare a v1 event ID, so its
  `rebuild` compares the signature over the stored ID instead. A policy
  server in a v1/v2 room falls back (`policy_v1`).
- **Synapse's `auth_events` order is random**: `auth_types_for_event` returns
  a Python set of tuples, and string hashing is randomised per process. Prev
  events are ordered by depth, ties arbitrary. Compare both as sets.
- The signing key file holds more than one key; Synapse signs with the first
  (`config/key.py`), which is what `synapsecfg.SigningKeyLine` returns.
- Synapse **lets you invite a deactivated user** (`@test01`, 2026-10-16);
  the invite sat there until kicked.
- A join by an already-joined user with an unchanged profile creates **no
  event** (`room_member.py`, duplicate rule: same sender, membership and
  content). The native path answers with the existing event (`membership_noop`).

## Native

Every endpoint, in every room (`internal/creator`): the requester from
`platform/clientauth`, txn dedup, the state before (resolved across up to ten
prev events), relation and content validation, build, auth, push rows, then
the creator's own writes (deletion bump, room participation, push staging,
state groups; `internal/creator/writer`, its tables listed in
`writer.Tables`), and `send_events` to the events writer. Anything else falls
back to main before the first write (`goevent_native_fallback_total{reason}`).
Each native event is verified against what was persisted
(`goevent_native_verify_total{check,result}`).

- **`context.rejected` is `str | None`: send `null`, never `false`.** On
  2026-10-16 twelve native events in TEST7TESTE were sent with `false`. The
  persister treated it as not rejected (state, stream and push were right) but
  wrote it into `events.rejection_reason` as the text `'false'`. Synapse loads
  rejection from `rejections`, so Synapse itself served them, but **gosync
  filters on `e.rejection_reason IS NULL` (hidden from /sync) and gofederation
  skips any non-empty reason (never sent to remote servers)**. The verify check
  `rejection_column` now catches it. Other Go workers reading Synapse's tables
  have stricter expectations than Synapse: test against them, not only Synapse.
  The operator repaired the 12 rows (NULL) the same day; after the fix the e2e
  suite against a native worker passed 31/31 with every native event verified.
- **No role of its own** (phase 7): goevent-creator wrote as
  `goevent_creator`, a role granted exactly its tables; goworker connects as
  Synapse does, through pgcat as homeserver.yaml's user, so `writer.Tables`
  and `TestQueriesUseOnlyTables` are what keep the writes to Synapse's event
  creator's. The old role can be dropped with the other workers' roles.
- **Policy servers (MSC4284) are served** (`creator/policy.go`), as
  `_create_and_send_nonmember_event_locked` uses `RoomPolicyHandler`: after
  the event is built and before dedup and the auth rules, a non-member event
  (not the policy event itself) is POSTed to the room's policy server
  (`/_matrix/policy/v1/sign`, the unstable path when that is unknown, 30 s),
  its signatures added; then the `ed25519:policy_server` signature must
  verify against `m.room.policy`'s key, or it is asked once more and the event
  refused ("This message has been rejected as probable spam"). A 4xx is
  relayed as `to_synapse_error` writes it, a 5xx, 429 or no answer is a 500,
  an empty answer the policy server's spam refusal. The policy server counts
  only when it is a valid server name, not ours, and has a member joined.
  Membership is never asked. A signature does not change the event ID.
- **The policy room** (`!FRudVQysKkIHAjrSzd`, v11, operator, 2026-10-20) has
  the fake policy server `policy.aguiarvieira.pt` (`cmd/goevent-policy`, the
  `goevent-policy` compose service: its own keys, one bot that joined over
  federation, `/send`, `/sign`, and a localhost control listener on 28449 to
  switch it between sign, refuse, empty and error). It signs everything but
  bodies with `goevent-spam`. Run the suite there with `-room` and
  `-policy-control http://127.0.0.1:28449` and
  `-golden cmd/goevent-e2e/refusals.policy.golden.json` (room IDs in some
  refusals make the room's own recording). Calibrated on Synapse (99/101,
  ACL and fork skipped), then 100/101 fully native, every non-member event
  stored with the policy signature. Every server in that room asks the fake:
  keep it running. Since 2026-10-20 the room's `m.room.policy` is `{}` (no
  policy server) and the bot was kicked, to retire the fake: putting a
  policy server back means inviting and joining the bot again
  (control `/join`) before restoring `m.room.policy`.
- **The v12 room** (`!goBCqT-LJRiRgPetKo34PrUe4GqYZ_R74c6SBGLwTo4`,
  operator, 2026-10-20): MSC4291 room ID, the creator's unlimited power,
  state resolution v2.1. `TestLiveRebuildsSynapseEvents` covers its events;
  the suite there (`-golden cmd/goevent-e2e/refusals.v12.golden.json`)
  passed 94 on Synapse and 95/97 fully native (ACL and policy skipped), the
  fork included.
- **State events**: dedup (`deduplicate_state_event`) is checked before auth,
  as Synapse does; Synapse stores the new state group *before* dedup and leaves
  it orphaned, we do not store it. The new group is a delta of exactly the
  event on the group before (`writer.StoreStateGroup`, goinbound's port, full
  group past 100 hops); the context's `state_group_deltas` is `(before,
  after, [event])` then the group before's own edge. `unsigned.replaces_state`
  is set. Power levels (`POWER_LEVELS_SCHEMA`), server ACL (must not deny us),
  aliases (255 characters), retention and tombstone are validated as
  `validate_new`/`validate_builder` do (`creator/validate.go`): served only when
  the content certainly passes, otherwise Synapse's. Membership, create and
  policy fall back.
- **Membership** (`creator/membership.go`): the servlets, then
  `update_membership`/`update_membership_locked` for a local change in a room
  we are in. Content is `{membership}` plus `reason` (and MSC4293's
  `redact_events`) from the membership servlet, or the whole body from
  `/join/`, plus the target's global profile (`profiles`) for joins and,
  with `include_profile_data_on_invite`, invites; a missing profile row leaves
  both fields out (Synapse's 404 is swallowed). `extra_users` is the target.
  Order matters and is Synapse's: the PUT success cache
  (`HttpTransactionCache`, 30 min, per process), ratelimits, the
  state-independent checks, then under the room lock the old-membership
  checks (kick of a leave, unban of a non-ban, anything but leave/ban/unban
  on a ban), the restricted-join check, and only then the event transaction
  lookup, as `_local_membership_update` has it. A join by a joined user with
  an unchanged profile is the generic state dedup (`deduplicate_state_event`),
  not `update_membership_locked`'s own comparison, which compares content
  without `membership` and never matches. A leave from a join tells main
  (`membership_change/.../left`, `_user_left_room`). Fallbacks: remote
  joins and knocks (main's in Synapse too), 3pid invites, `third_party_signed`,
  invite filtering account data (MSC4380, MSC4155), server notice rooms
  (except a profile change's join: for a join the room changes nothing but
  consent, and the background propagation has no main to hand it to),
  restricted joins needing an authorising server, the host not in the room,
  an avatar with media limits configured, and any module,
  `hs_disabled`/MAU limit or consent requirement in homeserver.yaml.
  Blocked rooms are refused here.
- **Membership ratelimits are this process's**, as each Synapse worker's are:
  `rc_joins.local` (joins by a user not yet joined), `rc_invites.*`, and
  `rc_joins_per_room`, which Synapse feeds from every join on the events
  stream and we replay from `room_memberships` (`RecentJoins`). A
  `ratelimit_override` row with no rate exempts. A refusal is answered here
  (429 after Synapse's half-second pause). A request that passes our limits
  and then falls back is counted again by main's.
- **`m.call.invite` is refused in public rooms** (`create_new_client_event`);
  native `/send` refused it (`call_invite_public`). Found while porting
  M5: it had been missed in M3.
- **Repeated JSON keys are Python's**: json.loads keeps the key's first
  position and its last value; gjson finds the first. `pyjson.Dedupe`
  rewrites the body that way before anything reads it.
- **Forks are resolved here** (M6): `resolve_state_groups_for_events` over
  the prev events' groups (`platform/stateres`, goinbound's v2 port, shared), then
  `_make_state_cache_entry`: an input group holding exactly the result is
  reused (and the context has no edge for it), otherwise a new group is
  stored on the input it differs least from, or whole when every input has a
  key the result lacks. Synapse tries the inputs in Python set order, random
  per process, so any of equal ones is Synapse's choice; ours is the lowest.
  Synapse stores that group in `calculate_context_info`, before the checks
  that may refuse the event (an orphan then); we store it only once nothing
  can be refused. All the inputs are bumped (`resolve_state_groups`). On
  2026-10-19 3.7% of the week's local events had several prev events (76 of
  them ten). The e2e step "send on a fork" races a native state event with
  a delayed one (added here, sent by main when due) and checks the resolved group
  (`state_before`). It usually forks within a few attempts (up to 10, the
  delay cycling 0-100 ms); a skip is timing, not a failure.
- **The suite's appservice** (`goevent-e2e`, operator, 2026-10-19):
  `goevent-e2e-appservice.yaml` beside homeserver.yaml, `url: null`,
  exclusive ghosts `@goevent-e2e-ghost*`, and `@test` non-exclusively to
  double-puppet. Its `as_token` is in `.claude/deployment-notes.md` only;
  the suite reads `GOEVENT_E2E_AS_TOKEN` and skips the steps without it.
  Steps: double puppeting with `ts` and its txn retry (the transaction
  cache), device masquerading, the one fixed ghost's join, message and
  leave, and five refusals. Calibrated on Synapse with a local shadow
  worker (`-record`), 89/90 there, the fork skipped as shadow cannot
  verify it; then 91/91 fully native locally and 90/91 deployed
  (`f1944de`, the fork skipped on timing). Synapse answers an appservice's
  txn retry from `HttpTransactionCache`, which shadow cannot see: no
  longer a `new_event` mismatch.
- **Requesters come from `platform/clientauth`** (phase 7; goevent-creator
  had its own `get_user_by_req` on the database, M6, operator: "we must not
  depend on synapse"). clientauth marks a token used and publishes USER_IP,
  as Synapse does. What it leaves to main's /whoami (a guest's macaroon,
  delegated auth, account_validity, a module, a regex RE2 cannot compile, an
  `ip_range_whitelist` appservice from elsewhere) the creator leaves to main
  whole (`auth_whoami`): a /whoami verdict lacks what the requester needs. A
  refusal, its own or /whoami's, is relayed byte for byte. The requester's
  `admin`, `shadow_banned` and `suspended` are read per request from `users`;
  a puppet token's owner is the verdict's `Entity`.
- **Appservices are served**: clientauth checks the namespace, the user and
  `?device_id=` (MSC3202); the creator adds `?ts=` as `origin_server_ts`,
  `UserID.from_string` on the user, and the registration's `rate_limited`
  (`platform/appservice`, by the verdict's ID). The requester and the context
  carry `app_service_id`, so the writer applies the appservice's
  `rate_limited`; membership ratelimits skip unlimited ones too. Without a
  device there is no transaction lookup: a retry is answered by the
  transaction cache (`answerCache`, Synapse's `HttpTransactionCache`, keyed
  as it keys: path plus user and device, guest, appservice, or token). The
  doublepuppet registration claims every local user, so bridged messages
  from real users come this way.
- **Refusals are given here, in Synapse's order and words** (M6). A check
  answers nil, a refusal (status, errcode, message, extra fields) or a
  fallback (`creator.stop`). A fallback may sit anywhere before the first
  write (main then decides); a refusal must come after every check Synapse
  makes before it, so `serve` and `prepare` follow Synapse's order: auth,
  `parse_json_object_from_request`, the servlet (sticky `parse_integer`,
  the redact servlet's `redacts`, a member event on `/send` is a 500), the
  txn lookup, `check_local_user_in_room`, `validate_builder`, the create
  resend check, `validate_new` (canonical JSON, aliases, retention, ACL, the
  power levels and mentions schemas), `_validate_event_relation`, the call
  invite check, state dedup, the writer's redaction checks, size limits,
  then the auth rules (`pdu.AuthError` now carries Synapse's exact text,
  status and MSC3848 errcode; `Inexact` ones fall back). Membership:
  `assert_params_in_dict`, `UserID.from_string` (its TypeErrors are 500s),
  the 3pid params check, blocked rooms, `Cannot invite this user`,
  `Invites have been disabled on this server`, then the old-membership
  refusals.
- **Python's words need Python's values**: jsonschema messages and some
  others hold `repr()` of the decoded JSON (`pyRepr`: quotes, `\x`/`\u`
  escapes, `True`/`None`), and `best_match` (jsonschema 4.26) picks the
  shallowest error, then the *greatest* path among siblings. Python's
  `Sequence` makes a string an array (`m.mentions.user_ids: "x"` passes); a
  non-string in `user_ids` is a TypeError, Synapse's 500. `1E400` is a float
  infinity that cannot be built into an event: a 500 before any "Bad JSON
  value: float". Content 127 levels deep (the content object counted)
  cannot be built into an event, Rust's serde_json stopping at 128 with the
  event around it: `400 M_BAD_JSON "Invalid event JSON"`, before any
  canonical JSON refusal. Python decodes and encodes up to ~9,996 levels;
  past 9,000 the request is main's. Lone surrogate escapes and integers past
  4300 digits fall back too. `internal/creator/testdata/oracle_validator.py`
  runs Synapse's own validator in its container (read-only) and its output is
  the test vectors; regenerate it rather than guessing.
- **Every kind of requester whose token is in the database is served**:
  shadow-banned (a non-member event or an invite is pretended, after
  Synapse's 1-10 s pause, with `generate_fake_event_id`'s `$` and 43
  letters, remembered for a retried transaction as `HttpTransactionCache`
  would; other membership goes through, per-room profiles dropped),
  suspended (MSC3823's `M_USER_SUSPENDED` refusals: only leaving, and
  redacting one's own events), guests (no redactions, only join and leave,
  joins where `guest_access` is `can_join`), puppet tokens (the puppet's
  owner is `authenticated_entity`) and tokens without a device (no txn
  lookup, no `device_id` in the metadata).
- **`PUT /state/m.room.member/{user}` is membership** (the state servlet
  calls `update_membership` with the body as content and its membership as
  the action): Element's per-room nicknames. Per-room `displayname` and
  `avatar_url` are served (`len(v or "")` checks, a TypeError being a 500;
  with `max_avatar_size` or `allowed_avatar_mimetypes` set the avatar is
  the media repository's to judge, and main's), and the global profile fills
  in only what the content leaves out. `update_membership_locked`'s own
  no-op (same sender, membership, and content compared as Python dicts, so
  `true == 1`) answers with the old event (`membership_noop`); for the
  membership servlet it never matches, the content having no membership.
- **Remote invitees' profiles are asked of their server** (the one outbound
  federation request: `internal/fedclient`, goinbound's client). Synapse's
  `make_query`, one field at a time (10 s), and a failure ends the lookups,
  swallowed by `update_membership_locked`; whatever the server answers is
  the content (a float there is then the canonical JSON refusal). Outbound
  connections obey `federation_ip_range_blocklist` (Synapse's default
  private ranges, `ip_range_whitelist`) and `federation_domain_whitelist`.
  Synapse asks once too: its retry loop retries only `if retries_left and
  not timeout`, and `make_query` passes `timeout=10000`. The
  container needs outbound HTTPS for it. The invite itself is the events
  writer's to send: an invitee on a server that does not exist is its 502,
  relayed (`Can't connect to server nonexistent.invalid`, an e2e step).
- **Knocks on rooms this server is in are served**: `KnockRoomAliasServlet` is POST only, refuses guests
  (`M_GUEST_ACCESS_FORBIDDEN`), needs a body, and keeps only `reason`; the
  knocker's profile is added as for a join, and the auth rules judge it. A
  knock on a room we are not in is a remote knock, main's, and so is an
  alias.
- **Delayed events are added here** (MSC4140, `DelayedEventsHandler.add`,
  v1.164.1). Since v1.164 an event that could not be sent now is refused, not
  stored: `create_event`'s checks, the event built at the current state, and
  the auth rules (`sendableNow`); a change to one's own membership is left to
  the rules when it is sent. The delay parameter is any integer, refused when
  msc4140 is off (403) and then when not positive:
  `?org.matrix.msc4140.delay=` on `/send` and `/state` (member events too,
  as sent, before the member path), after the servlet's JSON, `ts` and
  sticky parsing. `rc_message` is counted in this process (`update=True`,
  unlike the event path's pre-check), with the user's
  `ratelimit_override` rates; then `max_event_delay_duration`,
  `validate_builder`, and `add_delayed_event_txn` (per-user limit, its
  429 with `retry_after_ms` possibly null) writing `delayed_events`. Main, which schedules and sends delayed
  events, is told the next send time
  (`/_synapse/replication/added_delayed_event/`) and creates the event
  when due. Cancel, restart and send (`/delayed_events/...`) are not
  routed here.
- **Writer-side refusals** (`persist_and_notify_client_events`): redacting
  the create event (403), an event of another room (400) or a server ACL
  (403) is refused here, never sent. The writer ratelimits (`rc_message`)
  before those checks, so a refusal here is not counted there. A redaction
  of an event we do not hold is sent: the writer checks nothing either,
  unless the sender lacks redact power. Then (`check_redaction` answering
  True: any v3+ room, or v1/v2 on the same server) the writer refuses an
  unknown or rejected target (404 `Could not find event`) and another
  sender's event (403 `You don't have permission to redact events`), after
  the auth rules, which let it through (`pdu.Event.RedactOwnOnly`). Missed
  until 2026-10-26: shadow's refusal check caught one in a matrix.org room
  (`accepted`).
- **Presence, not `.get`**: jsonschema and `key in content` treat a present
  `null` as present and invalid; Python's `.get` (`pdu.Get`) reads it as
  absent. Use `pdu.Field` for the former (`m.mentions: null` is Synapse's
  400).
- **Read-your-writes holds only on the creating process.** Synapse's creator
  waits for the writer's streams before answering (`replication/http/_base.py`);
  other processes, main included, catch up over replication. A `GET /event`
  on main right after a native redaction can still show the old content for
  a few milliseconds. The e2e suite polls; that is Synapse's model whenever
  creator and reader differ (av-client-events-1 vs the request workers here).
- **Redactions**: in v11 `redacts` goes into the content (the servlet does
  it); a body naming another `redacts` is Synapse's 400. `recheck_redaction`
  goes in internal metadata when the auth rules set it. Targets that are the
  create event, a server ACL, unknown, or in another room fall back: the
  writer would refuse the first three, and a redaction of a room's create
  event is never sent from here. `msc3912 with_relations` and a set
  `redaction_allowed_period` fall back.

## End-to-end suite

`cmd/goevent-e2e` drives a hostname as a client, as the test account, in one
of its test rooms (`testRooms`), and checks statuses, errcodes, events read back
(`unsigned.transaction_id` proves our device and txn reached the writer), and
the worker's counters:

```sh
GOEVENT_E2E_PASSWORD=... GOEXPERIMENT=jsonv2 go run ./cmd/goevent-e2e \
  -base https://goevent.aguiarvieira.pt -metrics http://<container ip>:9200/metrics
```

Every refusal's body is compared with Synapse's recording,
`cmd/goevent-e2e/refusals.golden.json` (run IDs, the account and event IDs
normalised). `-record` rewrites it: record only against a hostname whose
event-creation paths go to Synapse itself. The negative-delay body was updated
by hand for v1.164 (2026-10-28).

History, on goevent-creator, whose shadow mode was Synapse's own behaviour
and calibrated the expectations: 57/57 on 2026-10-19, rising to 76/76 fully
native with the fork and per-room name steps, every native event verified
and 26 refusal bodies equal to Synapse's.

It never sends a valid tombstone: that would mark the room as replaced in
every client. Membership steps use `@goevent-e2e-probe` (no account, no
profile) as the target, and the test account only joins: **it must never
leave TEST7TESTE, which is invite-only**.
