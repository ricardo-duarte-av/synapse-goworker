# Working on synapse-goworker

The merge of gosync, goinbound, gofederation, gopro and goevent-creator into
one process. README.md says what it is. This file is for what is not derivable from the
code.

The approved plan is in
`/home/daedric/.claude/plans/pasted-content-id-eae5-recently-we-silly-simon.md`.

## Where things are

- Synapse source: `/home/daedric/synapse`. Port against the deployed tag
  (`git show v1.164.1:<path>`), not the checkout, which runs ahead. On
  2026-10-28 aguiarvieira.pt ran 1.164.1 and matrixbridg.es 1.164.0. Every
  part was reviewed to v1.164.1 on 2026-10-29 (the creator ported against
  it, the others caught up from v1.159 commit by commit), and to v1.164.2
  on 2026-11-04 (nothing mirrored changed); check
  with `docker exec <synapse container> python -c 'import synapse;
  print(synapse.__version__)'` and diff the tags for what you port.
  `synapse-watch.yaml` says which Synapse paths each part mirrors, the tag
  each was last reviewed against, and the Synapse PRs `handled` past it.
  Add a PR's number there when you port or dismiss it, move a part's
  `reviewed` forward once it is clear (the nightly issue closes when
  nothing is left), and add the Synapse file to the part's list whenever
  you port from one that is not there.
- The five workers merged, and their working notes, which hold the traps
  and measurements behind nearly every rule. Read the relevant one before
  porting from it:
  - `/home/daedric/synapse-gosync-worker` (CLAUDE.md, docs/)
  - `/home/daedric/synapse-goinbound-worker` (CLAUDE.md)
  - `/home/daedric/synapse-gofederation-worker` (CLAUDE.md, docs/)
  - `/home/daedric/synapse-gopro-worker` (CLAUDE.md, 2,000 lines; §4–§9)
  - `/home/daedric/synapse-goevent-creator` (its notes are carried over in
    `docs/creator.md`; the fake policy server stays there)

## Layout

| Path | What |
|---|---|
| `cmd/goworker` | main: one config, five components, one socket, the metrics listener, homeserver.yaml re-read on REPLICATE/SIGHUP |
| `internal/platform` | shared: `bus` (the one Redis subscription, a queue per part), `ratelimit` (Synapse's Ratelimiter, per process), `clientauth` (access tokens, as Synapse's InternalAuth checks them; /whoami for the rest), `statecache` (the one state cache), `fedauth` (X-Matrix verification and the one cache of other servers' keys), `cache` (the byte-bounded LRU), `config` (goworker.yaml), `synapsecfg` (homeserver.yaml: the one reader, `Source` the one copy), `proxy` (to main), `httpserve` (listener, router), `replserver` (the replication listener: what other Synapse processes call at our `instance_map` address, and the tracker of their stream positions), `streamwriter` (publishing the streams we write: RDATA, POSITION, the REPLICATE answer, get_repl_stream_updates), `streamid` (MultiWriterIdGenerator for a stream we write alone), `cachestream` (the caches stream: one writer per process, so every part's rows reach the bus in ID order; started at the table's greatest ID and announced on REPLICATE, as Synapse does), `pyjson` (Python's json: dict semantics on read, Synapse's json_encoder on write), `clientreq` (what every native client endpoint does as Synapse's servlets do), `roomhosts`, `userid`, `limits`; and what inbound and the creator both use: `pdu` (event format and auth rules, the union of both copies), `pdustore` (reading events and state), `stateres`, `push` (rules), `pushcheck` (the push evaluator), `e2ekeys` (local users' end-to-end keys, for client and federation key queries), `acl`, `appservice`, `fedclient`, `replication` (HTTP, to the stream writers and main; each part keeps its metrics) |
| `internal/sync` | gosync, as `app.New` + `Register`; also the client reads (C: `handlers/roomstate.go`, `roomevent.go`, `profile.go`, `keysquery.go`), which hand back to main what they do not reproduce, a federation query included (`Deps.Fallback`) |
| `internal/fedread` | gopro, as `app.New` + `Handler` |
| `internal/inbound` | goinbound, as `app.New` + `Handler` |
| `internal/sender` | gofederation, as `app.New` + `Run`, gated by `SetSynapse` |
| `internal/creator` | goevent-creator, as `app.New` + `Handler` + `Run`; its writes matched on the escaped path ahead of the client mux (`app.Match`); `docs/creator.md` holds its notes |
| `internal/typing` | the typing writer (`TypingWriterHandler`): PUT `.../typing/`, typing EDUs (in-process from inbound, `fed_send_edu` from others), the typing stream; writes while `stream_writers.typing` names `instance_name` |
| `internal/receipts` | the receipts writer: POST `.../receipt/...` and `.../read_markers` (m.fully_read goes to the account-data writer over replication), receipt EDUs, the receipts stream from `receipts_linearized`; writes when `stream_writers.receipts` names `instance_name` alone **at start** |
| `internal/accountdata` | the account-data writer: global and room account data, MSC3391 deletes, tags, the six replication endpoints other processes call on the writer, the account_data stream (three tables), ignored-user invalidations on the caches stream; writes when `stream_writers.account_data` names `instance_name` **at start** |
| `internal/todevice` | the to-device writer: PUT `.../sendToDevice/...` (Synapse's transaction cache, `rc_key_requests`), `m.direct_to_device` EDUs (in-process from inbound, `fed_send_edu`), the to_device stream (`device_inbox`, `device_federation_outbox`; destinations to our sender in-process); writes when `stream_writers.to_device` names `instance_name` **at start** |
| `internal/pushwriter` | the push-rules writer: PUT/DELETE/GET under `/pushrules/`, `copy_push_rules` (room upgrades), the push_rules stream (`push_rules_stream`, as "push_rules_stream" in stream_positions); writes when `stream_writers.push_rules` names `instance_name` **at start** |
| `internal/devicelists` | the device-lists writer (phase 6, stages 1 and 2 of 3): the five replication endpoints, the device_lists stream, the conversion of changes in rooms to outbound pokes (our sender poked in-process), remote device list and signing key EDUs, resyncs over federation with the 30 s retry loop, partial-state rooms; writes when `stream_writers.device_lists` names `instance_name` alone **at start**. Of the client endpoints (stage 3), GET /devices, GET and PUT /devices/{id} and POST /keys/upload are native (one-time keys under inbound's worker locks, so with inbound not writing it goes to main); the rest go to main |
| `internal/fedping` | MSC4524 ping/pong (not in Synapse; main's 404 is its "disabled"); started from the metrics listener's `/debug/federation/ping` |
| `cmd/goevent-e2e` | the creator's live suite, in its test rooms only |
| `cmd/*sweep` | offline tools; they generate traffic, nothing judges it |
| `synapse-watch.yaml`, `cmd/synapsewatch`, `.github/workflows/synapse-watch.yml` | nightly: Synapse release commits (not rcs) since `reviewed` that touch what we mirror, as one issue labelled synapse-watch |
| `grafana/goworker.json` | the dashboard: every metric has a panel; edit in Grafana, export over the file; `grafana/dashboard_test.go` checks it |
| `docs/cutover.md` | moving a homeserver over |
| `docs/creator.md` | the event creator's facts, traps and suite, from goevent-creator |

## Shared, and deliberately not shared

Shared: the Redis subscription (`platform/bus`), state groups' full state and
events' state groups (`platform/statecache`: the first read by fedread and
inbound, the second by fedread and sync; armed and purged by fedread's
replication consumer), and other servers' published keys (`platform/fedauth`
`Keys`, which /send and the federation reads verify with; it reads
server_keys_json through whichever component's pool offers first), and
server name resolutions (`platform/fedclient` `Resolutions`, operator,
2026-11-02: every federation client's; a resolution reached without a
.well-known delegation is kept 5 minutes, not Synapse's hour, and one we
cannot connect through is forgotten at once, after inbound's mautrix cache
pinned continuwuity.org to an unreachable :8448 for a day). Before the
keys were shared, fedread's copy of fedauth lacked inbound's 2026-10-17 fixes
and refused requests Synapse accepted. Since phase 7 also the event code
inbound and the creator had each copied (`pdu`, `pdustore`, `stateres`,
`push`, `acl`, `appservice`, `fedclient`, `replication`): the copies had
diverged both ways (the creator's exact auth error texts, inbound's
canonical escapes and key fixes), and two `pdustore`s registered the same
metrics. Since 2026-11-04 also the push evaluator (`platform/pushcheck`:
inbound checks received events against a snapshot, the creator computes
rows for an event it builds, `CheckOptions` the difference), the
appservice registrations (`platform/appservice`, the one strict parser;
clientauth's RE2 handling is its `Unsupported`), the creator's state-group
reads (`pdustore.CachedStateForGroup`, through `platform/statecache`), and
the federation IP filter (`platform/fedclient` `AllowIP`/`DialControl`:
Synapse's `federation_ip_range_blocklist` for every client, the sender's
dialer and .well-known lookups; mautrix's default was not that list, and
the sender had none).

Still per component, on purpose:

- **Filtered state**: sync's carries membership and visibility, fedread's only
  event IDs. Different values under similar keys.
- **inbound's keyring** (PDU signatures): Synapse's Keyring, per key ID with
  each key's own validity, over server_signature_keys. Not the per-response
  model mautrix's request verification uses; merging them would change what
  either accepts.
- **Destination backoff** (operator, 2026-10-27): inbound's `backoff` is
  Synapse's RetryDestinationLimiter exactly (reads `destinations` on every
  request; a 429 is a failure). The sender's `retry` departs from it on
  purpose: in memory, seeded from the table; a 429 is a cooldown, not a
  backoff; a host that answered with an error is capped at an hour on
  REMOTE_SERVER_UP rather than cleared. One limiter would mean one set of
  rules. They agree as separate workers did: both write `destinations`, and
  inbound's originup publishes REMOTE_SERVER_UP, which the sender hears (it
  carries no instance name, so the sender's echo drop does not catch it).
- **Database pools** (operator, 2026-10-27): one DSN, but a pool per part
  and use, so one part cannot take another's connections (an initial sync
  the sender's). Each read pool is sized on its own (`database.pools.
  sync_read`, `federation_reads_read`, `sender_read`, `creator_read`; unset
  takes `read`).

## Decisions that are easy to undo by accident

- **Access tokens are checked natively** (`platform/clientauth`, operator,
  2026-10-27): access_tokens, the appservice registrations, `used` marked,
  USER_IP published as a worker does. What it does not reproduce goes to
  main's /whoami, and the worker does not hold `macaroon_secret_key`: a
  guest's macaroon, a wrong token, an appservice regex RE2 cannot compile;
  everything, when homeserver.yaml enables MAS, account_validity or any
  module. Before this, /whoami calls from the worker recorded every sync
  client in user_ips as `Go-http-client/1.1` at the socket's path; the
  fallback now forwards X-Forwarded-For and User-Agent.

- **Every request we make sends Synapse's User-Agent** (operator,
  2026-11-02; `platform/useragent`): `Synapse/<main's version>`, learnt from
  main's Server header (probed at start and on each REPLICATE/SIGHUP, and
  seen on every proxied answer). Federation requests send it alone, as
  matrixfederationclient.py does; replication, the presence relay and
  .well-known lookups append homeserver.yaml's `user_agent_suffix`, as
  BaseHttpClient does. A new mautrix `federation.Client` must go through
  `useragent.Client`, or it sends mautrix's. Requests we only forward (the
  proxy, /whoami, inbound's sweep nudge) keep the caller's.
  `useragent.Fallback` (sent until main answers) moves with `reviewed`.
- **No component switches** (operator, 2026-10-26). The worker serves whatever
  nginx sends it and sends federation only while homeserver.yaml lists its
  `instance_name` in `federation_sender_instances`. Not being listed is a
  normal state. Work is given or taken away by editing nginx or homeserver.yaml,
  never this worker's config. A missing capability (a role we cannot create,
  a schema we cannot use) proxies that path to main with a warning; it does not
  stop the worker starting.
- **No shadow, canary or compare modes.** Every endpoint is native. Proxying to
  main exists only as the fallback for what we do not reproduce.
- **The event creator serves every room and every endpoint** (phase 7,
  operator, 2026-10-28). goevent-creator's room allowlist, per-endpoint
  modes and shadow mode existed only for its testing and were not ported;
  its live config was already `rooms: all`, every mode native. It writes as
  Synapse's user through pgcat, not as its old `goevent_creator` role. Its
  one table, `goevent_profile_propagations`, is created at start; a
  propagation it holds is resumed by whichever process starts with the
  creator, so goevent-creator and goworker's creator must never run side by
  side (docs/cutover.md). See docs/creator.md before changing it.
- **mautrix-go first** (operator, 2026-10-26). Use it wherever it matches
  Synapse. Our own code stays only where Synapse and the spec diverge, with
  the divergence written down next to it (goinbound's `event_auth.py` port,
  state resolution with CPython set order, push rules, `pyjson`).
- **`GOEXPERIMENT=jsonv2` is required**, for mautrix `federation/pdu`. The
  gosync, goinbound and gofederation suites passed under it unchanged
  (2026-10-26).
- **Main is the last resort.** It is single-threaded and busy; the fallback
  share should shrink, never grow.
- **Metric names keep their component prefixes** (gosync_, goinbound_,
  gofed_, gopro_, goevent_), so Prometheus history and alerts continue across the
  cutover; only whole-process metrics are goworker_. grafana/README.md has
  what changed.
- **instance_name is new, not an old worker's.** Two processes publishing
  caches rows under one name interleave stream IDs; see docs/cutover.md.
- **The typing writer** (2026-11-04). goworker's first stream: in memory,
  as Synapse's, so it starts at 0 whenever this process becomes the writer
  (start, or a re-read that names it), which followers take for a writer
  restart. Its rows reach our sender in-process (`senderapp.TypingRow`):
  the sender drops our own echo on the bus by instance name, and must keep
  doing so (inbound publishes caches rows under the same name). gosync, which
  drops nothing, reads them from the bus like any Python worker, which makes
  it a live check of what we publish. Positions a caller sends
  (`_INT_STREAM_POS`) are waited for only from writers the bus has shown us
  since start: Synapse knows the rest from the database, and waiting 5 s on
  each would make every call slow. Checked against a throwaway Synapse
  1.164.1 main following us (catch-up after its restart, reset after ours).
- **The receipts writer** (2026-11-05). goworker replaces the old writer;
  it does not join it. Two writers turn the receipt key of every sync token
  into a vector clock, which gosync builds from one aggregate position and
  would step over a receipt the other writer is still committing; and an
  idle writer must keep catching up to the others, or every follower's
  receipts stop. The role is taken at start only: receipts are rows under a
  shared sequence, and taking it on a re-read could write while the old
  writer, not yet restarted, still writes. `platform/streamid` takes the
  writer's position from the sequence, deletes other instances'
  stream_positions rows (as every Synapse process does at start), never
  publishes past an unfinished ID, and finishes failed IDs too. A receipt's
  data is stored as json_encoder writes it (`pyjson.Encode`), not as
  received. Checked against a throwaway Synapse 1.164.1 main: receipts in
  its sync, its unread counts, m.fully_read through its account data, both
  restarts.
- **The account-data writer** (2026-11-05). As receipts: sole writer
  (Synapse allows no other), role at start only. Writes run at Synapse's
  REPEATABLE READ with its five attempts on a serialization failure or
  deadlock. Its quirks are reproduced, not fixed: a remove that finds
  nothing still spends an ID; `instance_name` stays NULL in the three
  tables; a revision INSERT that loses a race to a concurrent first tag
  loses the whole tag write while answering 200 (Synapse catches the
  IntegrityError inside an aborted transaction, and psycopg2's commit of
  it is a silent ROLLBACK, checked against the Synapse image). Ignored-user
  writes put `ignored_by`/`ignored_users` rows on the caches stream through
  the process's one `platform/cachestream` writer, which inbound's
  originup shares: before it, goworker announced nothing on caches until
  its first row, and a process that has seen no POSITION for a stream on
  its connection drops its RDATA (live, the Python workers' announcements
  hid that). Checked against a throwaway Synapse 1.164.1 main: byte-equal
  GETs and errors, its sync and long-poll, ignoring hiding messages on
  main, a room upgrade on main copying tags and m.direct through us, both
  restarts.
- **The to-device writer** (2026-11-05). Same model: sole writer (Synapse
  allows no other), role at start only. Sizes are Synapse's: a message
  over 65536 bytes of `encode_canonical_json` is 413, and EDUs are packed
  by `split_device_messages_into_edus` (`pyjson.Canonical`, canonicaljson
  2.0's encoding, checked against the Synapse image; the partitions
  against Synapse's own splitter, `testdata/split.jsonl`). Every add takes
  a stream ID, the local batch even when empty, and a repeated remote
  `message_id` too. An EDU error Synapse raises as a SynapseError goes back
  over `fed_send_edu` with its status (a `replserver.Error`), since
  `on_edu` does not catch it. Checked against a throwaway Synapse 1.164.1
  main as the reference: the same requests to it as writer and to goworker
  gave identical responses (400, 413 and 500 texts included), IDs spent,
  `device_inbox` rows and outbox EDUs (bar the random `message_id`); then
  delivery in both syncs, main's long-poll, `fed_send_edu` and its
  repeat, both restarts. Native error bodies are rendered by
  `pyjson.Encode` now (encoding/json wrote `>` as `\u003e`).
- **The push-rules writer** (2026-11-05). Same model: sole writer, role at
  start only. push_rules.id and push_rules_enable.id are Synapse's
  in-memory IdGenerators, from MAX(id) at start. Request bodies are judged
  with Python's semantics (`in` on a dict, list or string, iteration of
  a dict's keys), and error texts with Python's `str()`/`repr()`
  (`pyjson.Str`/`Repr`, checked against the image). Synapse stores
  conditions its Rust then cannot parse (a dict or a string where a list
  belongs); from then on that user's rules fail to load, so the GETs are
  500 and `copy_push_rules` fails, and goworker does the same
  (`parses`). `event_stream_ordering` is the events stream's current token
  as a follower holds it. Checked against a throwaway Synapse 1.164.1
  main as the reference writer: 42 requests (errors included) and every
  push_rules, push_rules_enable and push_rules_stream row identical, the
  room-upgrade copy included; main's cache and both syncs follow our
  writes; both restarts.
- **A sender without a cursor starts at MIN(federation_stream_position)**,
  as Synapse does, not at the tip. The tip would drop events at a rename.

## Ground rules carried over from all four

- **Parity is the contract.** A response or a write must be indistinguishable
  from Synapse's. When unsure, defer to Synapse rather than approximate.
- **Never re-encode stored event JSON.** Splice `event_json.json` with sjson.
  14,654 events here contain escaped NULs that `jsonb` cannot even cast.
- **Everything goes through pgcat, as homeserver.yaml says** (operator,
  2026-10-27): one DSN for every pool, Synapse's own user, no narrow roles.
  So pgx must use `QueryExecModeExec` everywhere (DescribeExec breaks behind
  a transaction pooler: "unnamed prepared statement does not exist",
  26000), nothing may rely on session state (`SET LOCAL` inside a
  transaction is fine; a startup `search_path` is not), and every gosync
  table is schema-qualified, because Synapse has tables of the same names
  in public (TestEveryTableIsQualified). That reads only read is held by
  the code and its tests, not by a grant.
- **Read `bytea` as `encode(col, 'hex')`** (2026-11-05). Synapse runs `SET
  bytea_output TO escape` on every connection it opens, and through pgcat
  that session setting stays on the server connection, which pgx then hands
  us: in Exec mode results are text, binary result formats are ignored, and
  pgx cannot decode escape output ("invalid hex format"). It showed as a
  400 on clients' first `/sync` after each Synapse restart (the user
  filter). The five readers are hex-encoded in SQL; a new one must be too
  (`pdustore/bytea_test.go`).
- **Bound caches by size, never by count of things of unknown size.** A big
  room's state is 150k entries (31 MB as a Go map).
- **Never name a metric label `instance` or `job`.**
- **Tests that touch PostgreSQL or Redis use throwaway instances**, never
  `/var/sockets` or `/var/sockets/keydb`. A stray RDATA on the live bus
  invalidates caches in every Synapse process.
- **Most bugs here were silent**: a feature absent, a resource growing, a
  filter never called. When something looks fine, check it is running at all.
