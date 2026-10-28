# Working on synapse-goworker

The merge of gosync, goinbound, gofederation, gopro and goevent-creator into
one process. README.md says what it is. This file is for what is not derivable from the
code.

The approved plan is in
`/home/daedric/.claude/plans/pasted-content-id-eae5-recently-we-silly-simon.md`.

## Where things are

- Synapse source: `/home/daedric/synapse`. Port against the deployed tag
  (`git show v1.164.1:<path>`), not the checkout, which runs ahead. On
  2026-10-28 aguiarvieira.pt ran 1.164.1 and matrixbridg.es 1.164.0; the
  creator is ported against v1.164.1 (sync is not yet: MSC4222's stable
  `use_state_after` and the `state_after` fixes, TODO.md); check
  with `docker exec <synapse container> python -c 'import synapse;
  print(synapse.__version__)'` and diff the tags for what you port.
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
| `internal/platform` | shared: `bus` (the one Redis subscription, a queue per part), `clientauth` (access tokens, as Synapse's InternalAuth checks them; /whoami for the rest), `statecache` (the one state cache), `fedauth` (X-Matrix verification and the one cache of other servers' keys), `cache` (the byte-bounded LRU), `config` (goworker.yaml), `synapsecfg` (homeserver.yaml: the one reader, `Source` the one copy), `proxy` (to main), `httpserve` (listener, router), `limits`; and what inbound and the creator both use: `pdu` (event format and auth rules, the union of both copies), `pdustore` (reading events and state), `stateres`, `push` (rules), `acl`, `appservice`, `fedclient`, `replication` (HTTP, to the stream writers and main; each part keeps its metrics) |
| `internal/sync` | gosync, as `app.New` + `Register`; also the client reads (C: `handlers/roomstate.go` so far), which hand back to main what they do not reproduce (`Deps.Fallback`) |
| `internal/fedread` | gopro, as `app.New` + `Handler` |
| `internal/inbound` | goinbound, as `app.New` + `Handler` |
| `internal/sender` | gofederation, as `app.New` + `Run`, gated by `SetSynapse` |
| `internal/creator` | goevent-creator, as `app.New` + `Handler` + `Run`; its writes matched on the escaped path ahead of the client mux (`app.Match`); `docs/creator.md` holds its notes |
| `cmd/goevent-e2e` | the creator's live suite, in its test rooms only |
| `cmd/*sweep` | offline tools; they generate traffic, nothing judges it |
| `grafana/goworker.json` | the dashboard: every metric has a panel; edit in Grafana, export over the file; `grafana/dashboard_test.go` checks it |
| `docs/cutover.md` | moving a homeserver over |
| `docs/creator.md` | the event creator's facts, traps and suite, from goevent-creator |

## Shared, and deliberately not shared

Shared: the Redis subscription (`platform/bus`), state groups' full state and
events' state groups (`platform/statecache`: the first read by fedread and
inbound, the second by fedread and sync; armed and purged by fedread's
replication consumer), and other servers' published keys (`platform/fedauth`
`Keys`, which /send and the federation reads verify with; it reads
server_keys_json through whichever component's pool offers first). Before the
keys were shared, fedread's copy of fedauth lacked inbound's 2026-10-17 fixes
and refused requests Synapse accepted. Since phase 7 also the event code
inbound and the creator had each copied (`pdu`, `pdustore`, `stateres`,
`push`, `acl`, `appservice`, `fedclient`, `replication`): the copies had
diverged both ways (the creator's exact auth error texts, inbound's
canonical escapes and key fixes), and two `pdustore`s registered the same
metrics.

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
- **Push evaluation**: inbound's `pduverify` checks persisted events against
  a snapshot, the creator's `pushcalc` computes rows for an event it builds.
  Same algorithm, different framing; not merged yet (TODO.md). A change to
  `bulk_push_rule_evaluator` goes into both.
- **clientauth's registration parser**: its own, with the RE2 quirks
  (`quoteSafe`); `platform/appservice` is the creator's and inbound's.

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
- **Bound caches by size, never by count of things of unknown size.** A big
  room's state is 150k entries (31 MB as a Go map).
- **Never name a metric label `instance` or `job`.**
- **Tests that touch PostgreSQL or Redis use throwaway instances**, never
  `/var/sockets` or `/var/sockets/keydb`. A stray RDATA on the live bus
  invalidates caches in every Synapse process.
- **Most bugs here were silent**: a feature absent, a resource growing, a
  filter never called. When something looks fine, check it is running at all.
