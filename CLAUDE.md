# Working on synapse-goworker

The merge of gosync, goinbound, gofederation and gopro into one process.
README.md says what it is. This file is for what is not derivable from the
code.

The approved plan is in
`/home/daedric/.claude/plans/pasted-content-id-eae5-recently-we-silly-simon.md`.

## Where things are

- Synapse source: `/home/daedric/synapse`. Port against the deployed tag
  (`git show v1.163.0:<path>`), not the checkout, which runs ahead.
- The four workers being merged, and their working notes, which hold the traps
  and measurements behind nearly every rule. Read the relevant one before
  porting from it:
  - `/home/daedric/synapse-gosync-worker` (CLAUDE.md, docs/)
  - `/home/daedric/synapse-goinbound-worker` (CLAUDE.md)
  - `/home/daedric/synapse-gofederation-worker` (CLAUDE.md, docs/)
  - `/home/daedric/synapse-gopro-worker` (CLAUDE.md, 2,000 lines; §4–§9)
- Later: `/home/daedric/synapse-goevent-creator`.

## Layout

| Path | What |
|---|---|
| `cmd/goworker` | main: one config, four components, one socket, the metrics listener, homeserver.yaml re-read on REPLICATE/SIGHUP |
| `internal/platform` | shared: `bus` (the one Redis subscription, a queue per part), `statecache` (the one state cache), `cache` (the byte-bounded LRU), `config` (goworker.yaml), `synapsecfg` (homeserver.yaml: the one reader, `Source` the one copy), `proxy` (to main), `httpserve` (listener, router), `limits` |
| `internal/sync` | gosync, as `app.New` + `Register` |
| `internal/fedread` | gopro, as `app.New` + `Handler` |
| `internal/inbound` | goinbound, as `app.New` + `Handler` |
| `internal/sender` | gofederation, as `app.New` + `Run`, gated by `SetSynapse` |
| `cmd/*sweep` | offline tools; they generate traffic, nothing judges it |
| `grafana/gen_dashboard.py` | the dashboard; never edit the JSON |
| `docs/cutover.md` | moving a homeserver over |

## Not shared yet

Each component still has its own copy of these, which work but duplicate:

- **Smaller caches of immutable data.** State groups' full state is one
  cache now (`platform/statecache`, shared by fedread and inbound, armed and
  purged by fedread's replication consumer). Still per component: event ->
  state group (sync, fedread), filtered state (sync's carries membership and
  visibility, fedread's only event IDs), room versions, server keys (fedauth
  in inbound and fedread, inbound's keyring).
- **Destination backoff**: inbound's `backoff` (write mode's outbound
  requests) and the sender's `retry`. They agree through Synapse's
  `destinations` table and REMOTE_SERVER_UP, as separate workers did.
- **Read pools**: sync, fedread and the sender each open a read pool.

## Decisions that are easy to undo by accident

- **No component switches** (operator, 2026-10-26). The worker serves whatever
  nginx sends it and sends federation only while homeserver.yaml lists its
  `instance_name` in `federation_sender_instances`. Not being listed is a
  normal state. Work is given or taken away by editing nginx or homeserver.yaml,
  never this worker's config. A missing capability (a role we cannot create,
  a schema we cannot use) proxies that path to main with a warning; it does not
  stop the worker starting.
- **No shadow, canary or compare modes.** Every endpoint is native. Proxying to
  main exists only as the fallback for what we do not reproduce.
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
  gofed_, gopro_), so Prometheus history and alerts continue across the
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
