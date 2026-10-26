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

## Ground rules carried over from all four

- **Parity is the contract.** A response or a write must be indistinguishable
  from Synapse's. When unsure, defer to Synapse rather than approximate.
- **Never re-encode stored event JSON.** Splice `event_json.json` with sjson.
  14,654 events here contain escaped NULs that `jsonb` cannot even cast.
- **pgx must use `QueryExecModeExec`.** DescribeExec breaks behind pgcat in
  transaction mode ("unnamed prepared statement does not exist", 26000).
- **Bound caches by size, never by count of things of unknown size.** A big
  room's state is 150k entries (31 MB as a Go map).
- **Never name a metric label `instance` or `job`.**
- **Tests that touch PostgreSQL or Redis use throwaway instances**, never
  `/var/sockets` or `/var/sockets/keydb`. A stray RDATA on the live bus
  invalidates caches in every Synapse process.
- **Most bugs here were silent**: a feature absent, a resource growing, a
  filter never called. When something looks fine, check it is running at all.
