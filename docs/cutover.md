# Moving a homeserver onto goworker

goworker can start next to the old workers and take their work one
piece at a time, because what it does is decided by nginx and
homeserver.yaml, not by its own config. Until a location points at its
socket it serves nothing; until homeserver.yaml names it, it sends nothing.

## Before starting it

1. **Pick a new `instance_name`** (e.g. `av-goworker-1`). Not the old
   goinbound's `write.instance_name` (`goinbound-worker`): while both run,
   two processes publishing caches-stream rows under one name would
   interleave stream IDs, and every Synapse process drops a token at or
   below the last it saw from an instance. Not `av-gofederation-worker`
   either: see step 4.
2. **Database.** Every pool connects as homeserver.yaml does: through pgcat
   (`matrix-pgcat:6432`), as `synapse`. No role needs creating, and pgcat's
   `pool_size` for `synapse` is the real bound on server connections,
   shared with Synapse's own processes. The narrow roles the old workers
   used (`gopro_ro`, `gosync_inbox`, `gosync_ss`, `goinbound_ro/rw`) are no
   longer used and can be dropped once the old workers are gone.
3. **Check it.** `goworker -config goworker.yaml -check` connects every
   pool, reads homeserver.yaml, and logs `sending_federation=false`. On
   matrixbridg.es the same config works with its own homeserver.yaml: its
   database block names its own pgcat pool and user.

Start it. With no nginx location pointing at it and its name absent from
`federation_sender_instances`, it follows the replication stream and
serves nothing.

## Moving the work

Each step is independent and reversible by undoing it.

1. **Federation reads.** Point the `event|state|state_ids|get_missing_events`
   locations at `goworker`. Watch "Federation reads" on the dashboard: the
   native share should match gopro's (~all), fallbacks near zero. Stop
   gopro.
2. **Client sync.** Point the sync locations at `goworker` (gosync's
   second hostname first, if that is still how it is tested). Sliding sync
   connections live in the shared `gosync` schema, so clients keep their
   `pos` across the switch. Stop gosync.
3. **Inbound /send.** Point the `/send` location at `goworker`. Watch
   "Inbound /send": native share as goinbound's, `has_pdus` near zero (it
   means write mode is unavailable), and main's `FederationSendServlet`
   rate flat. Stop goinbound.
4. **Outbound federation.** In homeserver.yaml, replace
   `av-gofederation-worker` with the new `instance_name` in
   `federation_sender_instances`. Then, in a quiet window and in this order:
   - **stop gofederation**: it does not re-read homeserver.yaml, and would
     keep sending alongside goworker;
   - restart main, so Synapse's processes move `federation_stream_position`
     to the new name (at the minimum position, so nothing is skipped);
   - main's REPLICATE as it reconnects makes goworker re-read the file; it
     logs `federation sender role changed by homeserver.yaml`, and
     `gofed_sender_active` turns 1. (SIGHUP does the same without a
     restart.)

   Its first pickup starts where gofederation had got to (the lowest
   `federation_stream_position`), and its backoffs are seeded from
   Synapse's `destinations` table, which gofederation kept in step. Catch-up
   then reconciles every destination owed something. Watch transactions/s
   and "Destinations backing off" against gofederation's last week.

   To move it back, reverse the list in homeserver.yaml, restart main, and
   start gofederation.

5. **Event creation** (phase 7, aguiarvieira.pt first). goworker's creator
   is the goevent-creator that ran there, every endpoint native, every room.
   In one step:
   - **stop `av-goevent-creator-1`** before (or as) goworker with the
     creator starts: both would resume the same unfinished profile
     propagations from `goevent_profile_propagations` at start;
   - deploy goworker, and check `goworker_degraded{capability="creator"}`
     is 0 and its log has no "event creator unavailable";
   - point the seven event-creation locations in `proxy_host/2.conf`
     (`rooms/.*/redact`, `send`, `state/`, the membership verbs, `join/`,
     `knock/`, `profile/`) at goworker's upstream, and drop the
     `$av_room_state_upstream` map from `http_top.conf`: GET and PUT under
     `/state/` both go to goworker now. Check with
     `docker exec npm-npm-1 nginx -t`, then reload.

   Watch "Event creator" on the dashboard: `goevent_native_verify_total`
   mismatches at 0, fallbacks by reason against goevent-creator's last day,
   and run `cmd/goevent-e2e` in the test rooms. Then remove the
   `av-goevent-creator` upstream (before the container: see "Afterwards")
   and the container. The `goevent_creator` database role is unused and can
   be dropped with the old workers' roles; `goevent_profile_propagations`
   stays.

   On matrixbridg.es nothing changes until its nginx sends those paths.

   To move it back, point the locations at main (or start goevent-creator
   again and point them at it).

## Afterwards

- Remove the old containers.
- The `gofederation.*` rows keyed by `av-gofederation-worker` and the old
  `goinbound-worker` lock rows are no longer used and can be deleted.
