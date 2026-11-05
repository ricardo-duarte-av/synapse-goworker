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

6. **Typing** (the first stream goworker writes). Unlike everything above,
   this changes what every Synapse process believes, so it takes a restart
   of all of them, and so does moving it back.
   - Give goworker its replication listener (`listen.replication`, a socket
     under `/var/sockets` that Synapse's uid can open) and deploy it. Check
     its log: "the replication listener is open, and instance_map does not
     name this process" is expected at this point.
   - homeserver.yaml: add goworker to `instance_map` (`path:` that socket)
     and set `stream_writers.typing: [<goworker's instance_name>]`.
   - Restart every Synapse process (the old writer, e.g. `av-edu-worker`,
     included). Main's REPLICATE makes goworker re-read the file and start
     writing ("writing the typing stream" in its log,
     `goworker_typing_writer` 1); point nginx's
     `rooms/.*/typing` location at goworker and reload straight after.
     Between the restart and the reload, typing requests fail with 500, as
     they do on any Synapse instance that is not the writer.
   - Restart goworker once more: inbound's EDU routing is read at start, and
     until then it hands remote typing to the old writer, which forwards it.
   - Check: "Streams written" and "Typing writer" on the dashboard (RDATA
     published, requests `started`/`stopped`, EDUs `applied`); a Python
     sync worker and goworker's own sync both show typing; main's log has
     no "Failed to fetch" for typing.

   Clients holding a sync token from the old writer miss typing for at most
   one sync: Synapse replaces the typing key with the new writer's serial.

   To move it back, set `stream_writers.typing` to the old writer, restart
   every Synapse process and goworker, and point the location back.

7. **Receipts** (after typing; the replication listener and `instance_map`
   entry are already there). goworker takes the role only when it starts.
   - homeserver.yaml: `stream_writers.receipts: [<goworker's
     instance_name>]`, alone. account_data stays where it is: goworker
     stores `m.fully_read` on that writer over replication.
   - Restart every Synapse process (the old writer, e.g. `av-edu-worker`,
     included), then **goworker straight after**. Its log says "writing
     the receipts stream" with the position (the sequence's `last_value`).
     Until goworker is back, receipt requests fail with 500, as on any
     instance that is not the writer.
   - nginx: point both `rooms/.*/receipt` and `rooms/.*/read_markers` at
     goworker, and reload. `read_markers` must move too: it writes `m.read`
     receipts, which only the writer can.
   - Check:
     - "Receipts writer" on the dashboard: requests `stored`, "Receipts
       stored" `local new`, and "Stream IDs" with nothing left unfinished;
     - `stream_positions` has `(receipts, <goworker>)` following
       `receipts_sequence`, and no row for the old writer;
     - another user sees your read receipt through a Python sync worker and
       through goworker;
     - your unread count drops after reading (main's push summaries);
     - receipts from remote servers arrive (EDUs `handled`).

   To move it back, set the list to the old writer, restart every Synapse
   process and goworker, and point both locations back. The old writer
   starts again from the sequence, as Synapse does when a writer is added
   back.

8. **Account data** (after receipts; same model). goworker takes the role
   only when it starts.
   - homeserver.yaml: `stream_writers.account_data: [<goworker's
     instance_name>]` (Synapse allows exactly one). If homeserver.yaml
     lists `modules`, check none registers `on_account_data_updated`: those
     callbacks run on the writer, and goworker runs none.
   - Restart every Synapse process (the old writer, e.g. `av-edu-worker`,
     included), then **goworker straight after**. Its log says "writing
     the account_data stream" with the position. Until goworker is back,
     account data writes fail with 500 on every process.
   - nginx: point both `.*/account_data` and `.*/tags` at goworker, and
     reload.
   - Check:
     - "Account-data writer" on the dashboard: requests `ok`, writes by
       caller (`client`, `replication` from main, `local` for read
       markers), and "Caches stream" lines `sent`;
     - `stream_positions` has `(account_data, <goworker>)` following
       `account_data_sequence`, and no row for the old writer;
     - a tag or `m.direct` set in one client shows in another session;
     - ignoring a user hides their new messages, and un-ignoring shows
       them again (the ignored-user cache on every process is dropped
       through the caches stream);
     - `m.fully_read` still moves (now stored in-process).

   To move it back, set the list to the old writer, restart every Synapse
   process and goworker, and point both locations back.

9. **To-device** (after account data; same model). goworker takes the role
   only when it starts. Encryption depends on it: a lost to-device message
   is a message some device cannot decrypt.
   - homeserver.yaml: `stream_writers.to_device: [<goworker's
     instance_name>]` (Synapse allows exactly one).
   - Restart every Synapse process (the old writer, e.g. `av-edu-worker`,
     included), then **goworker straight after**: its log says "writing
     the to_device stream" with the position, and inbound's EDU routing,
     read at start, then hands remote to-device EDUs to it in-process.
     Until goworker is back, sendToDevice fails with 500.
   - nginx: point `^/_matrix/client/(r0|v3|unstable)/sendToDevice/` at
     goworker, and reload.
   - Check:
     - "To-device writer" on the dashboard: requests `ok`, messages
       `local -> local` and `local -> remote`, EDUs `in handled` and
       `out queued`;
     - `stream_positions` has `(to_device, <goworker>)` following
       `device_inbox_sequence`, and no row for the old writer;
     - a new login decrypts history (key sharing), and a verification
       between two sessions completes (both are to-device);
     - messages to another server's users leave: "Outbound federation"
       EDUs by type `m.direct_to_device`.

   To move it back, set the list to the old writer, restart every Synapse
   process and goworker, and point the location back.

10. **Push rules** (same model). goworker takes the role only when it
    starts.
    - homeserver.yaml: `stream_writers.push_rules: [<goworker's
      instance_name>]` (Synapse allows exactly one).
    - Restart every Synapse process (the old writer, e.g. `av-edu-worker`,
      included), then **goworker straight after**: its log says "writing
      the push_rules stream". It also loads push_rules.id and
      push_rules_enable.id from the table then, so the old writer must not
      write after that.
    - nginx: point `^/_matrix/client/(api/v1|r0|v3|unstable)/pushrules/`
      at goworker, and reload.
    - Check:
      - "Push-rules writer" on the dashboard: requests `ok`, writes;
      - `stream_positions` has `(push_rules_stream, <goworker>)` following
        `push_rules_stream_sequence` once something is written;
      - a notification setting changed in a client (a room muted, a
        keyword) shows in another session, and takes effect.

    To move it back, set the list to the old writer, restart every Synapse
    process and goworker, and point the location back.

11. **Device lists** (goworker as the writer; the client endpoints stay
    where nginx sends them, and call it over replication). Encryption
    across federation depends on it: a remote server with a stale device
    list cannot decrypt our users' messages, and nothing here reports it.
    - homeserver.yaml: `stream_writers.device_lists: [<goworker's
      instance_name>]`, alone.
    - Restart every Synapse process (the old writer, e.g. `av-edu-worker`,
      included), then **goworker straight after**: its log says "writing
      the device_lists stream"; inbound's EDU routing, read at start, then
      hands `m.device_list_update` and `m.signing_key_update` to it
      in-process. It takes over the conversion position the old writer left
      in `device_lists_changes_converted_stream_position`.
    - Check:
      - "Device-lists writer" on the dashboard: "Conversion lag" near zero
        and not growing; outbound pokes `poked`; remote EDUs `applied` and
        `resync`; "Stale remote device lists" falling or steady;
      - `stream_positions` has `(device_lists_stream, <goworker>)` following
        `device_lists_sequence`;
      - log in on a new device here, send to a room with users on another
        server, and have one of them decrypt it;
      - a remote user's new device shows in `/keys/query` here.

    The client endpoints (`delete_devices`, `devices`, the `keys/` uploads)
    move to goworker with phase 6's stage 3; until then they keep working
    wherever they are.

    To move it back, set the list to the old writer, restart every Synapse
    process and goworker.

## Afterwards

- Remove the old containers.
- The `gofederation.*` rows keyed by `av-gofederation-worker` and the old
  `goinbound-worker` lock rows are no longer used and can be deleted.
