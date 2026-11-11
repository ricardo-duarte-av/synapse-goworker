# synapse-goworker

One Go process for the Synapse work that five separate Go workers did:

| Work | Was | Endpoints |
|---|---|---|
| Client sync | [synapse-gosync-worker](https://github.com/ricardo-duarte-av/synapse-gosync-worker) | `/sync`, simplified sliding sync (MSC4186), `/events`, `initialSync` |
| Inbound federation | [synapse-goinbound-worker](https://github.com/ricardo-duarte-av/synapse-goinbound-worker) | `PUT /_matrix/federation/v1/send/{txnId}` |
| Federation reads | [synapse-gopro-worker](https://github.com/ricardo-duarte-av/synapse-gopro-worker) | `/_matrix/federation/v1/{event,state,state_ids,get_missing_events}` |
| Outbound federation | [synapse-gofederation-worker](https://github.com/ricardo-duarte-av/synapse-gofederation-worker) | none; sends transactions |
| Federation ping (MSC4524) | new; Synapse has none | `POST /_matrix/federation/{v1,unstable/uk.timedout.msc4524.tabletennis}/{ping,pong}` |
| Event creation | [synapse-goevent-creator](https://github.com/ricardo-duarte-av/synapse-goevent-creator) | `rooms/{id}/send`, `/state`, `/redact`, the membership verbs, `join/`, `knock/`, `profile/` (writes) |

The old repositories are the archive: their history, their docs and the
measurements behind each decision stay there.

## What it does is decided outside it

There is no setting that turns parts of the worker on or off.

- **nginx decides which requests it serves.** It handles every endpoint above
  that reaches it, and proxies anything else to Synapse's main process.
- **homeserver.yaml decides whether it sends federation.** It always follows
  the replication stream, and sends only while its `instance_name` is listed in
  `federation_sender_instances`. homeserver.yaml is re-read at runtime, so
  listing or unlisting it and restarting main is enough.

## Status

All five are ported and served by one process from one config file
(`deploy/goworker.example.yaml`), sharing one Redis subscription, one
homeserver.yaml reader, one access-token check and one state cache. The
event creator's notes are in [docs/creator.md](docs/creator.md).

## Running

`docker-compose.yaml` runs it beside the Synapse workers (uid 991, the
`npm-nw` networks, `/var/sockets`, Synapse's config directory read-only);
its header has the first-start steps.

Moving a homeserver over, one piece at a time: [docs/cutover.md](docs/cutover.md).

```sh
goworker -config /data/goworker.yaml -check   # connect everything, say whether it would send, exit
goworker -config /data/goworker.yaml
```

One socket serves both client and federation paths; point nginx's
`location` blocks for the endpoints above at it. Anything else that reaches
it is proxied to `upstream` (Synapse's main process). The metrics listener
serves `/metrics`, `/health` and `/debug/pprof`.

`listen.replication`, when set, opens a third listener: the address
homeserver.yaml's `instance_map` gives `instance_name`, where other Synapse
processes call this one over HTTP replication (`/_synapse/replication/...`,
checked against `worker_replication_secret`, and `/health`): for the
streams this worker writes, `get_repl_stream_updates`, and for typing,
`fed_send_edu`. Keep it off nginx. At start and on every re-read of
homeserver.yaml the worker logs where the two disagree.

### Typing

While homeserver.yaml's `stream_writers.typing` names `instance_name`, the
worker is Synapse's typing writer: it serves
`PUT /_matrix/client/*/rooms/{roomId}/typing/{userId}`, takes typing EDUs
from other servers, and publishes the typing stream every Synapse process
follows. It needs redis, the replication listener, and an `instance_map`
entry. A typing request while it is not the writer goes to main. See
docs/cutover.md.

### Receipts

When goworker starts while `stream_writers.receipts` names `instance_name`
alone, it is Synapse's receipts writer: it serves
`POST /_matrix/client/*/rooms/{roomId}/receipt/{type}/{eventId}` and
`POST .../rooms/{roomId}/read_markers` (the `m.fully_read` marker is stored
on the account-data writer), takes receipt EDUs from other servers, and
writes and publishes the receipts stream. A change to the list is taken at
the next start of goworker, not on a re-read. See docs/cutover.md.

### Account data

When goworker starts while `stream_writers.account_data` names
`instance_name`, it is Synapse's account-data writer: it serves global and
room account data (`PUT`/`GET .../user/{userId}[/rooms/{roomId}]/account_data/{type}`,
and MSC3391's `DELETE` under `unstable/org.matrix.msc3391` when enabled) and
room tags (`.../rooms/{roomId}/tags[/{tag}]`); answers the six replication
endpoints other Synapse processes call on the writer (room upgrades, server
notices, read markers stored elsewhere); writes and publishes the
account_data stream; and puts the ignored-user invalidations on the caches
stream. Module `on_account_data_updated` callbacks do not run. As for
receipts, the role is taken at start only. See docs/cutover.md.

### To-device

When goworker starts while `stream_writers.to_device` names `instance_name`,
it is Synapse's to-device writer: it serves `PUT /_matrix/client/*/sendToDevice/{type}/{txnId}`
(with Synapse's transaction cache and `rc_key_requests`), takes
`m.direct_to_device` EDUs from other servers, and writes and publishes the
to_device stream (`device_inbox` for our devices, `device_federation_outbox`
for other servers, whose destinations go to our federation sender
in-process). Delivering and deleting messages stays with sync. As for the
other stream-row writers, the role is taken at start only. See
docs/cutover.md.

### Push rules

When goworker starts while `stream_writers.push_rules` names
`instance_name`, it is Synapse's push-rules writer: it serves PUT, DELETE
and GET under `/_matrix/client/*/pushrules/`, answers `copy_push_rules`
(main's room upgrade), and writes and publishes the push_rules stream. The
role is taken at start only. See docs/cutover.md.

### Device lists

When goworker starts while `stream_writers.device_lists` names
`instance_name` alone, it is Synapse's device-lists writer: the five
replication endpoints other processes call on it, the device_lists stream,
the outbound pokes that tell other servers of our users' device changes,
and the remote device lists (their EDUs, resyncs over federation, and the
retry of stale ones). The client endpoints that change devices and keys
are not native yet. See docs/cutover.md.

### Presence

goworker answers `GET` and `PUT /_matrix/client/*/presence/{userId}/status`
itself. With `presence.enabled: false` it does what Synapse does then: GET
is always `offline`, PUT is `rc_presence.per_user` and its body's checks,
and nothing is stored. When goworker starts while `stream_writers.presence`
names `instance_name`, it is also Synapse's presence writer: the presence and
presence_federation streams and the replication endpoints other processes
call on it. Disabled, it marks offline at start any state left from when
presence was enabled. Enabled (aguiarvieira.pt since 2026-11-11), it runs
Synapse's PresenceHandler: the state machine and its timers, per-device
states, syncs on this process and USER_SYNC from others, incoming presence
EDUs, joins, and the fan-out to other servers through goworker's own sender,
pings every 25 minutes included. It refuses the enabled role unless it is the
only federation sender and no module or presence router is configured; the
client endpoints then go to main. Presence follows Synapse exactly, cost
included (operator, 2026-11-11). See docs/cutover.md.

### Federation ping (MSC4524)

[MSC4524](https://github.com/matrix-org/matrix-spec-proposals/pull/4524)
checks federation both ways: we ask another server a question, it answers,
then confirms the answer to us in a signed request of its own. Synapse does
not implement it; main answers the paths 404 `M_UNRECOGNIZED`, which the MSC
reads as "disabled", so routing the four paths to the worker is what enables
it. Pings are limited to one per origin every 30 s, and an unsigned one also
to one per client address (the last X-Forwarded-For entry, nginx's); each
pong waits 2-5 s. The `location`, before any broader federation one:

```nginx
## MSC4524 federation ping/pong: goworker (on main they are 404, disabled)
location ~ ^/_matrix/federation/(v1|unstable/uk\.timedout\.msc4524\.tabletennis)/(ping|pong)/?$ {
	include /data/nginx/custom/matrix-config.conf;
	proxy_pass http://av-matrix-goworker;
}
```

To ping a server, from wherever the metrics listener is reachable:

```sh
curl -X POST 'http://<metrics addr>/debug/federation/ping?server=example.org&wait=30s'
```

The JSON report lists each attempt (signed, then unsigned if the signature
was refused; the unstable path, then the stable one if unrecognised), the
other server's answer and details, whether its pong arrived, and a one-line
conclusion. The pong is only received if nginx routes `.../pong` here.

`homeserver.yaml` is re-read when a Synapse process restarts (it announces
that with REPLICATE on the bus), or on SIGHUP. Add `instance_name` to
`federation_sender_instances` and restart main to make this process the
federation sender; remove it and restart main to stop. An edit alone does
nothing, because Synapse's own senders only read the file at start.
`goworker_sender_active` says which it is.

## Building

```sh
GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test -race ./...
```

`GOEXPERIMENT=jsonv2` is required: mautrix's `federation/pdu` only exists with
it.
