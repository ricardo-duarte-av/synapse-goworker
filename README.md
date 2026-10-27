# synapse-goworker

One Go process for the Synapse work that four separate Go workers did:

| Work | Was | Endpoints |
|---|---|---|
| Client sync | [synapse-gosync-worker](https://github.com/ricardo-duarte-av/synapse-gosync-worker) | `/sync`, simplified sliding sync (MSC4186), `/events`, `initialSync` |
| Inbound federation | [synapse-goinbound-worker](https://github.com/ricardo-duarte-av/synapse-goinbound-worker) | `PUT /_matrix/federation/v1/send/{txnId}` |
| Federation reads | [synapse-gopro-worker](https://github.com/ricardo-duarte-av/synapse-gopro-worker) | `/_matrix/federation/v1/{event,state,state_ids,get_missing_events}` |
| Outbound federation | [synapse-gofederation-worker](https://github.com/ricardo-duarte-av/synapse-gofederation-worker) | none; sends transactions |

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

All four are ported and served by one process from one config file
(`deploy/goworker.example.yaml`). Each component still has its own Redis
subscriber, caches and homeserver.yaml reader; sharing those is the next
step.

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
