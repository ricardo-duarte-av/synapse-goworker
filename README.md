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

Phase 0 of the merge: the skeleton builds; nothing is ported yet.

## Building

```sh
GOEXPERIMENT=jsonv2 go build ./... && GOEXPERIMENT=jsonv2 go test -race ./...
```

`GOEXPERIMENT=jsonv2` is required: mautrix's `federation/pdu` only exists with
it.
