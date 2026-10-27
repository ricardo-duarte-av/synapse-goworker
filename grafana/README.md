# The goworker dashboard

`goworker.json` is generated. Edit `gen_dashboard.py`, then:

```sh
python3 grafana/gen_dashboard.py > grafana/goworker.json
```

The generator refuses to write a dashboard that queries a metric the code
does not register, which is how panels used to go silently empty after a
rename.

Import it with a Prometheus data source. `$job` and `$instance` come from
`goworker_build_info`; `$bucket` is the rate window (5s–15s windows return
nothing at a 15s scrape interval, so it starts at 1m).

## Rows

| Row | What it answers |
|---|---|
| Overview | Is this process the federation sender? Traffic per part, share answered here vs. by main, replication health, limits refusing. |
| Memory & limits | Heap and RSS against `memory.limit_mb`, cache bytes, goroutines, each limit's slots in use, waits and refusals. |
| Replication bus | The one Redis subscription: lines, reconnects, each part's queue, resyncs. |
| Process | CPU, file descriptors, restarts. |
| Database | Pool saturation and query times. |
| Client sync | /sync traffic, latency, long-polls, token verdicts, caches, replication gaps. |
| Sliding sync | MSC4186 outcomes, connections, writes. |
| To-device & presence | Deletions and presence relays. |
| Inbound /send | Route and reason, EDUs, origins marked up. |
| Inbound PDUs (write mode) | Events written, staging, state resolution cache. |
| Federation reads | Native answers, fallbacks to main, rate limiting, caches. |
| Outbound federation | Routing, transactions, lag, queues, destinations, bookkeeping. |

## Metric names

Each part keeps the prefix it had as a separate worker (`gosync_`,
`goinbound_`, `gofed_`, `gopro_`), so Prometheus history continues across
the cutover and existing alerts keep working. Only what is about the whole
process is new: `goworker_build_info`, `goworker_memory_limit_bytes`,
`goworker_limit_*{limit}`, `goworker_bus_*` (the one replication
subscription), `goworker_degraded` and `goworker_homeserver_yaml_reloads_total`.

Changed in the merge:

| Before | Now |
|---|---|
| `gofed_shadow_mode` | `gofed_sender_active` (1 while homeserver.yaml names this instance) and `gofed_sender_shards` |
| `gopro_requests_total{endpoint,mode,status}` | `gopro_requests_total{endpoint,status}` |
| `goinbound_mode` | gone: there are no modes |
| `gopro_shadow_*`, `gopro_canary_*`, `gopro_difflog_*`, `gopro_auth_verdicts_total`, `gopro_state_diagnoses_total` | gone with shadow and canary |
| `goinbound_shadow_*`, `goinbound_pdu_shadow_*`, `goinbound_pdu_plan_shadow_total`, `goinbound_write_verify_total`, `goinbound_write_test_dropped_total` | gone |
| `gofed_shadow_*` | gone |

The process collectors (`go_*`, `process_*`) carry no label of ours, so every
panel using them joins on `goworker_build_info`; a host runs many exporters
and an unjoined `go_*` sums all of them.

## Panels that mislead

- **Quantiles from few samples.** The sender routes on the order of an event
  a minute, so a p95 over a short window interpolates inside one bucket from
  two or three samples; a stage whose real p95 was 24ms once read 4.2s. Read
  a quantile beside its sample count (shown on the lag panel).
- **Sync latency.** Long-polls dominate the high quantiles by design; a 30s
  p99 is clients' timeouts.
- **Sync cache hit rate reads zero while a cache is disarmed** (replication
  down). Read "Caches armed" beside it.
- **Event processing lag is meaningless during a catch-up replay**; read
  stage time instead.
- **RSS lags the heap after a drop**, and thousands of file descriptors are
  normal for a sender.
- **Several series exist only after their first event** (send duration,
  bookkeeping writes, approximate routes, stage time): an empty panel on a
  quiet worker is not a fault.
- **403 and 404 on federation reads are normal**; watch 502 and `none`.
- **`goinbound_*` and `gofed_*` carry no `server_name`.** That is a label of
  Synapse's own metrics; filter goworker's by `job`/`instance`.

## Alerts

Kept outside the JSON, as before. Both need `or vector(0)` and "No data" set
to OK, or they fire on a worker that has simply had nothing to do:

```promql
# Staged events that are ours to sweep and are not being swept.
max(goinbound_staging_unattended_oldest_seconds) > 300

# Events write mode gave up on.
sum(increase(goinbound_write_events_total{outcome="given_up"}[15m])) > 0
```

Worth adding now that one process does the work of four:

```promql
# The sender role is what homeserver.yaml says; alert if nothing is sending.
max(gofed_sender_active) == 0   # only where this worker is meant to be the sender

# A limit refusing work for a sustained period.
sum by (limit) (rate(goworker_limit_refused_total[10m])) > 0
```

Do not alert on `goinbound_staging_oldest_seconds`: a process working through
a big room's missing prevs keeps an event staged for many minutes while it
holds the lock (over 8 for Matrix HQ, 2026-10-16), which is not a fault.
