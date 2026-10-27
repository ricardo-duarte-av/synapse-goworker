# The goworker dashboard

Import `goworker.json` into Grafana with a Prometheus data source. That is all.

It has every panel of the four dashboards it replaces (gosync, goinbound,
gofederation, gopro), less the shadow, canary and mode panels whose metrics
are gone, plus the process-wide rows and a panel for each metric none of them
showed. A part of the worker that nginx or homeserver.yaml gives no work
shows empty panels; nothing is hidden or switched off.

To change it, edit it in Grafana, export it ("Export for sharing externally"
off), and save the export over `goworker.json`. `go test ./grafana/` checks
the file, and runs in CI:

- every metric a panel queries is registered by the code, so a rename cannot
  leave a panel silently empty;
- every metric the code registers has a panel, so a new metric needs one;
- every panel uses the `${datasource}` variable and only defined variables,
  and no two panels share an id.

Variables: `$job` and `$instance` come from `goworker_build_info`; `$bucket`
is the rate window (windows under 30s return nothing at a 15s scrape
interval). `$sync_endpoint` and `$fedread_endpoint` filter the sync and
federation-read panels. `$synapse_server`, `$synapse_inbound_job` and
`$synapse_reader_job` name Synapse's own processes for the "vs Synapse"
comparison panels.

Prometheus must scrape the metrics listener (`listen.metrics.addr`,
`av-goworker-1:9200` with the compose file); the old workers' scrape jobs can
go once their containers do.

## Rows

The process-wide rows come first: Overview (is this the federation sender,
traffic per part, share answered here vs. by main, replication, limits),
Memory & limits, Replication bus, and Process & Go runtime. Then each part's
rows from its old dashboard, titled by part ("Client sync: Traffic",
"Inbound federation: Write mode", "Federation reads: Caches", "Outbound
federation: Queues and delivery", ...), each ending in an "other metrics" row
for what its old dashboard did not show.

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
