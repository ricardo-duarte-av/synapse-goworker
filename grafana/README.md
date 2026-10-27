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

Prometheus must scrape the metrics listener; see [Prometheus](#prometheus).

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
| `goinbound_edus_total{path,...}`, `goinbound_receipt_rooms_removed_by_acl_total{path}` | the same without `path`, which was always `native` once shadow went |
| `gopro_state_prev_content_emitted_total` | gone: it measured a blind spot of the shadow's /state digest, which is no longer computed |

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

## Prometheus

### Scrape job

Add to the `scrape_configs:` of `prometheus.yml` and reload Prometheus
(`kill -HUP`, or `POST /-/reload` with `--web.enable-lifecycle`). The target
is `listen.metrics.addr` from goworker.yaml, reached by container name over a
network Prometheus shares with the worker (`npm-nw` with the compose file):

```yaml
scrape_configs:
  - job_name: goworker
    scrape_interval: 15s       # $bucket's shortest useful window is 2x this
    scrape_timeout: 10s
    metrics_path: /metrics
    static_configs:
      - targets: ["av-goworker-1:9200"]
        labels:
          deployment: aguiarvieira.pt
      # One target per worker; the dashboard's $instance picks between them.
      # - targets: ["mb-goworker-1:9200"]
      #   labels:
      #     deployment: matrixbridg.es
```

Everything the dashboard needs comes from this one job: the process
collectors (`go_*`, `process_*`) are scraped from the same listener and joined
on `goworker_build_info`. Do not add a `job` or `instance` label of your own
beyond what Prometheus sets; the panels key on those two. The old workers'
jobs (`gosync-worker`, `goinbound-worker`, `gopro-worker`,
`av-gofederation-worker`) can go once their containers do; their history stays
queryable, under the same metric names, by selecting their job.

Check before reloading:

```sh
promtool check config prometheus.yml
```

### Alert rules

Kept outside the dashboard, as before. Save as `goworker.rules.yml`, list it
under `rule_files:` in `prometheus.yml`, and route the `goworker` alerts in
Alertmanager as you like:

```yaml
groups:
  - name: goworker
    rules:
      - alert: GoworkerDown
        expr: up{job="goworker"} == 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "goworker {{ $labels.instance }} is not being scraped"
          description: "Sync, inbound /send, federation reads and (if listed) the federation sender all stop with it; nginx sends their traffic nowhere."

      - alert: GoworkerReplicationDisconnected
        expr: max by (instance) (goworker_bus_connected{job="goworker"}) == 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "goworker {{ $labels.instance }} has lost Synapse's replication stream"
          description: "Every part reads the one Redis subscription. Without it sync stops waking, caches are disarmed and the sender stops picking up events."

      - alert: GoworkerNotSending
        # Only meaningful where homeserver.yaml is meant to list this worker in
        # federation_sender_instances; not being listed is a normal state.
        expr: max by (instance) (gofed_sender_active{job="goworker", instance="av-goworker-1:9200"}) == 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "goworker {{ $labels.instance }} is not sending federation"
          description: "homeserver.yaml does not name its instance_name in federation_sender_instances, or the role was not re-read (it is re-read on REPLICATE and SIGHUP)."

      - alert: GoworkerDegraded
        expr: max by (instance, capability) (goworker_degraded{job="goworker"}) > 0
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "goworker {{ $labels.instance }} hands {{ $labels.capability }} to main"
          description: "A part could not start (its log says why at startup) and its requests are proxied to main, which is single-threaded."

      - alert: GoworkerLimitRefusing
        expr: sum by (instance, limit) (rate(goworker_limit_refused_total{job="goworker"}[10m])) > 0
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "goworker {{ $labels.instance }}: limit {{ $labels.limit }} is refusing work"
          description: "Requests over the limit get 429/503. Raise it in goworker.yaml's limits, or find what is holding slots (Memory & limits row)."

      - alert: GoworkerNearMemoryLimit
        expr: |
          max by (instance) (go_memstats_heap_inuse_bytes{job="goworker"})
            / max by (instance) (goworker_memory_limit_bytes{job="goworker"} > 0) > 0.9
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "goworker {{ $labels.instance }} heap is over 90% of memory.limit_mb"
          description: "The collector is working hard to stay under the limit. Check the cache bytes panel against memory.state_cache_mb."

      - alert: GoinboundStagingUnattended
        # Staged events that are ours to sweep and are not being swept.
        expr: max by (instance) (goinbound_staging_unattended_oldest_seconds{job="goworker"}) > 300
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Staged inbound events are not being processed"
          description: "The oldest unattended staged event is {{ $value | humanizeDuration }} old."

      - alert: GoinboundWriteGivenUp
        # Events write mode dropped after retries.
        expr: sum by (instance) (increase(goinbound_write_events_total{job="goworker", outcome="given_up"}[15m])) > 0
        labels: {severity: warning}
        annotations:
          summary: "Inbound write mode gave up on {{ $value | humanize }} events in 15m"
          description: "See the Inbound federation: Write mode row and the worker's log."
```

A rule over a series that does not exist yet simply does not fire, so a
worker that has had nothing to do raises nothing; no `or vector(0)` is
needed here (it was for Grafana-managed alerts, whose "No data" state must
be set to OK if you keep using those).

Do not alert on `goinbound_staging_oldest_seconds`: a process working through
a big room's missing prevs keeps an event staged for many minutes while it
holds the lock (over 8 for Matrix HQ, 2026-10-16), which is not a fault.

Check before reloading:

```sh
promtool check rules goworker.rules.yml
```
