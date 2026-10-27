#!/usr/bin/env python3
"""Generates grafana/goworker.json, the one dashboard for goworker.

Edit this file, not the JSON:

    python3 grafana/gen_dashboard.py > grafana/goworker.json

It refuses to emit a dashboard that queries a metric the code does not
register, which is how a panel goes silently empty after a rename.
"""
import json, os, re, sys

DS = {"type": "prometheus", "uid": "${datasource}"}
# Every goworker series carries the scrape job and instance; one binary means
# one job, so the variables select a worker rather than a component.
G = 'job=~"$job", instance=~"$instance"'
R = "[$bucket]"

panels = []
pid = [0]
y = [0]
cur = {"x": 0, "h": 0}
used = set()


def nid():
    pid[0] += 1
    return pid[0]


def flush():
    y[0] += cur["h"]
    cur["x"] = 0
    cur["h"] = 0


def row(title):
    flush()
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"x": 0, "y": y[0], "w": 24, "h": 1}, "panels": []})
    y[0] += 1


def place(w, h):
    if cur["x"] + w > 24:
        flush()
    pos = {"x": cur["x"], "y": y[0], "w": w, "h": h}
    cur["x"] += w
    cur["h"] = max(cur["h"], h)
    return pos


def targets(exprs):
    out = []
    for i, (expr, legend) in enumerate(exprs):
        used.update(re.findall(r'\b(?:gosync|goinbound|gofed|gopro|goworker)_[a-z0-9_]+', expr))
        out.append({"datasource": DS, "editorMode": "code", "expr": expr,
                    "legendFormat": legend, "range": True, "refId": chr(65 + i)})
    return out


def stat(title, desc, exprs, unit="short", w=4, decimals=None, thresholds=None):
    fc = {"unit": unit, "color": {"mode": "thresholds"},
          "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "text", "value": None}]}}
    if decimals is not None:
        fc["decimals"] = decimals
    panels.append({"type": "stat", "title": title, "description": desc, "id": nid(), "datasource": DS,
                   "gridPos": place(w, 4), "fieldConfig": {"defaults": fc, "overrides": []},
                   "options": {"colorMode": "value", "graphMode": "area", "justifyMode": "auto",
                               "orientation": "auto", "textMode": "auto",
                               "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
                   "targets": targets(exprs)})


def ts(title, desc, exprs, unit="short", w=12, h=8, stack=False, decimals=None):
    custom = {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 40 if stack else 10,
              "showPoints": "never", "spanNulls": False,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    d = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if decimals is not None:
        d["decimals"] = decimals
    panels.append({"type": "timeseries", "title": title, "description": desc, "id": nid(), "datasource": DS,
                   "gridPos": place(w, h), "fieldConfig": {"defaults": d, "overrides": []},
                   "options": {"legend": {"displayMode": "table", "placement": "bottom",
                                          "calcs": ["mean", "max", "lastNotNull"]},
                               "tooltip": {"mode": "multi", "sort": "desc"}},
                   "targets": targets(exprs)})


def q(p, metric, by="le"):
    return f'histogram_quantile({p}, sum by ({by}) (rate({metric}_bucket{{{G}}}{R})))'


GREEN_RED = [{"color": "green", "value": None}, {"color": "red", "value": 1}]
# The process collectors carry no label of ours, so they are scoped by joining
# on goworker_build_info: a host runs many exporters, and an unjoined go_*
# sums all of them.
J = f'and on (job, instance) goworker_build_info{{{G}}}'

# ------------------------------------------------------------------ Overview
row("Overview")
stat("Sending federation", "1 while homeserver.yaml names this instance in federation_sender_instances. 0 is normal for a worker that is not the sender.",
     [(f'max(gofed_sender_active{{{G}}})', "sending")], decimals=0,
     thresholds=[{"color": "text", "value": None}, {"color": "green", "value": 1}])
stat("Senders named", "How many federation_sender_instances homeserver.yaml lists: the number of shards destinations are split into.",
     [(f'max(gofed_sender_shards{{{G}}})', "senders")], decimals=0)
stat("Client sync req/s", "", [(f'sum(rate(gosync_requests_total{{{G}}}{R}))', "req/s")], unit="reqps", decimals=1)
stat("Inbound /send req/s", "", [(f'sum(rate(goinbound_requests_total{{{G}}}{R}))', "req/s")], unit="reqps", decimals=1)
stat("Federation reads req/s", "", [(f'sum(rate(gopro_requests_total{{{G}}}{R}))', "req/s")], unit="reqps", decimals=2)
stat("Transactions sent/s", "", [(f'sum(rate(gofed_transactions_total{{{G}}}{R}))', "txn/s")], unit="ops", decimals=2)
stat("/send answered here", "Share of /send transactions answered here rather than by main.",
     [(f'sum(rate(goinbound_requests_total{{{G}, route="native"}}{R})) / sum(rate(goinbound_requests_total{{{G}}}{R}))', "native")],
     unit="percentunit", decimals=1)
stat("Reads answered here", "Share of federation reads answered natively. The rest fell back to main.",
     [(f'sum(rate(gopro_native_served_total{{{G}}}{R})) / sum(rate(gopro_requests_total{{{G}}}{R}))', "native")],
     unit="percentunit", decimals=1)
stat("Sync 5xx", "Share of client sync requests answered 5xx (499, the client hanging up, excluded).",
     [(f'sum(rate(gosync_requests_total{{{G}, status=~"5.."}}{R})) / sum(rate(gosync_requests_total{{{G}}}{R}))', "5xx")],
     unit="percentunit", decimals=2, thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 0.01}])
stat("Replication connected", "Every subscriber this process runs. Below the count of three, something is reconnecting.",
     [(f'max(gosync_replication_connected{{{G}}}) + max(gopro_replication_connected{{{G}}}) + max(gofed_replication_live{{{G}}})', "connected")],
     decimals=0, thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 3}])
stat("Limits refusing (window)", "Requests refused because a limit was full. See Memory & limits.",
     [(f'sum(increase(goworker_limit_refused_total{{{G}}}{R}))', "refused")], decimals=0,
     thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}])
stat("Degraded", "Parts that could not start and whose requests go to main (goworker_degraded). Each one is logged at start.",
     [(f'sum(goworker_degraded{{{G}}}) or vector(0)', "degraded")], decimals=0,
     thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}])
stat("homeserver.yaml reloads (window)", "Changes to homeserver.yaml picked up at runtime. An error keeps the last good copy.",
     [(f'sum(increase(goworker_homeserver_yaml_reloads_total{{{G}}}{R}))', "reloads")], decimals=0)

# ------------------------------------------------------------ Memory & limits
row("Memory & limits")
ts("Heap against the memory limit", "Heap in use and RSS against memory.limit_mb. The collector works harder as the heap nears the limit; RSS lags the heap after a drop.",
   [(f'go_memstats_heap_inuse_bytes {J}', "heap in use"),
    (f'process_resident_memory_bytes {J}', "RSS"),
    (f'max(goworker_memory_limit_bytes{{{G}}}) > 0', "limit")], unit="bytes", w=8)
ts("Cache bytes", "The byte-bounded caches of the federation reads, against their configured maxima.",
   [(f'sum by (cache) (gopro_cache_bytes{{{G}}})', "{{cache}}"),
    (f'sum(gopro_cache_max_bytes{{{G}}})', "sum of maxima")], unit="bytes", w=8, stack=True)
ts("Goroutines and GC", "Goroutines are cheap and spawned freely; a steady climb is a leak, not load.",
   [(f'go_goroutines {J}', "goroutines"),
    (f'rate(go_gc_duration_seconds_count{{{G}}}{R}) {J}', "GCs/s")], w=8)
ts("Slots in use by limit", "Work holding a slot, by limit; a line at its capacity is a limit binding.",
   [(f'sum by (limit) (goworker_limit_in_use{{{G}}})', "{{limit}}"),
    (f'sum by (limit) (goworker_limit_capacity{{{G}}})', "{{limit}} capacity")], w=8)
ts("Wait for a slot (p99)", "Zero waits are observed too, so this is meaningful at low traffic.",
   [(q(0.99, "goworker_limit_wait_seconds", "le, limit"), "{{limit}}")], unit="s", w=8)
ts("Refused by limit", "", [(f'sum by (limit) (rate(goworker_limit_refused_total{{{G}}}{R}))', "{{limit}}")], unit="ops", w=8)

# ------------------------------------------------------------------- Process
row("Process")
ts("CPU", "", [(f'rate(process_cpu_seconds_total{{{G}}}{R}) {J}', "cores")], w=8)
ts("Open file descriptors", "Thousands is normal: one connection per remote server being sent to.",
   [(f'process_open_fds {J}', "fds")], w=8)
ts("Restarts", "", [(f'changes(process_start_time_seconds{{{G}}}{R}) {J}', "restarts")], decimals=0, w=8)

# --------------------------------------------------------- Database
row("Database")
ts("Sender pool connections", "", [(f'sum by (pool, state) (gofed_db_pool_conns{{{G}}})', "{{pool}} {{state}}")], w=8)
ts("Sender query time (p99)", "Queries named 'other' should stay near zero; a hot unnamed query hides there.",
   [(q(0.99, "gofed_db_query_duration_seconds", "le, pool, query"), "{{pool}} {{query}}")], unit="s", w=8)
ts("Sync and inbound queries", "",
   [(f'sum by (query) (rate(gosync_db_queries_total{{{G}}}{R}))', "sync {{query}}"),
    (f'sum by (query) (rate(goinbound_db_query_duration_seconds_count{{{G}}}{R}))', "inbound {{query}}")], unit="ops", w=8)

# --------------------------------------------------------------- Client sync
row("Client sync")
ts("Requests by endpoint and status", "", [(f'sum by (endpoint, status) (rate(gosync_requests_total{{{G}}}{R}))', "{{endpoint}} {{status}}")], unit="reqps", stack=True)
ts("Latency", "Long-polls dominate the high quantiles by design; a 30s p99 is a client's timeout, not slowness.",
   [(q(0.5, "gosync_request_duration_seconds", "le, endpoint"), "p50 {{endpoint}}"),
    (q(0.99, "gosync_request_duration_seconds", "le, endpoint"), "p99 {{endpoint}}")], unit="s")
ts("Waiting long-polls", "", [(f'sum(gosync_sync_waiters{{{G}}})', "waiters")], w=8)
ts("Token verdicts", "", [(f'sum by (verdict) (rate(gosync_auth_verdicts_total{{{G}}}{R}))', "{{verdict}}")], unit="ops", w=8)
ts("Wakeups by stream", "", [(f'sum by (stream) (rate(gosync_notifier_wakeups_total{{{G}}}{R}))', "{{stream}}")], unit="ops", w=8)
ts("Derived cache hit rate", "Reads zero while a cache is disarmed (replication down); read cache_armed beside it.",
   [(f'sum by (cache) (rate(gosync_cache_hits_total{{{G}}}{R})) / (sum by (cache) (rate(gosync_cache_hits_total{{{G}}}{R})) + sum by (cache) (rate(gosync_cache_misses_total{{{G}}}{R})))', "{{cache}}")],
   unit="percentunit", w=8)
ts("Caches armed", "", [(f'min by (cache) (gosync_cache_armed{{{G}}})', "{{cache}}"),
                        (f'min by (stream) (gosync_stream_cache_armed{{{G}}})', "stream {{stream}}")], w=8, decimals=0)
ts("Replication rows and discontinuities", "",
   [(f'sum by (stream) (rate(gosync_replication_rows_total{{{G}}}{R}))', "{{stream}}"),
    (f'sum by (stream, reason) (increase(gosync_replication_stream_discontinuities_total{{{G}}}{R}))', "gap {{stream}} {{reason}}")], unit="ops", w=8)

# ------------------------------------------------------------- Sliding sync
row("Sliding sync")
ts("Responses by outcome", "", [(f'sum by (outcome) (rate(gosync_sliding_sync_responses_total{{{G}}}{R}))', "{{outcome}}")], unit="ops", w=8)
ts("Connections and positions", "", [(f'sum(gosync_sliding_sync_connections{{{G}}})', "connections"),
                                     (f'sum(gosync_sliding_sync_positions{{{G}}})', "positions")], w=8)
ts("Rows written", "", [(f'sum by (table) (rate(gosync_sliding_sync_rows_written_total{{{G}}}{R}))', "{{table}}")], unit="ops", w=8)

# --------------------------------------------------- To-device & presence
row("To-device & presence")
ts("To-device messages deleted", "", [(f'sum(rate(gosync_to_device_deleted_total{{{G}}}{R}))', "deleted")], unit="ops", w=8)
ts("Presence relays", "", [(f'sum(rate(gosync_presence_relays_total{{{G}}}{R}))', "relayed"),
                            (f'sum(rate(gosync_presence_relays_suppressed_total{{{G}}}{R}))', "suppressed"),
                            (f'sum by (reason) (rate(gosync_presence_relay_failures_total{{{G}}}{R}))', "failed {{reason}}")], unit="ops", w=8)
ts("Presence devices tracked", "", [(f'sum(gosync_presence_devices_tracked{{{G}}})', "devices")], w=8)

# ------------------------------------------------------------ Inbound /send
row("Inbound /send")
ts("Transactions by route and reason", "native, proxy (to main) or abandoned (the sender hung up), and why proxied: has_pdus (write mode unavailable), auth_failed, origin_in_backoff, uncertain, db_error, too_large, not_send...",
   [(f'sum by (route, reason) (rate(goinbound_requests_total{{{G}}}{R}))', "{{route}} {{reason}}")], unit="reqps", stack=True)
ts("Latency by route", "", [(q(0.5, "goinbound_request_duration_seconds", "le, route"), "p50 {{route}}"),
                            (q(0.99, "goinbound_request_duration_seconds", "le, route"), "p99 {{route}}")], unit="s")
ts("EDUs by type and action", "", [(f'sum by (edu_type, action) (rate(goinbound_edus_total{{{G}}}{R}))', "{{edu_type}} {{action}}")], unit="ops", stack=True, w=8)
ts("EDU forwards by writer and outcome", "", [(f'sum by (writer, result) (rate(goinbound_replication_send_edu_duration_seconds_count{{{G}}}{R}))', "{{writer}} {{result}}")], unit="ops", w=8)
ts("Origins marked up", "", [(f'sum by (outcome) (rate(goinbound_origin_up_total{{{G}}}{R}))', "{{outcome}}")], unit="ops", w=8)

# ----------------------------------------------- Inbound PDUs (write mode)
row("Inbound PDUs (write mode)")
ts("Events written by outcome", "given_up is an event dropped after retries; alert on it.",
   [(f'sum by (outcome) (rate(goinbound_write_events_total{{{G}}}{R}))', "{{outcome}}")], unit="ops", stack=True, w=8)
ts("Event write time", "", [(q(0.5, "goinbound_write_event_seconds"), "p50"), (q(0.99, "goinbound_write_event_seconds"), "p99")], unit="s", w=8)
ts("Persist over replication", "", [(f'sum by (writer, result) (rate(goinbound_replication_send_events_duration_seconds_count{{{G}}}{R}))', "{{writer}} {{result}}")], unit="ops", w=8)
ts("Staging", "Unattended staged events are ours to sweep and are not being swept: alert past 5 minutes. Don't alert on staging_oldest.",
   [(f'sum(goinbound_staging_events{{{G}}})', "staged"), (f'sum(goinbound_staging_unattended_events{{{G}}})', "unattended")], w=8)
ts("Oldest unattended", "", [(f'max(goinbound_staging_unattended_oldest_seconds{{{G}}})', "oldest unattended")], unit="s", w=8)
ts("Pulled, outliers, pruned, nudges", "", [
    (f'sum(rate(goinbound_write_pulled_events_total{{{G}}}{R}))', "pulled"),
    (f'sum(rate(goinbound_write_outliers_total{{{G}}}{R}))', "outliers"),
    (f'sum(rate(goinbound_write_pruned_total{{{G}}}{R}))', "pruned"),
    (f'sum by (outcome) (rate(goinbound_sweep_nudges_total{{{G}}}{R}))', "nudge {{outcome}}")], unit="ops", w=8)
ts("State resolution cache", "Entries, never groups: one big room's state is 150k entries.",
   [(f'sum(goinbound_pdu_state_cache_entries{{{G}}})', "entries"),
    (f'sum by (how) (rate(goinbound_pdu_state_cache_loads_total{{{G}}}{R}))', "loads {{how}}")], w=12)
ts("ACL lookups", "", [(f'sum by (source) (rate(goinbound_acl_lookups_total{{{G}}}{R}))', "{{source}}")], unit="ops", w=12)

# --------------------------------------------------------- Federation reads
row("Federation reads")
ts("Requests by endpoint and status", "403 and 404 are normal; watch 502 and none.",
   [(f'sum by (endpoint, status) (rate(gopro_requests_total{{{G}}}{R}))', "{{endpoint}} {{status}}")], unit="reqps", stack=True)
ts("Native answer time", "Our own time, without the client's draining of a streamed /state.",
   [(q(0.5, "gopro_native_request_seconds", "le, endpoint"), "p50 {{endpoint}}"),
    (q(0.99, "gopro_native_request_seconds", "le, endpoint"), "p99 {{endpoint}}")], unit="s")
ts("Fell back to main, by reason", "auth_rejected, timeout, error, panic, client_gone. Any sustained rate is worth a look.",
   [(f'sum by (endpoint, reason) (rate(gopro_native_fallback_total{{{G}}}{R}))', "{{endpoint}} {{reason}}")], unit="ops", w=8)
ts("Rate limiting", "", [(f'sum by (endpoint) (rate(gopro_rate_limited_total{{{G}}}{R}))', "rejected {{endpoint}}"),
                         (f'sum by (endpoint) (rate(gopro_rate_limit_slept_total{{{G}}}{R}))', "delayed {{endpoint}}")], unit="ops", w=8)
ts("Cache hit rate", "", [(f'max by (cache) (gopro_cache_hit_rate{{{G}}})', "{{cache}}")], unit="percentunit", w=8)
ts("Invalidations", "", [(f'sum by (cache_func) (rate(gopro_replication_invalidations_total{{{G}}}{R}))', "{{cache_func}}"),
                         (f'sum by (reason) (increase(gopro_replication_cache_flushes_total{{{G}}}{R}))', "flush {{reason}}")], unit="ops", w=12)
ts("Response size", "", [(q(0.99, "gopro_response_bytes", "le, endpoint"), "p99 {{endpoint}}")], unit="bytes", w=12)

# ----------------------------------------------------- Outbound federation
row("Outbound federation")
ts("Events routed and skipped", "", [(f'sum(rate(gofed_events_routed_total{{{G}}}{R}))', "routed"),
                                     (f'sum by (reason) (rate(gofed_events_skipped_total{{{G}}}{R}))', "skipped {{reason}}")], unit="ops", stack=True, w=8)
ts("Transactions, PDUs, EDUs", "", [(f'sum(rate(gofed_transactions_total{{{G}}}{R}))', "transactions"),
                                    (f'sum(rate(gofed_transaction_pdus_total{{{G}}}{R}))', "PDUs"),
                                    (f'sum(rate(gofed_transaction_edus_total{{{G}}}{R}))', "EDUs")], unit="ops", w=8)
ts("EDUs by type", "", [(f'sum by (type) (rate(gofed_transaction_edus_by_type_total{{{G}}}{R}))', "{{type}}")], unit="ops", w=8)
ts("Event processing lag", "Meaningless during a catch-up replay; read stage time then. Quantiles here are usually from a handful of samples: read them beside the count.",
   [(q(0.5, "gofed_event_processing_lag_seconds"), "p50"), (q(0.99, "gofed_event_processing_lag_seconds"), "p99"),
    (f'sum(rate(gofed_event_processing_lag_seconds_count{{{G}}}{R}))', "samples/s")], unit="s", w=8)
ts("Stage time (p99)", "", [(q(0.99, "gofed_stage_duration_seconds", "le, stage"), "{{stage}}")], unit="s", w=8)
ts("Send time by outcome", "", [(q(0.5, "gofed_send_duration_seconds", "le, outcome"), "p50 {{outcome}}"),
                                (q(0.99, "gofed_send_duration_seconds", "le, outcome"), "p99 {{outcome}}")], unit="s", w=8)
ts("Queues", "", [(f'sum(gofed_queued_destinations{{{G}}})', "destinations"), (f'sum(gofed_queued_pdus{{{G}}})', "PDUs"),
                  (f'sum(gofed_queued_edus{{{G}}})', "EDUs"), (f'sum(gofed_in_flight_sends{{{G}}})', "in flight")], w=8)
ts("Destinations", "Backing off is normal for dead servers; the backoff here reaches a year.",
   [(f'sum(gofed_known_destinations{{{G}}})', "known"), (f'sum(gofed_destinations_backing_off{{{G}}})', "backing off"),
    (f'sum(gofed_destination_goroutines{{{G}}})', "goroutines")], w=8)
ts("Bookkeeping writes", "", [(f'sum by (operation, outcome) (rate(gofed_write_operations_total{{{G}}}{R}))', "{{operation}} {{outcome}}")], unit="ops", w=8)
ts("Approximate routes", "Destinations computed without the state before the event (forked DAG, missing group).",
   [(f'sum by (cause) (rate(gofed_approximate_routes_total{{{G}}}{R}))', "{{cause}}")], unit="ops", w=12)
ts("Rows and positions", "", [(f'sum by (stream) (rate(gofed_replication_rows_total{{{G}}}{R}))', "{{stream}}")], unit="ops", w=12)
flush()


def check_metrics():
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    src = []
    for d in ("internal", "cmd"):
        for dirpath, _, files in os.walk(os.path.join(root, d)):
            for f in files:
                if f.endswith(".go") and not f.endswith("_test.go"):
                    src.append(open(os.path.join(dirpath, f)).read())
    text = "\n".join(src)
    prefixes = set(re.findall(r'"((?:gosync|goinbound|gofed|gopro|goworker)_[a-z0-9_]*_)"', text))
    missing = []
    for m in sorted(used):
        base = re.sub(r'_(bucket|count|sum)$', '', m)
        if f'"{base}"' in text:
            continue
        # gopro's metrics are registered as Namespace "gopro" + Name, and the
        # cache collectors build names as a quoted prefix plus a suffix.
        if base.startswith("gopro_") and f'"{base[len("gopro_"):]}"' in text:
            continue
        if any(f'"{pre}"' in text and f'"{base[len(pre):]}"' in text
               for pre in prefixes if base.startswith(pre)):
            continue
        missing.append(m)
    if missing:
        sys.exit("dashboard queries metrics the code does not register: " + ", ".join(missing))


check_metrics()

dashboard = {
    "title": "goworker",
    "uid": "goworker",
    "description": "Client sync, inbound /send, federation reads and the outbound federation sender, in one process.",
    "schemaVersion": 39,
    "editable": True,
    "time": {"from": "now-6h", "to": "now"},
    "refresh": "1m",
    "tags": ["matrix", "synapse", "goworker"],
    "annotations": {"list": [{
        "name": "Worker restarts", "datasource": DS, "enable": True, "iconColor": "orange",
        "expr": f'changes(process_start_time_seconds{{{G}}}[2m]) > 0 {J}', "step": "60s",
        "titleFormat": "restart", "textFormat": "{{instance}}"}]},
    "templating": {"list": [
        {"name": "datasource", "type": "datasource", "query": "prometheus", "label": "Data source"},
        {"name": "job", "type": "query", "datasource": DS, "label": "Job",
         "query": {"query": "label_values(goworker_build_info, job)", "refId": "job"},
         "definition": "label_values(goworker_build_info, job)", "refresh": 2,
         "multi": True, "includeAll": True, "allValue": ".*"},
        {"name": "instance", "type": "query", "datasource": DS, "label": "Worker",
         "query": {"query": 'label_values(goworker_build_info{job=~"$job"}, instance)', "refId": "instance"},
         "definition": 'label_values(goworker_build_info{job=~"$job"}, instance)', "refresh": 2,
         "multi": True, "includeAll": True, "allValue": ".*"},
        {"name": "bucket", "type": "interval", "label": "Window", "auto": False,
         "query": "1m,5m,15m,1h", "current": {"text": "5m", "value": "5m"},
         "options": [{"text": v, "value": v, "selected": v == "5m"} for v in ("1m", "5m", "15m", "1h")]},
    ]},
    "panels": panels,
}
json.dump(dashboard, sys.stdout, indent=2)
print()
