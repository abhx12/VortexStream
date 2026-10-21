# Real-Time OTT Video QoE & Buffer Analytics Engine — Implementation Plan

**Audience:** Antigravity (autonomous coding agent). Implement the whole project by executing the phases below in order.
**Course context:** "Big Data and Modern Databases – Final Package" (M.Sc., team of two). Required stack: **Python, Apache Spark (RDD, Spark SQL, MLlib, Structured Streaming), Kafka, HBase (wide-column), data warehouse (DuckDB/Hive), semi-structured + streaming + time-series data.**

---

## 0. How the agent must work (read first)

1. **Execute phases strictly in order (Phase 0 → 14).** Every phase ends with an **Exit Gate**: a list of commands/tests that must pass. Do not start the next phase until the gate is green.
2. **After each phase:** update `docs/PROGRESS.md` (what was built, decisions, test results), run `git add -A && git commit -m "phase N: <title>"`, then `git tag phase-N-complete`.
3. **Tests are part of the deliverable.** Each phase has unit tests (pytest) and integration/smoke tests. Integration tests must hit the **real** Kafka and HBase containers — never mock them. Unit tests may use a local `SparkSession`.
4. **Write transformation logic as pure functions** (`DataFrame -> DataFrame`, or `dict -> dict`) so the same code is tested in batch and used in streaming.
5. **All tunables live in `config/*.yaml`**, never hard-coded (thresholds, weights, watermarks, speeds, topic names, fault definitions).
6. **Honesty rule for data:** Real = Stanford Puffer fields. Everything else (city, ISP, CDN, device, app version, packet loss, errors, user/subscription data, injected faults) is **simulated** and must be labelled so in code comments, `docs/simulated_fields.md`, the dashboard footer, and the report.
7. **Human-only steps** (ask the user, do not stall): installing Docker Desktop, and downloading the Puffer day if the agent's network cannot reach the Puffer bucket (Phase 2).
8. **If a pinned version is unavailable,** pick the nearest compatible one, record it in `docs/PROGRESS.md`, and continue.
9. **Windows hosts:** run all Python/Spark/Kafka tooling inside Docker (or WSL2). Add `.gitattributes` forcing LF on `*.sh`, `*.py`, `*.yaml`, `Makefile`.
10. If a phase can be parallelised across agents, safe pairs are: (Phase 1 ‖ Phase 2), (Phase 10 ‖ Phase 11 after Phase 8), (Phase 12 ‖ Phase 13 after Phase 9).

---

## 1. Project summary

On OTT platforms (Netflix/Hotstar/YouTube) every player reports playback health. This project ingests that telemetry as a stream, computes **Quality of Experience (QoE)** in near real time (startup delay, rebuffering, bitrate, failures), alerts on regional/ISP/CDN/device degradation, stores hot state in **HBase** and history in a **warehouse**, and predicts session abandonment with **Spark MLlib**.

**Data strategy (hybrid):**
- **Real core:** *one day* of **Stanford Puffer** open data (live-TV streaming research platform) — `client_buffer`, `video_sent`, `video_acked` CSVs.
- A Python **replayer** turns the historical day into a *live Kafka stream* at a controlled speed.
- **Simulated dimensions** Puffer lacks (city, ISP, CDN, device, app version, user/subscription, packet loss, errors) are added deterministically and labelled as simulated.
- **Injected faults** (ISP outage, CDN slowdown, app-version bug) make alerts fire during the demo; a **fault ledger** is the ground truth for evaluating alerts and anomaly detection.

### 1.1 Requirement traceability (brief → where it is satisfied)

| Brief requirement | Satisfied by | Phase |
|---|---|---|
| Semi-structured data | JSON events with nullable/optional fields | 3, 5 |
| Streaming data | Kafka topics + Spark Structured Streaming | 1, 5–9 |
| Time-series data | Event-time windows, watermarks, reverse-timestamp HBase keys | 8 |
| High volume & dimensionality, multiple sources | Player telemetry + network metrics + reference data; dims: device/ISP/CDN/city/channel/scheme | 3–5 |
| DB: HBase (wide-column) | `session_state`, `qoe_metrics`, `alerts` tables with column families, salting, reversed-ts keys, TTL | 1, 7–9 |
| Spark RDD, transformations, actions | RDD batch demos | 10 |
| Spark SQL & ML libraries | SQL analytics + MLlib pipelines | 10, 11 |
| Real-time stream processing | Structured Streaming jobs (parse→validate→enrich→window→alert) | 6–9 |
| ETL, mapping, integration | Source-to-target mapping doc + reconciliation | 3, 6, 13 |
| Standardization, sorting, aggregation, validation, verification | Phase 6 + Phase 13 | 6, 13 |
| ML model / analytics | Abandonment classifier + anomaly detection (+ optional forecast) | 11 |
| Data warehouse (Hive/DuckDB/Snowflake) | DuckDB over partitioned Parquet | 8, 10 |
| Dashboard / reports | Streamlit live + historical | 12 |

---

## 2. Architecture

```
 Puffer day (CSV) ──► DuckDB preprocessing ──► unified events.parquet  (Phase 3)
 Simulated dims / ref data / faults ─────────────┘                       (Phase 4)
                                   │
                         Replay producers (Python)  speed-controlled, fault+chaos injection (Phase 5)
        ┌──────────────────────────┼───────────────────────────┐
  playback-events            network-metrics                  errors          (Kafka, key=session_id)
        └──────────────┬───────────┴───────────────┬───────────┘
                       ▼                           │
        Spark Q0 ingest_clean: parse → validate → standardize → dedup → enrich (static join)   (Phase 6)
            ├─► playback-clean / network-clean (Kafka)   ├─► quarantine + dlq   ├─► warehouse silver
            ▼
        Spark Q1 session_tracker (applyInPandasWithState)  → session-facts (Kafka) + HBase session_state  (Phase 7)
        Spark Q2 qoe_aggregator  (5-min window / 1-min slide, watermark 2 min)
                 → HBase qoe_metrics (cf m) + warehouse gold + alerts                                    (Phase 8, 9)
        Spark Q3 network_aggregator → HBase qoe_metrics (cf n) + warehouse gold
        Spark Q4 ml_scoring (PipelineModel on early-session features) → predictions                      (Phase 11)
                       │
        Dashboard (Streamlit: HBase live + DuckDB history) + alerts feed                                 (Phase 12)
```

Medallion naming: **bronze** = raw Kafka topics, **silver** = cleaned/enriched events, **gold** = QoE aggregates.

---

## 3. Key design decisions

| # | Decision | Reason |
|---|---|---|
| D1 | Use a Puffer day **on or after 2021-06-12** (`2021-06-12T11_2021-06-13T11` or later). | Only from that day do `video_sent/video_acked/client_buffer` include `buffer` and `cum_rebuf`, which the stall metrics need. |
| D2 | **QoE session = one Puffer stream** (`session_id` + `index`). Output id: `session_id = "<puffer_session_id>-<index>"`. | Puffer says two datapoints belong to one stream iff both fields match; a channel switch is a new stream. |
| D3 | **Simulated clock.** Event `ts = original_ts + shift`, `shift = replay_start_wall_time − data_window_start`. Windows, watermarks and alert durations are in **data time** (the shifted event time); `--speed N` only controls wall-clock emission. | Ratios are time-scale invariant, window semantics stay faithful, and a 2-hour slice replays in 12 min at 10×. |
| D4 | **Replay a slice, not the full day:** default 120 data-minutes (`--start-offset`, `--duration`) plus optional session sampling (`--max-sessions`). | A full day is several GB; local Spark must sustain ≥ 500 events/s. |
| D5 | Puffer `timer` events (every ~250 ms) are **downsampled to `heartbeat` every 5 s** per stream. | Avoids 4 events/s/stream of noise. |
| D6 | Puffer has **no** packet loss, errors, seeks, ISP, device, city. Errors/packet loss/dims are simulated; `seek` is not emitted (live TV). | Be explicit and honest. |
| D7 | Separate **clean topics** between Q0 and later queries. | Spark 3.5 restricts chained stateful operators; clean topics also decouple jobs and make each testable. |
| D8 | HBase `qoe_metrics` uses a **grain prefix**: `<grain>#<dimkey>#<reverse_minute>`. Grains: `global`, `city_isp`, `cdn`, `device`, `channel`. | One table, fast "latest N minutes for segment X" range scans, multiple groupings. |
| D9 | Live HBase writes use **update output mode** (idempotent overwrite of the same row key); warehouse gold stores every update with `updated_at`, and a DuckDB view keeps the latest row per key. | Dashboards see fresh partial windows; the warehouse stays queryable and de-duplicated. |
| D10 | **DuckDB** is the warehouse (Spark writes partitioned Parquet; DuckDB queries it). | Zero-ops, satisfies the "Hive/DuckDB/Snowflake" requirement. |
| D11 | Everything runs in Docker Compose: `kafka` (KRaft), `hbase` (+Thrift), `app` (Python 3.11 + Java 17 + PySpark 3.5.3). | Reproducible; avoids Windows/winutils problems. |
| D12 | Alert thresholds are **calibrated from a no-fault baseline run** (Phase 9), not guessed. | Real Puffer stall ratios are low; the brief's "3%" is the starting default, tuned by evidence. |

---

## 4. Tech stack (pin; adjust only if unavailable)

| Component | Version / choice |
|---|---|
| Python | 3.11 |
| Spark / PySpark | 3.5.3 (Scala 2.12), Kafka connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| Java in `app` image | OpenJDK 17 |
| Kafka | `apache/kafka:3.8.0` (KRaft, single broker); optional `provectuslabs/kafka-ui` |
| HBase | 2.5.x standalone, custom Dockerfile (download tarball from Apache archive), Thrift1 server on 9090 |
| HBase client | `happybase` (Thrift). Fallback: `starbase`/REST only if HappyBase fails to install |
| Kafka client | `confluent-kafka` |
| Warehouse | DuckDB (+ `pyarrow`, Parquet) |
| Preprocessing | DuckDB SQL (out-of-core; ASOF joins) |
| Dashboard | Streamlit + Plotly (+ `streamlit-autorefresh`) |
| Testing | pytest, jsonschema, pandas, numpy; Playwright (optional, dashboard smoke) |
| Config | PyYAML |

---

## 5. Repository layout

```
ott-qoe-engine/
├── README.md
├── Makefile
├── docker-compose.yml
├── .gitattributes  .gitignore  .env.example
├── docker/
│   ├── app.Dockerfile            # python3.11 + openjdk17 + pyspark 3.5.3 + deps
│   └── hbase.Dockerfile          # HBase standalone + Thrift
├── config/
│   ├── config.yaml               # topics, windows, watermarks, weights, thresholds, speeds, paths
│   ├── faults.yaml               # fault scenarios
│   ├── chaos.yaml                # duplicate / late / invalid / dirty-dimension rates
│   ├── sim_dims.yaml             # simulated dimension distributions
│   └── standardization.yaml      # alias maps for device/ISP/country/city
├── schemas/
│   ├── playback_event.schema.json
│   ├── network_metric.schema.json
│   └── session_fact.schema.json
├── data/                         # gitignored
│   ├── raw/puffer/<day>/         # downloaded CSVs
│   ├── processed/<day>/          # events.parquet, network.parquet, stream_truth.parquet
│   ├── ref/                      # content.parquet, users.parquet, subscriptions.parquet
│   ├── ledger/                   # faults.jsonl
│   ├── manifests/                # run-<id>.json (producer counts)
│   ├── checkpoints/              # Spark checkpoints
│   ├── warehouse/                # silver_*, gold_*, quarantine, session_facts, alerts, ml_predictions
│   └── models/                   # saved MLlib PipelineModels
├── src/qoe/
│   ├── common/   (config.py, logging.py, schema.py, timeutil.py, hbase_keys.py, kafka_utils.py)
│   ├── data/     (puffer_download.py, profile.py, preprocess.py, sim_dims.py, ref_data.py)
│   ├── producers/(replay.py, faults.py, chaos.py, manifest.py)
│   ├── streaming/(spark_session.py, parse.py, validate.py, standardize.py, enrich.py,
│   │              q0_ingest_clean.py, q1_session_tracker.py, q2_qoe_aggregator.py,
│   │              q3_network_aggregator.py, q4_ml_scoring.py, qoe_score.py,
│   │              alerting.py, sinks_hbase.py, sinks_warehouse.py)
│   ├── batch/    (rdd_demo.py, sql_demo.py, warehouse.py, hourly_rollup.py, backfill.py)
│   ├── ml/       (dataset.py, churn_model.py, anomaly.py, forecast.py)
│   ├── verify/   (reconcile.py, eval_alerts.py, eval_anomaly.py, lineage.py)
│   └── dashboard/(app.py, data_access.py)
├── scripts/      (create_topics.sh, hbase_init.py, reset_state.sh, doctor.py, demo.sh)
├── sql/          (spark_sql_queries.sql, warehouse_views.sql, report_queries.sql)
├── tests/        phase_0 … phase_14, fixtures/
└── docs/         PROGRESS.md, architecture.md, data_profile.md, simulated_fields.md,
                  etl_mapping.md, hbase_design.md, verification_report.md, demo_script.md, report/
```

Every phase's tests are runnable via `make test-phaseN`; `make test-all` runs everything that does not need the full stack; `make e2e` runs the full pipeline.

---

## 6. Canonical data contracts

### 6.1 Unified playback event (topic `playback-events`, JSON)

```json
{
  "schema_version": 1,
  "event_id": "e-<deterministic-hash>",
  "session_id": "<puffer_session_id>-<index>",
  "user_id": "u-5521",
  "ts": "2026-10-21T21:15:04.120Z",
  "event_type": "buffer_start",
  "video_id": "abc",
  "content_type": "live",
  "device": "android",
  "app_version": "5.2.1",
  "country": "IN",
  "city": "Coimbatore",
  "isp": "Airtel",
  "cdn": "Akamai",
  "scheme": "linear_bba/bbr",
  "bitrate_kbps": 1800,
  "resolution": "720p",
  "buffer_ms": 2300,
  "stall_ms": null,
  "interval_ms": null,
  "ssim_index": null,
  "throughput_kbps": 2100,
  "rtt_ms": 64,
  "packet_loss_pct": 0.8,
  "error_code": null
}
```

Field notes: `buffer_ms` = current playback buffer level (Puffer `buffer` × 1000). `stall_ms` appears on `buffer_end` (stall duration = Δ`cum_rebuf` × 1000). `interval_ms` appears on `heartbeat` (the playback time that heartbeat represents, normally 5000). `ssim_index` (real Puffer quality) appears on `heartbeat`. `scheme` = ABR/congestion-control name from `expt_settings` (real).

**Event types and field presence** (R = required, O = optional, – = null):

| event_type | bitrate/resolution | buffer_ms | stall_ms | interval_ms | error_code | Source |
|---|---|---|---|---|---|---|
| play_request | – | O | – | – | – | Puffer `client_buffer.event=init` |
| first_frame | R | O | – | – | – | `event=startup` |
| heartbeat | R | R | – | R | – | downsampled `timer` + as-of join to latest `video_sent` |
| buffer_start | O | R | – | – | – | `event=rebuffer` |
| buffer_end | O | R | R | – | – | `event=play` after a rebuffer |
| bitrate_switch | R (new) | O | – | – | – | `format` change between consecutive `video_sent` (also `prev_bitrate_kbps`, optional) |
| error | O | – | – | – | R | **Simulated** (errors topic) |
| session_end | – | O | – | – | O | last event of a stream (+1 ms) |

`seek`: not produced (live TV; documented limitation).

### 6.2 Network metric (topic `network-metrics`)

`event_id, session_id, ts, isp, cdn, city, device, throughput_kbps (= delivery_rate×8/1000), rtt_ms (= rtt/1000), min_rtt_ms, cwnd, in_flight, packet_loss_pct (**simulated**, correlated with rtt and inverse throughput), schema_version`. One record per `video_sent` row.

### 6.3 Session fact (topic `session-facts`, emitted by Q1)

`fact_id, session_id, ts, kind ∈ {startup, vsf, early_features, session_end}` plus the common dimension fields and: `startup_ms, vsf (bool), stall_count, stall_ms, watch_ms, avg_bitrate_kbps, switch_count, error_flag, first_N_s features (see Phase 11)`.

### 6.4 Topics

| Topic | Partitions | Key | Notes |
|---|---|---|---|
| `playback-events` | 6 | `session_id` | bronze |
| `network-metrics` | 6 | `session_id` | bronze |
| `errors` | 3 | `session_id` | bronze (simulated) |
| `playback-clean` | 6 | `session_id` | silver |
| `network-clean` | 6 | `session_id` | silver |
| `session-facts` | 6 | `session_id` | |
| `alerts` | 1 | `alert_id` | |
| `dlq` | 1 | `event_id` | quarantined records with reason |
| `predictions` | 3 | `session_id` | Q4 abandonment scores (Phase 11) |
| `ref-content`, `ref-users` | 1 each | `video_id` / `user_id` | **optional**, compacted (`cleanup.policy=compact`); demonstrates the third source (reference data) as a stream. Spark still joins from Parquet. |

Total: **9 mandatory topics** + 2 optional compacted ref topics. `scripts/create_topics.sh` creates all 11 (the ref topics only with `--with-ref`).

Partitioning by `session_id` keeps all events of one session ordered within a partition.

### 6.5 HBase schema (wide-column)

**`session_state`** — one row per active/recent session
- Row key: `{salt:02d}-{session_id}` with `salt = crc32(session_id) % 16`; table **pre-split on the 16 salt prefixes**.
- Column family `playback`: `state, last_bitrate, stall_count, stall_ms, startup_ms, watch_ms, last_event_ts`
- Column family `context`: `device, isp, cdn, city, channel, scheme, user_id`
- TTL 6 h, VERSIONS 1.

**`qoe_metrics`** — time-series rows per segment
- Row key: `{grain}#{dimkey}#{rev_minute}`; `rev_minute = str(Long.MAX_VALUE − window_end_epoch_minute).zfill(19)` so **newest sorts first**. `dimkey` examples: `Chennai|Airtel` (grain `city_isp`), `Akamai` (`cdn`), `android` (`device`), `abc` (`channel`), `ALL` (`global`).
- Column family `m`: `qoe_score, qoe_label, rebuffer_ratio, rebuffer_freq_per_min, avg_bitrate_kbps, hd_pct, switches_per_min, vsf_rate, error_rate, avg_startup_ms, sessions, window_start, window_end, updated_at`
- Column family `n`: `avg_throughput_kbps, avg_rtt_ms, avg_packet_loss_pct, net_samples`
- TTL 24 h, VERSIONS 1. (Demonstrate that different rows may hold different column sets, e.g. rows without `n` until the network query writes.)

**`alerts`** — row key `{rev_minute}#{alert_id}`; cf `a`: `rule, severity, grain, dimkey, metric, value, threshold, baseline, status (open|resolved), opened_at, resolved_at, window_end`.

### 6.6 Warehouse tables (partitioned Parquet under `data/warehouse/`)

`silver_playback_events`, `silver_network_metrics` (partition `dt`, `hr`); `quarantine_events` (+ `reason`, `raw`); `session_facts`; `gold_qoe_5m_raw` (every update, with `updated_at`) and view `gold_qoe_5m` (latest per `grain,dimkey,window_end`); `gold_qoe_hourly` (batch rollup); `alerts`; `ml_predictions`.

### 6.7 `config/config.yaml` (starting values)

```yaml
kafka: {bootstrap_internal: "kafka:19092", bootstrap_external: "localhost:9092"}
replay: {speed: 10, window_minutes: 120, start_offset_minutes: 0, max_sessions: 4000, seed: 42, heartbeat_s: 5}
streaming:
  watermark_ingest: "2 minutes"
  watermark_tracker: "1 minute"
  window: "5 minutes"
  slide: "1 minute"
  trigger_seconds: 5
  shuffle_partitions: 8
  max_offsets_per_trigger: 20000
  vsf_timeout_s: 10
  session_idle_s: 60                   # Q1 closes a stream with no events for this long (data time)
  checkpoint_root: "data/checkpoints"
qoe:
  weights: {startup: 15, rebuffer: 40, switch: 10, bitrate: 15, failure: 20}   # sum 100
  startup_good_s: 1.0
  startup_bad_s: 8.0
  rebuffer_full_penalty_ratio: 0.10
  switches_per_min_full_penalty: 2.0
  hd_min_height: 720
  failure_full_penalty_rate: 0.10
  labels: {good_min: 80, fair_min: 60}
alerts:
  min_sessions: 20
  rebuffer_ratio_threshold: 0.03      # to be calibrated in Phase 9
  vsf_rate_threshold: 0.15            # to be calibrated
  error_rate_threshold: 0.05          # to be calibrated (R4)
  critical_multiplier: 2.0            # severity=critical when value > threshold × this, else warn
  qoe_drop_points: 15
  baseline_lag_minutes: 5
  consecutive_windows: 2
  cooldown_minutes: 10
hbase: {host: "hbase", port: 9090, salt_buckets: 16, batch_size: 500, ttl_session_state_s: 21600, ttl_qoe_metrics_s: 86400}
ml: {early_window_s: 15, abandon_watch_s: 120, folds: 3, seed: 42, test_fraction: 0.3,
     anomaly: {robust_z_threshold: 3.5, kmeans_k_range: [2, 8], max_fp_per_baseline_hour: 3}}
paths: {data: "data", warehouse: "data/warehouse", models: "data/models", ledger: "data/ledger", manifests: "data/manifests"}
```

### 6.8 Other config files (starting contents — keys are the contract, values are editable)

`config/chaos.yaml`
```yaml
duplicate_rate: 0.005
late_rate: 0.005
late_delay_s: [20, 90]        # data seconds; must stay below watermark_ingest + margin for most, a few exceed it on purpose
invalid_rate: 0.003
invalid_kinds: {negative_buffer: 0.25, missing_session_id: 0.2, impossible_bitrate: 0.2, bad_ts: 0.2, missing_event_type: 0.15}
dirty_dims_rate: 0.02
```

`config/standardization.yaml` (alias → canonical, matched after trim + casefold)
```yaml
device: {android: android, "android phone": android, ios: ios, iphone: ios, web: web, browser: web, smarttv: smarttv, "smart tv": smarttv, tv: smarttv}
isp: {jio: Jio, "reliance jio": Jio, airtel: Airtel, "bharti airtel": Airtel, act: ACT, "act fibernet": ACT, vi: Vi, vodafone: Vi, bsnl: BSNL}
city: {chennai: Chennai, madras: Chennai, bengaluru: Bengaluru, bangalore: Bengaluru, mumbai: Mumbai, bombay: Mumbai, delhi: Delhi, "new delhi": Delhi, hyderabad: Hyderabad, coimbatore: Coimbatore, kolkata: Kolkata, calcutta: Kolkata}
country: {in: IN, india: IN, ind: IN}
unknown_value: "unknown"
```

`config/sim_dims.yaml` — keys: `seed_salt`, `user_pool_size: 50000`, `country: IN`, and for each of `city`, `isp`, `cdn`, `device`, `app_version` a `weights:` map (values in §Phase 4) plus optional `isp_by_city:` overrides; `errors: {rate_per_stream: 0.001, codes: {NET_TIMEOUT: 0.4, CDN_5XX: 0.3, DECODE_ERR: 0.2, DRM_ERR: 0.1}}`; `packet_loss: {mu: -2.3, sigma: 0.7, k1: 1.0, k2: 1.0, max_pct: 15}`.

### 6.9 Additional record contracts

**Alert (topic `alerts`, HBase `alerts`, warehouse `alerts`)** — `alert_id, rule ∈ {R1,R2,R3,R4}, severity ∈ {warn, critical}, grain, dimkey, metric, value, threshold, baseline (nullable), status ∈ {open, resolved}, opened_at, resolved_at (nullable), window_end, run_id`. One record on open, one on resolve (same `alert_id`).

**Prediction (topic `predictions`, warehouse `ml_predictions`)** — `session_id, ts, abandon_probability ∈ [0,1], predicted_label, model_version, device, isp, cdn, city, scheme`.

**DLQ record** — `event_id (nullable), reason, source_topic, raw, quarantined_at`.

### 6.10 JSON Schema field rules (used by `schemas/*.schema.json` and the Phase 6 validator — keep both in sync from one source)

| Field | Type | Rule |
|---|---|---|
| `schema_version` | int | `== 1` |
| `event_id`, `session_id` | string | non-empty; `session_id` matches `^[A-Za-z0-9+/=_-]+-\d+$` |
| `ts` | string | ISO-8601 UTC with `Z`, ms precision |
| `event_type` | enum | the 8 types in §6.1 |
| `bitrate_kbps` | int/number | `0 ≤ x ≤ 100000` |
| `resolution` | string | `^\d{3,4}p$` |
| `buffer_ms`, `stall_ms`, `interval_ms` | number | `≥ 0` (`interval_ms ≤ 10000`) |
| `ssim_index` | number | `0 ≤ x ≤ 1` (SSIM) — if Puffer stores dB, convert or document |
| `throughput_kbps`, `rtt_ms` | number | `≥ 0` |
| `packet_loss_pct` | number | `0 ≤ x ≤ 100` |
| `error_code` | enum/null | `NET_TIMEOUT, CDN_5XX, DECODE_ERR, DRM_ERR` (required iff `event_type=error`) |
| dims (`device, isp, cdn, city, country, app_version`) | string | non-empty (dirty variants are valid at bronze, fixed in standardize) |

Nullable fields are explicitly typed `["x","null"]`; unknown extra fields are allowed at bronze (forward compatibility) and dropped at silver.

---

# PHASES

---

## Phase 0 — Bootstrap & conventions
**Goal:** a runnable repo skeleton with tooling, config loader and test harness.
**Owner (team of two):** A. **Effort:** ~1 h.

**Tasks**
1. Create the repository layout in §5 (empty modules with docstrings are fine).
2. `src/qoe/common/config.py`: load `config/*.yaml`, merge env overrides (`QOE__section__key`), expose a typed accessor.
3. `src/qoe/common/logging.py`: JSON-line logging with `run_id`.
4. `Makefile` with **every** target in the index below (targets for later phases may be stubs that print `not implemented until phase N` and exit 2; each phase replaces its stubs). Targets run inside the `app` container via `docker compose exec app …` unless noted.

   | Target | Phase | Purpose |
   |---|---|---|
   | `doctor`, `build`, `up`, `down`, `init`, `reset-state`, `lint` | 0–1 | environment, containers, topics+tables, full clean |
   | `data`, `data-fake`, `profile` | 2 | download real day / fake sample, write `docs/data_profile.md` |
   | `preprocess` | 3 | Puffer CSV → `events/network/stream_truth.parquet` |
   | `sim-data` | 4 | dims, ref tables, `errors.parquet`, validates `faults.yaml` |
   | `replay`, `replay-demo` | 5 | run replayer (ARGS=…) / 3 min clean + 3 min with faults |
   | `q0`, `q1`, `q2`, `q3`, `q4`, `streams-stop` | 6–8, 11 | start/stop each Spark streaming query in the background (logs in `data/logs/`) |
   | `calibrate`, `eval-alerts` | 9 | baseline threshold calibration, alert evaluation vs ledger |
   | `batch-demo` | 10 | RDD + Spark SQL + DuckDB + hourly rollup demos |
   | `train`, `eval-anomaly` | 11 | MLlib training/eval, anomaly eval vs ledger |
   | `dashboard` | 12 | Streamlit on :8501 |
   | `verify` | 13 | reconciliation V1–V8 → `docs/verification_report.md` |
   | `demo`, `demo-stop`, `e2e` | 14 | one-command demo / teardown / full acceptance run |
   | `test-phase0`…`test-phase14`, `test-all` | all | pytest per phase / all tests that do not need the full stack |
5. `.gitignore` (data/, checkpoints, `__pycache__`, `.env`), `.gitattributes`, `.env.example`.
6. `docs/PROGRESS.md` template (phase table with status, date, notes), `docs/simulated_fields.md` skeleton (filled in Phase 4).
7. `scripts/doctor.py`: verifies Docker daemon, Compose v2, free ports (9092, 9090, 8081, 8501, 16010), free disk ≥ 20 GB, Python deps.

**Tests (`tests/phase_0`)**
- `test_config_loads`: loads all YAML, weights sum to 100, thresholds are in range.
- `test_env_override`: `QOE__replay__speed=50` changes `replay.speed`.
- `test_logging_json`: log line parses as JSON and contains `run_id`.
- `test_makefile_targets`: `make -n <target>` succeeds for every target in the index above (parse the table from this plan or a `tests/phase_0/targets.txt` copy).
- `test_no_hardcoded_tunables`: a grep-style check that topic names, thresholds and weights from `config/*.yaml` do not appear as literals in `src/` (allow-list for tests/docs).
- `make doctor` exits 0 (or prints actionable failures).

**Exit gate:** `make test-phase0` passes; repo committed and tagged.

---

## Phase 1 — Infrastructure (Docker: Kafka, HBase, app)
**Goal:** Kafka, HBase+Thrift and a Spark-capable app container all running; topics and tables created idempotently.
**Owner:** A. **Effort:** ~3–4 h (HBase container is the usual pain point).

**Tasks**
1. `docker/app.Dockerfile`: `python:3.11-slim-bookworm` + `openjdk-17-jre-headless` + `pyspark==3.5.3` + project requirements (`confluent-kafka, happybase, duckdb, pyarrow, pandas, numpy, pyyaml, jsonschema, pytest, streamlit, plotly, streamlit-autorefresh`). Mount repo at `/app`, data at `/app/data`. Cache the Ivy dir in a named volume so `spark.jars.packages` downloads once. Install the project with `pip install -e .`.
2. `docker/hbase.Dockerfile`: JDK 11/17 base, download HBase 2.5.x binary from the Apache archive, set `hbase.rootdir`/`hbase.zookeeper.property.dataDir` to a mounted volume, entrypoint starts `hbase master` (standalone) **and** `hbase thrift start -p 9090` (Thrift1). Set the container `hostname: hbase` and make the region server advertise a resolvable hostname (common failure: master can't resolve its own hostname). Add a healthcheck that runs `echo "status" | hbase shell -n`.
3. `docker-compose.yml`: services `kafka` (KRaft; two listeners: `INTERNAL kafka:19092`, `EXTERNAL localhost:9092`; auto-create topics disabled), `hbase` (ports 9090, 16010), `app` (depends_on healthy kafka+hbase; `command: sleep infinity`), optional `kafka-ui` (port 8081, profile `ui`).
4. `scripts/create_topics.sh` — creates all topics in §6.4 with the stated partition counts; idempotent (`--if-not-exists`).
5. `scripts/hbase_init.py` — creates `session_state` (pre-split, 2 CFs, TTL 6 h), `qoe_metrics` (CFs `m`, `n`, TTL 24 h), `alerts`; idempotent. Expose `--drop` for resets.
6. `src/qoe/common/hbase_keys.py`: `salted_key(session_id)`, `rev_minute(epoch_minute)`, `qoe_key(grain, dimkey, window_end)`, `alert_key(...)`, plus inverse parsers.
7. `src/qoe/common/kafka_utils.py`: producer/consumer factories with sane defaults (producer `acks=all`, `linger.ms=20`, `compression.type=lz4`).
8. `src/qoe/streaming/spark_session.py`: builds a `SparkSession` with Kafka package, `spark.sql.shuffle.partitions` from config, local checkpoint location, timezone UTC, `spark.sql.session.timeZone=UTC`.

**Tests (`tests/phase_1`)**
- `test_hbase_keys.py` (unit): salt is deterministic and within `[0,16)`; `rev_minute` is 19 digits; for minutes `m1 < m2`, `rev_minute(m2) < rev_minute(m1)` lexicographically; parsers round-trip.
- `test_kafka_roundtrip.py` (integration): produce 1,000 keyed JSON messages, consume, assert same count and per-key order; assert the 9 mandatory topics from §6.4 exist with the right partition counts (and `ref-*` topics are compacted when created with `--with-ref`).
- `test_hbase_crud.py` (integration): put/get/scan/delete on each table; scan with prefix and `limit=1` returns the *newest* `qoe_metrics` row for a segment after writing 5 minutes; **wide-column demo** — write two rows to the same table with different column sets and read both back.
- `test_hbase_ttl_and_cf.py`: table descriptors show expected CFs and TTLs.
- `test_spark_kafka.py`: a 10-second Structured Streaming query reads from `playback-events` and counts rows written by a test producer.

**Exit gate:** `make up && make init && make test-phase1` green; `docker compose ps` shows all services healthy; screenshot/log of Kafka UI or HBase UI (`:16010`) saved in `docs/`.

---

## Phase 2 — Puffer data acquisition & profiling
**Goal:** one Puffer day on disk, validated, and profiled.
**Owner:** B. **Effort:** ~2 h plus download time.

**Context (verified from Puffer's Data Description page):**
- Per day CSVs: `video_sent_X.csv`, `video_acked_X.csv`, `client_buffer_X.csv` (plus `video_size_X.csv`, `ssim_X.csv`) and `logs/expt_settings`. `X` looks like `2021-06-12T11_2021-06-13T11` (UTC 11:00 to 11:00).
- `time` is **nanoseconds since Unix epoch**; stream identity = (`session_id`, `index`); a channel switch starts a new stream in the same session.
- `video_sent` fields: `time, session_id, index, expt_id, channel, video_ts, format ("WxH-CRF"), size (bytes), ssim_index, cwnd, in_flight, min_rtt (µs), rtt (µs), delivery_rate (bytes/s), buffer (s), cum_rebuf (s)`. Chunks are 2.002 s long.
- `client_buffer` fields: `time, session_id, index, expt_id, channel, event ∈ {init, startup, rebuffer, play, timer}, buffer, cum_rebuf`. Startup delay = `startup` − `init`. `timer` fires nominally every 250 ms.
- A single day is **several GB**; Puffer asks users to download once. A small fake sample exists at `https://storage.cloud.google.com/puffer-data-release/puffer-fake-sample.tar.gz`. Daily data is linked from `https://puffer.stanford.edu/results/` (bucket `puffer-data-release`).

**Tasks**
1. `src/qoe/data/puffer_download.py` with `--day 2021-06-12T11_2021-06-13T11` (default; the user may override). Strategy: (a) try the public bucket listing linked from the Experiment Results page; (b) if the network blocks it, print exact manual-download instructions and **ask the user** to place the files in `data/raw/puffer/<day>/`. Never re-download if the files exist (checksum/size cache).
2. `make data-fake`: download/extract the fake sample (or generate a schema-identical synthetic mini-day with `src/qoe/data/make_fake_puffer.py`) so Phases 3–9 can be developed and tested without the multi-GB file. **CI-style tests must run on the fake/mini fixture.**
3. `src/qoe/data/profile.py` (DuckDB): detect header vs headerless CSV (assert column names; otherwise apply the documented positional order); compute and write `docs/data_profile.md` with: row counts per file, time range, distinct sessions/streams/channels/expt_ids, events per second over time (find the busiest 2-hour region), stream-duration distribution, startup-delay distribution (init→startup), share of streams with `init` but no `startup`, overall stall ratio (Σcum_rebuf / Σwatch), distribution of `format` values, null rates, and the `expt_settings` mapping (expt_id → ABR/CC name).
4. Pick the **replay slice**: the busiest contiguous 120 minutes unless overridden; store as `config.replay.start_offset_minutes`.

**Tests (`tests/phase_2`)**
- `test_schema_columns`: each CSV has all documented columns (on the fake/mini fixture and, if present, the real day's first 1,000 rows).
- `test_time_units`: `time` values are within the day's 24 h window when read as nanoseconds.
- `test_profile_report_generated`: `docs/data_profile.md` exists and contains all required sections.
- `test_no_redownload`: second run of the downloader makes zero network calls (mock the HTTP layer).

**Exit gate:** real day (or documented fallback) present; `docs/data_profile.md` generated; `make test-phase2` green.

---

## Phase 3 — Preprocess Puffer into unified events (DuckDB)
**Goal:** convert Puffer CSVs into the canonical event/network records in time order (Parquet), plus a hidden ground-truth table.
**Owner:** B. **Effort:** ~4–5 h.

**Tasks** (`src/qoe/data/preprocess.py`; SQL in DuckDB; out-of-core; filter to the replay slice *early*)
1. Parse timestamps: `ts_orig = make_timestamp(time/1000)` (ns → µs). Build `session_id = puffer_session_id || '-' || index`. Keep `puffer_session_id` for simulated-user mapping.
2. **client_buffer → playback events**
   - `init` → `play_request`; `startup` → `first_frame` (attach the as-of latest `video_sent` bitrate/resolution via DuckDB `ASOF JOIN`; if none yet, use the first `video_sent` of the stream); `rebuffer` → `buffer_start`; `play` → `buffer_end` with `stall_ms = (cum_rebuf − cum_rebuf_at_prev_rebuffer) × 1000` (use window functions per stream).
   - `timer` → **downsample** to one `heartbeat` per 5 s per stream (keep the last timer in each 5 s bucket); set `interval_ms = 5000` (use the actual elapsed time since the previous heartbeat, capped at 10 s, and exclude time spent stalled); attach as-of bitrate/resolution/`ssim_index`.
3. **video_sent → bitrate/network**
   - `bitrate_kbps = size × 8 / 2.002 / 1000`; `resolution = "<H>p"` from `format`; `ssim_index` carried.
   - Emit `bitrate_switch` when `format` differs from the stream's previous `video_sent` (with `prev_bitrate_kbps`).
   - Emit one **network metric** per `video_sent`: `throughput_kbps = delivery_rate × 8 / 1000`, `rtt_ms = rtt / 1000`, `min_rtt_ms = min_rtt / 1000`, `cwnd`, `in_flight`.
4. **session_end:** one per stream at `last_event_ts + 1 ms`.
5. **Scheme:** join `expt_id` → `scheme` ("abr/cc") using `logs/expt_settings`; fallback `expt_<id>` if missing.
6. **Ground truth table `stream_truth.parquet`** (never emitted to Kafka): per stream `final_cum_rebuf_s, watch_s, startup_delay_s, had_startup, n_rebuffers, abandoned_label (watch_s < ml.abandon_watch_s and had_startup)`.
7. Output sorted by `ts_orig`: `events.parquet` (playback), `network.parquet`, `stream_truth.parquet`. Dimension columns are **not** added here (Phase 4).
8. `docs/etl_mapping.md` — first half: **Puffer field → canonical field** table, one row per mapping above (source file, source field, transformation, target field).

**Tests (`tests/phase_3`)** — use a tiny hand-written fixture (`tests/fixtures/puffer_mini/`, ≥ 3 streams: one clean, one with 2 stalls, one `init` without `startup`)
- `test_event_mapping`: expected event types and counts per stream (golden file).
- `test_stall_duration`: Σ`stall_ms` of a stream = final `cum_rebuf` × 1000 (±1 ms); no negative `stall_ms`.
- `test_startup_pairing`: first_frame ts − play_request ts equals the CSV's `startup` − `init`.
- `test_heartbeat_downsample`: ≤ 1 heartbeat per 5 s per stream; `interval_ms` > 0.
- `test_bitrate_math`: a 1,000,000-byte chunk → ≈ 3,996 kbps; format `1280x720-22` → `720p`.
- `test_switch_detection`: switches equal the number of format changes.
- `test_sorted_output`: `ts_orig` is non-decreasing; per-stream order is lifecycle-valid (play_request first, session_end last).
- `test_no_leak_truth`: `events.parquet` has no `_truth`/`abandoned` columns.
- Integration on real/fake day: row counts are > 0 and `session_end` count = number of streams.

**Exit gate:** `make preprocess` produces the three Parquet files in < 15 min for the real slice; `make test-phase3` green.

---

## Phase 4 — Simulated dimensions, reference data, fault design
**Goal:** deterministic simulated context, reference tables for stream–static joins, and the fault ledger format.
**Owner:** A. **Effort:** ~3 h.

**Tasks**
1. `config/sim_dims.yaml` (editable) with weighted distributions:
   - `city` (country fixed `IN`): Chennai .20, Bengaluru .20, Mumbai .20, Delhi .15, Hyderabad .10, Coimbatore .08, Kolkata .07.
   - `isp` (global weights; optional per-city overrides): Jio .38, Airtel .32, ACT .12, Vi .10, BSNL .08.
   - `cdn`: Akamai .40, CloudFront .35, Fastly .25.
   - `device`: android .55, ios .20, web .15, smarttv .10.
   - `app_version`: 5.2.1 .50, 5.2.0 .30, 5.1.9 .20.
2. `src/qoe/data/sim_dims.py`: assign dims **per `puffer_session_id`** via `hash(seed, puffer_session_id)` (stable across runs; constant within a session; all streams of one viewer share them). `user_id` = `u-` + hash bucket (pool size 50,000).
3. `src/qoe/data/ref_data.py` → `data/ref/`: `content.parquet` (one row per Puffer `channel`: `video_id`, `title`, `genre` and `language` **simulated**, `content_type="live"`), `users.parquet` (`user_id, plan_tier ∈ {free, basic, premium}, signup_date, home_city` simulated), `subscriptions.parquet` (`user_id, plan_tier, status`). Document that these are simulated.
4. **Packet loss simulation** (network metrics): `loss = clip(lognormal(μ=-2.3, σ=0.7) + k₁·max(0, rtt_ms−80)/1000 + k₂·max(0, 1 − throughput/2000)/10, 0, 15)` (%). Seeded; documented as simulated.
5. **Baseline simulated errors:** ~0.1 % of streams emit one `error` event (`NET_TIMEOUT, CDN_5XX, DECODE_ERR, DRM_ERR`, weights in config) at a random time; produced onto the `errors` topic stream file `errors.parquet`.
6. **Fault model** — `config/faults.yaml` (data-time offsets from the slice start):

```yaml
faults:
  - id: F1
    type: isp_outage
    selector: {isp: Airtel, city: Chennai}
    start_min: 25
    duration_min: 15
    effects: {throughput_mult: 0.15, rtt_mult: 4.0, loss_add_pct: 4.0,
              stall_prob_per_heartbeat: 0.25, stall_ms_range: [1500, 9000],
              vsf_prob: 0.25, error_prob_per_heartbeat: 0.02, bitrate_downshift_steps: 2,
              early_abandon_prob: 0.30}
  - id: F2
    type: cdn_slowdown
    selector: {cdn: Akamai}
    start_min: 60
    duration_min: 15
    effects: {throughput_mult: 0.35, rtt_mult: 2.5, loss_add_pct: 1.5,
              stall_prob_per_heartbeat: 0.10, stall_ms_range: [800, 4000], bitrate_downshift_steps: 1}
  - id: F3
    type: app_version_bug
    selector: {device: android, app_version: "5.2.1"}
    start_min: 90
    duration_min: 10
    effects: {error_prob_per_heartbeat: 0.08, error_codes: [DECODE_ERR], early_abandon_prob: 0.20}
```
7. `data/ledger/faults.jsonl` schema (written by the replayer, Phase 5): `fault_id, type, selector, start_ts, end_ts (sim clock), params, run_id`. This ledger is the **ground truth** for alert and anomaly evaluation and is never visible to ML features.
8. Fill in `docs/simulated_fields.md` (every simulated field: how generated, seed, distribution, why).

**Tests (`tests/phase_4`)**
- `test_determinism`: two runs with the same seed give byte-identical dims; a different seed differs.
- `test_session_consistency`: all rows of a viewer share dims; no nulls.
- `test_distribution`: empirical shares are within ±3 percentage points of configured weights on ≥ 5,000 sessions.
- `test_ref_integrity`: every `video_id` in events exists in `content.parquet`; every `user_id` exists in `users.parquet`.
- `test_packet_loss_bounds_and_correlation`: 0 ≤ loss ≤ 15; Pearson correlation(loss, rtt) > 0.
- `test_faults_yaml_valid`: schema-valid; selectors only use known dimension names and values; fault windows lie inside the slice.

**Exit gate:** `make test-phase4` green; `docs/simulated_fields.md` complete.

---

## Phase 5 — Replay producers, fault injection, chaos
**Goal:** a Python tool that replays the processed day into Kafka as a live stream at controlled speed, with faults and data-quality chaos, plus a manifest for reconciliation.
**Owner:** A. **Effort:** ~5–6 h.

**Tasks** (`src/qoe/producers/`)
1. `replay.py` CLI (`python -m qoe.producers.replay …`): `--speed`, `--duration`, `--start-offset`, `--max-sessions`, `--faults config/faults.yaml|none`, `--chaos config/chaos.yaml|none`, `--seed`, `--run-id`, `--loop`, `--dry-run` (write JSONL files instead of Kafka), `--bootstrap`.
2. **Time mapping:** `shift = wall_start − slice_start`; event `ts = ts_orig + shift` (ISO-8601 UTC, ms precision, `Z`). Emission pacing: for each event compute the target wall time `wall_start + (ts_orig − slice_start)/speed`; sleep to the target in micro-batches (e.g., 20 ms); never busy-spin; log lag if behind by > 1 s.
3. **Three logical producers** on one engine: *player telemetry* → `playback-events`; *CDN/network logs* → `network-metrics`; *errors* (simulated) → `errors`. Reference data is loaded by Spark (static) — optionally also publish it once to compacted topics `ref-content`, `ref-users` as a demonstration of the third source.
4. Join simulated dims (Phase 4) onto each record by `puffer_session_id` before serialization; key = `session_id`; JSON via `orjson`/`json`; headers `run_id`, `source`.
5. `faults.py` — apply effects only to events whose (dims) match the selector **and** whose `ts` lies in `[start, end)`:
   - network records: throughput × mult, rtt × mult, loss + add;
   - heartbeats: with `stall_prob_per_heartbeat` insert a synthetic `buffer_start` + `buffer_end` pair (`stall_ms` sampled from range; buffer_ms low), lower bitrate by N ladder steps;
   - `play_request`: with `vsf_prob` **suppress** the matching `first_frame` (stream shows a video start failure);
   - emit `error` events at the configured probability; with `early_abandon_prob` terminate the stream shortly after a stall (`session_end`);
   - write the fault into the **ledger**.
   Mark nothing inside the event; evaluation uses the ledger.
6. `chaos.py` — configurable rates (defaults in `config/chaos.yaml`): `duplicate_rate 0.5 %` (re-emit same `event_id`), `late_rate 0.5 %` (delay emission by 20–90 data-seconds while keeping the original `ts`), `invalid_rate 0.3 %` (negative `buffer_ms`, missing `session_id`, `bitrate_kbps` > 100000, unparsable `ts`, missing `event_type`), `dirty_dims_rate 2 %` (e.g., `"ANDROID "`, `"bharti airtel"`, `"Chennai "`). A `--chaos none` mode yields clean data for the correctness tests.
7. `manifest.py` — writes `data/manifests/run-<id>.json`: counts per topic of emitted, injected duplicates, injected late, injected invalid, injected dirty-dims, per-fault event counts, first/last ts, speed, seed.
8. `event_id` must be **deterministic** (`sha1(run_id|source|row_key)`) so injected duplicates share an id.
9. JSON Schema files in `schemas/` + a `validate_events.py` helper used by tests.

**Tests (`tests/phase_5`)**
- `test_time_mapping` (unit): `ts` offsets equal `ts_orig` offsets; first event ≈ run start.
- `test_pacing`: dry-run 10 data-minutes at `--speed 60` completes in 10 s ±15 %.
- `test_fault_selector`: an event inside the window and matching the selector is modified; matching-but-outside-window and in-window-but-not-matching events are unchanged (byte-compare).
- `test_fault_effects`: during F1, mean throughput of Airtel-Chennai records ≈ 15 % of baseline (±20 %); stall rate rises; VSF fraction ≈ configured.
- `test_chaos_rates`: observed duplicate/late/invalid/dirty shares are within ±30 % relative of configured rates; manifest counts equal observed counts.
- `test_schema_validation`: 100 % of non-invalid events validate against JSON Schema; every injected-invalid event fails validation.
- `test_ledger`: ledger entries match `faults.yaml` after the time shift.
- Integration (real Kafka): replay 3 data-minutes at 60×; per-partition **per-session order** preserved (excluding injected-late); consumer counts per topic == manifest emitted counts; headers present.

**Exit gate:** `make replay-demo` (3-min clean + 3-min with faults) works; `make test-phase5` green.

---

## Phase 6 — Spark Q0: parse, validate, standardize, dedup, enrich (silver)
**Goal:** a streaming job that turns bronze into trustworthy silver data, with full accounting for every rejected record.
**Owner:** B. **Effort:** ~5–6 h.

**Tasks** (`src/qoe/streaming/`)
1. `parse.py`: explicit `StructType` schemas (no inference); `from_json(value, schema)` with `PERMISSIVE` handling; keep the raw string in `_raw`; records where parsing fails → quarantine reason `PARSE_ERROR`.
2. `validate.py`: returns `(valid_df, rejected_df)`; each rejected row has `reason` (first failing rule). Rules: `MISSING_SESSION_ID`, `MISSING_EVENT_TYPE`, `UNKNOWN_EVENT_TYPE`, `BAD_TIMESTAMP` (unparsable or outside `[run_start − 1 day, run_end + 1 day]`), `NEGATIVE_BUFFER`, `NEGATIVE_STALL`, `IMPOSSIBLE_BITRATE` (<0 or > 100000), `BAD_RTT/THROUGHPUT/LOSS` (negative or loss > 100), `MISSING_REQUIRED_FIELD` per the §6.1 presence matrix.
3. `standardize.py`: timestamps → UTC `timestamp` column `ts`; trim/casefold; alias maps from `config/standardization.yaml` for `device` (`"ANDROID "`→`android`), `isp` (`"bharti airtel"`→`Airtel`), `city`, `country` (→ ISO-2); resolution normalised to `<H>p`; unknown values → `"unknown"` (counted).
4. **Dedup:** `withWatermark("ts", "2 minutes")` then `dropDuplicatesWithinWatermark(["event_id"])` (Spark 3.5; fallback `dropDuplicates(["event_id","ts"])`). Duplicates are counted via a side metric (compare pre/post counts per batch in `foreachBatch`; log and persist to `data/warehouse/etl_counters`).
5. `enrich.py`: **stream–static join** — left join `content.parquet` on `video_id` (adds `genre`, `language`), left join `users.parquet` on `user_id` (adds `plan_tier`). Enrichment misses must not drop rows.
6. `q0_ingest_clean.py`: three input topics (playback, network, errors) → per-topic pipeline → writes: valid → Kafka `playback-clean` / `network-clean` (errors merged into `playback-clean` as `event_type=error`); rejected → `dlq` topic and `quarantine_events` Parquet (with `reason`, `raw`); valid → `silver_*` Parquet partitioned by `dt, hr` (file sink with checkpoint). Checkpoints under `data/checkpoints/q0_*`.
7. `sinks_warehouse.py` shared helpers (partitioned write, schema evolution policy).
8. `etl_counters` table per micro-batch: `batch_id, in_count, valid, rejected_by_reason, deduped, ts`.

**Tests (`tests/phase_6`)**
- Unit (local `SparkSession`, batch DataFrames): `test_validate_each_rule` (one fixture row per rule → correct `reason`); `test_standardize_aliases`; `test_enrich_left_join_keeps_rows`; `test_parse_bad_json_to_quarantine`.
- Integration (real stack): replay 5 data-minutes with chaos at 60× while Q0 runs, then assert from the manifest and the sinks:
  - `emitted = silver_valid + quarantined + duplicates_removed + dropped_late_beyond_watermark` (the last term reported, not hidden);
  - zero rows in `playback-clean` violate §6.1 rules (re-validate by consuming the topic);
  - no `event_id` appears twice in silver;
  - every quarantined row has a non-null `reason` and non-null `_raw`;
  - dirty dims are fully standardized (no uppercase/whitespace variants in silver).
- `test_restart_recovery`: stop Q0 mid-run, restart; no duplicate `event_id` in silver and no data loss beyond watermark policy.

**Exit gate:** reconciliation test green; `make test-phase6` green.

---

## Phase 7 — Spark Q1: stateful session tracker → `session-facts` + HBase `session_state`
**Goal:** per-stream lifecycle state with timeouts; emits derived facts and maintains the HBase hot store.
**Owner:** B. **Effort:** ~5–6 h.

**Tasks** (`q1_session_tracker.py`)
1. Read `playback-clean`; `withWatermark("ts", "<watermark_tracker>")`; `groupBy("session_id").applyInPandasWithState(...)` with `EventTimeTimeout`.
2. State per session: `state ∈ {requested, playing, stalled, ended, failed_start}`, `play_request_ts, first_frame_ts, startup_ms, stall_count, stall_ms, watch_ms, last_bitrate, switch_count, error_flag, bitrate_sum_weighted, early_emitted`, plus dims.
3. Transitions: `play_request` → `requested` and set timeout = `ts + vsf_timeout_s`; `first_frame` → `playing`, compute `startup_ms`, emit fact `kind=startup`; `buffer_start` → `stalled`; `buffer_end` → `playing`, add `stall_ms`/`stall_count`; `heartbeat` → add `interval_ms` to `watch_ms`, update bitrate; `bitrate_switch` → counters; `error` → `error_flag`; `session_end` → `ended`, emit `kind=session_end` and remove state.
4. **Timeout handling:** if the timeout fires while still `requested` → emit `kind=vsf` (video start failure); sessions with no events for `streaming.session_idle_s` (data time) → close as `ended` (emit `kind=session_end` with `closed_by=idle`).
5. **Early features:** when `watch_ms ≥ ml.early_window_s × 1000` for the first time, emit `kind=early_features` containing the first-N-seconds features (startup_ms, stalls and stall_ms so far, avg bitrate, mean throughput/rtt so far if available, dims, scheme). Used in Phase 11.
6. Output: Kafka `session-facts` (key `session_id`) and `foreachBatch` upsert into HBase `session_state` (batch API via `foreachPartition`, one Thrift connection per partition, `batch_size=500`). Warehouse sink `session_facts` Parquet.
7. `sinks_hbase.py`: reusable `write_rows(table, rows, batch_size)` with retry/backoff and connection reuse.

**Tests (`tests/phase_7`)**
- Unit: the state-update function is a **pure pandas function**; test sequences — normal start+playback+end; start with one stall; stall at start; `play_request` with no `first_frame` (timeout → `vsf`); error then end; duplicate `buffer_end`; out-of-order `first_frame` before `play_request` (documented behaviour: ignored/flagged).
- Integration: replay a small clean slice; for every stream assert `session_facts` end fact `stall_ms` ≈ Σ stall in silver for that stream (±1 ms); `startup_ms` matches `stream_truth.startup_delay_s` (±50 ms); streams in `stream_truth` with `had_startup=false` → exactly one `vsf` fact.
- HBase: `session_state` rows exist for active sessions; column families `playback` and `context` populated; point-get via `salted_key` works; rows for ended sessions have `state=ended`; a salted-prefix scan distributes evenly (max bucket ≤ 2× mean).
- Restart test: kill/restart Q1 with the same checkpoint — states recovered, no duplicate `session_end` facts.

**Exit gate:** `make test-phase7` green.

---

## Phase 8 — Spark Q2/Q3: windowed QoE + composite score → HBase `qoe_metrics` + warehouse gold
**Goal:** sliding-window QoE per grain (global, city_isp, cdn, device, channel) and network health, written to HBase and the warehouse.
**Owner:** B (streaming) with A (HBase/warehouse sinks). **Effort:** ~6–8 h.

**Tasks**
1. **Q2 input:** union of `playback-clean` and `session-facts` (only `kind ∈ {startup, vsf}`) normalised to a common columns set (`ts`, dims, numeric metric columns nullable). `withWatermark("ts", "2 minutes")`.
2. **Grain explosion:** `explode(array(struct("global","ALL"), struct("city_isp", concat(city,"|",isp)), struct("cdn",cdn), struct("device",device), struct("channel",video_id)))` → `groupBy(window(ts, "5 minutes", "1 minute"), grain, dimkey)`.
3. **Additive aggregates** (so every metric is derivable and re-computable):
   - `stall_ms_sum`, `stall_count` (from `buffer_end`), `watch_ms_sum` (heartbeat `interval_ms`), `bitrate_weighted_sum` (`bitrate_kbps × interval_ms`), `hd_ms_sum` (interval where height ≥ `hd_min_height`), `ssim_weighted_sum`, `switch_count`, `startup_ms_sum` + `startup_n`, `play_requests`, `vsf_count`, `error_sessions` (approx distinct session with `error`), `sessions` = `approx_count_distinct(session_id, 0.01)` over heartbeats.
4. **Derived metrics:** `rebuffer_ratio = stall_ms_sum / (watch_ms_sum + stall_ms_sum)`; `rebuffer_freq_per_min = stall_count / (watch_ms_sum / 60000)`; `avg_bitrate_kbps = bitrate_weighted_sum / watch_ms_sum`; `hd_pct = hd_ms_sum / watch_ms_sum`; `switches_per_min = switch_count / (watch_ms_sum / 60000)`; `avg_startup_ms = startup_ms_sum / startup_n`; `vsf_rate = vsf_count / play_requests`; `error_rate = error_sessions / sessions`. Guard divisions by zero (→ null) and define behaviour for tiny denominators.
5. **Composite QoE** (`qoe_score.py`, pure function, config-driven):

```
startup_pen   = clip((avg_startup_s − startup_good_s) / (startup_bad_s − startup_good_s), 0, 1)
rebuffer_pen  = clip(rebuffer_ratio / rebuffer_full_penalty_ratio, 0, 1)
switch_pen    = clip(switches_per_min / switches_per_min_full_penalty, 0, 1)
bitrate_pen   = 1 − hd_pct                         # share of watch time below HD
failure_pen   = clip((vsf_rate + error_rate) / failure_full_penalty_rate, 0, 1)
QoE = 100 − (w_startup·startup_pen + w_rebuffer·rebuffer_pen + w_switch·switch_pen
             + w_bitrate·bitrate_pen + w_failure·failure_pen)       # weights sum to 100
label: Good ≥ 80, Fair 60–79, Poor < 60
```
   Null metrics contribute 0 penalty and are flagged (`qoe_partial=true`).
6. **Output mode `update`** (live). `foreachBatch`: (a) upsert to HBase `qoe_metrics` cf `m` (key via `qoe_key`), (b) append to `gold_qoe_5m_raw` with `updated_at` and `batch_id`, (c) pass the batch to the alerting module (Phase 9).
7. **Q3 network aggregator:** `network-clean`, same grains/windows; `avg_throughput_kbps, avg_rtt_ms, avg_packet_loss_pct, net_samples` → HBase cf `n` (same row key) + `gold_network_5m_raw`.
8. DuckDB view `gold_qoe_5m` = latest `updated_at` per `(grain, dimkey, window_end)` (`sql/warehouse_views.sql`).
9. `docs/hbase_design.md`: explain wide-column modelling, CF grouping on disk, why salting (session_state) and reverse timestamps (qoe_metrics), hotspot avoidance, TTL, range-scan examples with the exact scan commands.
10. **Performance:** record sustained input rate and trigger durations from `StreamingQueryProgress`; tune `shuffle_partitions`, `maxOffsetsPerTrigger`, trigger interval. Target: keep up with ≥ 500 events/s end-to-end on a laptop; if not, reduce `--max-sessions`/`--speed` and document.

**Tests (`tests/phase_8`)**
- Unit: `test_qoe_formula_bounds` (0–100); `test_qoe_monotonic` (worse startup/rebuffer/switches/failures never raise the score; better HD% never lowers it); `test_qoe_labels`; `test_derived_metrics_zero_division`.
- **Streaming-vs-batch equivalence (core correctness test):** replay a clean slice (no chaos); after drain, recompute the same additive aggregates for *closed* windows with DuckDB/pandas directly from `silver_playback_events`; assert exact equality for sums/counts and ≤ 5 % for `sessions` (HLL). Differences must be explainable by events later than the watermark (report the count).
- HBase key tests: scan prefix `city_isp#Chennai|Airtel#` returns newest→oldest; `limit=1` equals the max `window_end`; segments don't bleed across prefixes (`Chennai|Airtel` vs `Chennai|Airtel2`-style prefix collisions guarded by the `#` delimiter).
- Row shape: `m` is populated for every row; `n` appears after Q3; two rows may have different column sets.
- Fault visibility: replay with F1; `qoe_score` for `city_isp#Chennai|Airtel` during the fault is ≥ 15 points lower than the pre-fault mean; other segments' change is < 5 points.
- TTL/config sanity and idempotency: re-running a micro-batch yields identical HBase cell values.

**Exit gate:** `make test-phase8` green; screenshot of an HBase scan showing the newest-first range scan saved to `docs/`.

---

## Phase 9 — Alerting
**Goal:** rule-based alerts with hysteresis, written to HBase `alerts` and Kafka `alerts`; thresholds calibrated on baseline.
**Owner:** A. **Effort:** ~4 h.

**Tasks** (`alerting.py`, called from Q2 `foreachBatch`)
1. Rules per segment/window (only if `sessions ≥ alerts.min_sessions`):
   - **R1** `rebuffer_ratio > rebuffer_ratio_threshold` for `consecutive_windows` consecutive windows;
   - **R2** `qoe_score` dropped ≥ `qoe_drop_points` vs the window `baseline_lag_minutes` earlier (read from HBase by key);
   - **R3** `vsf_rate > vsf_rate_threshold`;
   - **R4** `error_rate` > `alerts.error_rate_threshold`.
2. Alert lifecycle: `alert_id = hash(rule, grain, dimkey, opened_window)`; states `open → resolved` when the metric is back within threshold for `consecutive_windows`; `cooldown_minutes` prevents flapping; severity from magnitude (warn / critical).
3. Outputs: HBase `alerts` row (key `rev_minute#alert_id`), Kafka `alerts` (JSON), warehouse `alerts` Parquet (append on open and resolve).
4. **Calibration script** `make calibrate`: replay a **no-fault** slice and compute per-grain distributions of `rebuffer_ratio`, `vsf_rate`, `error_rate`, `qoe_score`; propose thresholds = baseline p99 + margin (or mean + k·σ) subject to `min_sessions`; write recommended values to `config/config.yaml` (committed) and `docs/calibration.md` with the plots/tables. Document that real Puffer baseline stall ratio is low, so absolute defaults must be evidence-based.
5. `src/qoe/verify/eval_alerts.py`: reads the fault ledger + alerts and reports per-fault **detection (yes/no), detection latency (data-minutes from fault start to first alert), resolve latency, false-positive count**, and overall precision/recall.

**Tests (`tests/phase_9`)**
- Unit: rule engine on synthetic window sequences — fires only after N consecutive breaches; cooldown works; resolves correctly; ignores segments below `min_sessions`; R2 uses the right baseline window.
- Integration **no-fault baseline** (≥ 15 data-minutes): ≤ 1 alert total (target 0).
- Integration **fault run** (F1+F2+F3): every fault detected; F1 detected within ≤ 8 data-minutes of start; alerts are on the correct segment (F1 → `city_isp#Chennai|Airtel`, F2 → `cdn#Akamai`, F3 → `device#android`); each alert resolves within ≤ 10 data-minutes after the fault ends; overall recall = 100 %, precision ≥ 0.8.
- Delivery: the same alert exists in HBase, Kafka `alerts` and the warehouse (equal `alert_id` sets).

**Exit gate:** `make eval-alerts` prints a summary meeting the thresholds above; `make test-phase9` green.

---

## Phase 10 — Batch analytics: RDD demos, Spark SQL, DuckDB warehouse
**Goal:** satisfy the "Spark RDD / transformations / actions" and "Spark SQL" requirements and the historical-reporting requirement.
**Owner:** B. **Effort:** ~4–5 h.

**Tasks**
1. `batch/rdd_demo.py` (run on archived JSONL/Parquet of silver events or on the raw JSONL archive): using **RDDs** only for the core logic:
   - load → `textFile`/`parallelize`, **`map`** (parse), **`filter`** (drop heartbeats / keep errors), **`flatMap`** (e.g., tokenize `format`/`error_code`, or expand a session into its events), **`reduceByKey`** (errors per ISP; stalls per channel), `aggregateByKey` (mean bitrate per device), `groupByKey` (demonstrate why it's worse), `sortBy`/`takeOrdered` (top-10 ISPs by error count), `distinct`, `union`, `join` (pair RDDs with reference data), `broadcast` + `accumulator` (count invalid rows), `persist/cache`;
   - **actions:** `collect()`, `count()`, `take(n)`, `first()`, `reduce`, `countByKey`, `saveAsTextFile`.
   - Print lineage with `toDebugString()` for one chain; comment each step.
2. `batch/sql_demo.py` + `sql/spark_sql_queries.sql`: register temp views over silver/gold Parquet and run: **worst 10 ISP–CDN pairs by rebuffering ratio in the last hour**; **peak-hour QoE by device**; QoE class distribution per city; day/hour heatmap data (`rollup`/`cube`); window functions (`LAG` QoE change per segment, `RANK` worst segments per hour); top error codes per app version; fault-period vs baseline comparison.
3. `batch/warehouse.py`: create DuckDB database file `data/warehouse/qoe.duckdb` with views over Parquet (`gold_qoe_5m` de-dup view, silver, alerts, predictions).
4. `batch/hourly_rollup.py`: Spark batch → `gold_qoe_hourly` (and daily) Parquet.
5. `sql/report_queries.sql`: the historical report — hourly QoE trend, top-5 worst segments, alert counts per hour/rule, fault-period impact table.
6. **Optional stretch (`batch/backfill.py`):** to enable *daily/weekly* trends from a single day of source data, generate N "synthetic days" by re-timing the slice with seeded perturbations (clearly labelled synthetic, written to a separate warehouse namespace `synthetic_backfill`).

**Tests (`tests/phase_10`)**
- `test_rdd_ops_present`: static check that the demo uses `map, filter, flatMap, reduceByKey` and the actions `collect, count, take`.
- `test_rdd_results_match_sql`: errors per ISP from the RDD job == the same query in Spark SQL == the same query in DuckDB.
- `test_sql_worst_pairs`: result has ≤ 10 rows, sorted descending; during F1/F2 the expected pair/CDN appears in the "worst" list.
- `test_duckdb_view_dedup`: `gold_qoe_5m` has one row per `(grain, dimkey, window_end)`.
- `test_hourly_rollup_matches`: hourly totals equal the sum of 5-min closed windows' additive columns (±0.5 %).

**Exit gate:** `make batch-demo` prints all demo outputs; `make test-phase10` green.

---

## Phase 11 — Machine learning (Spark MLlib)
**Goal:** one supervised model (abandonment prediction) and one unsupervised/statistical model (anomaly detection), both scored on the stream; optional QoE forecasting.
**Owner:** B. **Effort:** ~6–8 h.

**Tasks**
1. **Dataset (`ml/dataset.py`):** build from `session_facts` (`early_features` + `session_end`) in the warehouse, from a full-slice run **without faults** (training) and a held-out run **with faults** (testing generalisation). Population: streams with `first_frame` and ≥ `early_window_s` of playback. **Label** `abandoned = 1` if total `watch_s < abandon_watch_s` (real, from Puffer) — fault runs additionally contain *synthetic* abandonment (`early_abandon_prob`), which must be stated in the report.
2. **Features (strictly from the first `early_window_s` seconds — no leakage):** `startup_ms`, `stall_count_early`, `stall_ms_early`, `avg_bitrate_early`, `switch_count_early`, mean `throughput_kbps`/`rtt_ms`/`packet_loss_pct` (early), `buffer_ms_at_cutoff`, `scheme`, `device`, `isp`, `cdn`, `city`, `app_version`, `plan_tier`.
3. **Pipeline:** `StringIndexer` + `OneHotEncoder` → `VectorAssembler` → `StandardScaler` → classifier (`RandomForestClassifier` and `GBTClassifier`, compared) with class weights (`weightCol`) for imbalance; `CrossValidator` (`folds=3`, grid on `numTrees/maxDepth`), train/test split by **time** (train on earlier streams, test on later) and report **AUC-ROC, AUC-PR, accuracy, precision, recall, F1** plus a baseline (majority class, and logistic regression). Feature importance table. Save the best `PipelineModel` to `data/models/abandon_v1`.
4. **Anomaly detection (`ml/anomaly.py`):** on gold 5-min segment windows — (a) **robust z-score** (median/MAD) of `rebuffer_ratio`, `avg_throughput_kbps`, `qoe_score` per segment versus its own history; (b) **K-Means** (k chosen by silhouette) on standardised windowed metrics with distance-to-centroid threshold. Output `anomaly_flag, score` per window. Evaluate against the fault **ledger** (window-level precision/recall/F1, detection latency).
5. **Optional forecast (`ml/forecast.py`):** predict `qoe_drop_next_window` (QoE falls ≥ 10 points next window) from lagged window features with `GBTClassifier`; time-ordered split.
6. **Streaming scoring (`q4_ml_scoring.py`):** read `session-facts` where `kind=early_features`, apply the saved `PipelineModel.transform`, write `abandon_probability` to Kafka topic `predictions` (add to §6.4), warehouse `ml_predictions`, and (optionally) a `predicted_risk` column in HBase `session_state`.
7. `docs/ml_report.md`: data, labelling caveat, features, metrics tables, confusion matrix, feature importance, limitations (synthetic dims limit external validity).

**Tests (`tests/phase_11`)**
- `test_no_leakage`: all feature timestamps ≤ stream start + `early_window_s`; label-derived columns are absent from features.
- `test_pipeline_fits_and_transforms` on a 500-row fixture (schema, probability column in [0,1]).
- `test_metrics_floor`: on the real slice, AUC-ROC of the best model > 0.60 **and** exceeds the majority/LogReg baseline; if the real signal is weak, the test records the observed value and the plan requires documenting it rather than tuning on the test set.
- `test_model_roundtrip`: save → load → identical predictions on 100 rows.
- `test_anomaly_vs_ledger`: window-level recall ≥ 0.8 of fault windows for at least one method; false positives per baseline hour ≤ configured limit (default 3).
- Integration: Q4 running during a fault replay produces predictions for ≥ 95 % of `early_features` facts; mean predicted risk inside the F1 segment is higher than outside (report the ratio).

**Exit gate:** `make train` and `make eval-anomaly` produce metrics files in `docs/`; `make test-phase11` green.

---

## Phase 12 — Dashboard & alert feed (Streamlit)
**Goal:** a live operations view (HBase) and a historical view (DuckDB).
**Owner:** A. **Effort:** ~5 h.

**Tasks** (`src/qoe/dashboard/`)
1. `data_access.py` (no Streamlit imports; unit-testable): `latest_segment(grain, dimkey)` → HBase prefix scan `limit=1`; `segment_history(grain, dimkey, minutes)` → prefix scan limit N (newest-first, then reversed for plotting); `latest_all(grain)`; `recent_alerts(n)`; `active_sessions_count()`; `history_*` queries from DuckDB.
2. **Live tab** (auto-refresh every 5 s): KPI cards (global QoE score + label, rebuffer ratio, avg bitrate, VSF rate, concurrent sessions, active alerts); QoE and rebuffer **trend line** for a selected segment; **city × ISP heatmap** of QoE (Plotly; Poor = red); CDN and device bar charts; **alert feed** (newest first, open vs resolved, severity colours); network health panel; an "injected fault window" shaded band (read from the ledger, toggleable, labelled **simulated**).
3. **History tab (DuckDB):** hourly QoE trend, worst segments table, alert counts, fault-period impact, optional synthetic multi-day trend if Phase 10 stretch exists.
4. **Model tab:** abandonment-risk distribution for live sessions, top at-risk segments, model metrics summary.
5. Footer: "Real data: Stanford Puffer (one day, replayed). Simulated: city, ISP, CDN, device, app version, packet loss, errors, users, injected faults." Show data freshness (age of last window).
6. Graceful states: HBase/Kafka down, no data yet, stale data (> 60 s) warning.

**Tests (`tests/phase_12`)**
- Unit: `data_access` against the live HBase with seeded rows — newest-first ordering, `limit=1` correct, empty-segment returns `None`, history window length correct.
- `test_dashboard_boots`: `streamlit run` starts headless and `/_stcore/health` returns OK within 30 s.
- Browser smoke (Playwright or the agent's browser tool): during a fault replay the heatmap cell for Chennai|Airtel turns Poor, the alert feed shows a new open alert within the expected latency, and a screenshot is saved to `docs/screenshots/`.
- Freshness test: stop the replayer → the stale banner appears after 60 s.

**Exit gate:** `make dashboard` serves on `:8501`; `make test-phase12` green; screenshots committed.

---

## Phase 13 — ETL evidence, mapping & verification
**Goal:** produce the documentation and automated proofs the lab brief asks for (mapping, transformation, validation, verification).
**Owner:** B. **Effort:** ~3–4 h.

**Tasks**
1. `docs/etl_mapping.md` — complete **source-to-target mapping** tables: (a) Puffer CSV fields → canonical events; (b) simulated/reference sources → canonical dims; (c) canonical events → silver Parquet columns; (d) aggregates → HBase `qoe_metrics` (cf/qualifier) and warehouse gold columns; (e) session facts → HBase `session_state`. Include transformation type per row (standardize, derive, aggregate, validate, enrich, dedup, sort).
2. `src/qoe/verify/reconcile.py` (`make verify`): produces `docs/verification_report.md` with pass/fail per check:
   - **V1** Kafka bronze offsets total == manifest `emitted` (per topic).
   - **V2** `emitted = silver_valid + quarantined + duplicates_removed + late_dropped` (exact, with each term shown).
   - **V3** silver `event_id` uniqueness; zero rule violations in silver (re-run validator).
   - **V4** `session_facts` completeness: every stream with `play_request` has exactly one terminal fact (`session_end` or `vsf`).
   - **V5** Streamed aggregates vs batch recomputation for closed windows (additive columns exact; HLL ≤ 5 %).
   - **V6** HBase latest rows equal warehouse latest rows for the same keys.
   - **V7** Alerts consistent across HBase/Kafka/warehouse.
   - **V8** Standardisation coverage: share of `unknown` values per dimension ≤ 0.1 %.
3. `lineage.py`: emits a Mermaid/ASCII lineage diagram into `docs/architecture.md`.
4. Sorting evidence: show the Spark SQL `ORDER BY` on event time and the HBase reverse-timestamp ordering side by side.

**Tests (`tests/phase_13`)**
- `test_reconcile_runs_on_clean_run`: all V1–V8 pass on a clean-run fixture; deliberately corrupt one record count in a copy of the manifest → V1/V2 fail (the checker can actually fail).
- `test_mapping_doc_covers_all_fields`: every field in the JSON Schemas appears in `etl_mapping.md`.

**Exit gate:** `make verify` exits 0 and writes the report; `make test-phase13` green.

---

## Phase 14 — End-to-end run, performance, demo & report
**Goal:** one-command demo, final acceptance, and the written report.
**Owner:** Both. **Effort:** ~5–6 h.

**Tasks**
1. `scripts/demo.sh` / `make demo`: `up` → `init` → start Q0, Q1, Q2, Q3, Q4 → start dashboard → replay (speed 10, 120-min slice, faults + chaos on) → print the URLs and the expected demo timeline (F1 at data-minute 25 ≈ wall 2.5 min, F2 at 60, F3 at 90). `make demo-stop` and `make reset-state` clean everything.
2. `docs/demo_script.md` — 5–7 minute walkthrough: architecture (30 s) → Puffer-based real data + what is simulated (30 s) → clean baseline (1 min) → fault F1 appears, QoE falls, alert fires, HBase row shown via a range scan, resolved (2 min) → Spark RDD & SQL (1 min) → ML prediction and anomaly detection (1 min) → verification report (30 s).
3. **Performance & soak:** run the full pipeline for ≥ 30 wall-minutes at `speed 10`; record throughput, trigger latency, end-to-end latency (event ts → HBase row), CPU/RAM; save to `docs/performance.md`. Targets: sustained ≥ 500 events/s; p95 end-to-end latency < 30 s wall; no query failure; no unbounded state growth (state rows plateau).
4. **Report (`docs/report/`)** — outline: 1 Problem & motivation (QoE); 2 Data (Puffer + simulated, clearly labelled); 3 Architecture diagram (export PNG from Mermaid/draw.io); 4 Kafka design; 5 Spark streaming design (watermarks, windows, state); 6 HBase wide-column design (row keys, CFs, salting, reverse timestamp, TTL) with scan demos; 7 ETL (mapping, transformations, validation, verification results); 8 RDD/SQL analytics; 9 ML (methods, metrics, caveats); 10 Dashboard screenshots; 11 Fault-injection evaluation (alert detection table); 12 Limitations & future work (simulated dims, single day, no real errors, live-TV vs VOD, packet loss simulated); 13 Team contributions; Appendix: config, commands.
5. `README.md`: prerequisites, quick start (`make doctor build up init demo`), troubleshooting (HBase hostname, Kafka listeners, Windows line endings, port conflicts, memory limits), and the data-download note.
6. Final cleanup: remove dead code, `make lint`, pin `requirements.txt`, ensure `make test-all` is green from a clean clone.

**Tests (`tests/phase_14`)**
- `e2e_full.py` (`make e2e`): from a clean state, run the demo for a shortened slice (30 data-minutes at 30×, faults compressed in time), then assert: all five queries `isActive`; ≥ 1 alert per injected fault on the right segment; `make verify` green; HBase has rows in all three tables; warehouse tables non-empty; dashboard health OK; predictions topic non-empty.
- `test_reproducibility`: two e2e runs with the same seed give identical fault ledgers and identical per-window additive aggregates for closed windows (±HLL tolerance).
- `test_docs_complete`: required docs and screenshots exist; the simulated-fields disclaimer appears in README, dashboard footer and report.

**Exit gate — FINAL ACCEPTANCE CHECKLIST**
- [ ] `make doctor build up init test-all e2e verify` all green on a clean clone
- [ ] Real Puffer day replayed through Kafka with controlled speed; simulated fields labelled everywhere
- [ ] Three sources (player telemetry, network metrics, reference data) integrated
- [ ] Parse / validate / standardize / dedup / enrich / watermark / window / stateful tracking demonstrated and tested
- [ ] HBase wide-column tables with documented row-key design and scan demos
- [ ] Warehouse (DuckDB over Parquet) with historical report
- [ ] RDD transformations + actions, Spark SQL, MLlib pipeline (CV, metrics), model scored on stream
- [ ] Alerts fire on injected faults (precision/recall/latency reported) and not on baseline
- [ ] Verification report passes; reconciliation shows every record accounted for
- [ ] Dashboard live + history; screenshots; demo script rehearsed
- [ ] Report complete with architecture diagram and honest limitations

---

## 7. Cross-cutting requirements

| Topic | Requirement |
|---|---|
| Idempotency | All HBase writes are overwrites of deterministic keys; warehouse raw tables carry `batch_id`/`updated_at`; init scripts are re-runnable. |
| Checkpoints | One directory per query under `data/checkpoints/`; `make reset-state` clears checkpoints, topics, tables, warehouse, manifests together (never partially). |
| Error handling | Producers retry with backoff and surface delivery failures; Spark sinks retry HBase writes 3× then fail the batch loudly (no silent drops). |
| Observability | Log `StreamingQueryProgress` summaries every minute (input rate, processing rate, state rows, watermark); persist to `data/warehouse/query_progress`. |
| Resource limits | Document Docker memory needs (suggest ≥ 8 GB for Docker); set `spark.driver.memory`, `spark.sql.shuffle.partitions=8`, `local[4]`. |
| Security/privacy | Puffer data is anonymised; do not attempt re-identification; no secrets in repo. |
| Reproducibility | Single `seed` drives dims, faults, chaos, packet loss, sampling; run ids recorded in manifests. |
| Documentation hygiene | Every phase appends to `docs/PROGRESS.md`; decisions that deviate from this plan are recorded with the reason. |

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Multi-GB CSVs overwhelm memory | DuckDB out-of-core, filter to the slice first, sample sessions, work on the fake sample in CI. |
| HBase container networking/hostname problems | Fixed `hostname: hbase`, healthcheck, only Thrift exposed to other containers; document fix steps in README. |
| HappyBase install/compat issues | Pin versions; fallback to HBase REST client behind the same `sinks_hbase` interface. |
| Spark can't keep up with event rate | Lower `--speed`/`--max-sessions`, reduce grains, raise `maxOffsetsPerTrigger`, tune shuffle partitions; record limits honestly. |
| Real Puffer baseline has few stalls → weak signals | Faults supply controlled degradation; thresholds calibrated; ML limitations documented. |
| Stateful operator restrictions in Spark 3.5 | Q1 runs as a separate query writing to Kafka; windows read from topics (D7). |
| Late data lost after watermark | Quantified in reconciliation (term shown, not hidden). |
| Puffer download blocked or slow | Fake sample for development; ask the user to download manually; never re-download. |

## 9. Suggested schedule for a team of two

| Phase(s) | Owner | Notes |
|---|---|---|
| 0, 1, 4, 5 | A | Infra, simulation, replay/fault engine |
| 2, 3, 6, 7 | B | Data prep, Spark ingest, tracker |
| 8 | B + A | Streaming core + sinks |
| 9, 12 | A | Alerting, dashboard |
| 10, 11, 13 | B | Batch, ML, verification |
| 14 | Both | E2E, demo, report |

## 10. Human hand-offs and failure protocol

**Only these steps need a human** (ask once, early, then continue with fallbacks while waiting):

| # | Step | When | Fallback while waiting |
|---|---|---|---|
| H1 | Install Docker Desktop (≥ 8 GB RAM allocated; WSL2 backend on Windows) | Before Phase 1 | none — blocks Phase 1+; Phase 0 and pure-Python unit tests still proceed |
| H2 | Download one Puffer day into `data/raw/puffer/<day>/` if the agent's network cannot reach the bucket | Phase 2 | develop Phases 3–9 on the fake/mini fixture (`make data-fake`) |
| H3 | Rehearse the demo and fill in team-contribution text for the report | Phase 14 | agent drafts both |

**When a gate fails** the agent must: (1) read the failing test's output, (2) fix the root cause (never weaken or delete the test, never raise a threshold just to pass), (3) re-run the gate, (4) after 3 failed attempts, write the failure, hypotheses and what was tried into `docs/PROGRESS.md`, and ask the user. A threshold may be changed only through the calibration procedure (Phase 9) with the evidence recorded.

**Deviation rule:** any departure from this plan (version swap, scope cut, tolerance change) is recorded in `docs/PROGRESS.md` as `DEVIATION: <what> — <why> — <impact on requirements>`.

## 11. Fallback if Docker/HBase cannot run (Databricks Community Edition path)

The lab brief allows Databricks CE, but CE cannot run Kafka or HBase, so this is a **degraded fallback**, used only if H1 is impossible. It must be declared in the report.

| Requirement | Primary design | Fallback on Databricks CE |
|---|---|---|
| Kafka stream | Replayer → Kafka | Replayer writes JSON micro-files into a DBFS folder; Spark `readStream.format("json")` (or `rate` source) is the stream. Document that Kafka is replaced and why. |
| HBase wide-column | `session_state`, `qoe_metrics`, `alerts` | Not available. Implement the **same row-key design** over a Delta/Parquet table (key column + column-family-prefixed columns) and demonstrate prefix range scans with SQL; state clearly this models but does not satisfy the HBase requirement. Run the HBase tests locally if any machine can run Docker. |
| Spark RDD / SQL / MLlib / Streaming | as planned | unchanged (Phases 6, 8–11 logic is pure `DataFrame -> DataFrame`, so it ports directly) |
| Warehouse | DuckDB over Parquet | Delta tables or DuckDB in the driver |
| Dashboard | Streamlit | Databricks SQL / notebook display + exported screenshots |

Everything under `src/qoe/data`, `producers` (dry-run mode), `streaming/{parse,validate,standardize,enrich,qoe_score,alerting}.py`, `batch/` and `ml/` is written to run unchanged in a notebook.

## 12. Lab-brief compliance matrix (what the examiner will look for → where the evidence lives)

| Lab brief item | Evidence artefact | Produced in |
|---|---|---|
| Data: semi-structured, streaming, time series | `schemas/*.json`, Kafka topics, event-time windows; `docs/data_profile.md` | 2, 3, 5, 8 |
| DB: HBase, wide-column model | `docs/hbase_design.md`, scan screenshots in `docs/`, `tests/phase_1`, `tests/phase_8` | 1, 8 |
| Engine: Spark + Kafka | `docker-compose.yml`, `q0…q4`, Kafka UI screenshot | 1, 5–9, 11 |
| Language: Python | whole repo | all |
| Reference architecture diagram | `docs/architecture.md` (+ PNG in report) | 13, 14 |
| Domain with high volume, high dimensionality, multiple sources | three sources; dims device/ISP/CDN/city/channel/scheme/app_version | 3–5 |
| ETL pipelines, mapping and integration | `docs/etl_mapping.md`, Q0 pipeline, `etl_counters` | 3, 6, 13 |
| Standardization, sorting, aggregation, validation, verification | `standardize.py`, `validate.py`, Q2, `verification_report.md` (V1–V8) | 6, 8, 13 |
| ML model / analytics | `docs/ml_report.md`, `docs/calibration.md`, anomaly eval | 9, 11 |
| Spark RDD, transformations, actions | `batch/rdd_demo.py` output | 10 |
| Spark SQL & ML libraries | `sql/*.sql`, MLlib `PipelineModel` | 10, 11 |
| Real-time stream processing | live demo, `docs/performance.md` | 6–9, 14 |
| Sliding-window metrics (sample project analogue: "active rides, avg fare per neighbourhood, last 5 min") | Q2: 5-min window / 1-min slide per `city_isp`, `cdn`, `device`; `sessions` + QoE | 8 |
| Operational store + warehouse + historical reporting | HBase hot path, DuckDB `report_queries.sql` | 8, 10 |

## 13. Appendix — metric definitions (single source of truth; code, SQL, docs and dashboard must agree)

| Metric | Formula | Notes |
|---|---|---|
| Startup time | `first_frame.ts − play_request.ts` | per stream; window average over streams started in the window |
| Rebuffer ratio | `Σ stall_ms / (Σ watch_ms + Σ stall_ms)` | `watch_ms` = Σ heartbeat `interval_ms` (playing time only) |
| Rebuffer frequency | `stall_count / (watch_ms / 60000)` | stalls per playing minute |
| Average bitrate | `Σ(bitrate_kbps × interval_ms) / Σ interval_ms` | time-weighted |
| HD share | `hd_ms / watch_ms` | HD = height ≥ `qoe.hd_min_height` |
| Switches per minute | `switch_count / (watch_ms / 60000)` | |
| VSF rate | `vsf_count / play_requests` | a `vsf` = `play_request` with no `first_frame` within `vsf_timeout_s` |
| Playback error rate | `error_sessions / sessions` | |
| Concurrent sessions | `approx_count_distinct(session_id)` over heartbeats in window | HLL rsd 0.01 |
| Network health | mean `throughput_kbps`, `rtt_ms`, `packet_loss_pct` | loss is **simulated** |
| QoE score / label | §Phase 8 formula; Good ≥ 80, Fair 60–79, Poor < 60 | `qoe_partial=true` when inputs are null |
| Abandonment (label) | `watch_s < ml.abandon_watch_s` and `had_startup` | real Puffer behaviour, plus synthetic early-abandon in fault runs (state it) |

## 14. Appendix — index of generated documents

`docs/PROGRESS.md`, `architecture.md`, `data_profile.md`, `simulated_fields.md`, `etl_mapping.md`, `hbase_design.md`, `calibration.md`, `ml_report.md`, `verification_report.md`, `performance.md`, `demo_script.md`, `screenshots/`, `report/`. `test_docs_complete` (Phase 14) fails if any is missing or empty.

---

*Note on the source data:* the Puffer day's exact bucket paths were not verified when this plan was written — the agent must confirm file names and header presence against the actual download in Phase 2 and adjust the Phase 3 SQL if they differ.
