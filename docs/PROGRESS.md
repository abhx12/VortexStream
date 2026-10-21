# Project Implementation Progress

## Phases Tracking

| Phase | Title | Status | Date Completed | Notes / Test Results |
|---|---|---|---|---|
| Phase 0 | Bootstrap & conventions | COMPLETED | 2026-10-21 | 6/6 tests passing (config, logging, Makefile, doctor, tunables) |
| Phase 1 | Infrastructure (Docker: Kafka, HBase, app) | COMPLETED | 2026-10-21 | 7/7 tests passing (HBase keys, salts, rev_minute, topics, compose) |
| Phase 2 | Puffer data acquisition & profiling | COMPLETED | 2026-10-21 | 4/4 tests passing (schema, time units, profile report, caching) |
| Phase 3 | Preprocess Puffer into unified events | COMPLETED | 2026-10-21 | 7/7 tests passing (DuckDB ASOF join, downsampling, events/network/truth) |
| Phase 4 | Simulated dimensions, reference data, faults | COMPLETED | 2026-10-21 | 6/6 tests passing (deterministic dims, reference data, packet loss, faults) |
| Phase 5 | Replay producers, fault injection, chaos | COMPLETED | 2026-10-21 | 5/5 tests passing (pacing, fault injection, chaos rates, schema validation, ledger) |
| Phase 6 | Structured Streaming Q0: Ingest, clean, route | COMPLETED | 2026-10-21 | 5/5 tests passing (parse, validate 9 rules, standardize, dedup, enrich) |
| Phase 7 | Structured Streaming Q1: Session tracker | COMPLETED | 2026-10-21 | 6/6 tests passing (lifecycle, stalls, VSF timeout, idle timeout, early features, schema) |
| Phase 8 | Structured Streaming Q2 & Q3: Aggregators | COMPLETED | 2026-10-21 | 7/7 tests passing (QoE formula, monotonicity, zero guards, multi-grain windows, network cf n, HBase keys) |
| Phase 9 | Alerting & baseline calibration | PENDING | - | Rules R1-R4, alert state machine, calibration |
| Phase 10 | Batch analytics (RDD, SQL, DuckDB warehouse) | PENDING | - | RDD transformations, Spark SQL queries, rollups |
| Phase 11 | MLlib abandonment prediction & anomaly detection | PENDING | - | PipelineModel classification, Robust Z / KMeans |
| Phase 12 | Real-time operations dashboard | PENDING | - | Streamlit dashboard reading HBase & DuckDB |
| Phase 13 | Verification, reconciliation & data quality | PENDING | - | V1-V8 reconciliations, ledger evaluation |
| Phase 14 | Final package, demo automation & report | PENDING | - | End-to-end demo script, packaging, acceptance |

## Phase Decisions & Notes

### Phase 7: Structured Streaming Q1: Session tracker
- Implemented `src/qoe/streaming/q1_session_tracker.py` with pure transition functions and timeout checks.
- Handled state transitions across `requested`, `playing`, `stalled`, `ended`, `failed_start`.
- Computed `startup_ms` on `first_frame`, cumulative `stall_count` and `stall_ms` on `buffer_end`, and `watch_ms` from heartbeats.
- Implemented VSF detection timeout (`vsf_timeout_s=10`) when `first_frame` is missing, and session idle timeout (`session_idle_s=60`).
- Emitted `early_features` fact at 15s watch time containing early-window QoE telemetry for MLlib churn prediction.
- Emitted facts validated against `schemas/session_fact.schema.json`.
- Implemented HBase `session_state` upserts and warehouse Parquet sink.

### Phase 8: Structured Streaming Q2 & Q3: Aggregators
- Implemented `src/qoe/streaming/qoe_score.py` computing composite QoE score [0, 100] with configurable penalty weights and Good/Fair/Poor categorization.
- Implemented `src/qoe/streaming/q2_qoe_aggregator.py` executing 5-minute sliding window aggregations across 5 grains: `global`, `city_isp`, `cdn`, `device`, `channel`.
- Implemented `src/qoe/streaming/q3_network_aggregator.py` computing windowed network transport metrics (`avg_throughput_kbps`, `avg_rtt_ms`, `avg_packet_loss_pct`, `net_samples`).
- Implemented HBase writes for `qoe_metrics` (column families `m` and `n`) using reverse timestamp keys for fast latest-window range scans.
- Wrote `docs/hbase_design.md` detailing wide-column architecture, salting, reverse timestamp indexing, and scan commands.


### Phase 0: Bootstrap & conventions
- Created repository layout with standardized packages.
- Implemented `src/qoe/common/config.py` supporting `config/*.yaml`, typed dot-access, and `QOE__` env overrides.
- Implemented `src/qoe/common/logging.py` outputting structured JSON lines with `run_id`.
- Created comprehensive `Makefile` containing all phase targets.
- Created `scripts/doctor.py` checking ports, Python dependencies, disk space, Docker Compose, and daemon status.
- Added test suite in `tests/phase_0/`.
