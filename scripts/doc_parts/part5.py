# scripts/doc_parts/part5.py
"""Sections 24 to 28 for PROJECT_DEEP_DIVE.md"""

def get_part5() -> str:
    return """---

## 24. Comprehensive Viva Question Bank

This section provides an exhaustive, examiner-grade question bank designed specifically to prepare you for technical defenses, viva examinations, and architecture walkthroughs. Every question is grounded directly in the actual VortexStream code.

---

### Category A: Project Overview

#### Q1: What is the primary objective of Project VortexStream?
- **Short Viva Answer**: VortexStream is an end-to-end distributed stream processing and analytics platform for OTT video platforms that ingests player telemetry, computes multi-grain QoE scores in real time, triggers hysteresis-guarded operational alerts, and predicts early viewer abandonment using machine learning.
- **Deeper Technical Explanation**: Commercial video providers suffer severe subscriber churn when streaming quality degrades. VortexStream solves this by combining Apache Kafka (distributed buffering), Apache Spark Structured Streaming (stateful session tracking and 5-minute sliding window aggregations), Apache HBase (sub-millisecond wide-column operational store), and DuckDB (columnar Parquet data warehouse) into an enterprise lambda/kappa architecture.
- **Code Reference**: `README.md`, `src/qoe/streaming/q0_ingest_clean.py` through `q4_ml_scoring.py`.
- **Concrete Example**: A user in Chennai on Airtel experiences a CDN slowdown; within 5 seconds, VortexStream detects the rebuffering spike on a 5-minute sliding window, updates the 4x4 heatmap in HBase, and fires an operational alert.
- **Likely Follow-Up**: *Why not just use Grafana on top of an SQL database?*  
  *Answer*: Standard relational databases cannot sustain thousands of concurrent streaming write mutations without locking contention, and Grafana lacks built-in stateful session timeout tracking, VSF detection, and real-time ML churn scoring.

#### Q2: What are the primary inputs and outputs of the system?
- **Short Viva Answer**: Inputs are raw JSON playback telemetry (buffer levels, stalls, bitrates) and TCP network metrics. Outputs are real-time HBase metrics, operational alert events, early session abandonment probabilities, and historical DuckDB Parquet warehouse tables.
- **Deeper Technical Explanation**: Inputs enter Kafka bronze topics (`playback-events`, `network-metrics`). Spark cleans, normalizes, and enriches them. The outputs are multi-homed: hot operational state is written to HBase tables (`session_state`, `qoe_metrics`, `alerts`), while cold immutable facts are written to partitioned Parquet files (`data/warehouse/`) queried via DuckDB and visualized on a Streamlit dashboard.
- **Code Reference**: `schemas/playback_event.schema.json`, `src/qoe/producers/replay.py`.
- **Likely Follow-Up**: *What happens if a player emits corrupted JSON?*  
  *Answer*: It is caught by Rule R-VAL-1 in `validate.py` and routed to the `quarantine` dead-letter topic without halting the streaming pipeline.

#### Q3: What is the most significant technical contribution of this project?
- **Short Viva Answer**: The implementation of zero-leakage early churn prediction combined with multi-grain wide-column HBase storage optimized for sub-millisecond operational scans.
- **Deeper Technical Explanation**: Most churn models suffer from data leakage by accidentally including metrics collected after the user decided to leave. VortexStream strictly fences feature extraction to the first 15 seconds of playback (`early_features`), achieving 0.884 AUC-ROC. Architecturally, the project demonstrates advanced HBase row-key engineering (salting across 16 buckets to prevent write hotspots, and 19-digit reverse-timestamping for newest-first scans).
- **Code Reference**: `src/qoe/ml/dataset.py`, `src/qoe/common/hbase_keys.py`.

---

### Category B: Dataset and Preprocessing

#### Q4: Where does the underlying streaming data originate, and how much of it is real?
- **Short Viva Answer**: The core physical telemetry originates from Stanford University's open Puffer research dataset. Dimensions like Indian cities, ISPs, CDNs, and device types are simulated deterministically using SHA-256 session hashing.
- **Deeper Technical Explanation**: We download real raw CSVs (`client_buffer`, `video_sent`, `video_acked`) from Stanford Puffer (`https://puffer.stanford.edu/`). The buffer occupancy (`buffer_ms`), rebuffering stalls (`stall_ms`), delivery rates, and round-trip times are 100% empirical research measurements. However, because Puffer is an academic server in California, it lacks commercial metadata. We deterministically map sessions to Indian market distributions using `sha256(session_id + "42")`, ensuring 100% reproducibility across all runs.
- **Code Reference**: `src/qoe/data/puffer_download.py`, `src/qoe/data/preprocess.py`, `src/qoe/data/sim_dims.py`.
- **Likely Follow-Up**: *Why did you use deterministic hashing instead of random assignment?*  
  *Answer*: Deterministic hashing guarantees that if session `sess-12345` is processed across different batch jobs, unit tests, or replayer runs, it always maps to the exact same city, ISP, and device, guaranteeing test repeatability and data lineage integrity.

#### Q5: How did you join client buffer metrics with server-side TCP transport metrics?
- **Short Viva Answer**: We executed a high-performance DuckDB `ASOF JOIN` matching each client buffer record with the nearest preceding TCP acknowledgment within a 2.0-second tolerance window.
- **Deeper Technical Explanation**: Client buffer telemetry and TCP ACKs are emitted asynchronously on different clocks. A standard SQL equi-join on timestamp fails because timestamps do not match exactly. DuckDB's `ASOF JOIN` matches records where `client_buffer.session_id == video_acked.session_id` and `client_buffer.time >= video_acked.time`, finding the closest preceding network measurement.
- **Code Reference**: `src/qoe/data/preprocess.py` (lines 45–62).

#### Q6: Why did you downsample the raw Puffer telemetry from 100 ms to 5,000 ms?
- **Short Viva Answer**: Raw Puffer client buffer records are logged at 10 Hz (every 100 ms). Downsampling to 5-second heartbeats replicates commercial player SDK behavior (e.g., Exoplayer, Video.js) and reduces event volume by 50x while preserving buffer depletion trends.
- **Deeper Technical Explanation**: In commercial production environments, having millions of players emit telemetry every 100 ms would overwhelm mobile networks and ingestion brokers with negligible analytical benefit. Commercial SDKs emit a lightweight heartbeat every 5 to 10 seconds unless a state transition occurs (e.g. `buffer_start` or `bitrate_switch`).
- **Code Reference**: `src/qoe/data/preprocess.py`.

---

### Category C: Architecture & Systems

#### Q7: Why does the system maintain both an HBase database and a DuckDB Parquet warehouse?
- **Short Viva Answer**: This implements storage multi-homing: HBase serves as the hot operational store for low-latency point-lookups and dashboard scans, while DuckDB Parquet serves as the cold analytical store for historical SQL rollups, window functions, and ML training.
- **Deeper Technical Explanation**: No single database excels at both sub-millisecond point mutations and large-scale columnar scans. Inserting 1,000 streaming events per second into Parquet causes massive small-file fragmentation. Conversely, executing complex multi-hour analytical aggregations (e.g. `RANK() OVER (...)`) inside HBase requires expensive full-table MapReduce scans. By splitting storage into hot HBase (low-latency NoSQL) and cold Parquet/DuckDB (vectorised OLAP), both access patterns are satisfied optimally.
- **Code Reference**: `src/qoe/streaming/sinks_hbase.py`, `src/qoe/streaming/sinks_warehouse.py`, `src/qoe/batch/warehouse.py`.

#### Q8: What happens if the Spark streaming application crashes mid-execution?
- **Short Viva Answer**: Spark Structured Streaming recovers automatically from distributed checkpoint directories on disk, resuming consumption from the exact uncommitted Kafka offsets with at-least-once processing semantics.
- **Deeper Technical Explanation**: Spark persists write-ahead logs and state metadata in checkpoint directories (`/tmp/spark-checkpoints/`). When restarted, Spark inspects the offset commit log, identifies which micro-batch was in-flight, re-reads the records from Kafka, and re-executes the batch. Because our HBase and Parquet sinks are idempotent (keyed by row key and unique `event_id`), re-execution does not duplicate records.
- **Code Reference**: `src/qoe/streaming/spark_session.py`, `docker-compose.yml`.

---

### Category D: Apache Spark Deep Dive

#### Q9: How does Spark Structured Streaming differ from legacy Spark DStreams?
- **Short Viva Answer**: DStreams were based on discrete RDD micro-batches over processing time. Structured Streaming is built on top of Spark SQL's Catalyst Optimizer, treating streams as unbounded append-only tables with native event-time watermarking and stateful operators.
- **Deeper Technical Explanation**: DStreams lacked native handling for out-of-order events based on event timestamps, forcing developers to manage complex manual state tracking. Structured Streaming unifies batch and streaming APIs: the exact same DataFrame transformations written for offline Parquet files run unchanged over live Kafka streams.
- **Code Reference**: `src/qoe/streaming/q0_ingest_clean.py`, `src/qoe/streaming/q2_qoe_aggregator.py`.

#### Q10: What is the difference between a Narrow and a Wide transformation in Spark?
- **Short Viva Answer**: In a narrow transformation (e.g. `map`, `filter`), each input partition contributes to only one output partition with zero network shuffle. In a wide transformation (e.g. `groupBy`, `reduceByKey`, `join`), data must be shuffled across the cluster network based on a key hash.
- **Deeper Technical Explanation**: Wide transformations introduce stage boundaries in the Spark DAG. Network I/O and disk serialization during shuffles are the primary performance bottlenecks in distributed computing. In VortexStream, Query Q0 is almost entirely narrow (vectorized parsing and validation), allowing it to achieve maximum ingestion throughput.
- **Code Reference**: `src/qoe/batch/rdd_demo.py`.

#### Q11: How is stateful session tracking implemented in Spark Query Q1?
- **Short Viva Answer**: Q1 groups events by `session_id` and uses PySpark's stateful processing (`applyInPandasWithState`) with event-time timeout configuration to track player state machines across micro-batches.
- **Deeper Technical Explanation**: When events arrive out-of-order, Spark matches the incoming events with the persisted `SessionState` for that key. The state machine transitions states (`requested` -> `playing` -> `stalled`), updates cumulative counters (`watch_ms`, `stall_ms`), and emits lifecycle facts. If no events arrive before `session_idle_s` (60s), Spark triggers a state timeout, emitting a terminal `session_end` fact.
- **Code Reference**: `src/qoe/streaming/q1_session_tracker.py`.

#### Q12: Walk me through your RDD demonstration (`src/qoe/batch/rdd_demo.py`).
- **Short Viva Answer**: It demonstrates core low-level Spark primitives: parallelizing collections, using accumulators for error tracking, broadcast variables for lookup tables, caching RDDs with `persist()`, and executing transformations like `reduceByKey`, `aggregateByKey`, and `join`.
- **Deeper Technical Explanation**: In `rdd_demo.py`, we show how `aggregateByKey` computes average bitrate per device type in a single pass without collecting all values into memory, using a zero value of `(0, 0.0)`, a sequence operation that accumulates `(count + 1, sum + val)`, and a combiner operation that merges two partition accumulators `(c1 + c2, s1 + s2)`.
- **Code Reference**: `src/qoe/batch/rdd_demo.py` (lines 40–85).

---

### Category E: Apache HBase Deep Dive

#### Q13: What is HBase row-key salting and why did you use it?
- **Short Viva Answer**: Salting prepends a mathematical hash bucket (`crc32(session_id) % 16`) to the row key, distributing concurrent writes uniformly across 16 region buckets to eliminate RegionServer write hotspots.
- **Deeper Technical Explanation**: HBase stores rows lexicographically sorted. If session IDs or sequential IDs are inserted directly, writes cluster into the single RegionServer managing the current tail region, creating a CPU and disk I/O bottleneck while other nodes sit idle. Pre-splitting the table into 16 salted buckets (`00-` to `15-`) guarantees parallel write distribution.
- **Code Reference**: `src/qoe/common/hbase_keys.py` (`salted_key`), `docs/hbase_design.md`.

#### Q14: Explain the reverse-timestamp row key design in `qoe_metrics`.
- **Short Viva Answer**: We construct the key as `{grain}#{dimkey}#{9223372036854775807 - epoch_minute}`. Because HBase sorts keys in ascending order, subtracting the timestamp from `Long.MAX_VALUE` reverses the sort order, making the latest 5-minute window sort first on disk.
- **Deeper Technical Explanation**: When the operations dashboard loads, it needs the most recent metric window for a city or CDN. With standard timestamps, finding the latest row requires scanning to the end of the region. With reverse timestamps, a prefix scan with `LIMIT=1` immediately hits the very first row in the key range, returning the latest metrics in under 1 millisecond.
- **Code Reference**: `src/qoe/common/hbase_keys.py` (`rev_minute`, `qoe_key`).

#### Q15: Why are column families `m:` and `n:` separated in `qoe_metrics`?
- **Short Viva Answer**: HBase stores different column families in separate physical HFiles on disk. This allows Spark Q2 (playback) and Spark Q3 (network) to write to the same logical row concurrently without locking, and allows network queries to read only `n:` without loading playback blocks into memory.
- **Deeper Technical Explanation**: In HBase, the storage unit is the Store, which corresponds 1:1 with a column family. Storing playback metrics in `m:` and network metrics in `n:` ensures complete physical isolation on disk. When the dashboard queries only network bandwidth, the RegionServer BlockCache reads only the `n:` HFiles, reducing disk I/O by 50%.
- **Code Reference**: `docs/hbase_design.md`, `src/qoe/streaming/sinks_hbase.py`.

#### Q16: What happens inside HBase when a client executes a `Put` operation?
- **Short Viva Answer**: The mutation is first written sequentially to the Write-Ahead Log (WAL) on disk for durability, then added to the sorted in-memory MemStore. The client receives an acknowledgment immediately without waiting for disk HFile flush.
- **Deeper Technical Explanation**: Once the MemStore reaches its threshold (default 128 MB), a background thread flushes it to disk as an immutable HFile. Because HFiles are immutable, HBase never executes in-place disk overwrites. Updates and deletes are simply newer timestamped cells or tombstone markers that are consolidated during major compactions.

---

### Category F: Schema and Data Storage

#### Q17: What are the 9 data validation rules implemented in `validate.py`?
- **Short Viva Answer**: They are: `PARSE_ERROR`, `MISSING_SESSION_ID`, `BAD_TIMESTAMP`, `MISSING_EVENT_TYPE`, `UNKNOWN_EVENT_TYPE`, `NEGATIVE_BUFFER`, `NEGATIVE_STALL`, `IMPOSSIBLE_BITRATE`, and `MISSING_REQUIRED_FIELD`.
- **Deeper Technical Explanation**: These rules act as a strict firewall in Query Q0. Any event violating physical reality (e.g. buffer < 0 ms or bitrate > 100 Mbps) or missing mandatory presence keys (e.g. `first_frame` without `bitrate_kbps`) is immediately isolated into `quarantine_events` with an explicit reason string.
- **Code Reference**: `src/qoe/streaming/validate.py`.

#### Q18: Does your project support schema evolution?
- **Short Viva Answer**: Yes, at the JSON and wide-column layers via explicit `schema_version` headers and HBase's schema-less column qualifiers; Parquet schema evolution is handled through DuckDB column merging.
- **Deeper Technical Explanation**: All canonical schemas include a mandatory `schema_version: 1` field. HBase inherently supports schema evolution because column qualifiers are dynamic strings stored per cell; new metrics can be written to column family `m:` without altering table DDL. In the Parquet warehouse, DuckDB's `read_parquet` automatically unifies files with added optional columns.
- **Code Reference**: `schemas/playback_event.schema.json`, `docs/etl_mapping.md`.

---

### Category G: Machine Learning & Analytics

#### Q19: How do you prevent data leakage in your session abandonment classifier?
- **Short Viva Answer**: All feature values are extracted strictly from the first 15 seconds of playback (`kind = early_features`). No cumulative watch time, stall time, or events occurring after $T = 15$ seconds are exposed to the model.
- **Deeper Technical Explanation**: Data leakage occurs when a model is trained on features that contain information about the target variable that would not be available at inference time. Our target is `abandoned = 1` if total `watch_s < 120` seconds. If we included total session stall time or session end timestamps, the model would easily "cheat". By extracting features strictly from `early_features` emitted at 15 seconds, we guarantee zero target leakage.
- **Code Reference**: `src/qoe/ml/dataset.py`, `docs/ml_report.md`.

#### Q20: What are the top predictors of user session abandonment?
- **Short Viva Answer**: Rebuffering duration in the first 15 seconds (`stall_ms_early`, 34.2% feature importance) and startup delay (`startup_ms`, 24.8% importance).
- **Deeper Technical Explanation**: Viewers are least tolerant of interruptions during the very beginning of a stream. If a stream stalls within its first 15 seconds, abandonment probability increases more than 4-fold. Initial startup delay is the second strongest factor; viewers who experience > 4 seconds of initial black screen exhibit high immediate exit rates.
- **Code Reference**: `docs/ml_report.md` (Section 2.4).

#### Q21: How does your unsupervised anomaly detection engine work?
- **Short Viva Answer**: It combines a Robust Z-Score based on Median Absolute Deviation (MAD > 3.5) with KMeans clustering distance to detect anomalous window metric degradations.
- **Deeper Technical Explanation**: Standard Z-score uses the mean and standard deviation, which are themselves distorted by severe outliers. Robust Z-score uses the median and Median Absolute Deviation ($\text{MAD} = \text{median}(|x - \text{median}|)$):
  $$\text{Robust } Z = 0.6745 \times \frac{x - \text{median}}{\text{MAD}}$$
  This flags severe drops in QoE score or spikes in rebuffering ratio even when the baseline distribution is skewed, achieving **95.2% recall** against the fault ledger.
- **Code Reference**: `src/qoe/ml/anomaly.py`.

---

### Category H: Algorithms & Correctness

#### Q22: Explain the composite QoE score formula and its weighting rationale.
- **Short Viva Answer**: $\text{QoE} = 100 - \sum (W_i \times \text{penalty}_i)$, with weights: Rebuffering (40), Failures (20), Startup (15), Bitrate Quality (15), and Bitrate Switches (10).
- **Deeper Technical Explanation**: The weights reflect established human perceptual quality studies in video streaming (ITU-T Rec. P.1203). Rebuffering is assigned the heaviest penalty (40 points) because freezing video causes the most severe user irritation. Failures (VSF and fatal errors) carry 20 points. Startup delay and low video resolution carry 15 points each. Bitrate switching carries 10 points.
- **Code Reference**: `src/qoe/streaming/qoe_score.py`.

#### Q23: How does the alerting engine prevent alert flapping?
- **Short Viva Answer**: Through a stateful hysteresis state machine requiring 2 consecutive breaching windows to open an alert, 2 consecutive healthy windows to resolve it, a 10-minute cooldown, and a minimum sessions guard (`min_sessions >= 20`).
- **Deeper Technical Explanation**: In real networks, transient packet drops cause temporary 1-minute metric spikes that resolve themselves immediately. If an alert opened on a single 1-minute spike, operations engineers would suffer alert fatigue. Requiring 2 consecutive window confirmations filters out transient spikes, ensuring high alert precision.
- **Code Reference**: `src/qoe/streaming/alerting.py`.

---

### Category I: Performance and Scalability

#### Q24: What is the primary performance bottleneck in this architecture?
- **Short Viva Answer**: Python-to-JVM serialization across PySpark workers and synchronous Thrift 1 RPC socket latency to HBase.
- **Deeper Technical Explanation**: PySpark must serialize Python objects to Java JVM objects across worker pipes. In heavy micro-batches, this serialization consumes CPU. Additionally, HappyBase communicates with HBase through an intermediate Thrift 1 daemon using synchronous blocking sockets. We mitigated this by implementing 500-row batch puts and vectorised Pandas operations.
- **Code Reference**: `src/qoe/streaming/sinks_hbase.py`, `docs/performance.md`.

#### Q25: How would this architecture scale if traffic increased from 1,000 to 100,000 events/sec?
- **Short Viva Answer**: Scale Kafka partitions from 1 to 32; run Spark on a distributed Kubernetes or YARN cluster with multiple executor nodes; scale HBase RegionServers backed by distributed HDFS; and replace Thrift with native Java bulk-loading.
- **Deeper Technical Explanation**: Because all state in Kafka and HBase is partitioned by key (Kafka partition key `session_id`; HBase salted row key `crc32(session_id) % 16`), the entire system is horizontally scalable. Adding more Kafka broker nodes and Spark worker nodes allows each node to process an independent subset of partitions with linear throughput scaling.

---

### Category J: Limitations and Future Work

#### Q26: What is the most significant architectural shortcut currently in the codebase?
- **Short Viva Answer**: Running Apache HBase in single-node standalone mode inside Docker rather than a distributed multi-node HDFS cluster, and utilizing HappyBase Thrift rather than native Java HBase connectors.
- **Deeper Technical Explanation**: Standalone HBase stores HFiles directly on the local Linux container filesystem. While all table schemas, column families, salting algorithms, and HappyBase queries are 100% production-authentic, single-node standalone mode lacks distributed HDFS block replication.
- **Code Reference**: `docker/hbase.Dockerfile`, `docker-compose.yml`.

#### Q27: If you had another month to work on this project, what would you improve first?
- **Short Viva Answer**: Replace the Thrift RPC sink with native Java Spark HBase bulk-loading, and implement deep learning LSTM buffer forecasting for predictive ABR client guidance.
- **Deeper Technical Explanation**: Native Spark-HBase connectors write HFiles directly into HDFS using MapReduce bulk-load routines, completely bypassing RegionServer RPC overhead and increasing write throughput by 10x. On the analytics side, an LSTM neural network could predict buffer starvation 30 seconds before it occurs, allowing the client to downshift bitrate preventatively.

---

## 25. Code-Reading and Learning Roadmap

To master this codebase thoroughly, study the files in the following dependency-ordered 7-day sequence:

```mermaid
flowchart TD
    D1[Day 1: Config & Schemas] --> D2[Day 2: Data Preprocessing & Replay]
    D2 --> D3[Day 3: Ingestion & Validation Q0]
    D3 --> D4[Day 4: Stateful Session Tracking Q1]
    D4 --> D5[Day 5: Multi-Grain Aggregation Q2, Q3 & Alerting]
    D5 --> D6[Day 6: Machine Learning Pipelines & Anomaly Detection]
    D6 --> D7[Day 7: HBase Sinks, Reconciliation & Dashboard UI]
```

### Day 1: Foundations, Schemas & Configuration
- **Files to Read**: `config/config.yaml`, `schemas/playback_event.schema.json`, `src/qoe/common/config.py`, `src/qoe/common/hbase_keys.py`.
- **Core Concept**: Understand how row keys are mathematically salted and reverse-timestamped.
- **Verification Experiment**: Run `python -c "from qoe.common.hbase_keys import salted_key, rev_minute; print(salted_key('sess-100')); print(rev_minute(29000000))"` in terminal.

### Day 2: Data Preprocessing & Replay Engine
- **Files to Read**: `src/qoe/data/puffer_download.py`, `src/qoe/data/preprocess.py`, `src/qoe/producers/replay.py`.
- **Core Concept**: Understand how DuckDB executes ASOF joins between client buffer and TCP ack CSVs.
- **Verification Experiment**: Run `python -m qoe.producers.replay --speed 60 --duration 5 --dry-run`.

### Day 3: Ingestion, Validation & Dead-Letter Queue (Q0)
- **Files to Read**: `src/qoe/streaming/validate.py`, `src/qoe/streaming/standardize.py`, `src/qoe/streaming/q0_ingest_clean.py`.
- **Core Concept**: Understand the 9 validation rules and how dirty records are routed to quarantine.
- **Verification Experiment**: Run `pytest tests/phase_6/test_phase_6.py`.

### Day 4: Stateful Session Tracking (Q1)
- **Files to Read**: `src/qoe/streaming/q1_session_tracker.py`.
- **Core Concept**: Master the session state machine (`requested` -> `playing` -> `stalled` -> `ended`) and timeout triggers.
- **Verification Experiment**: Run `pytest tests/phase_7/test_phase_7.py`.

### Day 5: Multi-Grain Aggregation & Alerting (Q2, Q3)
- **Files to Read**: `src/qoe/streaming/qoe_score.py`, `src/qoe/streaming/q2_qoe_aggregator.py`, `src/qoe/streaming/q3_network_aggregator.py`, `src/qoe/streaming/alerting.py`.
- **Core Concept**: Understand multi-grain explosion (5 grains) and 2-window hysteresis alerting.
- **Verification Experiment**: Run `pytest tests/phase_8/test_phase_8.py` and `pytest tests/phase_9/test_phase_9.py`.

### Day 6: Machine Learning & Anomaly Detection
- **Files to Read**: `src/qoe/ml/dataset.py`, `src/qoe/ml/churn_model.py`, `src/qoe/ml/anomaly.py`.
- **Core Concept**: Understand how the 15-second zero-leakage feature extraction works.
- **Verification Experiment**: Run `pytest tests/phase_11/test_phase_11.py`.

### Day 7: Storage Sinks, Reconciliation & Streamlit UI
- **Files to Read**: `src/qoe/streaming/sinks_hbase.py`, `src/qoe/dashboard/data_access.py`, `src/qoe/dashboard/app.py`, `src/qoe/verify/reconcile.py`.
- **Core Concept**: Understand how HBase prefix scans power the live Streamlit dashboard and how V1–V8 gates prove pipeline conservation.
- **Verification Experiment**: Run `make reconcile` and launch `make dashboard`.

---

## 26. Final Project Understanding Checklist

Use this 25-point checklist to assess your readiness before a viva or presentation:

- [ ] 1. I can name the exact source of our real telemetry: **Stanford University Puffer Project** (`https://puffer.stanford.edu/`).
- [ ] 2. I can explain the 3 raw Puffer CSVs: `client_buffer`, `video_sent`, and `video_acked`.
- [ ] 3. I can explain why Indian cities, ISPs, and CDNs are simulated (Puffer lacks commercial viewer metadata).
- [ ] 4. I can explain how DuckDB's `ASOF JOIN` correlates asynchronous buffer health with TCP acknowledgments.
- [ ] 5. I can explain why we downsampled 100 ms buffer telemetry to 5-second heartbeats.
- [ ] 6. I can explain the 3 injected faults: F1 (Chennai Airtel throttle), F2 (Akamai edge 5xx), F3 (Android DRM crash).
- [ ] 7. I can draw the end-to-end architecture diagram showing Kafka bronze, Spark Q0–Q4, HBase, DuckDB, and Streamlit.
- [ ] 8. I can explain why Spark Structured Streaming was chosen over pure Python or Flink.
- [ ] 9. I can explain why HBase was chosen over relational databases (PostgreSQL) or document stores (MongoDB).
- [ ] 10. I can explain the difference between a Narrow and a Wide Spark transformation.
- [ ] 11. I can walk through the RDD demo operations: `parallelize`, `accumulator`, `broadcast`, `aggregateByKey`, and `join`.
- [ ] 12. I can explain HBase row-key salting: `crc32(session_id) % 16` to prevent RegionServer write hotspots.
- [ ] 13. I can explain the reverse-timestamp formula: `9223372036854775807 - epoch_minute` for newest-first scans.
- [ ] 14. I can explain why column families `m:` and `n:` are stored in physically separate HFiles in HBase.
- [ ] 15. I can list at least 5 of the 9 data validation rules in `validate.py`.
- [ ] 16. I can explain what happens to corrupted records: routed to the `quarantine` dead-letter queue.
- [ ] 17. I can explain the 5 states in the Q1 session state machine: `requested`, `playing`, `stalled`, `ended`, `failed_start`.
- [ ] 18. I can explain the VSF timeout (10 seconds) and session idle timeout (60 seconds).
- [ ] 19. I can state the composite QoE score formula and explain the weights: Rebuffer 40, Failure 20, Startup 15, Bitrate 15, Switch 10.
- [ ] 20. I can explain the alerting hysteresis logic: 2 consecutive breaches to open, 2 healthy to resolve, 10-min cooldown.
- [ ] 21. I can explain how we achieve **zero target leakage** in our ML abandonment classifier (first 15 seconds features only).
- [ ] 22. I can explain the unsupervised anomaly detection formula: Robust Z-Score using Median Absolute Deviation (MAD).
- [ ] 23. I can explain the pipeline record conservation equation in Gate V2: $\text{Emitted} = \text{Silver} + \text{Quarantine} + \text{Dups} + \text{Late Dropped}$.
- [ ] 24. I can honestly state what is implemented vs standalone mode vs configured.
- [ ] 25. I can navigate the repository and point to the exact line of code implementing any of these features.

---

## 27. Glossary of Technical Terms

| Term | Definition in the Context of VortexStream |
|---|---|
| **ABR (Adaptive Bitrate)** | Dynamic streaming algorithm (e.g. HLS or MPEG-DASH) where player clients adjust video chunk quality based on observed network bandwidth. |
| **ASOF Join** | Non-equi relational join supported by DuckDB that matches an asynchronous event with the closest preceding timestamped event in another table. |
| **AUC-ROC** | Area Under the Receiver Operating Characteristic curve; evaluation metric for binary classification measuring discrimination across all thresholds. |
| **Bronze Tier** | The raw, unvalidated ingestion layer in a Medallion architecture (Kafka topics `playback-events`, `network-metrics`). |
| **Column Family** | Physical storage container in Apache HBase. Qualifiers belonging to the same column family are stored together in the same HFile on disk. |
| **DStreams** | Discretized Streams; Spark's legacy streaming API based on discrete RDD micro-batches over processing time. |
| **HFile** | The physical immutable on-disk file format used by Apache HBase to store sorted key-value cells. |
| **Hysteresis** | Lagging of an effect behind its cause; used in alerting to require 2 consecutive breaching windows to open and 2 healthy windows to resolve. |
| **KRaft Mode** | Kafka Raft Metadata mode; modern Apache Kafka consensus protocol that eliminates the need for an external Apache ZooKeeper cluster. |
| **MAD (Median Absolute Deviation)** | Robust statistical dispersion measure: $\text{median}(|x - \text{median}|)$; highly resistant to outlier distortion. |
| **Medallion Architecture** | Data design pattern organizing lakehouse data into Bronze (raw), Silver (cleaned/validated), and Gold (aggregated) quality tiers. |
| **MemStore** | In-memory write buffer per column family in Apache HBase; flushed to disk as an HFile when reaching memory thresholds. |
| **OTT (Over-The-Top)** | Delivery of video content directly to viewers over the public Internet, bypassing traditional broadcast television or cable providers. |
| **QoE (Quality of Experience)** | Subjective satisfaction of the human viewer regarding video streaming playback (measured via buffering, startup time, and resolution). |
| **QoS (Quality of Service)** | Objective physical network parameters (throughput, round-trip time, packet loss, network jitter). |
| **RowKey** | Primary unique identifier for a row in Apache HBase; rows are stored in ascending byte-lexicographical order by RowKey. |
| **Salting** | Prepending a hash prefix to a database row key to distribute concurrent writes uniformly across all physical region partitions. |
| **Silver Tier** | Cleaned, validated, standardized, and enriched event stream (Kafka `playback-clean`, Parquet `silver_playback_events`). |
| **Thrift** | Cross-language Remote Procedure Call (RPC) framework used by Apache HBase to expose a TCP interface for Python clients (HappyBase). |
| **VSF (Video Start Failure)** | Failure of a video player to render the first video frame within a designated timeout window (10 seconds) after the user clicked play. |
| **WAL (Write-Ahead Log)** | Append-only sequential log on disk where database mutations are committed before modifying in-memory buffers to guarantee durability. |
| **Watermark** | Streaming threshold specifying how far behind the current maximum observed event time late-arriving events are accepted. |

---

## 28. References and Verified Source Links

1. **Stanford Puffer Project**:
   - Official Project Portal: [https://puffer.stanford.edu/](https://puffer.stanford.edu/)
   - Research Paper: Francis Y. Yan, Hudson Ayers, Chenzhi Zhu, Sadjad Fouladi, James Hong, Keyi Zhang, Philip Levis, Keith Winstein. *"Learning in situ: a randomized experiment in video streaming"*, USENIX Symposium on Networked Systems Design and Implementation (NSDI), 2020. [https://www.usenix.org/conference/nsdi20/presentation/yan](https://www.usenix.org/conference/nsdi20/presentation/yan)
   - Public Dataset Archive: [https://puffer.stanford.edu/results/](https://puffer.stanford.edu/results/)
2. **Apache Spark Documentation**:
   - Structured Streaming Programming Guide: [https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html](https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html)
   - RDD Programming Guide: [https://spark.apache.org/docs/3.5.3/rdd-programming-guide.html](https://spark.apache.org/docs/3.5.3/rdd-programming-guide.html)
3. **Apache HBase Documentation**:
   - HBase Reference Guide: [https://hbase.apache.org/book.html](https://hbase.apache.org/book.html)
   - HBase Architecture Overview: [https://hbase.apache.org/book.html#arch.overview](https://hbase.apache.org/book.html#arch.overview)
4. **Apache Kafka Documentation**:
   - KRaft Overview: [https://kafka.apache.org/documentation/#kraft](https://kafka.apache.org/documentation/#kraft)
5. **DuckDB Documentation**:
   - ASOF Joins in DuckDB: [https://duckDB.org/docs/guides/sql_features/asof_join.html](https://duckDB.org/docs/guides/sql_features/asof_join.html)
   - Parquet Ingestion & Lakehouse Querying: [https://duckDB.org/docs/data/parquet/overview.html](https://duckDB.org/docs/data/parquet/overview.html)
6. **Video Streaming Quality Standards**:
   - ITU-T Recommendation P.1203: *"Parametric bitstream-based quality assessment of progressive download and adaptive audiovisual streaming services over reliable transport"*, International Telecommunication Union, 2017. [https://www.itu.int/rec/T-REC-P.1203/en](https://www.itu.int/rec/T-REC-P.1203/en)
"""
