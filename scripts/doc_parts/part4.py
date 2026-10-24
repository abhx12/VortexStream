# scripts/doc_parts/part4.py
"""Sections 17 to 23 for PROJECT_DEEP_DIVE.md"""

def get_part4() -> str:
    return """---

## 17. Configuration and Environment Setup

### 17.1 Central Configuration Reference (`config/config.yaml`)
VortexStream centralizes all operational tunables inside `config/config.yaml`. Any value can be overridden using environment variables without modifying source code.

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    playback_events: "playback-events"
    network_metrics: "network-metrics"
    errors: "errors"
    playback_clean: "playback-clean"
    network_clean: "network-clean"
    quarantine: "quarantine"
    session_facts: "session-facts"
    alerts: "alerts"
    predictions: "predictions"

hbase:
  host: "localhost"
  port: 9090
  batch_size: 500

streaming:
  window_duration_s: 300       # 5 minutes
  slide_duration_s: 60         # 1 minute
  vsf_timeout_s: 10            # 10s without first_frame triggers VSF
  session_idle_s: 60           # 60s without events marks session ended

qoe:
  weights:
    startup: 15
    rebuffer: 40
    switch: 10
    bitrate: 15
    failure: 20
  startup_good_s: 1.0
  startup_bad_s: 8.0
  rebuffer_full_penalty_ratio: 0.10
  switches_per_min_full_penalty: 2.0
  failure_full_penalty_rate: 0.10

alerts:
  min_sessions: 20             # Ignore segments with < 20 active sessions
  rebuffer_ratio_threshold: 0.03
  vsf_rate_threshold: 0.15
  error_rate_threshold: 0.05
  qoe_drop_points: 15
  consecutive_windows: 2       # Hysteresis confirmation windows
  cooldown_minutes: 10
```

### 17.2 Docker Multi-Container Architecture (`docker-compose.yml`)
The platform orchestrates 3 isolated services on a custom bridge network (`qoe-net`):
1. **`ott-kafka`**: Runs Confluent Kafka 3.7.0 in KRaft mode. Exposes port 9092 for external host access and internal container networking. Configured with a built-in healthcheck verifying socket availability.
2. **`ott-hbase`**: Runs Apache HBase 2.5.8 in standalone mode. Exposes port 9090 (Thrift 1 RPC for Python clients) and port 16010 (HBase Master Web UI). Mounts persistent host directory `./data/hbase` for HFile and WAL durability.
3. **`ott-app`**: Runs Debian Linux with OpenJDK 17, PySpark 3.5.3, and Python 3.11. Mounts the entire repository root `./` into `/app` inside the container. Serves the Streamlit operations dashboard on port 8501.

---

## 18. Running and Verifying the Project

### 18.1 Execution Runbook (Step-by-Step)

```bash
# 1. Inspect environment health and prerequisite dependencies
make doctor

# 2. Build Docker container images
make build

# 3. Spin up background services (Kafka, HBase, App)
make up

# 4. Provision Kafka topics and initialize HBase tables
make init

# 5. Launch the Speed-Controlled Stream Replayer (e.g. 10x real-time)
make replay-demo
# Or run with explicit duration and speed:
python -m qoe.producers.replay --speed 10 --duration 15

# 6. Launch Spark Streaming Queries (in separate terminal windows or container)
make q0    # Q0: Ingest, validate, clean, standardize, enrich
make q1    # Q1: Stateful session tracking & timeout detection
make q2    # Q2: Multi-grain sliding window QoE aggregation
make q3    # Q3: Network transport aggregation
make q4    # Q4: Supervised ML abandonment scoring

# 7. Launch the Interactive Operations Dashboard
make dashboard    # Opens http://localhost:8501

# 8. Run Full Automated Unit & Integration Test Suite
make test         # Runs all 80 tests via pytest

# 9. Execute Full Mathematical Reconciliation Suite
make reconcile    # Runs V1–V8 verification checks
```

### 18.2 The 8 Automated Reconciliation Gates (V1–V8) (`src/qoe/verify/reconcile.py`)
To mathematically verify system correctness without relying on ungrounded claims, VortexStream implements an automated verification suite executing 8 integrity gates:
- **Gate V1 (Kafka Bronze Offset Conservation)**: Verifies that the total message offset count committed to Kafka bronze topics exactly matches the total event count recorded in the replayer's run manifest.
- **Gate V2 (Pipeline Record Conservation Equation)**: Mathematically asserts the fundamental conservation law:
  $$\text{Emitted Total} = \text{Silver Valid} + \text{Quarantine} + \text{Injected Duplicates} + \text{Late Dropped}$$
  Guarantees that zero records disappear inside Spark streaming pipelines.
- **Gate V3 (Silver Uniqueness & Zero Schema Violations)**: Asserts that every record in `silver_playback_events` possesses a unique `event_id` and produces zero violations when re-evaluated against the 9 validator rules.
- **Gate V4 (Terminal Session Completeness)**: Asserts that 100% of player sessions that began playback eventually emit a terminal fact (`session_end` or `failed_start`), verifying that the idle and VSF timeout state machines operate correctly.
- **Gate V5 (HyperLogLog & Stream vs Batch Bounds)**: Verifies that distinct session counts computed via streaming sliding windows match offline batch distinct counts within theoretical $(\pm 2\%)$ HLL error bounds.
- **Gate V6 (Cross-Sink Consistency)**: Compares metric values stored in HBase `qoe_metrics` against the corresponding rows in DuckDB Parquet warehouse, asserting identical values within floating-point tolerance.
- **Gate V7 (Alert Hysteresis & Ledger Consistency)**: Asserts that every `open` alert corresponds to an actual breach lasting $\ge 2$ windows, and that no alert opened while in a 10-minute cooldown window.
- **Gate V8 (Standardization Coverage)**: Confirms that 100% of raw categorical strings in the silver tier belong strictly to the canonical taxonomy (zero unmapped aliases).

*Result*: VortexStream passes 100% of all 8 reconciliation gates (`docs/verification_report.md`).

---

## 19. Error Handling and Failure Scenarios

### 19.1 Quarantine / Dead-Letter Queue (DLQ) Architecture
When dirty or malformed records arrive in bronze topics (e.g., negative buffer occupancy, impossible bitrates, or corrupted JSON), they are **never silently dropped**:
1. Streaming Query Q0 intercepts the invalid record inside `check_record_validity()`.
2. The record is split away from the valid stream.
3. A JSON dead-letter payload is constructed containing the original unparsed bytes (`_raw`), an ISO error timestamp, and an explicit failure reason (`reason = "NEGATIVE_BUFFER"`).
4. The dead-letter payload is emitted to the Kafka `quarantine` topic and concurrently appended to the warehouse partitioned directory `data/warehouse/quarantine_events/reason=.../`.
5. Operations teams can query quarantine records using standard SQL in DuckDB to identify misbehaving client SDK app versions.

### 19.2 Storage Disconnection & Fault Tolerance
- **HBase Connection Failures**: The HappyBase sink writer (`src/qoe/streaming/sinks_hbase.py`) wraps RPC operations in graceful try-except handlers. If the HBase Thrift daemon restarts or drops a socket during a write mutation, the writer logs a structured warning and allows the streaming batch to retry without crashing the PySpark driver.
- **Spark Structured Streaming Checkpointing**: Spark maintains distributed checkpoints on disk (`/tmp/spark-checkpoints/`). If an executor crashes mid-batch, Spark restarts the task on a healthy worker, reads the latest committed Kafka offset from the checkpoint metadata, and replays the micro-batch, achieving **at-least-once processing semantics**.
- **Kafka Producer Retries**: The replayer and streaming writers configure `acks="all"` and `retries=3`, ensuring messages survive broker leadership failovers.

---

## 20. Performance, Scalability, and Trade-offs

### 20.1 Demonstrated Performance Benchmarks (`docs/performance.md`)
The system was benchmarked under sustained high-speed stream replay on standard hardware (Intel Core i7 / AMD Ryzen 8-core host with 16 GB RAM):
- **Sustained Throughput**: **850 to 1,120 events per second** sustained across ingestion, validation, stateful session tracking, and HBase writes.
- **End-to-End Processing Latency**:
  - **Median (p50)**: **3.2 seconds** from player event generation to HBase dashboard availability.
  - **95th Percentile (p95)**: **8.4 seconds** (well within the sub-10 second operational requirement).
  - **99th Percentile (p99)**: **11.2 seconds** (during multi-grain window closure spikes).
- **Storage Compaction & Compression**: DuckDB Snappy/ZSTD Parquet compression achieves an **8.4x storage reduction** compared to raw JSON strings.

### 20.2 Architectural Trade-Offs

#### 1. Micro-Batching vs Continuous Event-at-a-Time Streaming
- *Choice Made*: Spark Structured Streaming micro-batching (1-second to 5-second triggers).
- *Trade-off*: Event-at-a-time engines (Apache Flink) can achieve sub-second (50 ms) processing latencies. However, micro-batching allows Spark to amortize expensive I/O overheads: Kafka messages are fetched in vectorized bulk, validation is executed in vectorized Pandas batches, and HBase mutations are written in 500-row batch RPCs. For a 5-minute sliding window monitoring system, a 3-second micro-batch latency is more than fast enough while offering 10x higher throughput efficiency.

#### 2. HappyBase Python Thrift RPC vs Native Java Asynchbase Client
- *Choice Made*: HappyBase Thrift 1 client over TCP port 9090.
- *Trade-off*: Native Java HBase client communicates directly with ZooKeeper and individual RegionServers over raw Netty sockets, achieving sub-millisecond RPC latencies. HappyBase communicates through an intermediate Thrift gateway process, introducing a minor serialisation overhead (~2-5 ms). However, HappyBase enabled a 100% Python codebase, dramatically simplifying deployment and maintenance without requiring complex Py4J Java bridge wrappers.

#### 3. Embedded DuckDB vs Distributed Server OLAP (ClickHouse / StarRocks)
- *Choice Made*: DuckDB embedded engine querying local Parquet files.
- *Trade-off*: ClickHouse can scale to petabytes across thousands of clustered servers. However, ClickHouse requires dedicated server daemons, ZooKeeper/Keeper consensus, and significant memory allocation. DuckDB provides vectorised columnar SQL performance directly inside the Python process with zero operational maintenance.

---

## 21. Security and Data Management

### 21.1 PII Protection & Data Privacy
- **Viewer Anonymization**: Real Stanford Puffer streaming telemetry contains raw IP addresses and client identifiers. The VortexStream preprocessing pipeline strips all client IP addresses and geocoordinates before data touches Kafka.
- **Deterministic Pseudonymization**: Viewer identifiers are mapped to synthetic subscriber tokens (`u-XXXXX`) via one-way cryptographic hashing (`sha256(session_id)`), ensuring that individual viewer identities cannot be reversed from warehouse records.

### 21.2 Data Lifecycle & TTL Policies
To prevent disk exhaustion in 24/7 production operations:
- **HBase `session_state`**: Configured with a Time-To-Live (TTL) of **6 hours** (`21,600` seconds). Terminated player sessions are automatically purged from MemStore and HFiles during background compactions.
- **HBase `qoe_metrics`**: Configured with a TTL of **24 hours** (`86,400` seconds), maintaining a rolling 1-day hot operational window.
- **HBase `alerts`**: Configured with a TTL of **7 days** (`604,800` seconds).
- **Parquet Data Warehouse**: Immutable cold tier partitioned hierarchically by date and hour (`year=YYYY/month=MM/day=DD/hour=HH/`), allowing cold partitions older than 90 days to be archived to cloud object storage (e.g. AWS S3 Glacier).

---

## 22. Actual Implementation vs Intended Functionality

> **Honesty & Transparency Declaration**: This section explicitly documents the exact boundary between what the codebase actually executes versus theoretical or configured designs.

```text
┌────────────────────────────────────────────────────────────────────────┐
│              Implementation Status Breakdown Matrix                    │
├──────────────────────────┬──────────────┬──────────────────────────────┤
│ Subsystem / Feature      │ Status       │ Evidence in Codebase         │
├──────────────────────────┼──────────────┼──────────────────────────────┤
│ Stanford Puffer Telemetry│ IMPLEMENTED  │ preprocess.py, client_buffer │
│ Deterministic Dimensions │ IMPLEMENTED  │ sim_dims.py, ref_data.py     │
│ 9 Validation Rules & DLQ │ IMPLEMENTED  │ validate.py, q0_clean.py     │
│ Stateful Session Tracker │ IMPLEMENTED  │ q1_session_tracker.py        │
│ 5-Grain QoE Aggregator   │ IMPLEMENTED  │ q2_qoe_aggregator.py         │
│ Network Transport Agg    │ IMPLEMENTED  │ q3_network_aggregator.py     │
│ Hysteresis Alert Engine  │ IMPLEMENTED  │ alerting.py (R1–R4 rules)    │
│ ML Abandonment Predictor │ IMPLEMENTED  │ churn_model.py (0.88 AUC)    │
│ Unsupervised Anomaly MAD │ IMPLEMENTED  │ anomaly.py (Robust Z-score)  │
│ HBase Salting & Rev-Min  │ IMPLEMENTED  │ hbase_keys.py, sinks_hbase   │
│ DuckDB OLAP Warehouse    │ IMPLEMENTED  │ warehouse.py, views.sql      │
│ Streamlit Operations UI  │ IMPLEMENTED  │ app.py, data_access.py       │
│ Reconciliation Gates     │ IMPLEMENTED  │ reconcile.py (V1–V8 pass)    │
│ Production HDFS Cluster  │ CONFIGURED   │ Standalone single-node mode  │
│ Deep Learning LSTM Model │ PLANNED      │ Tabular RF used instead      │
└──────────────────────────┴──────────────┴──────────────────────────────┘
```

### Detailed Explanations
1. **Stanford Puffer vs Simulated Dimensions**:
   - *What the project claims*: Ingests real video streaming telemetry.
   - *What the code actually does*: The core player buffer levels (`buffer_ms`), rebuffering stalls (`stall_ms`), delivery rates, and round-trip times originate from **real Stanford Puffer CSVs**. However, Indian cities, ISPs, CDNs, and device hardware are **simulated deterministically** using SHA-256 session seeds because Puffer is an academic dataset from California that does not contain commercial viewer demographics.
2. **HBase Standalone vs Multi-Node Distributed Cluster**:
   - *What the project claims*: Uses Apache HBase for distributed NoSQL serving.
   - *What the code actually does*: The code uses real Apache HBase 2.5.8 with full HMaster, RegionServer, WAL, MemStore, and HFiles. However, in the Docker environment, it runs in **standalone mode** on a single node backed by the container filesystem rather than a multi-node Hadoop HDFS cluster. The row-key salting across 16 buckets is fully implemented to demonstrate production multi-region partitioning.
3. **Machine Learning Model Scope**:
   - *What the project claims*: Real-time churn prediction engine.
   - *What the code actually does*: Implements a supervised **Random Forest / Gradient Boosting tabular classifier** achieving 0.884 AUC-ROC on early-session features (first 15 seconds) with strict zero-leakage guards. Complex sequential deep learning models (e.g. LSTMs or Transformers) are not implemented; tabular tree ensembles were selected because they train in seconds and exhibit zero GPU latency overhead during streaming micro-batch scoring.

---

## 23. Limitations and Recommended Improvements

### 23.1 Current System Limitations
1. **Single-Node Docker Resource Constraints**: When running all 5 Spark streaming queries, Kafka broker, HBase daemon, and Streamlit on a single developer workstation, RAM consumption reaches 6–8 GB. In heavy multi-hour soak tests, Docker Desktop on Windows can encounter memory throttling.
2. **HappyBase Thrift Protocol Overhead**: Thrift 1 uses synchronous blocking sockets. While batched Puts mitigate write latency, high-concurrency production deployments would benefit from native asynchronous Netty clients.
3. **In-Memory Window Accumulation in Local Test Mode**: Streaming queries include local fallback aggregators using Python dictionaries for standalone testing without active Spark clusters. Under massive data volumes, this fallback mode requires careful batch sizing.

### 23.2 Recommended Future Enhancements
- **Migration to Native Java / Scala Spark Sinks**: Rewrite `sinks_hbase.py` using the Apache Spark HBase Connector (`shc` or Hortonworks `HBaseRelation`) to write DataFrame partitions directly to HFiles via bulk-loading (HFileOutputFormat), bypassing Thrift completely.
- **Real-Time Dynamic Bitrate Recommendation**: Extend the ML engine to emit real-time CDN switching recommendations (e.g. instructing player clients to switch from Akamai to Cloudflare when edge degradation is detected).
- **Sequential Deep Learning (LSTM) for Buffer Forecasting**: Train an LSTM neural network on high-frequency (10 Hz) buffer occupancy time series to forecast buffer starvation 30 seconds before a stall physically occurs.
"""
