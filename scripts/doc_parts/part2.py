# scripts/doc_parts/part2.py
"""Sections 7 to 11 for PROJECT_DEEP_DIVE.md"""

def get_part2() -> str:
    return """---

## 7. High-Level Architecture

### 7.1 Architecture at Three Conceptual Levels

#### Level A: Layman's Explanation
Imagine VortexStream as a massive **International Airport Air Traffic Control System**:
- The **Airplanes** are millions of video players (smartphones, smart TVs, laptops) constantly flying through airspace and radioing their flight stats (altitude, fuel, airspeed).
- **Apache Kafka** is the **Radio Communications Relay Tower**. It receives thousands of incoming radio reports every second, queues them reliably without dropping any, and broadcasts them on designated frequencies so controllers can listen without overwhelming the pilots.
- **Apache Spark** is the **Chief Air Traffic Controller and Radar Processing Computer**. It ingests the noisy radio bursts, verifies the flight codes, tracks each flight from takeoff to landing, calculates if flights are experiencing severe turbulence or delays, and spots dangerous regional weather storms.
- **Apache HBase** is the **High-Speed Emergency Flight Status Board**. When a controller needs to check a specific active flight's status instantly, HBase provides that answer in a fraction of a millisecond.
- **DuckDB & Parquet Data Warehouse** is the **Flight Black Box and Historical Aviation Archive**. It stores every past flight log permanently on disk in compressed binders so aviation analysts can study historical traffic patterns or conduct investigations.
- **Machine Learning (Scikit-Learn)** is the **Predictive Co-Pilot**. By looking at how an airplane behaves in its first 30 seconds after takeoff (engine stutter, takeoff delay), it predicts whether the flight will make an emergency turnaround before reaching its destination.
- **Streamlit** is the **Glass Cockpit and Radar Screen**. Operations engineers glance at heatmaps showing which cities or airlines are having trouble right now.

#### Level B: Student Presentation Explanation
From a software engineering perspective, VortexStream implements a **Distributed Real-Time Stream Processing Architecture** adhering to the **Medallion Lakehouse Pattern**:
1. **Bronze Ingestion Tier (Unvalidated Kafka Topics)**: Raw, unvalidated JSON telemetry is published by client player SDKs into Kafka bronze topics (`playback-events`, `network-metrics`, `errors`).
2. **Silver Processing Tier (Validated & Stateful Spark Queries)**:
   - **Query Q0 (`q0_ingest_clean`)**: Ingests bronze events, enforces 9 deterministic validation rules, routes malformed records to a Dead-Letter Queue (`quarantine`), normalizes taxonomies via alias mapping, enriches events with static user and content catalogs, and emits clean streams (`playback-clean`, `network-clean`).
   - **Query Q1 (`q1_session_tracker`)**: Maintains an in-memory stateful session state machine across distributed micro-batches using PySpark's stateful processing (`applyInPandasWithState`). Tracks state transitions (`requested` -> `playing` -> `stalled` -> `ended` / `failed_start`), computes initial startup latency, aggregates cumulative buffering duration, detects Video Start Failures (10-second timeout) and session inactivity (60-second idle timeout), and emits early feature facts at 15 seconds.
3. **Gold Analytical Tier (Sliding Windows & Storage Multi-Homing)**:
   - **Query Q2 (`q2_qoe_aggregator`)**: Explodes clean events across 5 analytical grains (`global`, `city_isp`, `cdn`, `device`, `channel`), evaluates 5-minute sliding windows sliding every 60 seconds, and computes the composite QoE score [0, 100].
   - **Query Q3 (`q3_network_aggregator`)**: Computes parallel 5-minute sliding aggregates for network transport health (throughput, RTT, packet loss).
   - **Alerting Engine (`alerting.py`)**: Runs post-window evaluation against rules R1–R4, managing stateful hysteresis (2 consecutive breaches to open, 2 healthy to resolve) and 10-minute cooldowns.
   - **Query Q4 (`q4_ml_scoring`)**: Consumes 15-second early session facts, applies a pre-trained supervised machine learning pipeline, and writes churn probabilities to Kafka `predictions` and Parquet.
4. **Storage & Serving Dual-Path**:
   - **Hot Operational Store**: Apache HBase tables (`session_state`, `qoe_metrics`, `alerts`) provide sub-millisecond point lookups and prefix scans for the active operations dashboard.
   - **Cold Analytical Store**: Partitioned columnar Parquet lakehouse queried via DuckDB for historical rollups, complex SQL window functions, and model retraining.

#### Level C: Implementation-Level Explanation
At the systems programming and runtime layer:
- The streaming pipeline runs inside a Debian Linux Docker container (`ott-app`) equipped with **OpenJDK 17** and **PySpark 3.5.3**, communicating with **Apache Kafka 3.7.0** over port 9092 and **Apache HBase 2.5.8** via Thrift 1 RPC over port 9090.
- Spark Structured Streaming runs in micro-batch mode. In live streaming, `readStream.format("kafka")` pulls binary JSON payloads, unpacks them with `from_json()`, and executes transformations before sinking to Kafka or calling `foreachBatch()` to trigger Python batch writers.
- **HBase Key Architecture**:
  - `session_state` uses a salted key: `crc32(session_id) % 16` -> `{salt:02d}-{session_id}`, distributing write mutations across 16 pre-split region buckets.
  - `qoe_metrics` uses a reverse-timestamp key: `{grain}#{dimkey}#{9223372036854775807 - epoch_minute}`, guaranteeing that the most recent 5-minute aggregation sorts first lexicographically on disk.
- **Column Family Isolation**: `qoe_metrics` stores playback metrics in column family `m:` and network metrics in column family `n:`. Because HBase stores distinct column families in separate physical HFiles, Query Q2 and Query Q3 concurrently write to the same logical row without read-modify-write locking or schema contention.

### 7.2 End-to-End System Architecture Diagram

```mermaid
flowchart TD
    subgraph S_SRC[Data Sources & Simulation]
        Puffer[Stanford Puffer CSVs]
        SimDims[Deterministic Sim Dims]
        RefCat[Synthetic User & Content Catalogs]
        ChaosEngine[Chaos & Fault Injector]
    end

    subgraph S_INGEST[Phase 3 & 5: Ingestion Engine]
        DuckPrep[DuckDB ASOF Preprocessing]
        Replayer[Speed-Controlled Replayer Engine]
    end

    subgraph S_KAFKA_BRONZE[Kafka Bronze Ingest Topics]
        T_PB[playback-events]
        T_NET[network-metrics]
        T_ERR[errors]
    end

    subgraph S_SPARK[Spark Structured Streaming Pipeline]
        Q0[Spark Q0: Ingest, Validate & Clean]
        Q1[Spark Q1: Stateful Session Tracker]
        Q2[Spark Q2: Windowed QoE Aggregator]
        Q3[Spark Q3: Network Aggregator]
        Q4[Spark Q4: ML Abandonment Scorer]
        AlertsEng[Hysteresis Alert Evaluator]
    end

    subgraph S_KAFKA_SILVER[Kafka Silver & Gold Topics]
        T_PBClean[playback-clean]
        T_NETClean[network-clean]
        T_DLQ[quarantine / DLQ]
        T_Facts[session-facts]
        T_Alerts[alerts]
        T_Preds[predictions]
    end

    subgraph S_STORAGE[Dual Storage Layer]
        subgraph Hot Store: Apache HBase
            HB_State[(session_state)]
            HB_QoE[(qoe_metrics)]
            HB_Alerts[(alerts)]
        end
        subgraph Cold Store: Parquet Lakehouse & DuckDB
            WH_Silver[(silver_playback_events)]
            WH_GoldQoE[(gold_qoe_5m_raw)]
            WH_GoldNet[(gold_network_5m_raw)]
            WH_Hourly[(gold_qoe_hourly)]
            WH_ML[(ml_predictions)]
            DuckDB_Engine[(qoe.duckdb)]
        end
    end

    subgraph S_UI[Serving & Presentation]
        StreamlitUI[Streamlit Operations Dashboard :8501]
    end

    Puffer --> DuckPrep
    SimDims --> DuckPrep
    RefCat --> DuckPrep
    DuckPrep --> Replayer
    ChaosEngine -.-> Replayer

    Replayer --> T_PB
    Replayer --> T_NET
    Replayer --> T_ERR

    T_PB --> Q0
    T_NET --> Q0
    T_ERR --> Q0

    Q0 --> T_PBClean
    Q0 --> T_NETClean
    Q0 --> T_DLQ
    Q0 --> WH_Silver

    T_PBClean --> Q1
    Q1 --> T_Facts
    Q1 --> HB_State
    Q1 --> DuckDB_Engine

    T_PBClean --> Q2
    T_Facts --> Q2
    Q2 --> HB_QoE
    Q2 --> WH_GoldQoE
    Q2 --> AlertsEng

    AlertsEng --> T_Alerts
    AlertsEng --> HB_Alerts

    T_NETClean --> Q3
    Q3 --> HB_QoE
    Q3 --> WH_GoldNet

    T_Facts --> Q4
    Q4 --> T_Preds
    Q4 --> WH_ML

    HB_QoE --> StreamlitUI
    HB_Alerts --> StreamlitUI
    HB_State --> StreamlitUI
    DuckDB_Engine --> StreamlitUI
```

---

## 8. End-to-End Data Flow

### 8.1 Step-by-Step Data Journey
1. **Replay Ingestion**: The replayer (`src/qoe/producers/replay.py`) reads preprocessed events from disk, calculates the temporal delta between historical event timestamps, scales that delta by a configurable `--speed` multiplier (e.g. 10x or 60x real-time), applies fault mutations, and publishes JSON messages to Kafka.
2. **Validation and Cleansing (Q0)**:
   - Records are consumed from `playback-events`.
   - `src/qoe/streaming/validate.py` executes 9 checks. If any field fails (e.g. negative buffer or missing session ID), the record is routed to `quarantine` with a failure reason string.
   - `src/qoe/streaming/standardize.py` normalizes string casing and aliases.
   - `src/qoe/streaming/enrich.py` joins the stream with `users.parquet` and `content.parquet`.
   - Valid records are emitted to `playback-clean` and appended to `silver_playback_events.parquet`.
3. **Session Lifecycle Processing (Q1)**:
   - Consumes from `playback-clean`.
   - Maintains a `SessionState` object per `session_id`.
   - `play_request` sets state to `requested`.
   - `first_frame` computes `startup_ms = first_frame_ts - play_request_ts`, sets state to `playing`, and emits an initial session fact.
   - `buffer_start` sets state to `stalled`.
   - `buffer_end` calculates stall duration `stall_ms = buffer_end_ts - buffer_start_ts`, increments `stall_count`, and returns state to `playing`.
   - `heartbeat` accumulates watch time `watch_ms += interval_ms` and computes time-weighted bitrate.
   - At exactly 15 seconds of cumulative playback, Q1 emits a special fact: `kind = "early_features"`, containing early startup delay, stall count, and bitrate for machine learning scoring.
   - On `session_end` or timeout, Q1 writes final session summaries and upserts the record to HBase `session_state`.
4. **Windowed Metric Aggregation (Q2 & Q3)**:
   - Q2 reads `playback-clean` and `session-facts`. Every event is exploded across 5 grains (`global`, `city_isp`, `cdn`, `device`, `channel`).
   - Events are placed into 5-minute sliding buckets sliding every 60 seconds.
   - Upon window closure, Q2 computes `rebuffer_ratio`, `avg_startup_s`, `hd_pct`, `vsf_rate`, and calls `compute_qoe_score()`.
   - Simultaneously, Q3 aggregates network metrics (`avg_throughput_kbps`, `avg_rtt_ms`, `avg_packet_loss_pct`).
   - Both sinks upsert into HBase `qoe_metrics` using the identical row key `{grain}#{dimkey}#{rev_minute}`.
5. **Alerting & Anomaly Evaluation**:
   - `alerting.py` checks rules R1–R4 against the newly aggregated window.
   - If a rule breaches for 2 consecutive windows and is not in cooldown, an alert record is created with status `open`, saved to HBase `alerts`, and published to Kafka topic `alerts`.
   - When the segment returns to healthy metrics for 2 consecutive windows, a `resolved` alert event is emitted.
6. **ML Abandonment Scoring (Q4)**:
   - Consumes `session-facts` where `kind == "early_features"`.
   - Passes the 10 early features through the Scikit-Learn `Pipeline`.
   - Calculates `abandon_probability`. Emits high-risk predictions (`prob >= 0.70`) to Kafka `predictions` and warehouse.
7. **Dashboard Visualization**:
   - The Streamlit dashboard polls HBase for the newest row (`LIMIT=1`) of each dimension key to render live KPI scorecards and the 4x4 City x ISP heatmap.
   - Historical trend lines and distribution histograms are queried via DuckDB over Parquet files.

---

## 9. Why Each Technology Was Chosen

### 9.1 Technology Comparison Matrix

| Technology in Project | Plausible Alternative | Why VortexStream Selected the Chosen Technology | Why the Alternative Was Not Selected |
|---|---|---|---|
| **Apache Kafka** | RabbitMQ / AWS SQS / Redis Streams | Kafka provides distributed, disk-backed, partitioned commit-log architecture. Messages remain replayable across consumer groups (Spark Q0, Q1, Q2 read concurrently at their own pace). Enables high write throughput (100k+ msgs/sec). | RabbitMQ is an AMQP broker designed for transactional message queuing; once consumed, messages are deleted, preventing multi-stage streaming queries from independently re-reading streams. Redis Streams lacks distributed disk persistence for multi-gigabyte backlogs. |
| **Apache Spark (Structured Streaming)** | Apache Flink / Pure Python (Pandas/Faust) | Spark provides unified batch and streaming APIs. PySpark allows seamless integration with Python ML ecosystems (Scikit-Learn). Structured Streaming provides out-of-the-box watermarking, windowing, and distributed checkpointing. | Apache Flink has superior sub-second event-at-a-time latency, but its PyFlink ecosystem is substantially less mature than PySpark, requiring complex Java wrappers for custom sinks. Pure Python/Pandas cannot scale horizontally across cluster nodes and lacks built-in fault-tolerant checkpointing. |
| **Apache HBase** | Cassandra / PostgreSQL / MongoDB | HBase is a wide-column NoSQL store built on top of Hadoop/HDFS principles. It excels at sparse tables with independent column families (`m` and `n`), sub-millisecond point lookups by row key, and rapid range scans via lexicographical sorting. | PostgreSQL is relational and row-oriented; inserting thousands of streaming updates per second creates severe write-amplification and write-ahead log (WAL) locking contention. Cassandra lacks native reverse-timestamp prefix scanning without secondary clustering keys. |
| **DuckDB** | SQLite / ClickHouse / Trino | DuckDB is an embedded, vectorised in-process OLAP database. It can query multi-gigabyte Parquet directories directly using standard SQL without launching a separate background server daemon. | SQLite is row-oriented and lacks columnar vectorization, making multi-million row analytical aggregations 50x slower. ClickHouse is an outstanding distributed OLAP engine, but requires dedicated server clusters and Zookeeper/Keeper orchestration, introducing unnecessary operational overhead for local deployment. |
| **Scikit-Learn** | Spark MLlib / TensorFlow / PyTorch | Scikit-Learn provides high-performance, mature tabular classifiers (Random Forest, Gradient Boosting) that can be trained in seconds on tabular feature matrices and serialized into lightweight pickle artifacts. | Spark MLlib was explored for batch training, but Scikit-Learn's Python pipeline API offers cleaner integration with `ColumnTransformer` and zero JVM serialization overhead during real-time Python micro-batch inference. Deep learning (PyTorch/TensorFlow) is overkill for 10 tabular telemetry features and requires GPU acceleration. |
| **Streamlit** | React.js + FastAPI / Grafana | Streamlit allows 100% Python-native frontend development, enabling direct access to `data_access.py`, HappyBase, and DuckDB without building and maintaining separate REST API backends, CORS configs, and React state stores. | React + FastAPI requires maintaining two separate codebases and serialization layers. Grafana is great for time-series metrics but cannot render custom interactive ML churn inference interfaces or complex 4x4 interactive confusion matrix heatmaps. |

---

## 10. Apache Spark Deep Dive

### 10.1 Spark Architecture & Execution Mechanics
Apache Spark operates under a **Driver-Executor Master-Worker Topology**:
- **Driver Process**: The central coordinator that runs the user's Python script, instantiates the `SparkSession`, translates user code into a Directed Acyclic Graph (DAG) of logical execution stages, schedules tasks, and coordinates distributed workers.
- **Executors**: Worker JVM processes running on cluster nodes (or inside the Docker container) that execute individual computational tasks in parallel threads and store cached data partitions in memory.
- **Micro-Batch Processing**: Spark Structured Streaming treats incoming streaming data as an **unbounded append-only table**. At regular trigger intervals (default or configured), the engine discretizes newly arrived Kafka records into a micro-batch, executes the DataFrame DAG using Spark's Catalyst Optimizer, and commits offsets upon successful batch write.

### 10.2 Lazy Evaluation, Transformations vs Actions
Spark computations are divided strictly into **Transformations** and **Actions**:
- **Transformations (Lazy)**: Operations that define a new DataFrame or RDD from an existing one without immediately evaluating the data. Spark merely appends the operation to its internal query execution plan.
  - *Narrow Transformations*: Operations where each input partition contributes to at most one output partition (no network shuffle required). Examples in VortexStream: `filter()`, `map()`, `withColumn()`, `standardize()`.
  - *Wide Transformations*: Operations where data must be shuffled across network partitions based on a hash key. Examples: `groupBy()`, `join()`, `reduceByKey()`, `window()`.
- **Actions (Eager)**: Operations that force Spark to execute the execution plan and return a result to the driver or write data to an external sink. Examples: `count()`, `collect()`, `writeStream.start()`, `saveAsNewAPIHadoopFile()`.

### 10.3 Core RDD Demonstration (`src/qoe/batch/rdd_demo.py`)
To demonstrate deep proficiency in Apache Spark's core low-level API, `src/qoe/batch/rdd_demo.py` implements an exhaustive suite of fundamental RDD operations:
1. **Parallelization & Partitioning**: `sc.parallelize(records, 2)` distributes an in-memory collection across 2 worker partitions.
2. **Accumulators**: `inv_acc = sc.accumulator(0)` creates a distributed write-only counter used to track malformed records across worker nodes without race conditions.
3. **Broadcast Variables**: `bc_genres = sc.broadcast(dict(ref_channels))` distributes a read-only lookup table to every worker node once, avoiding repeated network serialization inside task closures.
4. **Caching / Persistence**: `rdd.persist()` caches the evaluated RDD partitions in memory across subsequent actions.
5. **Transformations**:
   - `filter()`: Selects error events where `event_type == 'error'`.
   - `map()` & `reduceByKey()`: Calculates total error occurrences per ISP:
     ```python
     isp_errors = rdd.filter(lambda x: x.get("event_type") == "error") \\
                     .map(lambda x: (x.get("isp"), 1)) \\
                     .reduceByKey(lambda a, b: a + b).collect()
     ```
   - `flatMap()`: Extracts individual tags or error tokens from nested message strings.
   - `aggregateByKey()`: Computes average bitrate per device type in a single pass without holding all values in memory:
     ```python
     # zeroValue = (count, sum_bitrate)
     seqOp = lambda acc, val: (acc[0] + 1, acc[1] + val)
     combOp = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
     dev_avg_br = rdd.map(lambda x: (x.get("device"), x.get("bitrate_kbps", 0))) \\
                     .aggregateByKey((0, 0.0), seqOp, combOp) \\
                     .mapValues(lambda v: v[1] / max(v[0], 1)).collect()
     ```
   - `join()`: Performs a pair RDD join combining playback records with broadcast channel genres.

### 10.4 Advanced Analytical SQL Queries (`sql/spark_sql_queries.sql`)
The project implements enterprise SQL analytics demonstrating window functions and multidimensional cubes:
```sql
-- Window Functions: LAG QoE delta and RANK worst segments
SELECT 
    grain,
    dimkey,
    window_end,
    qoe_score,
    lag(qoe_score, 1) OVER (PARTITION BY grain, dimkey ORDER BY window_end) AS prev_qoe,
    qoe_score - lag(qoe_score, 1) OVER (PARTITION BY grain, dimkey ORDER BY window_end) AS qoe_delta,
    rank() OVER (PARTITION BY window_end ORDER BY qoe_score ASC) AS worst_rank
FROM gold_qoe_5m;
```

---

## 11. Apache HBase Deep Dive

### 11.1 HBase Internal Architecture
Apache HBase is a distributed, versioned, column-oriented NoSQL database modeled after Google Bigtable:
- **HMaster**: Manages cluster metadata, coordinates RegionServer assignments, handles failover, and executes table DDL operations (create, alter, drop).
- **RegionServer**: Serves and manages data partitions called **Regions**. Handles all client read and write requests (Get, Put, Scan).
- **Region**: A contiguous range of row keys stored across one or more Store objects (one Store per column family).
- **Write-Ahead Log (WAL)**: When a write arrives, the RegionServer first appends the mutation to disk in the WAL. This guarantees zero data loss if the server crashes before memory is flushed.
- **MemStore**: In-memory write buffer per column family. Writes are sorted lexicographically by row key in memory.
- **HFile**: When the MemStore reaches its threshold (default 128 MB), it flushes its sorted contents to an immutable **HFile** on disk. Periodic background compactions (minor and major) merge multiple HFiles and prune expired TTL cells.

### 11.2 Key Design & Mathematical Algorithms (`src/qoe/common/hbase_keys.py`)

#### 1. Salting for Write Hotspot Prevention (`session_state`)
In high-throughput streaming systems, monotonically increasing or clustered keys cause all concurrent writes to target a single RegionServer (the "hotspot"), leaving all other nodes idle.
- **Algorithm**:
  $$\text{salt} = \text{CRC32}(\text{session\_id}) \pmod{16}$$
  $$\text{RowKey} = \text{format}(\text{salt}, \text{"02d"}) + \text{"-"} + \text{session\_id}$$
- **Impact**: Keys are evenly distributed across 16 pre-split region buckets (`00-sess-...` through `15-sess-...`), achieving uniform parallel write utilization across all RegionServers.

#### 2. Reverse-Timestamping for Newest-First Prefix Scans (`qoe_metrics`)
HBase stores rows in ascending lexicographical order. Under standard Unix timestamps, older records sort first, requiring a full region scan to find the latest window.
- **Algorithm**:
  $$\text{rev\_minute} = \text{string}(9223372036854775807 - \text{epoch\_minute}).\text{zfill}(19)$$
  $$\text{RowKey} = \text{grain} + \text{"#"} + \text{dimkey} + \text{"#"} + \text{rev\_minute}$$
- **Impact**: Subtracting the current epoch minute from $2^{63}-1$ (`Long.MAX_VALUE`) reverses the sort order. A prefix scan with `ROWPREFIXFILTER => 'city_isp#Chennai|Airtel#'` and `LIMIT => 1` instantly retrieves the newest 5-minute metric window in sub-millisecond time.

### 11.3 Column Family Design & Storage Physical Isolation
HBase physical storage is partitioned by Column Family. The `qoe_metrics` table defines two distinct families:
- **Column Family `m:` (Playback Metrics)**: Stores `qoe_score`, `rebuffer_ratio`, `avg_startup_s`, `hd_pct`, `vsf_rate`, `error_rate`, penalties. Written by Streaming Query Q2.
- **Column Family `n:` (Network Transport Metrics)**: Stores `avg_throughput_kbps`, `avg_rtt_ms`, `avg_packet_loss_pct`, `net_samples`. Written by Streaming Query Q3.

**Why this is a crucial architectural design choice**:
1. **Independent Writes**: Q2 and Q3 run as completely separate PySpark streaming jobs writing to the same logical dimension and window end. Because mutations target different column families, they append to independent MemStores without read-modify-write locks.
2. **I/O Disk Efficiency**: An operational query that only checks network health (`scan 'qoe_metrics', {COLUMNS => ['n:avg_rtt_ms']}`) instructs the RegionServer to read *only* the HFiles belonging to family `n:`. Blocks containing family `m:` are never loaded into the BlockCache, drastically reducing disk I/O and memory churn.
"""
