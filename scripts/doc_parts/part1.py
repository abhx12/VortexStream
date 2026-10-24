# scripts/doc_parts/part1.py
"""Sections 1 to 6 for PROJECT_DEEP_DIVE.md"""

def get_part1() -> str:
    return """# VortexStream: Real-Time OTT Video Quality of Experience (QoE) Analytics, Fault Detection & Churn Prediction Platform

# COMPREHENSIVE TECHNICAL DEEP DIVE & CODEBASE HANDBOOK

> **Document Classification**: Engineering Technical Reference, Architecture Defense Guide & Viva Preparation Manual  
> **Target Audience**: Engineers, Technical Reviewers, Examiners, System Architects  
> **Codebase Root**: `C:\\bigdata` / `E:\\bigdata` (Windows Host & Docker Container Runtime)  
> **Project Status**: 100% Implemented (Phases 0–14 Complete, 80/80 Unit & Integration Tests Passing, Verified V1–V8 Reconciled)  
> **Git Repository**: `https://github.com/abhx12/VortexStream.git`

---

## Table of Contents

1. [Project at a Glance](#1-project-at-a-glance)
2. [Problem Statement and Objectives](#2-problem-statement-and-objectives)
3. [Repository Structure and File Inventory](#3-repository-structure-and-file-inventory)
4. [Technology Stack](#4-technology-stack)
5. [Dataset Sources and Data Provenance](#5-dataset-sources-and-data-provenance)
6. [Data Cleaning and Preprocessing](#6-data-cleaning-and-preprocessing)
7. [High-Level Architecture](#7-high-level-architecture)
8. [End-to-End Data Flow](#8-end-to-end-data-flow)
9. [Why Each Technology Was Chosen](#9-why-each-technology-was-chosen)
10. [Apache Spark Deep Dive](#10-apache-spark-deep-dive)
11. [Apache HBase Deep Dive](#11-apache-hbase-deep-dive)
12. [Complete Schema Reference](#12-complete-schema-reference)
13. [File-by-File Code Explanation](#13-file-by-file-code-explanation)
14. [Algorithms and Transformations](#14-algorithms-and-transformations)
15. [API and Component Integration](#15-api-and-component-integration)
16. [Concrete End-to-End Worked Example](#16-concrete-end-to-end-worked-example)
17. [Configuration and Environment Setup](#17-configuration-and-environment-setup)
18. [Running and Verifying the Project](#18-running-and-verifying-the-project)
19. [Error Handling and Failure Scenarios](#19-error-handling-and-failure-scenarios)
20. [Performance, Scalability, and Trade-offs](#20-performance-scalability-and-trade-offs)
21. [Security and Data Management](#21-security-and-data-management)
22. [Actual Implementation vs Intended Functionality](#22-actual-implementation-vs-intended-functionality)
23. [Limitations and Recommended Improvements](#23-limitations-and-recommended-improvements)
24. [Comprehensive Viva Question Bank](#24-comprehensive-viva-question-bank)
25. [Code-Reading and Learning Roadmap](#25-code-reading-and-learning-roadmap)
26. [Final Project Understanding Checklist](#26-final-project-understanding-checklist)
27. [Glossary of Technical Terms](#27-glossary-of-technical-terms)
28. [References and Verified Source Links](#28-references-and-verified-source-links)

---

## 1. Project at a Glance

| Attribute | Specification |
|---|---|
| **System Name** | **VortexStream** |
| **Domain** | Over-The-Top (OTT) Video Streaming Telemetry & Infrastructure Observability |
| **Core Functionality** | End-to-end distributed stream processing platform that ingests raw player telemetry; cleans and standardizes events; statefully tracks player session lifecycles; computes sliding-window multi-grain QoE scores; fires hysteresis-guarded operational alerts; detects regional/CDN network anomalies; and predicts user session abandonment from early-session telemetry (<= 15 seconds) with zero target leakage. |
| **Primary Streaming Engine** | Apache Spark 3.5.3 (Structured Streaming micro-batch processing with PySpark) |
| **Distributed Message Broker** | Apache Kafka 3.7.0 (KRaft mode, 9 distinct topics) |
| **Hot Operational Database** | Apache HBase 2.5.8 (Distributed Wide-Column NoSQL store queried via HappyBase Thrift RPC) |
| **Analytical Data Warehouse** | DuckDB 1.1.0 querying partitioned columnar Parquet files (`data/warehouse/`) |
| **Machine Learning Stack** | Scikit-Learn 1.5.0 (RandomForest & GBT Classifier; Unsupervised MAD Robust Z-Score + KMeans) |
| **User Interface** | Streamlit 1.39.0 real-time operations dashboard with Plotly 4x4 heatmaps & metrics |
| **Primary Core Dataset** | Real Stanford Puffer Video Streaming Telemetry (Stanford University academic research) |
| **Simulated Extensions** | Deterministic Indian metro cities, ISPs, CDNs, client devices, and chaos fault injections |
| **Verification & Quality Gate** | Automated reconciliation engine verifying 8 mathematical and system conservation gates (V1–V8) |
| **Test Suite Coverage** | 80/80 passing unit & integration tests covering bootstrap, schemas, streaming, ML, and reconciliation |

---

## 2. Problem Statement and Objectives

### 2.1 The Industry Problem
In modern commercial OTT video platforms (e.g., Netflix, Disney+ Hotstar, Amazon Prime Video, YouTube), user retention and subscription revenue depend directly on **Quality of Experience (QoE)**:
1. **Rebuffering & Stalling**: When video playback freezes to buffer data, user frustration spikes exponentially. Industry research shows that every 1% increase in buffering time leads to a 3-5% increase in viewer drop-off.
2. **Video Start Failure (VSF) & Startup Delay**: If a viewer clicks "Play" and waits more than 4 seconds without rendering the first frame, or if playback fails completely to initialize within 10 seconds, abandonment rates exceed 40%.
3. **Multi-Dimensional Edge Faults**: Playback degradations rarely affect the entire global platform simultaneously. Instead, failures emerge in specific intersections—e.g., *Airtel 4G users in Chennai experiencing CDN edge cache misses on Akamai*, or *Android app v2.1 encountering DRM decrypt timeout errors*. Traditional global monitoring misses these regional and device-specific degradations until widespread churn has already occurred.
4. **Late Reactive Intervention**: Most monitoring systems aggregate metrics hourly. By the time an operational dashboard reports that a region had poor streaming quality, hundreds of thousands of sessions have already terminated in anger.

### 2.2 System Objectives
VortexStream was engineered to solve these challenges with high technical rigor:
- **Low-Latency Detection**: Compute multi-grain QoE metrics within 5-minute sliding windows (sliding every 60 seconds) with sub-10 second p95 latency from event generation to dashboard visualization.
- **Stateful Lifecycle Tracking**: Reconstruct full player lifecycles (`requested` -> `playing` -> `stalled` -> `ended` / `failed_start`) across millions of concurrent sessions, maintaining accurate startup latency and cumulative stall durations.
- **Multi-Grain Metric Explosion**: Automatically aggregate metrics across 5 distinct dimensions: `global`, `city_isp`, `cdn`, `device`, and `channel`.
- **Hysteresis-Guarded Alerting**: Prevent alert storms and transient notification flapping by requiring 2 consecutive breaching windows to trigger an alert and 2 consecutive healthy windows to resolve it, coupled with a 10-minute cooldown and a minimum sessions guard (`min_sessions >= 20`).
- **Zero-Leakage Early Churn Prediction**: Train a supervised machine learning model to predict whether a session will terminate early (< 120 seconds watch time) based strictly and exclusively on the telemetry emitted in the first 15 seconds of playback.
- **Enterprise Storage Tiering**: Route hot operational state into HBase for sub-millisecond point-lookups and prefix scans, while concurrently writing immutable historical data into columnar Parquet files queried via DuckDB.

---

## 3. Repository Structure and File Inventory

### 3.1 Repository Directory Hierarchy
```text
C:\\bigdata\\ (or E:\\bigdata\\)
├── .env.example                     # Environment template (hosts, ports, paths)
├── .gitignore                       # Git ignore filters (caches, parquet, checkpoints)
├── docker-compose.yml               # Multi-container orchestration (Kafka, HBase, App)
├── Makefile                         # Unified CLI automation entrypoint
├── pyproject.toml                   # Project metadata, dependencies, tool settings
├── README.md                        # High-level architecture and quickstart overview
├── IMPLEMENTATION_PLAN (1).md       # 15-phase architectural specification
├── config/                          # Centralized YAML configuration files
│   ├── config.yaml                  # System-wide configuration (Kafka, HBase, QoE, alerts)
│   ├── chaos.yaml                   # Network delay, loss, and event drop tunables
│   ├── faults.yaml                  # Active fault scenarios (F1, F2, F3)
│   ├── sim_dims.yaml                # Categorical distributions for cities, ISPs, CDNs
│   └── standardization.yaml         # Taxonomy normalization alias maps
├── data/                            # Persistent and generated data storage
│   ├── manifests/                   # Replay runs, fault ledgers, and stream manifests
│   ├── models/                      # Serialized ML model artifacts (RandomForest, scaler)
│   ├── raw/                         # Raw Stanford Puffer CSV archives
│   ├── reference/                   # Synthetic user catalog and content catalog
│   └── warehouse/                   # Columnar Parquet warehouse tiers and DuckDB database
├── docker/                          # Container build definitions
│   ├── app.Dockerfile               # PySpark 3.5.3, OpenJDK 17, Python 3.11 image
│   └── hbase.Dockerfile             # Standalone Apache HBase 2.5.8 image with Thrift
├── docs/                            # In-depth architectural reports and verifications
│   ├── architecture.md              # Medallion tiering and Mermaid architecture
│   ├── calibration.md               # Empirical QoE baseline metric distributions
│   ├── data_profile.md              # Profiling metrics of Stanford Puffer CSVs
│   ├── demo_script.md               # 5-7 minute oral presentation and demo runbook
│   ├── etl_mapping.md               # Comprehensive source-to-target field mapping
│   ├── hbase_design.md              # Row-key salting, reverse-minutes, column families
│   ├── ml_report.md                 # Machine learning feature importance and metrics
│   ├── performance.md               # Soak testing, throughput, and latency benchmarks
│   ├── PROGRESS.md                  # Detailed phase-by-phase completion log
│   ├── simulated_fields.md          # Real vs simulated field transparency declaration
│   └── verification_report.md       # V1–V8 mathematical verification results
├── schemas/                         # Canonical JSON schema validation definitions
│   ├── network_metric.schema.json   # Schema for raw network telemetry
│   ├── playback_event.schema.json   # Schema for raw playback telemetry
│   └── session_fact.schema.json     # Schema for lifecycle session facts
├── scripts/                         # Operational shell and Python administration scripts
│   ├── create_topics.py             # Kafka topic creator via confluent-kafka
│   ├── create_topics.sh             # Kafka topic creator via container CLI
│   ├── demo.py                      # One-command interactive live demonstration
│   ├── demo.sh                      # Shell wrapper for demo pipeline
│   ├── doctor.py                    # Environment and dependency diagnostic checker
│   ├── hbase_init.py                # HBase table and column family provisioner
│   ├── reset_state.py               # State reset and wipe utility
│   └── reset_state.sh               # Shell wrapper for state reset
├── sql/                             # Analytical SQL query definitions
│   ├── report_queries.sql           # KPI report SQL queries
│   ├── spark_sql_queries.sql        # Advanced Spark SQL analytical queries (LAG, RANK)
│   └── warehouse_views.sql          # DuckDB analytical view creation DDL
├── src/                             # Source code root
│   └── qoe/                         # Main VortexStream package
│       ├── batch/                   # Batch processing, RDD demos, and rollups
│       ├── common/                  # Configuration, logging, and HBase key utilities
│       ├── dashboard/               # Streamlit application and data access layer
│       ├── data/                    # Puffer acquisition, preprocessing, simulation
│       ├── ml/                      # Abandonment classifier and anomaly detector
│       ├── producers/               # Stream replayer, fault injector, chaos engine
│       ├── streaming/               # Structured Streaming queries (Q0–Q4), alerting
│       └── verify/                  # Reconciliation engine (V1–V8) and lineage generator
└── tests/                           # Exhaustive pytest verification test suite
    ├── conftest.py                  # Pytest fixtures and mock environments
    ├── fixtures/                    # Mini Puffer CSV telemetry fixtures
    └── phase_0/ ... phase_14/       # 15 distinct phase test packages (80 tests total)
```

### 3.2 Complete File Inventory
| File Path | Primary Responsibility | Calling / Dependent Components | Input Received | Output / Side Effect Produced |
|---|---|---|---|---|
| `config/config.yaml` | System configuration (Kafka brokers, HBase connection, QoE weights, alert thresholds, streaming parameters). | Read by `qoe.common.config.get_config()`. | YAML file on disk. | Centralized configuration dictionary. |
| `config/faults.yaml` | Injected fault scenarios: F1 (Chennai-Airtel throttle), F2 (Akamai edge 5xx), F3 (Android DRM crash). | Read by `qoe.producers.faults.FaultEngine`. | YAML configuration. | Active fault time-windows and impairment rules. |
| `config/standardization.yaml` | Mapping tables for normalizing noisy device, ISP, CDN, and OS strings to canonical names. | Read by `qoe.streaming.standardize.Standardizer`. | Raw string aliases. | Canonical taxonomy strings. |
| `schemas/playback_event.schema.json` | JSON Schema specifying required fields and types for playback events. | Used by `validate.py` and unit tests. | Raw event dictionary. | Boolean validity / validation errors. |
| `src/qoe/common/config.py` | Thread-safe singleton configuration loader with environment variable overrides. | All modules in `src/qoe/`. | `config/config.yaml` & OS env. | Python configuration dictionary. |
| `src/qoe/common/hbase_keys.py` | Mathematical row-key generation: CRC32 salting and 19-digit reverse-minute formatting. | `sinks_hbase.py`, `q1_session_tracker.py`, `q2_qoe_aggregator.py`, `data_access.py`. | Session IDs, timestamps, dimensions. | Formatted HBase row-key strings. |
| `src/qoe/common/logging.py` | Structured JSON log formatter emitting ISO-8601 timestamps and run metadata. | All modules across `src/qoe/`. | Python log records. | Single-line JSON strings to stdout. |
| `src/qoe/data/puffer_download.py` | Downloads raw Stanford Puffer telemetry CSV archives from Stanford University research storage. | Invoked by preprocessing scripts. | HTTPS requests to Puffer servers. | `client_buffer.csv`, `video_sent.csv`, `video_acked.csv`. |
| `src/qoe/data/preprocess.py` | Performs DuckDB ASOF join of buffer and network CSVs, downsampling to 5-second heartbeats. | Data preparation pipeline. | Raw Puffer CSVs. | Canonical unified JSON events in `data/raw/unified_events.jsonl`. |
| `src/qoe/data/sim_dims.py` | Deterministically generates synthetic Indian metro cities, ISPs, CDNs, and device hardware. | `preprocess.py`, `ref_data.py`. | Session IDs & SHA-256 hash seeds. | Enriched categorical dimensions. |
| `src/qoe/data/ref_data.py` | Synthesizes reference catalogs: 50,000 users with subscription tiers and content catalog. | Ingestion and enrichment pipeline. | Random seeds & distribution configs. | `data/reference/users.parquet`, `content.parquet`. |
| `src/qoe/producers/replay.py` | Speed-controlled streaming event replayer emitting JSON events into Kafka bronze topics. | `make replay`, `scripts/demo.py`. | Unified event files. | Messages published to Kafka topics (`playback-events`, etc.). |
| `src/qoe/producers/faults.py` | Injects synthetic operational impairments (VSF, buffer stalls, CDN 5xx errors) and writes fault ledger. | Called inside `replay.py`. | Raw events & `faults.yaml`. | Mutated events & `data/manifests/fault_ledger_*.json`. |
| `src/qoe/producers/chaos.py` | Simulates real-world network chaos (jitter, out-of-order delivery, duplicate emissions). | Called inside `replay.py`. | Events list & `chaos.yaml`. | Shuffled, duplicated, or delayed event stream. |
| `src/qoe/streaming/spark_session.py` | Configures and instantiates local or cluster PySpark `SparkSession` with Kafka and Parquet packages. | All streaming queries (Q0–Q4) and batch jobs. | Spark configuration parameters. | Active PySpark `SparkSession` object. |
| `src/qoe/streaming/validate.py` | Evaluates 9 deterministic data validation rules, splitting records into valid and quarantine. | Streaming Query Q0 (`q0_ingest_clean.py`). | Raw event dictionaries. | Tuple of `(valid_records, quarantine_records)`. |
| `src/qoe/streaming/standardize.py` | Normalizes noisy client attributes (`"Android 12"` -> `"android"`, `"airtel_broadband"` -> `"Airtel"`). | Streaming Query Q0 (`q0_ingest_clean.py`). | Valid event records. | Standardized canonical records. |
| `src/qoe/streaming/enrich.py` | Performs stream-static joins enriching playback events with user subscription tier and content genre. | Streaming Query Q0 (`q0_ingest_clean.py`). | Standardized records & Parquet catalogs. | Fully enriched event records. |
| `src/qoe/streaming/q0_ingest_clean.py` | Structured Streaming Query Q0: Ingests from bronze Kafka, validates, standardizes, enriches, writes silver. | `make q0`. | Kafka `playback-events`, `network-metrics`. | Kafka `playback-clean`, `network-clean`, `quarantine`. |
| `src/qoe/streaming/q1_session_tracker.py` | Structured Streaming Query Q1: Stateful session tracking (`requested`, `playing`, `stalled`, `ended`). | `make q1`. | Kafka `playback-clean`. | Kafka `session-facts`, HBase `session_state`, Parquet. |
| `src/qoe/streaming/q2_qoe_aggregator.py` | Structured Streaming Query Q2: Multi-grain 5-min sliding window QoE aggregation and composite scoring. | `make q2`. | Kafka `playback-clean` & `session-facts`. | HBase `qoe_metrics` (cf `m`), `gold_qoe_5m_raw`. |
| `src/qoe/streaming/q3_network_aggregator.py` | Structured Streaming Query Q3: Multi-grain 5-min sliding window network metric aggregation (throughput, RTT). | `make q3`. | Kafka `network-clean`. | HBase `qoe_metrics` (cf `n`), `gold_network_5m_raw`. |
| `src/qoe/streaming/q4_ml_scoring.py` | Structured Streaming Query Q4: Evaluates supervised abandonment model on early session facts in real time. | `make q4`. | Kafka `session-facts` (`early_features`). | Kafka `predictions`, Parquet `ml_predictions`. |
| `src/qoe/streaming/qoe_score.py` | Mathematical formulation of composite QoE score [0, 100] with individual penalty subtractions. | `q2_qoe_aggregator.py`, batch rollups, UI. | Window metric dictionary. | Calculated score, label (`Good`/`Fair`/`Poor`), penalties. |
| `src/qoe/streaming/alerting.py` | Evaluates rules R1–R4 with 2-window hysteresis, 10-minute cooldown, and minimum sessions guard. | Evaluated after Q2 window aggregation. | Window aggregate records. | Opened and resolved alert records. |
| `src/qoe/streaming/sinks_hbase.py` | High-throughput batch upsert writers for HBase tables using HappyBase Thrift client. | Streaming queries Q1, Q2, Q3, alerting. | Record lists & row-key mappings. | Batch RPC `put` operations into HBase. |
| `src/qoe/streaming/sinks_warehouse.py` | Columnar partitioned Parquet writers managing local data lakehouse directories. | Streaming queries Q0–Q4. | Record lists & partition columns. | Partitioned Parquet files on disk. |
| `src/qoe/ml/dataset.py` | Extracts supervised ML training dataset with strict zero-leakage guard (first 15 seconds only). | `churn_model.py`. | Parquet warehouse session facts. | Clean Pandas feature matrix and labels. |
| `src/qoe/ml/churn_model.py` | Trains and serializes Random Forest & Gradient Boosting abandonment prediction pipelines. | ML training pipeline. | Feature matrix from `dataset.py`. | Serialized model artifact `data/models/abandon_v1/`. |
| `src/qoe/ml/anomaly.py` | Dual-model unsupervised anomaly detection: Robust Z-Score (Median/MAD) + KMeans clustering. | Operational diagnostics & verification. | Window metrics DataFrame. | Anomaly flags and distance-to-centroid scores. |
| `src/qoe/batch/rdd_demo.py` | Core Spark RDD transformation and action demonstration (map, reduceByKey, join, broadcast, accumulator). | `python -m qoe.batch.rdd_demo`. | Synthetic/real event records. | Aggregation dictionaries and action outputs. |
| `src/qoe/batch/sql_demo.py` | Spark SQL analytical engine executing complex analytical queries (LAG, RANK, Rollup). | `python -m qoe.batch.sql_demo`. | Silver and Gold Parquet tables. | Analytical DataFrames and formatted tables. |
| `src/qoe/batch/warehouse.py` | Manages DuckDB warehouse connection, views, and hourly Parquet rollups. | Batch jobs & Streamlit dashboard. | Gold Parquet tables. | DuckDB database `data/warehouse/qoe.duckdb`. |
| `src/qoe/dashboard/app.py` | Multi-page Streamlit web dashboard displaying live KPIs, QoE trends, 4x4 heatmaps, alerts, ML insights. | Web browser client (:8501). | Data from `data_access.py`. | Interactive UI rendering. |
| `src/qoe/dashboard/data_access.py` | Abstraction layer querying HBase (live hot state) and DuckDB (historical cold warehouse). | `app.py`. | Queries from UI widgets. | Clean Pandas DataFrames for dashboard display. |
| `src/qoe/verify/reconcile.py` | Comprehensive verification suite executing 8 mathematical reconciliation gates (V1–V8). | `make reconcile`, CI/CD. | Manifests, Kafka offsets, HBase, Parquet. | `docs/verification_report.md` (100% pass). |
| `scripts/hbase_init.py` | Idempotent DDL script creating HBase tables (`session_state`, `qoe_metrics`, `alerts`) and column families. | Docker compose startup / `make init`. | Thrift connection to HBase. | Initialized HBase table metadata. |
| `scripts/demo.py` | Fully automated interactive end-to-end demonstration script. | `make demo`. | User terminal prompts. | Orchestrates full replay, streaming, and UI launch. |

---

## 4. Technology Stack

### 4.1 Technology Overview
VortexStream combines distributed message ingestion, stateful stream computation, wide-column NoSQL hot storage, columnar embedded OLAP querying, and machine learning into an enterprise-grade lambda/kappa architecture.

| Technology | Version | Exact Role in VortexStream | Configuration & Connection Parameters |
|---|---|---|---|
| **Apache Kafka** | `3.7.0` (Confluent Community) | Distributed commit log acting as the bronze and silver ingestion backbone. Buffers bursty player events, decouples stream replayers from Spark processors, and provides replayable message persistence. | Runs in KRaft mode (no ZooKeeper dependency). Listens on `localhost:9092` (host) and `kafka:9092` (container). Configured with 9 topics, replication factor 1 (standalone development), 7-day retention, snappy compression. |
| **Apache Spark** | `3.5.3` (PySpark runtime) | Primary distributed processing engine. Executes 5 streaming queries (Q0–Q4) using Spark Structured Streaming, processes windowed aggregations, maintains stateful sessions, and runs batch RDD/SQL jobs. | Running with OpenJDK 17 on Linux container (`ott-app`). Configured with `spark.sql.shuffle.partitions=4`, `spark.driver.memory=2g`, checkpointing under `/tmp/spark-checkpoints/`. |
| **Apache HBase** | `2.5.8` | Distributed wide-column NoSQL store. Serves as the operational serving layer for hot session states, low-latency sliding-window metrics, and active alerts. | Standalone mode with local HFiles and embedded ZooKeeper. Exposes Thrift 1 API on port `9090` and Web Master UI on port `16010`. Tables: `session_state` (TTL 6h), `qoe_metrics` (TTL 24h), `alerts`. |
| **DuckDB** | `1.1.0` | In-process columnar OLAP SQL database. Manages preprocessing of raw Puffer CSVs (ASOF joins), queries partitioned Parquet files directly without server overhead, and powers historical dashboard views. | Embedded database stored at `data/warehouse/qoe.duckdb`. Directly executes SQL queries against multi-level partitioned Parquet lakehouse directories (`read_parquet(...)`). |
| **Scikit-Learn** | `1.5.0` | Machine learning engine. Provides `ColumnTransformer`, `StandardScaler`, `OneHotEncoder`, `RandomForestClassifier`, and `GradientBoostingClassifier` for abandonment scoring, plus MAD and `KMeans` for anomaly detection. | Models serialized using `pickle` under `data/models/abandon_v1/`. Inference executed in streaming micro-batches by Query Q4. |
| **Streamlit** | `1.39.0` | Frontend web application framework. Renders the real-time operations dashboard, KPI scorecards, interactive 4x4 City x ISP heatmap, alert feed, and ML model diagnostic charts. | Bound to port `8501`. Queries HBase hot tables via HappyBase and DuckDB historical warehouse via `duckdb.connect()`. Auto-refreshes every 5 seconds. |
| **Docker Compose**| `v2.x` | Containerization runtime orchestrating the 3 foundational services: `ott-kafka`, `ott-hbase`, and `ott-app`. | Configured in `docker-compose.yml`. Configures internal bridge networking (`qoe-net`), persistent host-mounted volumes, healthchecks, and dependency sequencing. |
| **Python** | `3.11` (Container) / `3.13` (Host) | Core implementation language across all pipelines, producers, streaming jobs, APIs, and verification suites. | Managed with `pyproject.toml` and standard virtual environment (`.venv`). |

---

## 5. Dataset Sources and Data Provenance

### 5.1 Real Telemetry: Stanford Puffer Research Dataset
VortexStream is built upon real, empirical video streaming telemetry collected by **Stanford University's Puffer Project** (`https://puffer.stanford.edu/`), an academic research initiative studying adaptive bitrate (ABR) streaming algorithms over real public Internet paths:
- **Official Dataset Name**: Stanford Puffer Video Streaming Telemetry Archive.
- **Publisher**: Stanford University Networking Research Group (Francis Y. Yan, Hudson Ayers, Chenzhi Zhu, Sadjad Fouladi, James Hong, Keyi Zhang, Philip Levis, Keith Winstein).
- **Primary Research Publication**: *"Learning in situ: a randomized experiment in video streaming"* (USENIX NSDI 2020).
- **Public Data Repository**: Hosted on Google Cloud Storage bucket `gs://puffer-data-release/` and Stanford servers at `https://puffer.stanford.edu/results/`.
- **Download Automation**: Implemented in `src/qoe/data/puffer_download.py`, which fetches daily gzip tarballs containing 3 core CSV files.

#### Core Puffer CSV Files & Physical Attributes
1. **`client_buffer_*.csv`**: Player client buffer health reported at high frequency (~10 Hz / every 100 ms):
   - `time`: Monotonic client event timestamp in nanoseconds.
   - `session_id`: Unique integer identifying the viewer streaming session.
   - `index`: Monotonic event sequence counter within the session.
   - `buffer`: Instantaneous player buffer occupancy in seconds (float).
   - `cum_rebuf`: Cumulative rebuffering/stall duration experienced by the client from session start in seconds (float).
2. **`video_sent_*.csv`**: Server-side transmission telemetry for each 1-second video chunk:
   - `time`: Chunk transmission timestamp in nanoseconds.
   - `session_id`: Viewer session identifier.
   - `format`: Encoded video resolution and profile (e.g., `1920x1080`, `1280x720`, `640x360`).
   - `size`: Size of the transmitted chunk in bytes.
   - `ssim_index`: Structural Similarity Index (SSIM) measuring visual compression fidelity.
3. **`video_acked_*.csv`**: TCP-level acknowledgment telemetry from client back to streaming server:
   - `time`: Acknowledgment arrival timestamp in nanoseconds.
   - `session_id`: Viewer session identifier.
   - `delivery_rate`: Instantaneous TCP delivery rate / bandwidth estimate in bits per second.
   - `rtt`: Instantaneous round-trip time in microseconds.
   - `min_rtt`: Minimum observed RTT across the TCP connection lifecycle in microseconds.

### 5.2 Simulated Dimensions & The Engineering Rationale
While Stanford Puffer provides ground-truth client buffer dynamics and TCP transport measurements, it originates from an academic streaming server in California serving public television streams. Consequently, the raw Puffer dataset lacks commercial dimensions essential to enterprise OTT platforms:
- No geographic distribution across commercial metropolitan regions.
- No Internet Service Provider (ISP) network categorization (Jio, Airtel, Vi, BSNL).
- No Content Delivery Network (CDN) edge routing tags (Akamai, Cloudflare, Fastly).
- No client device hardware classification (Android, iOS, SmartTV, Web Browser).
- No commercial user accounts, subscription tiers, or video content genres.

To bridge this gap while maintaining scientific integrity, VortexStream implements a **deterministic dimension synthesis engine** (`src/qoe/data/sim_dims.py` and `src/qoe/data/ref_data.py`):
1. **Deterministic Session Hashing**: Categorical dimensions are assigned by computing a SHA-256 hash of the real Puffer `session_id` combined with a fixed global seed (`42`). This guarantees that every time session `12345` is processed, it maps to the exact same city, ISP, CDN, and device, ensuring 100% reproducibility across batch and streaming pipelines.
2. **Realistic Indian Market Distributions**: Categorical probabilities reflect actual Indian OTT streaming market shares:
   - **Cities**: Mumbai (30%), Delhi (25%), Bengaluru (20%), Chennai (15%), Hyderabad (10%).
   - **ISPs**: Jio (45%), Airtel (30%), Vi (15%), BSNL (10%).
   - **CDNs**: Akamai (45%), Cloudflare (35%), Fastly (20%).
   - **Devices**: Android Mobile (50%), SmartTV (25%), iOS (15%), Web Desktop (10%).
3. **Network Impairment Simulation**: Packet loss percentage (`packet_loss_pct`) is synthesized using a lognormal distribution correlated with the real Puffer `rtt` and delivery rate.
4. **Reference Catalogs**: `src/qoe/data/ref_data.py` creates a synthetic user database of 50,000 subscriber profiles (`user_id`: `u-XXXXX`, subscription tier: `free`, `vip`, `premium`) and a video content catalog (content genres: `sports`, `drama`, `movies`, `news`).

### 5.3 Injected Fault Scenarios (Ground-Truth Fault Ledger)
To test and validate real-time alerting and ML anomaly detection, 3 realistic operational fault scenarios are deterministically injected during stream replay via `config/faults.yaml` and `src/qoe/producers/faults.py`:
- **Fault F1 (ISP Regional Throttling)**:
  - *Target Selector*: `city = "Chennai"`, `isp = "Airtel"`.
  - *Time Window*: Minute 10 to Minute 30 of replay.
  - *Impairment*: 60% reduction in delivery rate (`throughput_mult: 0.4`), 2.5x latency inflation (`rtt_mult: 2.5`), +8% packet loss (`loss_add_pct: 8.0`), heartbeat stall probability set to 25%, and 30% early abandonment probability.
- **Fault F2 (CDN Edge Cache Miss & Latency Spike)**:
  - *Target Selector*: `cdn = "Akamai"`.
  - *Time Window*: Minute 35 to Minute 50 of replay.
  - *Impairment*: 50% throughput reduction, 2.0x RTT inflation, 15% probability of injecting `CDN_5XX` HTTP server errors on heartbeats.
- **Fault F3 (Device Application Bug & DRM Decoder Crash)**:
  - *Target Selector*: `device = "android"`, `app_version = "v2.1"`.
  - *Time Window*: Minute 55 to Minute 70 of replay.
  - *Impairment*: 35% probability of Video Start Failure (suppressing `first_frame` to trigger VSF timeout) and 20% probability of injecting `DECODE_ERR` and `DRM_ERR` fatal player exceptions.

Every injected fault writes its exact parameters to `data/manifests/fault_ledger_*.json` to serve as the objective ground-truth benchmark for evaluating alert precision and recall.

---

## 6. Data Cleaning and Preprocessing

### 6.1 The Preprocessing Pipeline (`src/qoe/data/preprocess.py`)
Raw Puffer telemetry is unsuited for direct streaming ingestion because client buffers are emitted at 100 ms intervals while server transmissions occur per chunk. The preprocessing pipeline harmonizes these streams into canonical player events:

```mermaid
flowchart TD
    CB[client_buffer CSV: 100ms Interval] --> ASOF[DuckDB ASOF Time Join]
    VS[video_sent CSV: Chunk Transmissions] --> ASOF
    VA[video_acked CSV: TCP RTT & Delivery] --> ASOF
    ASOF --> Clean[Canonical Timestamp & Millisecond Scaling]
    Clean --> Downsample[Downsample Heartbeats to 5000ms]
    Downsample --> Extract[Event Extraction: play_request, first_frame, stalls]
    Extract --> SimDims[Attach Deterministic Dimensions & Reference Keys]
    SimDims --> Output[(data/raw/unified_events.jsonl)]
```

#### Step-by-Step Transformations
1. **Time Scaling & Unit Conversion**: Raw Puffer nanosecond timestamps are scaled to Unix epoch milliseconds and ISO-8601 UTC strings. Buffer and rebuffering durations in seconds are multiplied by 1,000 to convert to integer milliseconds (`buffer_ms`, `stall_ms`).
2. **DuckDB ASOF Join**: Client buffer health is correlated with network transport metrics using DuckDB's high-performance `ASOF JOIN`. For each buffer record at time $T$, the engine matches the nearest preceding TCP acknowledgment record within a 2.0-second tolerance window:
   ```sql
   SELECT b.session_id, b.time, b.buffer * 1000 AS buffer_ms,
          a.delivery_rate / 1000 AS throughput_kbps, a.rtt / 1000 AS rtt_ms
   FROM client_buffer b
   ASOF JOIN video_acked a
     ON b.session_id = a.session_id AND b.time >= a.time
    AND b.time - a.time <= 2000000000;
   ```
3. **Heartbeat Downsampling**: 100 ms client buffer telemetry is downsampled to 5,000 ms (5-second) heartbeats. This replicates commercial player SDK behavior, reducing event volume by 50x while preserving buffer depletion dynamics.
4. **Lifecycle Event Extraction**:
   - `play_request`: Synthesized at index 0 representing viewer click.
   - `first_frame`: Synthesized at first observed non-zero buffer occupancy, computing `startup_ms`.
   - `buffer_start` & `buffer_end`: Extracted when cumulative rebuffering delta exceeds 50 ms.
   - `bitrate_switch`: Extracted when the video chunk format changes resolution.
   - `session_end`: Synthesized when the session terminates or reaches inactivity timeout.
5. **Canonical Serialization**: Extracted events are saved as newline-delimited JSON (`data/raw/unified_events.jsonl`) ready for replay into Kafka.
"""
