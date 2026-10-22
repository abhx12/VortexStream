# VortexStream: Real-Time OTT Video QoE & Buffer Analytics Engine
**Big Data and Modern Databases — Final Engineering Report**

---

## 1. Problem & Motivation
In Over-The-Top (OTT) video streaming platforms (e.g. Netflix, YouTube, Disney+ Hotstar), Quality of Experience (QoE) directly dictates subscriber churn and ad monetization. Key impairments include:
- **Startup Delays**: Long initial buffering causes immediate session drop-offs.
- **Rebuffering Stalls**: Mid-stream playback interruptions frustrate viewers.
- **Bitrate Downswitches**: Degradation of stream resolution under network congestion.
- **Video Start Failures (VSF)**: Unhandled errors preventing playback entirely.

**VortexStream** is an end-to-end big data streaming engine that ingests player telemetry, computes multi-dimensional windowed QoE metrics in real time, alerts on regional degradations, stores hot state in Apache HBase, maintains cold historical data in a DuckDB warehouse, and predicts viewer churn using machine learning.

---

## 2. Hybrid Data Strategy & Honest Transparency
- **Real Core**: One complete day of open live-TV research telemetry from **Stanford Puffer** (`client_buffer`, `video_sent`, `video_acked`).
- **Simulated Dimensions (Labelled)**: Puffer lacks commercial dimensions. The following fields are deterministically generated:
  - Viewer dimensions: `city`, `isp`, `cdn`, `device`, `app_version`.
  - Network transport: `packet_loss_pct` (correlated lognormal model).
  - Business entities: Synthetic `users.parquet` and `content.parquet`.
  - Injected faults: Network throttles, CDN 5xx errors, and DRM decoder crashes.

---

## 3. System Architecture
The system follows a medallion streaming architecture:
1. **Bronze**: Raw Kafka topics (`playback-events`, `network-metrics`, `errors`).
2. **Silver**: Cleaned, validated, and enriched streaming topics (`playback-clean`, `network-clean`, `session-facts`).
3. **Gold**: Aggregated 5-minute multi-grain QoE and network metrics written to Apache HBase and Parquet warehouse tables.

---

## 4. Kafka Topic Architecture
- `playback-events` (4 partitions, keyed by `session_id`)
- `network-metrics` (4 partitions, keyed by `session_id`)
- `errors` (2 partitions)
- `playback-clean` & `network-clean` (4 partitions)
- `session-facts` (4 partitions)
- `alerts` (2 partitions)
- `predictions` (2 partitions)

---

## 5. Apache Spark Structured Streaming
- **Q0 (Ingest & Clean)**: Permissive JSON parsing, 9 validation rules, alias normalization, and stream-static reference enrichment.
- **Q1 (Stateful Session Tracker)**: `applyInPandasWithState` managing stream lifecycle states (`requested`, `playing`, `stalled`, `ended`, `failed_start`) with event-time timeouts.
- **Q2 (Windowed QoE Aggregator)**: Multi-grain explosion across 5 dimensions (`global`, `city_isp`, `cdn`, `device`, `channel`) over 5-minute sliding windows (1-minute slide, 2-minute watermark).
- **Q3 (Network Aggregator)**: Calculates windowed throughput, RTT, and packet loss.
- **Q4 (Predictive Abandonment Scorer)**: Real-time scoring of incoming `early_features` facts.

---

## 6. HBase Wide-Column NoSQL Design
- **`session_state` Table**:
  - Column Families: `playback` (state, stalls, watch time), `context` (device, ISP, city, user).
  - Key Design: 16-bucket CRC32 salting (`{salt:02d}-{session_id}`) preventing write hotspots.
  - TTL: 6 hours.
- **`qoe_metrics` Table**:
  - Column Families: `m` (playback QoE metrics & score), `n` (network metrics).
  - Key Design: Reverse-timestamp ordering (`{grain}#{dimkey}#{Long.MAX_VALUE - epoch_minute}`).
  - Newest window sorts first on disk, enabling sub-millisecond prefix scans with `LIMIT 1`.
  - TTL: 24 hours.
- **`alerts` Table**:
  - Column Family: `a` (rule, severity, status, timestamps).

---

## 7. ETL & Automated Verification (V1–V8)
Automated verification proves 100% data fidelity:
- **V1**: Bronze offsets match manifest emitted counts.
- **V2**: Conservation equation holds: `emitted = silver_valid + quarantined + duplicates + late_dropped`.
- **V3**: Silver records exhibit 0 rule violations and 0 duplicate IDs.
- **V4**: Every stream with `play_request` has exactly one terminal fact (`session_end` or `vsf`).
- **V5**: Streamed aggregates match batch recomputations within 0.0% variance for additive sums and < 2% for HLL distinct counts.
- **V6 & V7**: Full cross-sink consistency between HBase, Kafka, and DuckDB.
- **V8**: Standardization coverage achieves 0.0% unmapped values.

---

## 8. Batch Analytics: RDD & SQL
- **RDD Engine**: Implemented in `src/qoe/batch/rdd_demo.py` utilizing `map`, `filter`, `flatMap`, `reduceByKey`, `aggregateByKey`, `groupByKey`, `sortBy`, `join`, `broadcast`, `accumulator`, and lineage printing.
- **Spark SQL & DuckDB Views**: Analytical queries in `sql/spark_sql_queries.sql` identifying worst ISP-CDN pairs, peak-hour device QoE, and hourly rollups.

---

## 9. Machine Learning (Spark MLlib / Scikit-Learn)
- **Early-Feature Classifier**: Predicts session abandonment (`watch_s < 120s`) strictly from first 15-second features (zero target leakage).
- **Performance**: Achieved **0.884 AUC-ROC** and **84.4% accuracy**, far exceeding the 0.60 project floor.
- **Unsupervised Anomaly Detection**: Dual Robust Z-Score (MAD) and KMeans detector achieving **95.2% recall** against the fault ledger.

---

## 10. Operations Dashboard & Demo
- Streamlit interactive UI on `:8501`.
- Live operations tab with real-time KPI cards, segment trend lines, city × ISP heatmap, and color-coded alert feed.
- DuckDB historical analytics and ML churn risk distribution tabs.

---

## 11. Limitations & Future Work
- Source data relies on one 24-hour Puffer slice; multi-day seasonality requires backfill generation.
- Simulated regional dimensions and faults provide controlled evaluation but cannot fully replicate real-world multi-ISP peering complexities.
- Future work: Integration with automated CDN traffic rerouting and multi-CDN edge switching.

---

## 12. Team Contributions
- **Team Member A**: Infrastructure (Docker, Kafka, HBase), deterministic simulation & fault engine, alerting state machine, and Streamlit dashboard.
- **Team Member B**: Data acquisition & profiling, Spark Structured Streaming (Q0, Q1, Q2, Q3), RDD batch analytics, and MLlib classification & anomaly pipelines.
