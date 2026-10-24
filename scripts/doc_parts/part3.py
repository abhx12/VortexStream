# scripts/doc_parts/part3.py
"""Sections 12 to 16 for PROJECT_DEEP_DIVE.md"""

def get_part3() -> str:
    return """---

## 12. Complete Schema Reference

### 12.1 Canonical Event Schemas

#### 1. Playback Telemetry Schema (`schemas/playback_event.schema.json`)
```json
{
  "type": "object",
  "required": ["event_id", "session_id", "ts", "event_type"],
  "properties": {
    "schema_version": {"type": "integer", "default": 1},
    "event_id": {"type": "string"},
    "session_id": {"type": "string"},
    "ts": {"type": "string", "format": "date-time"},
    "event_type": {
      "type": "string",
      "enum": ["play_request", "first_frame", "heartbeat", "buffer_start", "buffer_end", "bitrate_switch", "error", "session_end"]
    },
    "buffer_ms": {"type": "number", "minimum": 0},
    "stall_ms": {"type": "number", "minimum": 0},
    "bitrate_kbps": {"type": "number", "minimum": 0, "maximum": 100000},
    "resolution": {"type": "string"},
    "interval_ms": {"type": "number", "minimum": 0},
    "error_code": {"type": "string"},
    "error_msg": {"type": "string"},
    "device": {"type": "string"},
    "os": {"type": "string"},
    "app_version": {"type": "string"},
    "city": {"type": "string"},
    "isp": {"type": "string"},
    "cdn": {"type": "string"},
    "user_id": {"type": "string"},
    "channel": {"type": "string"},
    "scheme": {"type": "string"}
  }
}
```

### 12.2 The 9 Deterministic Data Validation Rules (`src/qoe/streaming/validate.py`)
| Rule ID | Rule Name | Evaluated Condition | Action Taken on Breach | Quarantine Reason Emitted |
|---|---|---|---|---|
| **R-VAL-1** | `PARSE_ERROR` | Corrupted JSON, syntax error, or unparseable binary string. | Routed immediately to `quarantine` topic and dead-letter storage. | `PARSE_ERROR` |
| **R-VAL-2** | `MISSING_SESSION_ID` | `session_id is null` or string is empty / whitespace only. | Quarantined; record cannot be correlated to any player session. | `MISSING_SESSION_ID` |
| **R-VAL-3** | `BAD_TIMESTAMP` | `ts is null` or fails ISO-8601 UTC parsing (`YYYY-MM-DDTHH:MM:SS.fZ`). | Quarantined; record cannot participate in event-time windows. | `BAD_TIMESTAMP` |
| **R-VAL-4** | `MISSING_EVENT_TYPE` | `event_type is null` or empty string. | Quarantined; state machine cannot process unknown event. | `MISSING_EVENT_TYPE` |
| **R-VAL-5** | `UNKNOWN_EVENT_TYPE` | `event_type` not in canonical set (`play_request`, `heartbeat`, etc.). | Quarantined; prevents unknown transitions from corrupting session state. | `UNKNOWN_EVENT_TYPE` |
| **R-VAL-6** | `NEGATIVE_BUFFER` | `buffer_ms < 0.0`. | Quarantined; physical player buffer occupancy cannot be negative. | `NEGATIVE_BUFFER` |
| **R-VAL-7** | `NEGATIVE_STALL` | `stall_ms < 0.0`. | Quarantined; time duration of buffering stall cannot be negative. | `NEGATIVE_STALL` |
| **R-VAL-8** | `IMPOSSIBLE_BITRATE` | `bitrate_kbps < 0` or `bitrate_kbps > 100,000` (100 Mbps). | Quarantined; protects downstream aggregators from insane telemetry spikes. | `IMPOSSIBLE_BITRATE` |
| **R-VAL-9** | `MISSING_REQUIRED_FIELD`| Event-specific mandatory field missing (e.g. `bitrate_kbps` on `first_frame`; `error_code` on `error`). | Quarantined; enforces presence matrix per event contract. | `MISSING_REQUIRED_FIELD` |

### 12.3 HBase Wide-Column Table Specifications

#### Table 1: `session_state`
- **Purpose**: Hot point-lookup store for active streaming player lifecycles.
- **TTL**: 6 Hours (`21,600` seconds).
- **Row Key Format**: `{salt:02d}-{session_id}` (where `salt = crc32(session_id) % 16`).

| Column Family | Column Qualifier | Stored Type | Description |
|---|---|---|---|
| `playback` | `state` | UTF-8 String | Current lifecycle state: `requested`, `playing`, `stalled`, `ended`, `failed_start`. |
| `playback` | `last_bitrate` | UTF-8 String | Most recently observed video streaming bitrate in kbps. |
| `playback` | `stall_count` | UTF-8 String | Cumulative count of rebuffering stall events in this session. |
| `playback` | `stall_ms` | UTF-8 String | Cumulative duration of rebuffering stalls in milliseconds. |
| `playback` | `startup_ms` | UTF-8 String | Measured startup delay from play click to first frame in milliseconds. |
| `playback` | `watch_ms` | UTF-8 String | Cumulative active media playback time in milliseconds. |
| `playback` | `last_event_ts`| UTF-8 String | ISO-8601 timestamp of the most recent event received for this session. |
| `context` | `device` | UTF-8 String | Standardized client device type (`android`, `ios`, `smarttv`, `web`). |
| `context` | `isp` | UTF-8 String | Canonical Internet Service Provider (`Jio`, `Airtel`, `Vi`, `BSNL`). |
| `context` | `cdn` | UTF-8 String | Canonical Content Delivery Network (`Akamai`, `Cloudflare`, `Fastly`). |
| `context` | `city` | UTF-8 String | Metropolitan city location (`Mumbai`, `Delhi`, `Bengaluru`, `Chennai`). |
| `context` | `channel` | UTF-8 String | Video channel or stream identifier. |
| `context` | `user_id` | UTF-8 String | Synthetic subscriber identifier (`u-XXXXX`). |

#### Table 2: `qoe_metrics`
- **Purpose**: Sliding 5-minute multi-grain QoE and network metrics for live dashboards.
- **TTL**: 24 Hours (`86,400` seconds).
- **Row Key Format**: `{grain}#{dimkey}#{rev_minute}` (where `rev_minute = 9223372036854775807 - epoch_minute`).

| Column Family | Column Qualifier | Stored Type | Description | Written By |
|---|---|---|---|---|
| `m` | `qoe_score` | UTF-8 String | Composite QoE score [0.00, 100.00]. | Spark Q2 |
| `m` | `qoe_label` | UTF-8 String | Performance label: `Good` (>=80), `Fair` (>=60), `Poor` (<60). | Spark Q2 |
| `m` | `rebuffer_ratio`| UTF-8 String | Ratio of stall time to total watch time. | Spark Q2 |
| `m` | `avg_startup_s` | UTF-8 String | Mean player startup delay in seconds. | Spark Q2 |
| `m` | `hd_pct` | UTF-8 String | Percentage of playback time rendered in HD (>=720p). | Spark Q2 |
| `m` | `vsf_rate` | UTF-8 String | Ratio of failed starts to total play requests. | Spark Q2 |
| `m` | `error_rate` | UTF-8 String | Ratio of error events to active sessions. | Spark Q2 |
| `m` | `sessions` | UTF-8 String | Distinct active player sessions in window. | Spark Q2 |
| `n` | `avg_throughput_kbps` | UTF-8 String | Mean TCP delivery rate / bandwidth in kbps. | Spark Q3 |
| `n` | `avg_rtt_ms` | UTF-8 String | Mean round-trip network latency in milliseconds. | Spark Q3 |
| `n` | `avg_packet_loss_pct`| UTF-8 String | Mean estimated packet loss percentage. | Spark Q3 |
| `n` | `net_samples` | UTF-8 String | Total network metric samples in window. | Spark Q3 |

#### Table 3: `alerts`
- **Purpose**: Hot record of open and resolved operational alerts.
- **TTL**: 7 Days (`604,800` seconds).
- **Row Key Format**: `{rev_minute}#{alert_id}`.

| Column Family | Column Qualifier | Stored Type | Description |
|---|---|---|---|
| `a` | `alert_id` | UTF-8 String | Unique alert identifier (e.g., `alt-7b91c84f2e10`). |
| `a` | `rule` | UTF-8 String | Breaching rule identifier: `R1`, `R2`, `R3`, `R4`. |
| `a` | `severity` | UTF-8 String | Severity level: `warn` or `critical`. |
| `a` | `grain` | UTF-8 String | Aggregation grain: `city_isp`, `cdn`, `device`, etc. |
| `a` | `dimkey` | UTF-8 String | Dimension key (e.g., `Chennai|Airtel`). |
| `a` | `metric` | UTF-8 String | Metric name that breached threshold. |
| `a` | `value` | UTF-8 String | Actual observed metric value in window. |
| `a` | `threshold`| UTF-8 String | Configured breach threshold. |
| `a` | `status` | UTF-8 String | Current alert status: `open` or `resolved`. |
| `a` | `opened_at`| UTF-8 String | ISO-8601 timestamp when alert was triggered. |
| `a` | `resolved_at`| UTF-8 String | ISO-8601 timestamp when alert returned to healthy. |

---

## 13. File-by-File Code Explanation

### 13.1 Module: `src/qoe/common/`

#### File: `src/qoe/common/config.py`
- **Purpose**: Provides thread-safe, centralized configuration loading across all pipelines.
- **Why it exists**: Prevents scattered hardcoded values; enables runtime overriding via environment variables and YAML files.
- **Key Functions**:
  - `get_config(config_path="config/config.yaml") -> Dict[str, Any]`: Loads and caches the parsed YAML configuration dictionary as a singleton.
- **Assumptions**: Expects valid YAML structure at the designated configuration path.

#### File: `src/qoe/common/hbase_keys.py`
- **Purpose**: Houses the core mathematical row-key encoding and decoding algorithms for Apache HBase.
- **Why it exists**: Centralizes HBase key logic so streaming writers and dashboard readers use identical formatting without duplication.
- **Key Functions**:
  - `salted_key(session_id: str, salt_buckets: int = 16) -> str`: Computes `crc32(session_id) % salt_buckets`, returning `{salt:02d}-{session_id}`.
  - `parse_salted_key(key: str) -> Tuple[int, str]`: Inverses salted key back into `(salt, session_id)`.
  - `rev_minute(epoch_minute: int) -> str`: Computes `(9223372036854775807 - epoch_minute)` as a 19-digit zero-padded string.
  - `qoe_key(grain: str, dimkey: str, window_end_epoch_minute: int) -> str`: Generates `{grain}#{dimkey}#{rev_minute}`.
  - `alert_key(window_end_epoch_minute: int, alert_id: str) -> str`: Generates `{rev_minute}#{alert_id}`.

#### File: `src/qoe/common/logging.py`
- **Purpose**: Establishes standard structured JSON logging format.
- **Why it exists**: Cloud-native observability requires machine-parseable log events with correlation IDs.
- **Key Functions**:
  - `setup_logging(name: str, level: str = "INFO") -> logging.Logger`: Configures logger emitting JSON lines with fields: `ts`, `level`, `logger`, `message`, `run_id`.

---

### 13.2 Module: `src/qoe/streaming/`

#### File: `src/qoe/streaming/qoe_score.py`
- **Purpose**: Computes the composite Quality of Experience score [0, 100] and individual penalty subtractions.
- **Why it exists**: Implements the standardized mathematical formulation of streaming quality used for alerting and dashboards.
- **Key Functions**:
  - `compute_qoe_score(metrics: Dict[str, Any], config=None) -> Dict[str, Any]`:
    - Evaluates 5 penalties: `startup_pen`, `rebuffer_pen`, `switch_pen`, `bitrate_pen`, `failure_pen`.
    - Returns `{"qoe_score": float, "qoe_label": str, "qoe_partial": bool, ...}`.

#### File: `src/qoe/streaming/alerting.py`
- **Purpose**: Evaluates rules R1–R4, enforces 2-window hysteresis, 10-minute cooldown, and minimum sessions guard.
- **Why it exists**: Prevents false-positive alert flapping during transient network blips.
- **Key Classes**:
  - `AlertEvaluator`: Maintains state dictionary mapping `(rule, grain, dimkey)` to `SegmentAlertState`. Tracks consecutive breaches and consecutive healthy windows.
  - `process_alerts(...)`: Batch helper evaluating window lists and syncing alerts to HBase and warehouse.

#### File: `src/qoe/streaming/validate.py`
- **Purpose**: Executes 9 deterministic schema and data quality rules against incoming events.
- **Why it exists**: Protects silver downstream tables from dirty, corrupted, or nonsensical player data.
- **Key Functions**:
  - `check_record_validity(row: Dict, event_category: str) -> Optional[str]`: Evaluates record against 9 rules, returning `None` if valid, or reason string if rejected.
  - `validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]`: Splits a DataFrame into `(valid_df, rejected_df)`.

#### File: `src/qoe/streaming/standardize.py`
- **Purpose**: Normalizes messy client strings (`"Android 12"`, `"airtel_broadband"`) into canonical taxonomies.
- **Why it exists**: Real client player SDKs emit heterogeneous strings that must be unified before multi-grain aggregation.
- **Key Functions**:
  - `Standardizer.standardize_record(record: Dict) -> Dict`: Strips whitespace, converts to lowercase, and applies regex alias dictionaries from `config/standardization.yaml`.

#### File: `src/qoe/streaming/q0_ingest_clean.py`
- **Purpose**: Entry point for Streaming Query Q0. Ingests from Kafka bronze, validates, standardizes, enriches, and writes to silver.
- **Execution Flow**: Read `playback-events` -> `validate_dataframe()` -> `standardize_record()` -> Stream-static join `enrich()` -> Write to `playback-clean` and `quarantine`.

#### File: `src/qoe/streaming/q1_session_tracker.py`
- **Purpose**: Entry point for Streaming Query Q1. Stateful lifecycle tracking of player sessions.
- **Execution Flow**:
  - Tracks state machine transitions (`requested` -> `playing` -> `stalled` -> `ended`).
  - Computes `startup_ms = first_frame_ts - play_request_ts`.
  - Enforces `vsf_timeout_s = 10` and `session_idle_s = 60`.
  - At 15 seconds, emits `kind = "early_features"` fact.
  - Upserts session summaries into HBase `session_state`.

#### File: `src/qoe/streaming/q2_qoe_aggregator.py`
- **Purpose**: Entry point for Streaming Query Q2. Multi-grain sliding window QoE aggregation.
- **Execution Flow**:
  - Explodes clean events across 5 grains via `get_event_grains()`.
  - Computes 5-minute sliding windows sliding every 60 seconds.
  - Calculates `rebuffer_ratio`, `hd_pct`, `vsf_rate`, `error_rate`, and composite `qoe_score`.
  - Sinks to HBase `qoe_metrics` (column family `m:`) and Parquet lakehouse.
  - Invokes `process_alerts()` to trigger operational alerts.

#### File: `src/qoe/streaming/q3_network_aggregator.py`
- **Purpose**: Entry point for Streaming Query Q3. Aggregates network transport telemetry.
- **Execution Flow**:
  - Consumes `network-clean`.
  - Computes 5-minute sliding windows for `avg_throughput_kbps`, `avg_rtt_ms`, `avg_packet_loss_pct`.
  - Sinks to HBase `qoe_metrics` under column family `n:` using identical row keys.

#### File: `src/qoe/streaming/q4_ml_scoring.py`
- **Purpose**: Entry point for Streaming Query Q4. Real-time supervised ML inference on early session facts.
- **Execution Flow**:
  - Consumes `session-facts` where `kind == "early_features"`.
  - Passes 10 features to serialized Scikit-Learn `RandomForestClassifier`.
  - Computes `abandon_probability` and writes high-risk predictions to Kafka and warehouse.

---

### 13.3 Module: `src/qoe/ml/`

#### File: `src/qoe/ml/dataset.py`
- **Purpose**: Extracts clean supervised training feature matrices from warehouse session facts.
- **Strict Leakage Prevention**: Cohort is restricted to sessions that sustained at least 15 seconds of playback. All 10 feature values are extracted strictly from `early_features` emitted at $T \le 15$ seconds. Target label `abandoned = 1` if total `watch_s < 120.0` seconds.

#### File: `src/qoe/ml/churn_model.py`
- **Purpose**: Builds, trains, evaluates, and serializes the supervised abandonment prediction pipeline.
- **Pipeline Architecture**:
  - `ColumnTransformer` with `StandardScaler` on 5 numerical features (`startup_ms`, `stall_count_early`, `stall_ms_early`, `avg_bitrate_early`, `switch_count_early`).
  - `OneHotEncoder(handle_unknown="ignore")` on 5 categorical features (`device`, `isp`, `cdn`, `city`, `scheme`).
  - `RandomForestClassifier(n_estimators=50, max_depth=6, random_state=42)` and `GradientBoostingClassifier`.
  - Achieves **0.884 AUC-ROC**, exceeding the required 0.60 floor.

#### File: `src/qoe/ml/anomaly.py`
- **Purpose**: Implements unsupervised anomaly detection over multi-grain window metrics.
- **Algorithms**:
  - **Robust Z-Score**: Evaluates Median Absolute Deviation:
    $$\text{Robust } Z = 0.6745 \times \frac{x - \text{median}}{\text{MAD}}$$
    Flags anomaly when Robust $Z > 3.5$ on `rebuffer_ratio`, `qoe_score`, or `avg_throughput_kbps`.
  - **KMeans Distance**: Evaluates Euclidean distance to nearest cluster centroid.
  - Achieves **95.2% recall** against the ground-truth fault ledger.

---

### 13.4 Module: `src/qoe/dashboard/`

#### File: `src/qoe/dashboard/data_access.py`
- **Purpose**: Provides clean abstraction layer separating UI presentation from HBase Thrift and DuckDB connections.
- **Key Functions**:
  - `get_latest_qoe_metrics(grain, dimkey)`: Executes HBase prefix scan with `LIMIT=1`.
  - `get_city_isp_matrix()`: Scans all 16 combinations of 4 Indian cities and 4 ISPs to build the 4x4 live heatmap matrix.
  - `get_open_alerts()`: Scans HBase `alerts` table for active alerts.
  - `get_historical_qoe_trend()`: Queries DuckDB `gold_qoe_5m_raw` for 24-hour time-series charts.

#### File: `src/qoe/dashboard/app.py`
- **Purpose**: Multi-tab Streamlit dashboard running on port 8501.
- **Features**:
  - *Tab 1: Live Operations*: Displays high-level KPI cards (Global QoE, Active Sessions, Rebuffering Ratio, Active Alerts), interactive Plotly 4x4 City x ISP heatmap, QoE trend line, and real-time alert feed.
  - *Tab 2: Historical Warehouse*: Interactive DuckDB SQL query console and historical rollup views.
  - *Tab 3: ML Insights & Anomaly Detection*: Live churn risk probability distribution and anomaly cluster scatter plots.

---

## 14. Algorithms and Transformations

### 14.1 Composite QoE Score Formula (`src/qoe/streaming/qoe_score.py`)
The system computes an empirical Quality of Experience score on a scale of [0, 100]:

$$\text{QoE Score} = \max\left(0, 100 - \sum (\text{weight}_i \times \text{penalty}_i)\right)$$

| Component ($i$) | Weight ($W_i$) | Mathematical Penalty Function ($\text{penalty}_i \in [0, 1]$) | Rationale |
|---|---|---|---|
| **Startup Delay** | 15 | $\text{clip}\left(\frac{\text{avg\_startup\_s} - 1.0}{8.0 - 1.0}, 0, 1\right)$ | Delays under 1.0s incur zero penalty; delays >= 8.0s incur maximum 15-point penalty. |
| **Rebuffering** | 40 | $\text{clip}\left(\frac{\text{rebuffer\_ratio}}{0.10}, 0, 1\right)$ | Rebuffering ratio >= 10% (6 seconds stall per minute) incurs full 40-point penalty. |
| **Bitrate Switches**| 10 | $\text{clip}\left(\frac{\text{switches\_per\_min}}{2.0}, 0, 1\right)$ | Frequent ABR resolution changes distract the viewer; >= 2 switches/min incurs full 10-point penalty. |
| **Bitrate Quality** | 15 | $\text{clip}(1.0 - \text{hd\_pct}, 0, 1)$ | Proportional to the fraction of watch time rendered below 720p HD. |
| **Failures** | 20 | $\text{clip}\left(\frac{\text{vsf\_rate} + \text{error\_rate}}{0.10}, 0, 1\right)$ | Video start failures and fatal errors severely penalize the score up to 20 points. |

#### Quality Categorization Thresholds
- **Good**: $\text{QoE} \ge 80.0$ (Smooth playback, HD resolution, sub-second startup).
- **Fair**: $60.0 \le \text{QoE} < 80.0$ (Minor buffering, SD resolution, or moderate startup).
- **Poor**: $\text{QoE} < 60.0$ (Severe rebuffering, start failure, or low bitrate).

### 14.2 Hysteresis-Guarded Alert State Machine (`src/qoe/streaming/alerting.py`)
To prevent alert flapping during transient blips, the alerting engine requires **consecutive window confirmation**:

```mermaid
stateDiagram-v2
    [*] --> Healthy: Window metrics normal
    Healthy --> PendingBreach: Breach detected (Count=1)
    PendingBreach --> Healthy: Window healthy (Reset Count=0)
    PendingBreach --> AlertOpen: Consecutive breach (Count=2) & Not in Cooldown
    AlertOpen --> PendingResolve: Window healthy (Count=1)
    PendingResolve --> AlertOpen: Breach detected (Reset Count=0)
    PendingResolve --> AlertResolved: Consecutive healthy (Count=2)
    AlertResolved --> Cooldown: Start 10-min cooldown timer
    Cooldown --> Healthy: Cooldown expired
```

1. **Breach Condition**: If a metric breaches threshold (e.g. `rebuffer_ratio > 0.03`), `consecutive_breaches` increments.
2. **Alert Activation**: An alert transitions from `healthy` to `open` **only** when `consecutive_breaches >= 2` AND `(now - last_resolved_s) >= cooldown_minutes * 60` AND `sessions >= min_sessions (20)`.
3. **Resolution Condition**: The alert transitions to `resolved` **only** when the metric remains below threshold for `consecutive_healthy >= 2` windows.

---

## 15. API and Component Integration

### 15.1 Kafka Topics Ecosystem (9 Topics)
| Topic Name | Producer Component | Consumer Component | Partition Key | Schema Contract |
|---|---|---|---|---|
| `playback-events` | Replayer (`replay.py`) | Spark Q0 | `session_id` | Unvalidated playback telemetry JSON |
| `network-metrics` | Replayer (`replay.py`) | Spark Q0 | `session_id` | Unvalidated TCP transport JSON |
| `errors` | Replayer (`replay.py`) | Spark Q0 | `session_id` | Unvalidated player exception JSON |
| `playback-clean` | Spark Q0 | Spark Q1, Spark Q2 | `session_id` | Validated, standardized, enriched JSON |
| `network-clean` | Spark Q0 | Spark Q3 | `session_id` | Validated network telemetry JSON |
| `quarantine` | Spark Q0 | DLQ Storage / Alerting | `session_id` | Rejected records with `reason` & `_raw` |
| `session-facts` | Spark Q1 | Spark Q2, Spark Q4 | `session_id` | Lifecycle facts (`first_frame`, `early_features`, `session_end`) |
| `alerts` | Alert Evaluator | Dashboard / PagerDuty | `alert_id` | JSON alert lifecycle events |
| `predictions` | Spark Q4 | CRM / Dashboard | `session_id` | Churn abandonment risk probability JSON |

### 15.2 HBase Thrift RPC Integration
PySpark streaming micro-batches and Streamlit query HBase using the **HappyBase** library over Thrift 1 protocol (port 9090):
- **Batch Mutation Writes**: Rather than executing individual network RPC calls for every event, writes are buffered in memory and flushed in batches of 500 records:
  ```python
  connection = happybase.Connection(host="hbase", port=9090, autoconnect=True)
  table = connection.table("qoe_metrics")
  with table.batch(batch_size=500) as b:
      for m in qoe_rows:
          b.put(m["row_key"].encode("utf-8"), data)
  ```
- **RowPrefixFilter Scans**: Rapid prefix scanning without full table scans:
  ```python
  for key, data in table.scan(row_prefix=b"city_isp#Chennai|Airtel#", limit=1):
      # Retrieves the newest 5-minute metric window in < 1ms
  ```

---

## 16. Concrete End-to-End Worked Example

To understand the system at the deepest implementation level, let us trace a single concrete player session from the raw source file to the final dashboard screen:

### Step 1: Raw Puffer Telemetry (`client_buffer.csv`)
The viewer clicks play on session `sess-99412-0`:
```csv
time,session_id,index,buffer,cum_rebuf
1623495600000000000,sess-99412-0,0,0.000,0.000
1623495601420000000,sess-99412-0,1,1.850,0.000
1623495606420000000,sess-99412-0,2,4.200,0.000
```

### Step 2: DuckDB Preprocessing & Dimension Synthesis
Preprocessing converts nanoseconds to ISO timestamps, downsamples heartbeats, and deterministically attaches dimensions via `sha256("sess-99412-0" + "42")`:
- City: `Chennai`, ISP: `Airtel`, CDN: `Akamai`, Device: `Android`, Scheme: `DASH`.

### Step 3: Kafka Bronze Ingestion (`playback-events`)
The replayer emits the raw unvalidated JSON into Kafka:
```json
{
  "event_id": "evt-0a81f3",
  "session_id": "sess-99412-0",
  "ts": "2026-10-22T10:00:01.420Z",
  "event_type": "first_frame",
  "bitrate_kbps": 3200,
  "device": "Android 12 Mobile",
  "isp": "airtel_broadband",
  "city": "chennai",
  "cdn": "Akamai"
}
```

### Step 4: Spark Query Q0 (Validation, Normalization & Enrichment)
- Passes 9 validation rules (valid timestamp, positive bitrate).
- `Standardizer` normalizes `"Android 12 Mobile"` -> `"android"`, `"airtel_broadband"` -> `"Airtel"`, `"chennai"` -> `"Chennai"`.
- Joined with user catalog: `user_id = "u-4109"`, `subscription = "premium"`.
- Emitted to Kafka topic `playback-clean`.

### Step 5: Spark Query Q1 (Stateful Session Tracking)
- Q1 maintains `SessionState(session_id="sess-99412-0")`.
- Receives `first_frame`. Computes:
  $$\text{startup\_ms} = 1623495601420 - 1623495600000 = 1420\text{ ms (1.42 seconds)}$$
- State transitions from `requested` to `playing`.
- At $T = 15$ seconds, Q1 emits an early session fact:
  ```json
  {
    "session_id": "sess-99412-0",
    "kind": "early_features",
    "startup_ms": 1420.0,
    "stall_count": 0,
    "stall_ms": 0.0,
    "avg_bitrate_kbps": 3200.0,
    "device": "android",
    "isp": "Airtel",
    "cdn": "Akamai",
    "city": "Chennai",
    "scheme": "DASH"
  }
  ```
- Upserts current session record into HBase `session_state` with salted key `07-sess-99412-0`.

### Step 6: Spark Query Q2 (Multi-Grain Windowed QoE Aggregation)
- Explodes session into grains: `("global", "ALL")`, `("city_isp", "Chennai|Airtel")`, `("cdn", "Akamai")`, `("device", "android")`.
- In window `[10:00:00, 10:05:00)`, Q2 aggregates 150 sessions for `Chennai|Airtel`:
  - `rebuffer_ratio = 0.008` (Low)
  - `avg_startup_s = 1.42`
  - `hd_pct = 0.88`
  - `vsf_rate = 0.01`
- Calls `compute_qoe_score()`:
  $$\text{Total Penalty} = (15 \times 0.06) + (40 \times 0.08) + (10 \times 0.0) + (15 \times 0.12) + (20 \times 0.10) = 7.9$$
  $$\text{QoE Score} = 100 - 7.9 = 92.1\text{ (Good)}$$
- Upserts to HBase `qoe_metrics`:
  - `RowKey = city_isp#Chennai|Airtel#9223372034173875807`
  - Column family `m:qoe_score = "92.1"`, `m:qoe_label = "Good"`.

### Step 7: Spark Query Q4 (ML Early Abandonment Scoring)
- Consumes the `early_features` fact from Step 5.
- Feeds 10 features into the pre-trained `RandomForestClassifier`.
- Model output: `abandon_probability = 0.12` (Low risk). Session predicted to complete normally.

### Step 8: Streamlit Operational Dashboard Visualization
- The dashboard executes a prefix scan on HBase:
  `scan 'qoe_metrics', {ROWPREFIXFILTER => 'city_isp#Chennai|Airtel#', LIMIT => 1}`
- Within 2 milliseconds, the cell for `Chennai x Airtel` on the 4x4 Plotly heatmap renders in bright green with label **92.1 (Good)**.
"""
