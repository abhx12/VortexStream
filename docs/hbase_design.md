# HBase Wide-Column Schema Design & Access Patterns

## 1. Overview & Architecture

VortexStream utilizes **Apache HBase** (wide-column NoSQL store) for hot-state storage and fast, low-latency analytical queries across two primary tables:
1. **`session_state`**: Low-latency point-lookups and point-updates for active player session lifecycles.
2. **`qoe_metrics`**: Aggregated 5-minute sliding window time-series for multi-grain QoE and network metrics.
3. **`alerts`**: High-priority alert state tracking.

---

## 2. Table Design & Column Families

### 2.1 Table: `session_state`
- **Purpose**: Tracks live streaming session state transitions (`requested`, `playing`, `stalled`, `ended`, `failed_start`) and running counters.
- **TTL**: 6 hours (`21600` seconds) to purge terminated sessions automatically.
- **Column Families**:
  - `playback`: Contains dynamic metrics (`state`, `last_bitrate`, `stall_count`, `stall_ms`, `startup_ms`, `watch_ms`, `last_event_ts`).
  - `context`: Contains static/enriched dimensions (`device`, `isp`, `cdn`, `city`, `channel`, `scheme`, `user_id`).
- **Key Design (Salting)**:
  - Format: `{salt:02d}-{session_id}`
  - Salt calculation: `crc32(session_id) % 16`
  - **Hotspot Avoidance**: Because session IDs arrive with arbitrary distributions and high write velocity, salting across 16 pre-split buckets (`00` through `15`) evenly distributes write loads across all HBase RegionServers, preventing single-region write hotspots.

### 2.2 Table: `qoe_metrics`
- **Purpose**: Stores sliding 5-minute QoE aggregates and network metrics exploded across 5 grains: `global`, `city_isp`, `cdn`, `device`, `channel`.
- **TTL**: 24 hours (`86400` seconds).
- **Column Families**:
  - `m`: Playback QoE metrics, additive counters, and composite QoE score (`qoe_score`, `qoe_label`, `rebuffer_ratio`, `avg_bitrate_kbps`, `hd_pct`, `vsf_rate`, `error_rate`, penalties). Written by Streaming Query Q2.
  - `n`: Network transport metrics (`avg_throughput_kbps`, `avg_rtt_ms`, `avg_packet_loss_pct`, `net_samples`). Written by Streaming Query Q3.
- **Key Design (Reversed Timestamp)**:
  - Format: `{grain}#{dimkey}#{rev_minute}`
  - `rev_minute = 9223372036854775807 - epoch_minute` (19-digit zero-padded string).
  - **Newest-First Ordering**: In HBase, row keys are sorted in ascending byte order. By subtracting the epoch minute from `Long.MAX_VALUE`, newer windows sort *before* older windows. A prefix scan with `LIMIT=1` immediately yields the most recent 5-minute window for that dimension without scanning historical rows.

---

## 3. HBase Shell Queries & Access Patterns

### 3.1 Point-Get Active Session State
Retrieve the current state and metrics for session `sess-12345-0` with salt `07`:
```ruby
get 'session_state', '07-sess-12345-0'
```

### 3.2 Latest 5-Minute QoE for City & ISP
Retrieve the most recent window for `Chennai|Airtel`:
```ruby
scan 'qoe_metrics', {
  ROWPREFIXFILTER => 'city_isp#Chennai|Airtel#',
  LIMIT => 1
}
```

### 3.3 Historical Window Range Scan
Scan the last 10 minutes of CDN performance for `Akamai`:
```ruby
scan 'qoe_metrics', {
  ROWPREFIXFILTER => 'cdn#Akamai#',
  LIMIT => 10,
  COLUMNS => ['m:qoe_score', 'm:rebuffer_ratio', 'n:avg_throughput_kbps']
}
```

### 3.4 Wide-Column Independent Evolution
Because column families `m` and `n` are stored in separate HFiles on disk:
- Updates from Q2 (playback) and Q3 (network) write to the same logical row key without read-modify-write locks or schema conflicts.
- Dashboards querying only network health (`n:avg_rtt_ms`) avoid reading playback metric blocks from disk, maximizing cache efficiency.
