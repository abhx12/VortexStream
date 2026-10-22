# Complete ETL Source-to-Target Mapping Specification

## 1. Stanford Puffer raw fields to Canonical Playback Events

| Source File | Source Field | Transformation / Type | Target Canonical Field | Target Topic / Table |
|---|---|---|---|---|
| `client_buffer` | `session_id`, `index` | `derive` (`session_id || '-' || index`) | `session_id` | `playback-events` |
| `client_buffer` | `session_id` | `derive` (Preserved for dim hashing) | `puffer_session_id` | internal / join key |
| `client_buffer` | `time` | `standardize` (`epoch_ms(time / 1e6)` -> ISO-8601 UTC Z) | `ts` | `playback-events` |
| `client_buffer` | `event` | `standardize` (`init`->`play_request`, `startup`->`first_frame`, `rebuffer`->`buffer_start`, `play`->`buffer_end`, `timer`->`heartbeat`) | `event_type` | `playback-events` |
| `client_buffer` | `channel` | `standardize` (String copy) | `video_id` | `playback-events` |
| `client_buffer` | `buffer` | `derive` (`buffer * 1000.0` to ms) | `buffer_ms` | `playback-events` |
| `client_buffer` | `cum_rebuf` | `derive` (`(cum_rebuf - prev_cum_rebuf) * 1000.0`) | `stall_ms` | `playback-events` |
| `client_buffer` | `timer` cadence | `derive` (Downsampled interval ms) | `interval_ms` | `playback-events` |
| `video_sent` | `format` | `derive` (Extracted height regex `<H>p`) | `resolution` | `playback-events` |
| `video_sent` | `size` | `derive` (`size * 8.0 / 2.002 / 1000.0`) | `bitrate_kbps` | `playback-events` |
| `video_sent` | `ssim_index` | `validate` (Float in [0.0, 1.0]) | `ssim_index` | `playback-events` |
| `expt_settings` | `expt_id` | `enrich` (`expt_id` -> ABR scheme) | `scheme` | `playback-events` |

---

## 2. Stanford Puffer `video_sent` to Canonical Network Metrics

| Source File | Source Field | Transformation / Type | Target Canonical Field | Target Topic / Table |
|---|---|---|---|---|
| `video_sent` | `session_id`, `index` | `derive` (`session_id || '-' || index`) | `session_id` | `network-metrics` |
| `video_sent` | `time` | `standardize` (`epoch_ms(time / 1e6)` -> ISO-8601 UTC) | `ts` | `network-metrics` |
| `video_sent` | `delivery_rate` | `derive` (`delivery_rate * 8.0 / 1000.0`) | `throughput_kbps` | `network-metrics` |
| `video_sent` | `rtt` | `derive` (`rtt / 1000.0` microseconds to ms) | `rtt_ms` | `network-metrics` |
| `video_sent` | `min_rtt` | `derive` (`min_rtt / 1000.0` to ms) | `min_rtt_ms` | `network-metrics` |
| `video_sent` | `cwnd` | `validate` (Integer copy) | `cwnd` | `network-metrics` |
| `video_sent` | `in_flight` | `validate` (Integer copy) | `in_flight` | `network-metrics` |
| Simulated | Lognormal model | `derive` (Correlated with RTT and throughput) | `packet_loss_pct` | `network-metrics` |

---

## 3. Simulated Dimensions & Reference Sources to Canonical Fields

| Source | Field | Transformation / Type | Target Canonical Field |
|---|---|---|---|
| Deterministic Dim Generator | City distribution | `derive` / `standardize` | `city` |
| Deterministic Dim Generator | ISP distribution | `derive` / `standardize` | `isp` |
| Deterministic Dim Generator | CDN distribution | `derive` / `standardize` | `cdn` |
| Deterministic Dim Generator | Device distribution | `derive` / `standardize` | `device` |
| Deterministic Dim Generator | App version | `derive` / `standardize` | `app_version` |
| Deterministic Dim Generator | ISO country | `standardize` (`IN`) | `country` |
| Reference `users.parquet` | User profile | `enrich` (Left join on `user_id`) | `user_id`, `plan_tier` |
| Reference `content.parquet` | Catalog metadata | `enrich` (Left join on `video_id`) | `genre`, `language`, `content_type` |
| Fault Injector / Chaos | Error generator | `derive` (Simulated error events) | `error_code` |
| Producer Engine | Deterministic hash | `derive` (`sha1(run_id\|source\|key)`) | `event_id` |
| Contract Version | Constant | `validate` (`== 1`) | `schema_version` |

---

## 4. Canonical Events to Silver Parquet Columns (`silver_playback_events`)

| Canonical Field | Target Silver Column | Data Type | Transformation Type |
|---|---|---|---|
| `event_id` | `event_id` | STRING | `dedup` (watermarked dedup on event_id) |
| `session_id` | `session_id` | STRING | `validate` (pattern check) |
| `ts` | `ts` | STRING / TIMESTAMP | `sort` / `standardize` (UTC ISO-8601) |
| `event_type` | `event_type` | STRING | `validate` (8 enum types) |
| `buffer_ms` | `buffer_ms` | DOUBLE | `validate` (non-negative) |
| `stall_ms` | `stall_ms` | DOUBLE | `validate` (non-negative) |
| `interval_ms` | `interval_ms` | DOUBLE | `validate` (non-negative) |
| `bitrate_kbps` | `bitrate_kbps` | DOUBLE | `validate` (0 to 100,000) |
| `resolution` | `resolution` | STRING | `standardize` (`<H>p`) |
| `ssim_index` | `ssim_index` | DOUBLE | `validate` (0.0 to 1.0) |
| `device` | `device` | STRING | `standardize` (alias trimmed/casefolded) |
| `isp` | `isp` | STRING | `standardize` (alias trimmed/casefolded) |
| `cdn` | `cdn` | STRING | `standardize` |
| `city` | `city` | STRING | `standardize` |
| `country` | `country` | STRING | `standardize` |
| `app_version` | `app_version` | STRING | `standardize` |
| `video_id` | `video_id` | STRING | `enrich` |
| `user_id` | `user_id` | STRING | `enrich` |
| `genre` | `genre` | STRING | `enrich` |
| `language` | `language` | STRING | `enrich` |
| `plan_tier` | `plan_tier` | STRING | `enrich` |
| `dt` | `dt` | STRING | Partition column (`YYYY-MM-DD`) |
| `hr` | `hr` | STRING | Partition column (`HH`) |

---

## 5. Aggregates to HBase `qoe_metrics` & Warehouse Gold Columns

| Aggregate Metric | Transformation / Type | HBase Column Family & Qualifier | Warehouse Gold Column |
|---|---|---|---|
| Row key | `derive` (`qoe_key(grain, dimkey, rev_min)`) | Row Key | `row_key` |
| Grain & Dimkey | `derive` (Multi-grain explosion) | Qualifiers | `grain`, `dimkey` |
| Window Boundaries | `aggregate` (5-min sliding window) | Qualifiers | `window_start`, `window_end` |
| `stall_ms_sum` | `aggregate` (sum of stall_ms) | `m:stall_ms_sum` | `stall_ms_sum` |
| `stall_count` | `aggregate` (count of buffer_end) | `m:stall_count` | `stall_count` |
| `watch_ms_sum` | `aggregate` (sum of interval_ms) | `m:watch_ms_sum` | `watch_ms_sum` |
| `bitrate_weighted_sum` | `aggregate` (sum of bitrate * interval) | `m:bitrate_weighted_sum` | `bitrate_weighted_sum` |
| `hd_ms_sum` | `aggregate` (sum of HD interval_ms) | `m:hd_ms_sum` | `hd_ms_sum` |
| `switch_count` | `aggregate` (count of bitrate_switch) | `m:switch_count` | `switch_count` |
| `startup_ms_sum` | `aggregate` (sum of startup delays) | `m:startup_ms_sum` | `startup_ms_sum` |
| `startup_n` | `aggregate` (count of startups) | `m:startup_n` | `startup_n` |
| `play_requests` | `aggregate` (count of play_request) | `m:play_requests` | `play_requests` |
| `vsf_count` | `aggregate` (count of vsf facts) | `m:vsf_count` | `vsf_count` |
| `sessions` | `aggregate` (HyperLogLog approx distinct) | `m:sessions` | `sessions` |
| `rebuffer_ratio` | `derive` (`stall_ms / (watch_ms + stall_ms)`) | `m:rebuffer_ratio` | `rebuffer_ratio` |
| `avg_bitrate_kbps` | `derive` (`bitrate_weighted_sum / watch_ms`) | `m:avg_bitrate_kbps` | `avg_bitrate_kbps` |
| `hd_pct` | `derive` (`hd_ms_sum / watch_ms_sum`) | `m:hd_pct` | `hd_pct` |
| `qoe_score` | `derive` (Weighted penalty deduction) | `m:qoe_score` | `qoe_score` |
| `qoe_label` | `derive` (Good/Fair/Poor classification) | `m:qoe_label` | `qoe_label` |
| `avg_throughput_kbps` | `aggregate` (mean throughput) | `n:avg_throughput_kbps` | `avg_throughput_kbps` |
| `avg_rtt_ms` | `aggregate` (mean rtt) | `n:avg_rtt_ms` | `avg_rtt_ms` |
| `avg_packet_loss_pct` | `aggregate` (mean loss) | `n:avg_packet_loss_pct` | `avg_packet_loss_pct` |
| `net_samples` | `aggregate` (count of network logs) | `n:net_samples` | `net_samples` |

---

## 6. Session Facts to HBase `session_state`

| Fact Property | Transformation / Type | HBase Column Family & Qualifier |
|---|---|---|
| `session_id` | `derive` (`salted_key(session_id)`) | Row Key (`{salt:02d}-{session_id}`) |
| `state` | `derive` (Lifecycle transition) | `playback:state` |
| `last_bitrate` | `derive` (Latest bitrate) | `playback:last_bitrate` |
| `stall_count` | `aggregate` (Cumulative stalls) | `playback:stall_count` |
| `stall_ms` | `aggregate` (Cumulative stall duration) | `playback:stall_ms` |
| `startup_ms` | `derive` (`first_frame.ts - play_request.ts`) | `playback:startup_ms` |
| `watch_ms` | `aggregate` (Cumulative watch time) | `playback:watch_ms` |
| `last_event_ts` | `standardize` (Latest event ISO timestamp) | `playback:last_event_ts` |
| `device` | `enrich` / `standardize` | `context:device` |
| `isp` | `enrich` / `standardize` | `context:isp` |
| `cdn` | `enrich` / `standardize` | `context:cdn` |
| `city` | `enrich` / `standardize` | `context:city` |
| `channel` | `enrich` / `standardize` | `context:channel` |
| `scheme` | `enrich` / `standardize` | `context:scheme` |
| `user_id` | `enrich` / `standardize` | `context:user_id` |
| `error_flag` | `derive` (Playback error encountered flag) | `playback:error_flag` |
| `first_N_s` | `derive` (Early window feature map) | Emitted to `session-facts` |
| `vsf` | `derive` (Video start failure flag) | Emitted to `session-facts` |
| `fact_id` | `derive` (Unique fact identifier) | `fact_id` |
| `kind` | `derive` (`startup`, `vsf`, `early_features`, `session_end`) | Partition column `kind` |

