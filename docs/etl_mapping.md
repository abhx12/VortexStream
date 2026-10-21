# ETL Source-to-Target Mapping Specification

## 1. Stanford Puffer raw fields to Canonical Playback Events

| Source File | Source Field | Transformation / Derivation | Target Canonical Field | Target Topic / Table |
|---|---|---|---|---|
| `client_buffer` | `session_id`, `index` | `session_id || '-' || index` | `session_id` | `playback-events` |
| `client_buffer` | `session_id` | Preserved for simulated dimension hashing | `puffer_session_id` | internal / join key |
| `client_buffer` | `time` | `epoch_ms(time / 1e6)` -> ISO-8601 UTC with ms | `ts` | `playback-events` |
| `client_buffer` | `event` | Mapping: `init` -> `play_request`<br>`startup` -> `first_frame`<br>`rebuffer` -> `buffer_start`<br>`play` -> `buffer_end`<br>`timer` (5s bucketed) -> `heartbeat`<br>`session_end` (end of stream + 1ms) | `event_type` | `playback-events` |
| `client_buffer` | `channel` | Direct string copy | `video_id` | `playback-events` |
| `client_buffer` | `buffer` | `buffer * 1000.0` (seconds to milliseconds) | `buffer_ms` | `playback-events` |
| `client_buffer` | `cum_rebuf` | On `buffer_end`: `(cum_rebuf - prev_cum_rebuf) * 1000.0` | `stall_ms` | `playback-events` |
| `client_buffer` | `timer` cadence | Elapsed interval in ms (default 5000.0 ms) | `interval_ms` | `playback-events` |
| `video_sent` | `format` | Extracted height regex `(\\d+)x(\\d+)` -> `<H>p` | `resolution` | `playback-events` |
| `video_sent` | `size` | `size * 8.0 / 2.002 / 1000.0` | `bitrate_kbps` | `playback-events` |
| `video_sent` | `ssim_index` | Direct floating-point copy (0.0 to 1.0) | `ssim_index` | `playback-events` |
| `expt_settings` | `expt_id` | Join `expt_id` -> `abr_name/cc_name` | `scheme` | `playback-events` |

## 2. Stanford Puffer `video_sent` to Canonical Network Metrics

| Source File | Source Field | Transformation / Derivation | Target Canonical Field | Target Topic / Table |
|---|---|---|---|---|
| `video_sent` | `session_id`, `index` | `session_id || '-' || index` | `session_id` | `network-metrics` |
| `video_sent` | `time` | `epoch_ms(time / 1e6)` -> ISO-8601 UTC | `ts` | `network-metrics` |
| `video_sent` | `delivery_rate` | `delivery_rate * 8.0 / 1000.0` (bytes/s to kbps) | `throughput_kbps` | `network-metrics` |
| `video_sent` | `rtt` | `rtt / 1000.0` (microseconds to milliseconds) | `rtt_ms` | `network-metrics` |
| `video_sent` | `min_rtt` | `min_rtt / 1000.0` (microseconds to ms) | `min_rtt_ms` | `network-metrics` |
| `video_sent` | `cwnd` | Direct integer copy | `cwnd` | `network-metrics` |
| `video_sent` | `in_flight` | Direct integer copy | `in_flight` | `network-metrics` |
| Simulated | Lognormal model | Correlated with RTT and inverse throughput | `packet_loss_pct` | `network-metrics` |

## 3. Hidden Ground Truth Table (`stream_truth.parquet`)

*Never emitted to live streaming topics; used solely for ML and alert evaluation ground truth.*
- `final_cum_rebuf_s`: total cumulative stall seconds across stream.
- `watch_s`: total duration from first event to last event.
- `startup_delay_s`: time between `init` and `startup`.
- `had_startup`: boolean indicating whether playback ever started.
- `abandoned_label`: `1` if `had_startup` is True and `watch_s < config.ml.abandon_watch_s` (120 s), else `0`.
