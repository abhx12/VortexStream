# Simulated Dimensions & Fields Specification

> **Honesty Rule**: Real fields originate strictly from the Stanford Puffer open dataset (`client_buffer`, `video_sent`, `video_acked`). All other dimensions, user profiles, reference catalogs, network impairments, and fault injections are **simulated** and documented below.

## Summary of Simulated vs Real Fields

| Field Name | Category | Origin | Notes / Simulation Method |
|---|---|---|---|
| `session_id`, `index` | Playback | Real | Stanford Puffer stream identifier |
| `time` / `ts_orig` | Playback | Real | Original Puffer timestamp (nanoseconds) |
| `buffer` / `buffer_ms` | Playback | Real | Converted to ms (`buffer * 1000`) |
| `cum_rebuf` / `stall_ms` | Playback | Real | Converted to ms (`delta cum_rebuf * 1000`) |
| `delivery_rate` / `throughput_kbps` | Network | Real | Converted from delivery_rate |
| `rtt`, `min_rtt` / `rtt_ms` | Network | Real | Converted from microseconds |
| `ssim_index` | Quality | Real | Real Puffer perceptual metric |
| `device` | Dimension | Simulated | Deterministic hash assignment (android, ios, web, smarttv) |
| `app_version` | Dimension | Simulated | Weighted sampling (5.2.1, 5.2.0, 5.1.9) |
| `city` | Dimension | Simulated | Indian metros (Chennai, Bengaluru, Mumbai, Delhi, etc.) |
| `isp` | Dimension | Simulated | Indian ISPs (Jio, Airtel, ACT, Vi, BSNL) |
| `cdn` | Dimension | Simulated | CDNs (Akamai, CloudFront, Fastly) |
| `user_id` | Dimension | Simulated | Hash bucket into 50k user pool (`u-XXXX`) |
| `packet_loss_pct` | Network | Simulated | Lognormal + RTT/throughput impairment model |
| `error_code` | Fault/Error | Simulated | Injected errors (NET_TIMEOUT, CDN_5XX, DECODE_ERR, DRM_ERR) |
| Reference catalogs | Metadata | Simulated | Synthetic users, subscription tiers, content genres |
| Fault scenarios | Chaos | Simulated | F1 (ISP outage), F2 (CDN slowdown), F3 (app bug) |

*(Detailed distribution parameters, generation seeds, and mathematical models will be populated in Phase 4)*.
