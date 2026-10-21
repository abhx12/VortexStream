# Stanford Puffer Telemetry Profile: 2021-06-12T11_2021-06-13T11

## 1. Executive Summary
- **client_buffer Rows**: 3,361
- **video_sent Rows**: 420
- **Time Span**: 0.23 hours (Min NS: `1623495660000000000`, Max NS: `1623496501950000000`)
- **Distinct Sessions**: 15
- **Distinct Streams**: 15
- **Distinct Channels**: 4 (abc, nbc, fox, cbs)
- **Distinct Experiment IDs**: 3

## 2. Playback & QoE Metrics
- **Average Startup Delay**: 1.550 s (Median: 1.550 s)
- **Streams with Init but no Startup**: 6.67% (1 streams)
- **Overall Stall Ratio (sum cum_rebuf / sum watch)**: 0.466%
- **Average Stream Duration**: 57.2 s

## 3. Video Format Distribution (`video_sent`)
| Format | Count | Share (%) |
|---|---|---|
| 640x360-26 | 140 | 33.33% |
| 1280x720-22 | 140 | 33.33% |
| 854x480-24 | 140 | 33.33% |

## 4. Experiment Settings Mapping
```
1 linear_bba bbr
2 cubic cubic
3 bba cubic
```
