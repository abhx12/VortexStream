-- Spark SQL Analytical Queries (Phase 10)

-- 1. Worst 10 ISP-CDN pairs by rebuffering ratio in the last hour
SELECT 
    isp,
    cdn,
    count(distinct session_id) AS active_sessions,
    sum(stall_ms) / nullif(sum(watch_ms + stall_ms), 0) AS rebuffer_ratio,
    avg(bitrate_kbps) AS avg_bitrate_kbps
FROM silver_playback
GROUP BY isp, cdn
HAVING active_sessions >= 1
ORDER BY rebuffer_ratio DESC
LIMIT 10;

-- 2. Peak-hour QoE by device
SELECT 
    device,
    substring(ts, 12, 2) AS hour_of_day,
    avg(qoe_score) AS avg_qoe,
    count(*) AS window_count
FROM gold_qoe_5m
WHERE grain = 'device'
GROUP BY device, substring(ts, 12, 2)
ORDER BY hour_of_day, avg_qoe ASC;

-- 3. QoE class distribution per city
SELECT 
    city,
    qoe_label,
    count(*) AS count_windows,
    round(count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY city), 2) AS pct_of_city
FROM gold_qoe_5m
WHERE grain = 'city_isp'
GROUP BY city, qoe_label
ORDER BY city, count_windows DESC;

-- 4. Rollup & Cube for multidimensional aggregation
SELECT 
    coalesce(grain, 'ALL_GRAINS') AS grain,
    coalesce(dimkey, 'ALL_KEYS') AS dimkey,
    avg(qoe_score) AS avg_qoe,
    sum(stall_count) AS total_stalls
FROM gold_qoe_5m
GROUP BY ROLLUP (grain, dimkey);

-- 5. Window Functions: LAG QoE delta and RANK worst segments
SELECT 
    grain,
    dimkey,
    window_end,
    qoe_score,
    lag(qoe_score, 1) OVER (PARTITION BY grain, dimkey ORDER BY window_end) AS prev_qoe,
    qoe_score - lag(qoe_score, 1) OVER (PARTITION BY grain, dimkey ORDER BY window_end) AS qoe_delta,
    rank() OVER (PARTITION BY window_end ORDER BY qoe_score ASC) AS worst_rank
FROM gold_qoe_5m;

-- 6. Top error codes per app version
SELECT 
    app_version,
    error_code,
    count(*) AS error_count
FROM silver_playback
WHERE event_type = 'error'
GROUP BY app_version, error_code
ORDER BY app_version, error_count DESC;

-- 7. Fault-period vs Baseline comparison
SELECT 
    dimkey,
    avg(CASE WHEN ts BETWEEN '2026-10-22T10:00:00Z' AND '2026-10-22T10:30:00Z' THEN qoe_score END) AS fault_avg_qoe,
    avg(CASE WHEN ts NOT BETWEEN '2026-10-22T10:00:00Z' AND '2026-10-22T10:30:00Z' THEN qoe_score END) AS baseline_avg_qoe
FROM gold_qoe_5m
GROUP BY dimkey;
