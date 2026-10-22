-- Historical Operations Report Queries (Phase 10 & Phase 12)

-- 1. Hourly QoE trend
SELECT 
    hour_window,
    grain,
    dimkey,
    qoe_score,
    rebuffer_ratio,
    avg_bitrate_kbps,
    sessions
FROM gold_qoe_hourly
WHERE grain = 'global'
ORDER BY hour_window ASC;

-- 2. Top-5 worst performing segments
SELECT 
    grain,
    dimkey,
    avg(qoe_score) AS avg_qoe,
    sum(stall_ms_sum) / nullif(sum(watch_ms_sum + stall_ms_sum), 0) AS overall_rebuffer_ratio,
    sum(stall_count) AS total_stalls
FROM gold_qoe_hourly
WHERE grain IN ('city_isp', 'cdn', 'device')
GROUP BY grain, dimkey
ORDER BY avg_qoe ASC
LIMIT 5;

-- 3. Alert counts per hour and rule
SELECT 
    substring(window_end, 1, 13) || ':00:00Z' AS alert_hour,
    rule,
    severity,
    count(*) AS alert_count
FROM alerts
GROUP BY substring(window_end, 1, 13), rule, severity
ORDER BY alert_hour DESC, alert_count DESC;

-- 4. Fault-period impact evaluation
SELECT 
    dimkey AS affected_segment,
    avg(CASE WHEN window_end BETWEEN '2026-10-22T10:00:00Z' AND '2026-10-22T10:30:00Z' THEN qoe_score END) AS fault_qoe,
    avg(CASE WHEN window_end NOT BETWEEN '2026-10-22T10:00:00Z' AND '2026-10-22T10:30:00Z' THEN qoe_score END) AS baseline_qoe,
    avg(CASE WHEN window_end NOT BETWEEN '2026-10-22T10:00:00Z' AND '2026-10-22T10:30:00Z' THEN qoe_score END) - 
    avg(CASE WHEN window_end BETWEEN '2026-10-22T10:00:00Z' AND '2026-10-22T10:30:00Z' THEN qoe_score END) AS qoe_drop
FROM gold_qoe_5m
GROUP BY dimkey
HAVING fault_qoe IS NOT NULL;
