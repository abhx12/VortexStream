"""Spark SQL and DuckDB analytical queries demonstration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
import pandas as pd

from qoe.common.config import get_config


def run_sql_demo(
    warehouse_dir: str | Path = "data/warehouse",
    gold_df: Optional[pd.DataFrame] = None,
    silver_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Execute analytical SQL queries over warehouse Parquet using DuckDB engine."""
    con = duckdb.connect(":memory:")

    # Prepare silver view
    if silver_df is not None:
        con.register("silver_playback", silver_df)
    else:
        s_path = Path(warehouse_dir) / "silver_playback_events"
        if s_path.is_dir() and any(s_path.glob("**/*.parquet")):
            con.execute(f"CREATE VIEW silver_playback AS SELECT * FROM read_parquet('{s_path.as_posix()}/**/*.parquet');")
        else:
            # Seed demo data
            demo_silver = pd.DataFrame([
                {"session_id": "s1", "isp": "Airtel", "cdn": "Akamai", "stall_ms": 5000.0, "watch_ms": 25000.0, "bitrate_kbps": 1800, "event_type": "buffer_end", "app_version": "v2.1", "error_code": None},
                {"session_id": "s2", "isp": "Jio", "cdn": "Cloudflare", "stall_ms": 0.0, "watch_ms": 30000.0, "bitrate_kbps": 4500, "event_type": "heartbeat", "app_version": "v2.0", "error_code": None},
                {"session_id": "s3", "isp": "Airtel", "cdn": "Akamai", "stall_ms": 8000.0, "watch_ms": 20000.0, "bitrate_kbps": 1200, "event_type": "error", "app_version": "v2.1", "error_code": "CDN_5XX"},
                {"session_id": "s4", "isp": "Vi", "cdn": "Fastly", "stall_ms": 1000.0, "watch_ms": 29000.0, "bitrate_kbps": 3000, "event_type": "heartbeat", "app_version": "v2.0", "error_code": None},
            ])
            con.register("silver_playback", demo_silver)

    # Prepare gold view
    if gold_df is not None:
        con.register("gold_qoe_5m", gold_df)
    else:
        g_path = Path(warehouse_dir) / "gold_qoe_5m_raw"
        if g_path.is_dir() and any(g_path.glob("**/*.parquet")):
            con.execute(f"CREATE VIEW gold_qoe_5m AS SELECT * FROM read_parquet('{g_path.as_posix()}/**/*.parquet');")
        else:
            # Seed demo gold
            demo_gold = pd.DataFrame([
                {"grain": "device", "dimkey": "android", "ts": "2026-10-22T10:05:00Z", "window_end": "2026-10-22T10:05:00Z", "qoe_score": 75.0, "qoe_label": "Fair", "city": "Mumbai", "stall_count": 2},
                {"grain": "device", "dimkey": "ios", "ts": "2026-10-22T10:05:00Z", "window_end": "2026-10-22T10:05:00Z", "qoe_score": 88.0, "qoe_label": "Good", "city": "Delhi", "stall_count": 0},
                {"grain": "device", "dimkey": "smarttv", "ts": "2026-10-22T10:05:00Z", "window_end": "2026-10-22T10:05:00Z", "qoe_score": 92.0, "qoe_label": "Good", "city": "Bengaluru", "stall_count": 0},
                {"grain": "city_isp", "dimkey": "Chennai|Airtel", "ts": "2026-10-22T10:10:00Z", "window_end": "2026-10-22T10:10:00Z", "qoe_score": 52.0, "qoe_label": "Poor", "city": "Chennai", "stall_count": 5},
                {"grain": "city_isp", "dimkey": "Mumbai|Jio", "ts": "2026-10-22T10:10:00Z", "window_end": "2026-10-22T10:10:00Z", "qoe_score": 89.0, "qoe_label": "Good", "city": "Mumbai", "stall_count": 1},
            ])
            con.register("gold_qoe_5m", demo_gold)

    results: Dict[str, Any] = {}

    # Query 1: Worst ISP-CDN pairs by rebuffering ratio
    q1 = """
        SELECT 
            isp,
            cdn,
            count(distinct session_id) as active_sessions,
            sum(stall_ms) / nullif(sum(watch_ms + stall_ms), 0) as rebuffer_ratio,
            avg(bitrate_kbps) as avg_bitrate_kbps
        FROM silver_playback
        GROUP BY isp, cdn
        ORDER BY rebuffer_ratio DESC NULLS LAST
        LIMIT 10;
    """
    results["worst_pairs"] = con.execute(q1).df().to_dict(orient="records")

    # Query 2: Peak hour QoE by device
    q2 = """
        SELECT 
            dimkey as device,
            avg(qoe_score) as avg_qoe,
            count(*) as window_count
        FROM gold_qoe_5m
        WHERE grain = 'device'
        GROUP BY dimkey
        ORDER BY avg_qoe ASC;
    """
    results["peak_hour_device"] = con.execute(q2).df().to_dict(orient="records")

    # Query 3: QoE class distribution per city
    q3 = """
        SELECT 
            city,
            qoe_label,
            count(*) as count_windows
        FROM gold_qoe_5m
        WHERE city IS NOT NULL
        GROUP BY city, qoe_label
        ORDER BY city, count_windows DESC;
    """
    results["class_dist_city"] = con.execute(q3).df().to_dict(orient="records")

    # Query 4: LAG window function on QoE
    q4 = """
        SELECT 
            grain,
            dimkey,
            window_end,
            qoe_score,
            lag(qoe_score, 1) OVER (PARTITION BY grain, dimkey ORDER BY window_end) as prev_qoe
        FROM gold_qoe_5m
        ORDER BY window_end;
    """
    results["lag_window"] = con.execute(q4).df().to_dict(orient="records")

    con.close()
    return results


if __name__ == "__main__":
    res = run_sql_demo()
    print("SQL Demo Results:")
    print("Worst pairs:", res["worst_pairs"])
    print("Peak hour device:", res["peak_hour_device"])
