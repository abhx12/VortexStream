"""Batch hourly rollup computation for gold warehouse tier."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
import pandas as pd

from qoe.common.config import get_config
from qoe.streaming.qoe_score import compute_qoe_score
from qoe.streaming.sinks_warehouse import write_partitioned_parquet


def compute_hourly_rollup(
    warehouse_dir: str | Path = "data/warehouse",
    gold_5m_df: Optional[pd.DataFrame] = None,
    output_dir: Optional[str | Path] = None,
) -> pd.DataFrame:
    """Aggregate 5-minute closed windows into hourly rollup summaries."""
    w_dir = Path(warehouse_dir)
    con = duckdb.connect(":memory:")

    if gold_5m_df is not None:
        con.register("raw_5m", gold_5m_df)
    else:
        raw_path = w_dir / "gold_qoe_5m_raw"
        if raw_path.is_dir() and any(raw_path.glob("**/*.parquet")):
            con.execute(f"CREATE VIEW raw_5m AS SELECT * FROM read_parquet('{raw_path.as_posix()}/**/*.parquet');")
        else:
            return pd.DataFrame()

    # Deduplicate 5m windows first
    con.execute("""
        CREATE VIEW dedup_5m AS
        WITH ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY grain, dimkey, window_end 
                       ORDER BY updated_at DESC
                   ) as rn
            FROM raw_5m
        )
        SELECT * EXCLUDE (rn)
        FROM ranked
        WHERE rn = 1;
    """)

    rollup_sql = """
        SELECT
            grain,
            dimkey,
            substring(window_end, 1, 13) || ':00:00.000Z' as hour_window,
            substring(window_end, 1, 10) as dt,
            substring(window_end, 12, 2) as hr,
            sum(stall_ms_sum) as stall_ms_sum,
            sum(stall_count) as stall_count,
            sum(watch_ms_sum) as watch_ms_sum,
            sum(bitrate_weighted_sum) as bitrate_weighted_sum,
            sum(hd_ms_sum) as hd_ms_sum,
            sum(switch_count) as switch_count,
            sum(startup_ms_sum) as startup_ms_sum,
            sum(startup_n) as startup_n,
            sum(play_requests) as play_requests,
            sum(vsf_count) as vsf_count,
            sum(error_sessions) as error_sessions,
            max(sessions) as sessions
        FROM dedup_5m
        GROUP BY grain, dimkey, substring(window_end, 1, 13), substring(window_end, 1, 10), substring(window_end, 12, 2)
    """

    df = con.execute(rollup_sql).df()
    if df.empty:
        con.close()
        return df

    # Recompute derived metrics and QoE scores at hourly grain
    cfg = get_config()
    records = df.to_dict(orient="records")
    for r in records:
        w_ms = r["watch_ms_sum"]
        s_ms = r["stall_ms_sum"]
        tot = w_ms + s_ms
        r["rebuffer_ratio"] = (s_ms / tot) if tot > 0 else 0.0
        r["avg_bitrate_kbps"] = (r["bitrate_weighted_sum"] / w_ms) if w_ms > 0 else 0.0
        r["hd_pct"] = (r["hd_ms_sum"] / w_ms) if w_ms > 0 else 0.0
        r["switches_per_min"] = (r["switch_count"] / (w_ms / 60000.0)) if w_ms > 0 else 0.0
        r["avg_startup_ms"] = (r["startup_ms_sum"] / r["startup_n"]) if r["startup_n"] > 0 else None
        r["avg_startup_s"] = (r["avg_startup_ms"] / 1000.0) if r["avg_startup_ms"] is not None else None
        r["vsf_rate"] = (r["vsf_count"] / r["play_requests"]) if r["play_requests"] > 0 else 0.0
        r["error_rate"] = (r["error_sessions"] / r["sessions"]) if r["sessions"] > 0 else 0.0

        qoe_res = compute_qoe_score(r, config=cfg)
        r["qoe_score"] = qoe_res["qoe_score"]
        r["qoe_label"] = qoe_res["qoe_label"]

    result_df = pd.DataFrame(records)

    target_out = Path(output_dir) if output_dir else w_dir / "gold_qoe_hourly"
    write_partitioned_parquet(result_df, output_dir=target_out, partition_cols=["grain"])

    con.close()
    return result_df


if __name__ == "__main__":
    res_df = compute_hourly_rollup()
    print(f"Hourly rollup computed. Rows: {len(res_df)}")
