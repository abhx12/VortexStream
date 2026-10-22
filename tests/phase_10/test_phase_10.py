"""Unit tests for Phase 10: RDD demo, Spark SQL, DuckDB warehouse, and hourly rollup."""

from __future__ import annotations

import inspect
from pathlib import Path
import duckdb
import pandas as pd
import pytest

from qoe.batch.hourly_rollup import compute_hourly_rollup
from qoe.batch.rdd_demo import run_rdd_demo
import qoe.batch.rdd_demo as rdd_module
from qoe.batch.sql_demo import run_sql_demo
from qoe.batch.warehouse import init_warehouse

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_rdd_ops_present():
    """Verify rdd_demo.py contains required RDD transformations and actions."""
    src = inspect.getsource(rdd_module)
    for transform in ["map", "filter", "flatMap", "reduceByKey", "aggregateByKey"]:
        assert f".{transform}(" in src, f"Expected transformation '{transform}' in rdd_demo.py"

    for action in ["collect(", "count(", "take("]:
        assert f".{action}" in src, f"Expected action '{action}' in rdd_demo.py"


def test_rdd_results_match_sql():
    """Verify errors per ISP matches across RDD job and DuckDB SQL query."""
    records = [
        {"session_id": "s1", "isp": "Jio", "event_type": "error", "error_code": "NET_TIMEOUT", "device": "android", "bitrate_kbps": 1000},
        {"session_id": "s2", "isp": "Jio", "event_type": "error", "error_code": "NET_TIMEOUT", "device": "android", "bitrate_kbps": 1500},
        {"session_id": "s3", "isp": "Airtel", "event_type": "error", "error_code": "CDN_5XX", "device": "ios", "bitrate_kbps": 2000},
        {"session_id": "s4", "isp": "Airtel", "event_type": "heartbeat", "error_code": None, "device": "ios", "bitrate_kbps": 3000},
        {"session_id": "s5", "isp": "Vi", "event_type": "error", "error_code": "DECODE_ERR", "device": "web", "bitrate_kbps": 1200},
    ]

    rdd_res = run_rdd_demo(input_records=records)
    rdd_errs = rdd_res["errors_per_isp"]

    con = duckdb.connect(":memory:")
    df = pd.DataFrame(records)
    con.register("events", df)
    sql_df = con.execute("""
        SELECT isp, count(*) as err_count
        FROM events
        WHERE event_type = 'error'
        GROUP BY isp
    """).df()
    sql_errs = dict(zip(sql_df["isp"], sql_df["err_count"]))

    assert rdd_errs == sql_errs
    assert rdd_errs["Jio"] == 2
    assert rdd_errs["Airtel"] == 1
    assert rdd_errs["Vi"] == 1


def test_sql_worst_pairs():
    """Verify worst ISP-CDN pairs query produces sorted results with <= 10 rows."""
    res = run_sql_demo()
    worst = res["worst_pairs"]
    assert len(worst) <= 10
    # Sorted descending by rebuffer_ratio
    ratios = [w["rebuffer_ratio"] for w in worst if w["rebuffer_ratio"] is not None]
    assert ratios == sorted(ratios, reverse=True)


def test_duckdb_view_dedup(tmp_path):
    """Verify gold_qoe_5m warehouse view keeps exactly one latest row per (grain, dimkey, window_end)."""
    raw_dir = tmp_path / "gold_qoe_5m_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Write two records with same (grain, dimkey, window_end) but different updated_at
    records = [
        {"grain": "cdn", "dimkey": "Akamai", "window_end": "2026-10-22T10:05:00.000Z", "updated_at": "2026-10-22T10:05:05.000Z", "qoe_score": 70.0},
        {"grain": "cdn", "dimkey": "Akamai", "window_end": "2026-10-22T10:05:00.000Z", "updated_at": "2026-10-22T10:05:30.000Z", "qoe_score": 75.0},
    ]
    pd.DataFrame(records).to_parquet(raw_dir / "part1.parquet")

    con = init_warehouse(warehouse_dir=tmp_path, db_path=tmp_path / "test.duckdb")
    dedup_df = con.execute("SELECT * FROM gold_qoe_5m WHERE grain = 'cdn' AND dimkey = 'Akamai'").df()

    assert len(dedup_df) == 1
    # Latest updated_at must be retained (qoe_score 75.0)
    assert dedup_df["qoe_score"].iloc[0] == 75.0
    con.close()


def test_hourly_rollup_matches(tmp_path):
    """Verify hourly rollup additive columns equal sum of 5-min closed windows."""
    # 3 x 5-minute windows within the 10:00 hour
    raw_5m = pd.DataFrame([
        {
            "grain": "global",
            "dimkey": "ALL",
            "window_end": "2026-10-22T10:05:00.000Z",
            "updated_at": "2026-10-22T10:05:01.000Z",
            "stall_ms_sum": 2000.0,
            "stall_count": 2,
            "watch_ms_sum": 60000.0,
            "bitrate_weighted_sum": 120000000.0,
            "hd_ms_sum": 60000.0,
            "switch_count": 1,
            "startup_ms_sum": 1000.0,
            "startup_n": 1,
            "play_requests": 1,
            "vsf_count": 0,
            "error_sessions": 0,
            "sessions": 10,
        },
        {
            "grain": "global",
            "dimkey": "ALL",
            "window_end": "2026-10-22T10:10:00.000Z",
            "updated_at": "2026-10-22T10:10:01.000Z",
            "stall_ms_sum": 3000.0,
            "stall_count": 1,
            "watch_ms_sum": 60000.0,
            "bitrate_weighted_sum": 150000000.0,
            "hd_ms_sum": 60000.0,
            "switch_count": 2,
            "startup_ms_sum": 1500.0,
            "startup_n": 1,
            "play_requests": 1,
            "vsf_count": 0,
            "error_sessions": 1,
            "sessions": 12,
        },
    ])

    hourly_df = compute_hourly_rollup(
        warehouse_dir=tmp_path,
        gold_5m_df=raw_5m,
        output_dir=tmp_path / "hourly",
    )

    assert len(hourly_df) == 1
    h0 = hourly_df.iloc[0]
    # Sum of stall_ms (2000 + 3000 = 5000)
    assert h0["stall_ms_sum"] == pytest.approx(5000.0, rel=0.005)
    # Sum of watch_ms (60000 + 60000 = 120000)
    assert h0["watch_ms_sum"] == pytest.approx(120000.0, rel=0.005)
    assert h0["stall_count"] == 3
    assert h0["switch_count"] == 3
    assert h0["startup_ms_sum"] == 2500.0
    assert h0["qoe_score"] > 0.0
