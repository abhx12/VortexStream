"""Unit tests for Phase 12: Operations dashboard data access layer and application."""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

from qoe.dashboard.app import create_dashboard_app
from qoe.dashboard.data_access import DashboardDataAccess


def test_data_access_empty_handling(tmp_path):
    """Verify data_access methods handle empty warehouse gracefully without crashing."""
    da = DashboardDataAccess(warehouse_dir=tmp_path / "empty_wh")

    assert da.latest_segment("global", "ALL") is None
    assert da.segment_history("global", "ALL", minutes=30) == []
    assert da.latest_all("city_isp") == []
    assert da.recent_alerts(limit=5) == []
    assert da.active_sessions_count() == 0
    assert da.get_data_freshness_seconds() == 0.0


def test_data_access_with_seeded_data(tmp_path):
    """Verify data access queries against seeded parquet warehouse files."""
    gold_dir = tmp_path / "gold_qoe_5m_raw"
    gold_dir.mkdir(parents=True, exist_ok=True)

    records = [
        {
            "grain": "city_isp",
            "dimkey": "Chennai|Airtel",
            "window_end": "2026-10-22T10:05:00.000Z",
            "updated_at": "2026-10-22T10:05:01.000Z",
            "qoe_score": 55.0,
            "qoe_label": "Poor",
            "rebuffer_ratio": 0.08,
            "stall_ms_sum": 4000.0,
            "watch_ms_sum": 46000.0,
            "stall_count": 2,
            "sessions": 45,
        },
        {
            "grain": "city_isp",
            "dimkey": "Chennai|Airtel",
            "window_end": "2026-10-22T10:10:00.000Z",
            "updated_at": "2026-10-22T10:10:01.000Z",
            "qoe_score": 62.0,
            "qoe_label": "Fair",
            "rebuffer_ratio": 0.04,
            "stall_ms_sum": 2000.0,
            "watch_ms_sum": 48000.0,
            "stall_count": 1,
            "sessions": 50,
        },
    ]
    pd.DataFrame(records).to_parquet(gold_dir / "part1.parquet")

    da = DashboardDataAccess(warehouse_dir=tmp_path)

    # 1. latest_segment returns newest window (10:10)
    latest = da.latest_segment("city_isp", "Chennai|Airtel")
    assert latest is not None
    assert latest["window_end"] == "2026-10-22T10:10:00.000Z"
    assert latest["qoe_score"] == 62.0

    # 2. segment_history returns chronological order (10:05 then 10:10)
    history = da.segment_history("city_isp", "Chennai|Airtel", minutes=30)
    assert len(history) == 2
    assert history[0]["window_end"] == "2026-10-22T10:05:00.000Z"
    assert history[1]["window_end"] == "2026-10-22T10:10:00.000Z"

    # 3. latest_all
    all_city = da.latest_all("city_isp")
    assert len(all_city) == 1
    assert all_city[0]["dimkey"] == "Chennai|Airtel"


def test_dashboard_app_boot(tmp_path):
    """Verify create_dashboard_app runs and returns valid status."""
    da = DashboardDataAccess(warehouse_dir=tmp_path)
    res = create_dashboard_app(data_access=da)
    assert res is not None
    assert "status" in res
