"""Data warehouse Parquet sink writers for silver tier, quarantine, and ETL accounting."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional
import duckdb
import pandas as pd


def write_silver_playback(
    df: pd.DataFrame,
    warehouse_dir: str | Path = "data/warehouse",
    partition_dt: Optional[str] = None,
    partition_hr: Optional[str] = None,
) -> Path:
    """Append validated, cleaned playback events to silver_playback_events Parquet."""
    if df.empty:
        return Path(warehouse_dir) / "silver_playback_events"

    out_base = Path(warehouse_dir) / "silver_playback_events"
    now = datetime.now(timezone.utc)
    dt_str = partition_dt or now.strftime("%Y-%m-%d")
    hr_str = partition_hr or now.strftime("%H")

    part_dir = out_base / f"dt={dt_str}" / f"hr={hr_str}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / f"part-{now.strftime('%Y%m%d%H%M%S%f')}.parquet"

    con = duckdb.connect(":memory:")
    con.register("in_df", df)
    con.execute(f"COPY in_df TO '{out_file.as_posix()}' (FORMAT PARQUET);")
    return out_file


def write_silver_network(
    df: pd.DataFrame,
    warehouse_dir: str | Path = "data/warehouse",
    partition_dt: Optional[str] = None,
    partition_hr: Optional[str] = None,
) -> Path:
    """Append validated network metrics to silver_network_metrics Parquet."""
    if df.empty:
        return Path(warehouse_dir) / "silver_network_metrics"

    out_base = Path(warehouse_dir) / "silver_network_metrics"
    now = datetime.now(timezone.utc)
    dt_str = partition_dt or now.strftime("%Y-%m-%d")
    hr_str = partition_hr or now.strftime("%H")

    part_dir = out_base / f"dt={dt_str}" / f"hr={hr_str}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / f"part-{now.strftime('%Y%m%d%H%M%S%f')}.parquet"

    con = duckdb.connect(":memory:")
    con.register("in_df", df)
    con.execute(f"COPY in_df TO '{out_file.as_posix()}' (FORMAT PARQUET);")
    return out_file


def write_quarantine(
    df: pd.DataFrame,
    warehouse_dir: str | Path = "data/warehouse",
) -> Path:
    """Append rejected records with reason and raw payload to quarantine_events."""
    if df.empty:
        return Path(warehouse_dir) / "quarantine_events"

    out_base = Path(warehouse_dir) / "quarantine_events"
    out_base.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    out_file = out_base / f"quarantine-{now.strftime('%Y%m%d%H%M%S%f')}.parquet"

    con = duckdb.connect(":memory:")
    con.register("rej_df", df)
    con.execute(f"COPY rej_df TO '{out_file.as_posix()}' (FORMAT PARQUET);")
    return out_file


def write_etl_counters(
    counters: Dict[str, Any],
    warehouse_dir: str | Path = "data/warehouse",
) -> Path:
    """Record batch ETL accounting metrics to etl_counters table."""
    out_base = Path(warehouse_dir) / "etl_counters"
    out_base.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    out_file = out_base / f"counters-{now.strftime('%Y%m%d%H%M%S%f')}.parquet"

    df = pd.DataFrame([counters])
    con = duckdb.connect(":memory:")
    con.register("c_df", df)
    con.execute(f"COPY c_df TO '{out_file.as_posix()}' (FORMAT PARQUET);")
    return out_file
