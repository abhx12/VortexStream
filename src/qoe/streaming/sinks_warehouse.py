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


def write_partitioned_parquet(
    data: Any,
    output_dir: str | Path,
    partition_cols: Optional[list[str]] = None,
) -> Path:
    """Generic parquet sink with optional Hive-style partitioning."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, list):
        if not data:
            return out_dir
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame([data])

    if df.empty:
        return out_dir

    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%d%H%M%S%f")
    out_file = out_dir / f"part-{ts_str}.parquet"

    # If dict columns exist (like first_N_s), convert to JSON string for parquet compatibility
    clean_df = df.copy()
    for col in clean_df.columns:
        if clean_df[col].apply(lambda x: isinstance(x, dict)).any():
            clean_df[col] = clean_df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    con = duckdb.connect(":memory:")
    con.register("df_view", clean_df)
    if partition_cols and any(p in clean_df.columns for p in partition_cols):
        valid_parts = [p for p in partition_cols if p in clean_df.columns]
        con.execute(
            f"COPY df_view TO '{out_dir.as_posix()}' (FORMAT PARQUET, PARTITION_BY ({', '.join(valid_parts)}), OVERWRITE_OR_IGNORE);"
        )
    else:
        con.execute(f"COPY df_view TO '{out_file.as_posix()}' (FORMAT PARQUET);")
    return out_file


def write_session_facts(
    facts: Any,
    warehouse_dir: str | Path = "data/warehouse",
) -> Path:
    """Write session facts to data/warehouse/session_facts partitioned by kind."""
    return write_partitioned_parquet(
        facts,
        output_dir=Path(warehouse_dir) / "session_facts",
        partition_cols=["kind"],
    )


def write_gold_qoe_5m(
    rows: Any,
    warehouse_dir: str | Path = "data/warehouse",
) -> Path:
    """Append aggregated QoE metrics to gold_qoe_5m_raw."""
    return write_partitioned_parquet(
        rows,
        output_dir=Path(warehouse_dir) / "gold_qoe_5m_raw",
        partition_cols=["grain"],
    )


def write_gold_network_5m(
    rows: Any,
    warehouse_dir: str | Path = "data/warehouse",
) -> Path:
    """Append network aggregates to gold_network_5m_raw."""
    return write_partitioned_parquet(
        rows,
        output_dir=Path(warehouse_dir) / "gold_network_5m_raw",
        partition_cols=["grain"],
    )


def write_alerts(
    alert_records: Any,
    warehouse_dir: str | Path = "data/warehouse",
) -> Path:
    """Append alert events to warehouse alerts Parquet."""
    return write_partitioned_parquet(
        alert_records,
        output_dir=Path(warehouse_dir) / "alerts",
        partition_cols=["rule"],
    )


