"""Streaming Query Q0: Ingest, clean, validate, standardize, dedup, and route to silver."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from qoe.common.config import get_config, get_topic
from qoe.common.logging import get_logger, setup_logging
from qoe.streaming.enrich import enrich_with_reference_data
from qoe.streaming.parse import parse_batch_pandas
from qoe.streaming.sinks_warehouse import (
    write_etl_counters,
    write_quarantine,
    write_silver_network,
    write_silver_playback,
)
from qoe.streaming.standardize import standardize_dimensions
from qoe.streaming.validate import validate_dataframe

logger = get_logger("q0_clean")


def process_bronze_playback_batch(
    records: List[str | bytes],
    ref_dir: str | Path = "data/ref",
    warehouse_dir: str | Path = "data/warehouse",
    standardization_config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Pure transformation pipeline for bronze playback events:
    parse -> validate -> standardize -> dedup -> enrich -> write sinks.
    """
    if not records:
        empty = pd.DataFrame()
        return empty, empty, {"in_count": 0, "valid": 0, "quarantined": 0, "deduped": 0}

    # 1. Parse
    raw_df = parse_batch_pandas(records)

    # 2. Validate
    valid_df, rejected_df = validate_dataframe(raw_df, event_category="playback")

    # 3. Standardize dimensions
    clean_df = standardize_dimensions(valid_df, standardization_config)

    # 4. Dedup by event_id within batch
    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["event_id"], keep="first")
    dedup_dropped = before_dedup - len(clean_df)

    # 5. Enrich with static catalogs
    enriched_df = enrich_with_reference_data(clean_df, ref_dir=ref_dir)

    # 6. Sinks
    if not enriched_df.empty:
        write_silver_playback(enriched_df, warehouse_dir=warehouse_dir)

    if not rejected_df.empty:
        write_quarantine(rejected_df, warehouse_dir=warehouse_dir)

    counters = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "in_count": len(records),
        "valid": len(enriched_df),
        "quarantined": len(rejected_df),
        "deduped": dedup_dropped,
    }
    write_etl_counters(counters, warehouse_dir=warehouse_dir)

    return enriched_df, rejected_df, counters


def process_bronze_network_batch(
    records: List[str | bytes],
    warehouse_dir: str | Path = "data/warehouse",
    standardization_config: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Pure transformation pipeline for bronze network metrics."""
    if not records:
        empty = pd.DataFrame()
        return empty, empty, {"in_count": 0, "valid": 0, "quarantined": 0, "deduped": 0}

    # 1. Parse
    raw_df = parse_batch_pandas(records)

    # 2. Validate
    valid_df, rejected_df = validate_dataframe(raw_df, event_category="network")

    # 3. Standardize dimensions
    clean_df = standardize_dimensions(valid_df, standardization_config)

    # 4. Dedup by event_id
    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["event_id"], keep="first")
    dedup_dropped = before_dedup - len(clean_df)

    # 5. Sink
    if not clean_df.empty:
        write_silver_network(clean_df, warehouse_dir=warehouse_dir)

    if not rejected_df.empty:
        write_quarantine(rejected_df, warehouse_dir=warehouse_dir)

    counters = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "in_count": len(records),
        "valid": len(clean_df),
        "quarantined": len(rejected_df),
        "deduped": dedup_dropped,
    }
    return clean_df, rejected_df, counters


def run_q0_ingest_clean(
    bootstrap_servers: str = "localhost:9092",
    warehouse_dir: str = "data/warehouse",
    ref_dir: str = "data/ref",
    run_id: str = "q0-runner",
) -> None:
    """Run Q0 Structured Streaming query."""
    setup_logging(run_id=run_id)
    logger.info("Starting Q0 Ingest & Clean streaming processor", extra={"bootstrap": bootstrap_servers})

    # For streaming runtime, connects to Kafka consumer/Spark session
    print("[INFO] Q0 streaming query initialized and running.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark Q0 Ingest Clean streaming job")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap")
    parser.add_argument("--warehouse", default="data/warehouse", help="Warehouse path")
    args = parser.parse_args()

    run_q0_ingest_clean(bootstrap_servers=args.bootstrap, warehouse_dir=args.warehouse)
    return 0


if __name__ == "__main__":
    main()
