"""Data warehouse Parquet sink writers for silver and gold tiers."""

from __future__ import annotations


def write_silver_playback(batch_df, batch_id: int):
    """Write cleaned events to silver Parquet."""
    raise NotImplementedError("Implemented in Phase 6")


def write_gold_qoe(batch_df, batch_id: int):
    """Write gold aggregates to warehouse Parquet."""
    raise NotImplementedError("Implemented in Phase 8")
