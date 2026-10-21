"""HBase streaming foreachBatch sink writers."""

from __future__ import annotations


def write_session_state_to_hbase(batch_df, batch_id: int):
    """Write session state rows to HBase session_state table."""
    raise NotImplementedError("Implemented in Phase 7")


def write_qoe_metrics_to_hbase(batch_df, batch_id: int):
    """Write QoE metric rows to HBase qoe_metrics table."""
    raise NotImplementedError("Implemented in Phase 8")
