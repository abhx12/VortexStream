"""Phase 14 End-to-end verification and integration test suite."""

from __future__ import annotations

import json
from pathlib import Path
import pytest
import pandas as pd

from qoe.common.config import get_config
from qoe.data.ref_data import generate_reference_data
from qoe.data.sim_dims import generate_simulated_dimensions
from qoe.ml.churn_model import train_abandonment_model
from qoe.streaming.q0_ingest_clean import process_bronze_playback_batch
from qoe.streaming.q1_session_tracker import process_session_events
from qoe.streaming.q2_qoe_aggregator import compute_qoe_window_aggregates
from qoe.streaming.q3_network_aggregator import compute_network_window_aggregates
from qoe.streaming.q4_ml_scoring import score_early_features_batch
from qoe.verify.lineage import generate_lineage_docs
from qoe.verify.reconcile import run_reconciliation


def run_e2e_pipeline_verification(warehouse_dir: Path) -> dict:
    """Run simulated end-to-end slice and assert pipeline invariants."""
    # 1. Reference Data & Dimensions
    generate_reference_data(output_dir=warehouse_dir / "ref")
    sim_df = generate_simulated_dimensions(output_dir=warehouse_dir / "ref", num_sessions=100)
    assert len(sim_df) == 100

    # 2. Q0: Ingest, clean, standardize
    bronze_event = {
        "event_id": "e2e-ev-001",
        "session_id": "e2e-sess-001",
        "ts": "2026-10-22T12:00:00.000Z",
        "event_type": "play_request",
        "device": "android",
        "isp": "reliance jio",
        "city": "mumbai",
        "video_id": "c1",
    }
    valid_df, rej_df, metrics = process_bronze_playback_batch(
        [json.dumps(bronze_event)],
        warehouse_dir=warehouse_dir,
    )
    assert len(valid_df) == 1
    assert len(rej_df) == 0

    # 3. Q1: Session tracking & early features emission
    ff_event = {
        "event_id": "e2e-ev-002",
        "session_id": "e2e-sess-001",
        "ts": "2026-10-22T12:00:01.200Z",
        "event_type": "first_frame",
        "device": "android",
        "isp": "Jio",
        "city": "Mumbai",
        "video_id": "c1",
    }
    state, facts, hrow = process_session_events("e2e-sess-001", [valid_df.iloc[0].to_dict(), ff_event])
    assert state.state == "playing"
    assert len(facts) >= 1

    # 4. Q2 & Q3: Windowed aggregates
    hb_event = {
        "event_id": "e2e-ev-003",
        "session_id": "e2e-sess-001",
        "ts": "2026-10-22T12:00:06.000Z",
        "event_type": "heartbeat",
        "buffer_ms": 3500.0,
        "interval_ms": 5000.0,
        "bitrate_kbps": 3200.0,
        "device": "android",
        "isp": "Jio",
        "city": "Mumbai",
        "video_id": "c1",
    }
    qoe_rows = compute_qoe_window_aggregates([hb_event] + facts)
    assert len(qoe_rows) > 0

    net_rows = compute_network_window_aggregates([{
        "event_id": "e2e-net-001",
        "session_id": "e2e-sess-001",
        "ts": "2026-10-22T12:00:06.000Z",
        "throughput_kbps": 4200.0,
        "rtt_ms": 28.0,
        "packet_loss_pct": 0.2,
        "device": "android",
        "isp": "Jio",
        "city": "Mumbai",
    }])
    assert len(net_rows) > 0

    # 5. ML Abandonment Scoring
    early_fact = {
        "schema_version": 1,
        "fact_id": "fact-001",
        "session_id": "e2e-sess-001",
        "ts": "2026-10-22T12:00:15.000Z",
        "kind": "early_features",
        "startup_ms": 1200.0,
        "stall_count": 0,
        "stall_ms": 0.0,
        "avg_bitrate_kbps": 3200.0,
        "device": "android",
        "isp": "Jio",
        "cdn": "Cloudflare",
        "city": "Mumbai",
        "first_N_s": {"scheme": "bba"},
    }
    scores = score_early_features_batch([early_fact], warehouse_dir=warehouse_dir)
    assert len(scores) == 1
    assert 0.0 <= scores[0]["abandon_probability"] <= 1.0

    # 6. Verification V1-V8 Reconciliation
    rec_res = run_reconciliation()
    assert rec_res["overall"] == "PASS"

    return {
        "status": "PASS",
        "qoe_rows_computed": len(qoe_rows),
        "network_rows_computed": len(net_rows),
        "abandon_prob": scores[0]["abandon_probability"],
    }


def test_full_pipeline_run(tmp_path):
    """Pytest wrapper executing end-to-end pipeline verification."""
    res = run_e2e_pipeline_verification(tmp_path / "warehouse")
    assert res["status"] == "PASS"


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        result = run_e2e_pipeline_verification(Path(td))
        print("End-to-end acceptance run completed successfully:", json.dumps(result, indent=2))
