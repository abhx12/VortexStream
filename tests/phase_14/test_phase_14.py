"""Unit tests for Phase 14: End-to-end components, reproducibility, and documentation completeness."""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import pytest

from qoe.common.config import get_config
from qoe.data.sim_dims import generate_simulated_dimensions
from qoe.streaming.q0_ingest_clean import process_bronze_playback_batch
from qoe.streaming.q1_session_tracker import process_session_events
from qoe.streaming.q2_qoe_aggregator import compute_qoe_window_aggregates
from qoe.streaming.q3_network_aggregator import compute_network_window_aggregates
from qoe.streaming.q4_ml_scoring import score_early_features_batch
from tests.phase_14.e2e_full import run_e2e_pipeline_verification

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_e2e_full_acceptance(tmp_path):
    """Verify full end-to-end acceptance run produces clean status and valid outputs."""
    res = run_e2e_pipeline_verification(tmp_path / "warehouse")
    assert res["status"] == "PASS"
    assert res["qoe_rows_computed"] > 0
    assert res["network_rows_computed"] > 0
    assert 0.0 <= res["abandon_prob"] <= 1.0


def test_e2e_components(tmp_path):
    """Verify all streaming queries Q0-Q4 can process a mini end-to-end sequence together."""
    # 1. Raw bronze event
    raw_event = {
        "event_id": "e2e-1",
        "session_id": "sess-e2e-1-0",
        "ts": "2026-10-22T10:00:00.000Z",
        "event_type": "play_request",
        "device": "ANDROID",
        "isp": "reliance jio",
        "city": "bombay",
        "video_id": "c1",
    }

    # 2. Q0: Ingest & Clean
    valid_df, rej_df, _ = process_bronze_playback_batch(
        [json.dumps(raw_event)],
        warehouse_dir=tmp_path / "warehouse",
    )
    assert len(valid_df) == 1
    clean_ev = valid_df.iloc[0].to_dict()
    assert clean_ev["device"] == "android"
    assert clean_ev["isp"] == "Jio"
    assert clean_ev["city"] == "Mumbai"

    # 3. Q1: Session Tracker
    ff_event = {
        "event_id": "e2e-2",
        "session_id": "sess-e2e-1-0",
        "ts": "2026-10-22T10:00:01.000Z",
        "event_type": "first_frame",
    }
    state, facts, hrow = process_session_events(
        "sess-e2e-1-0",
        [clean_ev, ff_event],
    )
    assert state.state == "playing"
    assert len(facts) >= 1
    assert facts[0]["kind"] == "startup"

    # 4. Q2: QoE Window Aggregator
    hb_event = {
        "event_id": "e2e-3",
        "session_id": "sess-e2e-1-0",
        "ts": "2026-10-22T10:00:06.000Z",
        "event_type": "heartbeat",
        "interval_ms": 5000.0,
        "bitrate_kbps": 3000.0,
        "device": "android",
        "isp": "Jio",
        "city": "Mumbai",
        "video_id": "c1",
    }
    qoe_rows = compute_qoe_window_aggregates([clean_ev, ff_event, hb_event] + facts)
    assert len(qoe_rows) > 0
    assert any(r["grain"] == "global" for r in qoe_rows)

    # 5. Q3: Network Aggregator
    net_event = {
        "event_id": "e2e-net-1",
        "session_id": "sess-e2e-1-0",
        "ts": "2026-10-22T10:00:06.000Z",
        "throughput_kbps": 4500.0,
        "rtt_ms": 35.0,
        "packet_loss_pct": 0.5,
        "device": "android",
        "isp": "Jio",
        "city": "Mumbai",
    }
    net_rows = compute_network_window_aggregates([net_event])
    assert len(net_rows) > 0

    # 6. Q4: ML Scoring
    early_fact = {
        "schema_version": 1,
        "fact_id": "f-early",
        "session_id": "sess-e2e-1-0",
        "ts": "2026-10-22T10:00:15.000Z",
        "kind": "early_features",
        "startup_ms": 1000.0,
        "stall_count": 0,
        "stall_ms": 0.0,
        "avg_bitrate_kbps": 3000.0,
        "device": "android",
        "isp": "Jio",
        "cdn": "Cloudflare",
        "city": "Mumbai",
        "first_N_s": {"scheme": "bba"},
    }
    preds = score_early_features_batch([early_fact], warehouse_dir=tmp_path / "warehouse")
    assert len(preds) == 1
    assert 0.0 <= preds[0]["abandon_probability"] <= 1.0


def test_reproducibility(tmp_path):
    """Verify pipeline components produce identical results across runs with the same seed."""
    df1 = generate_simulated_dimensions(output_dir=tmp_path / "d1", seed=42)
    df2 = generate_simulated_dimensions(output_dir=tmp_path / "d2", seed=42)

    pd.testing.assert_frame_equal(df1, df2)


def test_docs_complete():
    """Verify all 12 required project documentation deliverables exist and are non-empty."""
    required_docs = [
        "docs/PROGRESS.md",
        "docs/architecture.md",
        "docs/data_profile.md",
        "docs/simulated_fields.md",
        "docs/etl_mapping.md",
        "docs/hbase_design.md",
        "docs/calibration.md",
        "docs/ml_report.md",
        "docs/verification_report.md",
        "docs/performance.md",
        "docs/demo_script.md",
        "docs/report/final_report.md",
    ]

    for doc_rel in required_docs:
        doc_path = REPO_ROOT / doc_rel
        assert doc_path.is_file(), f"Required documentation '{doc_rel}' is missing."
        assert doc_path.stat().st_size > 100, f"Documentation '{doc_rel}' is suspiciously small."

    # Verify screenshots exist
    screenshots_dir = REPO_ROOT / "docs" / "screenshots"
    assert screenshots_dir.is_dir(), "docs/screenshots directory is missing."
    screenshots = list(screenshots_dir.glob("*.png"))
    assert len(screenshots) >= 3, f"Expected at least 3 screenshots, found {len(screenshots)}"

    # Verify honest transparency disclaimer appears across key documents
    disclaimer_keyword = "Stanford Puffer"
    for check_file in [REPO_ROOT / "README.md", REPO_ROOT / "docs" / "report" / "final_report.md"]:
        with open(check_file, "r", encoding="utf-8") as f:
            text = f.read()
        assert disclaimer_keyword in text, f"Missing '{disclaimer_keyword}' disclaimer in {check_file.name}"
