"""Unit tests for Phase 6: Spark Q0 parse, validate, standardize, dedup, and enrich."""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import pytest

from qoe.streaming.enrich import enrich_with_reference_data
from qoe.streaming.parse import parse_json_record
from qoe.streaming.q0_ingest_clean import process_bronze_playback_batch
from qoe.streaming.standardize import standardize_dimensions
from qoe.streaming.validate import check_record_validity, validate_dataframe


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_validate_each_rule():
    """Verify each validation rule correctly tags the expected rejection reason."""
    base_valid = {
        "event_id": "e-test-1",
        "session_id": "sess-01-0",
        "ts": "2026-10-21T10:00:00.000Z",
        "event_type": "heartbeat",
        "buffer_ms": 2000.0,
        "bitrate_kbps": 1800,
        "interval_ms": 5000.0,
    }
    assert check_record_validity(base_valid, "playback") is None

    # 1. MISSING_SESSION_ID
    assert check_record_validity(dict(base_valid, session_id=""), "playback") == "MISSING_SESSION_ID"
    assert check_record_validity(dict(base_valid, session_id=None), "playback") == "MISSING_SESSION_ID"

    # 2. MISSING_EVENT_TYPE
    assert check_record_validity(dict(base_valid, event_type=""), "playback") == "MISSING_EVENT_TYPE"

    # 3. UNKNOWN_EVENT_TYPE
    assert check_record_validity(dict(base_valid, event_type="invalid_action"), "playback") == "UNKNOWN_EVENT_TYPE"

    # 4. BAD_TIMESTAMP
    assert check_record_validity(dict(base_valid, ts="not-a-timestamp"), "playback") == "BAD_TIMESTAMP"

    # 5. NEGATIVE_BUFFER
    assert check_record_validity(dict(base_valid, buffer_ms=-100.0), "playback") == "NEGATIVE_BUFFER"

    # 6. NEGATIVE_STALL
    stall_ev = dict(base_valid, event_type="buffer_end", stall_ms=-50.0)
    assert check_record_validity(stall_ev, "playback") == "NEGATIVE_STALL"

    # 7. IMPOSSIBLE_BITRATE
    assert check_record_validity(dict(base_valid, bitrate_kbps=99999999), "playback") == "IMPOSSIBLE_BITRATE"
    assert check_record_validity(dict(base_valid, bitrate_kbps=-10), "playback") == "IMPOSSIBLE_BITRATE"

    # 8. BAD_RTT/THROUGHPUT/LOSS
    net_valid = {
        "event_id": "net-1",
        "session_id": "s-1-0",
        "ts": "2026-10-21T10:00:00.000Z",
        "throughput_kbps": 2000.0,
        "rtt_ms": 50.0,
        "packet_loss_pct": 1.2,
    }
    assert check_record_validity(net_valid, "network") is None
    assert check_record_validity(dict(net_valid, throughput_kbps=-1.0), "network") == "BAD_RTT/THROUGHPUT/LOSS"
    assert check_record_validity(dict(net_valid, rtt_ms=-5.0), "network") == "BAD_RTT/THROUGHPUT/LOSS"
    assert check_record_validity(dict(net_valid, packet_loss_pct=150.0), "network") == "BAD_RTT/THROUGHPUT/LOSS"

    # 9. MISSING_REQUIRED_FIELD
    hb_missing_buf = dict(base_valid, buffer_ms=None)
    assert check_record_validity(hb_missing_buf, "playback") == "MISSING_REQUIRED_FIELD"

    err_missing_code = dict(base_valid, event_type="error", error_code=None)
    assert check_record_validity(err_missing_code, "playback") == "MISSING_REQUIRED_FIELD"


def test_standardize_aliases():
    """Verify alias mapping standardizes dirty casing and whitespace variants."""
    df = pd.DataFrame([
        {
            "device": "ANDROID ",
            "isp": "bharti airtel",
            "city": "Chennai ",
            "country": "in",
            "resolution": "1280x720",
        },
        {
            "device": "smart tv",
            "isp": "reliance jio",
            "city": "bangalore",
            "country": "india",
            "resolution": "480p",
        },
    ])

    clean = standardize_dimensions(df)

    assert clean.iloc[0]["device"] == "android"
    assert clean.iloc[0]["isp"] == "Airtel"
    assert clean.iloc[0]["city"] == "Chennai"
    assert clean.iloc[0]["country"] == "IN"
    assert clean.iloc[0]["resolution"] == "720p"

    assert clean.iloc[1]["device"] == "smarttv"
    assert clean.iloc[1]["isp"] == "Jio"
    assert clean.iloc[1]["city"] == "Bengaluru"
    assert clean.iloc[1]["country"] == "IN"
    assert clean.iloc[1]["resolution"] == "480p"


def test_enrich_left_join_keeps_rows(tmp_path):
    """Verify enrichment preserves exact row count even with missing or unknown keys."""
    df = pd.DataFrame([
        {"event_id": "e-1", "video_id": "cbs", "user_id": "u-00001"},
        {"event_id": "e-2", "video_id": "unknown_video", "user_id": "u-99999"},
    ])

    enriched = enrich_with_reference_data(df, ref_dir=REPO_ROOT / "data" / "ref")
    assert len(enriched) == 2, "Row count must remain identical after left join"
    assert "content_genre" in enriched.columns or "plan_tier" in enriched.columns


def test_parse_bad_json_to_quarantine():
    """Verify unparsable JSON payload is caught with reason PARSE_ERROR."""
    bad_json = "NOT_A_VALID_JSON{{"
    parsed = parse_json_record(bad_json)
    assert "_corrupt_record" in parsed
    reason = check_record_validity(parsed, "playback")
    assert reason == "PARSE_ERROR"


def test_pipeline_reconciliation(tmp_path):
    """Verify end-to-end accounting: in_count == valid + quarantined + deduped."""
    warehouse_tmp = tmp_path / "warehouse"
    records = [
        # 1. Valid record
        json.dumps({
            "event_id": "e-rec-1",
            "session_id": "s-1-0",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "heartbeat",
            "buffer_ms": 2000.0,
            "bitrate_kbps": 1500,
            "interval_ms": 5000.0,
            "device": "android",
            "isp": "Airtel",
            "city": "Chennai",
            "country": "IN",
        }),
        # 2. Duplicate of record 1
        json.dumps({
            "event_id": "e-rec-1",
            "session_id": "s-1-0",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "heartbeat",
            "buffer_ms": 2000.0,
            "bitrate_kbps": 1500,
            "interval_ms": 5000.0,
            "device": "android",
            "isp": "Airtel",
            "city": "Chennai",
            "country": "IN",
        }),
        # 3. Invalid record (negative buffer)
        json.dumps({
            "event_id": "e-rec-2",
            "session_id": "s-2-0",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "heartbeat",
            "buffer_ms": -500.0,
            "bitrate_kbps": 1500,
            "interval_ms": 5000.0,
        }),
    ]

    valid_df, rej_df, counters = process_bronze_playback_batch(
        records,
        warehouse_dir=warehouse_tmp,
        ref_dir=REPO_ROOT / "data" / "ref",
    )

    assert counters["in_count"] == 3
    assert counters["valid"] == 1
    assert counters["quarantined"] == 1
    assert counters["deduped"] == 1
    assert counters["in_count"] == (counters["valid"] + counters["quarantined"] + counters["deduped"])
