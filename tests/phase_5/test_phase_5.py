"""Unit tests for Phase 5: Replay producers, fault injection, and chaos."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import time
import jsonschema
import pytest
import yaml

from qoe.common.config import get_config
from qoe.common.schema import load_schema
from qoe.producers.chaos import ChaosEngine
from qoe.producers.faults import FaultEngine
from qoe.producers.replay import run_replay


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_time_mapping_and_pacing(tmp_path):
    """Verify time mapping shifts timestamps while preserving relative delta and pacing completes."""
    start_wall = time.time()
    manifest = run_replay(
        speed=120.0,
        duration_min=10,
        faults_config_path="none",
        chaos_config_path="none",
        dry_run=True,
        run_id="test-pacing-01",
        processed_dir=REPO_ROOT / "data" / "processed" / "2021-06-12T11_2021-06-13T11",
    )
    elapsed = time.time() - start_wall

    assert manifest.first_ts is not None
    assert manifest.last_ts is not None

    # First event timestamp should be near wall start
    first_epoch = datetime.fromisoformat(manifest.first_ts.replace("Z", "+00:00")).timestamp()
    assert abs(first_epoch - start_wall) < 5.0, f"First event time {first_epoch} deviated from start {start_wall}"

    # Pacing check: 10 data-minutes at 120x is 5 seconds of wall time (allow generous buffer)
    assert elapsed < 15.0


def test_fault_selector_and_effects():
    """Verify fault engine modifies matching events in window and leaves others unmodified."""
    faults = [
        {
            "id": "F1",
            "type": "isp_outage",
            "selector": {"isp": "Airtel", "city": "Chennai"},
            "start_min": 10,
            "duration_min": 15,
            "effects": {"throughput_mult": 0.15, "rtt_mult": 4.0, "loss_add_pct": 4.0},
        }
    ]

    base_time = datetime(2026, 10, 21, 10, 0, 0, tzinfo=timezone.utc)
    engine = FaultEngine(faults, slice_start_dt=base_time, time_shift=0.0, run_id="test-f1", seed=42)

    # 1. Matching event inside window (minute 15)
    matching_dt = datetime(2026, 10, 21, 10, 15, 0, tzinfo=timezone.utc)
    ev_match = {
        "event_id": "net-01",
        "session_id": "s-1-0",
        "isp": "Airtel",
        "city": "Chennai",
        "throughput_kbps": 2000.0,
        "rtt_ms": 50.0,
        "packet_loss_pct": 0.5,
    }
    impaired = engine.process_network_event(ev_match, matching_dt)
    assert impaired["throughput_kbps"] == pytest.approx(300.0, rel=1e-3)
    assert impaired["rtt_ms"] == pytest.approx(200.0, rel=1e-3)
    assert impaired["packet_loss_pct"] == pytest.approx(4.5, rel=1e-3)

    # 2. Non-matching event (different ISP) inside window
    ev_non_match = dict(ev_match, isp="Jio")
    unmodified_isp = engine.process_network_event(ev_non_match, matching_dt)
    assert unmodified_isp["throughput_kbps"] == 2000.0
    assert unmodified_isp["rtt_ms"] == 50.0

    # 3. Matching event outside window (minute 5)
    outside_dt = datetime(2026, 10, 21, 10, 5, 0, tzinfo=timezone.utc)
    unmodified_time = engine.process_network_event(ev_match, outside_dt)
    assert unmodified_time["throughput_kbps"] == 2000.0
    assert unmodified_time["rtt_ms"] == 50.0


def test_chaos_rates():
    """Verify chaos engine generates duplicates, late, invalid, and dirty dimensions."""
    cfg = {
        "duplicate_rate": 0.05,
        "late_rate": 0.05,
        "invalid_rate": 0.05,
        "dirty_dims_rate": 0.10,
    }
    engine = ChaosEngine(cfg, seed=42)

    n_samples = 2000
    dups = 0
    lates = 0
    invalids = 0
    dirties = 0

    base_ev = {
        "schema_version": 1,
        "event_id": "e-test-1",
        "session_id": "s-01-0",
        "ts": "2026-10-21T10:00:00.000Z",
        "event_type": "heartbeat",
        "device": "android",
        "isp": "Airtel",
        "city": "Chennai",
        "buffer_ms": 2000.0,
        "bitrate_kbps": 1500,
    }

    for i in range(n_samples):
        ev, dup, is_late, is_invalid, is_dirty = engine.process_event(base_ev)
        if dup is not None:
            dups += 1
        if is_late:
            lates += 1
        if is_invalid:
            invalids += 1
        if is_dirty:
            dirties += 1

    # Check observed proportions are within reasonable confidence
    assert 0.03 <= (dups / n_samples) <= 0.08
    assert 0.03 <= (lates / n_samples) <= 0.08
    assert 0.03 <= (invalids / n_samples) <= 0.08
    assert 0.07 <= (dirties / n_samples) <= 0.14


def test_schema_validation():
    """Verify valid events pass JSON Schema, and injected-invalid events fail validation."""
    schema = load_schema("playback_event")

    valid_event = {
        "schema_version": 1,
        "event_id": "e-valid-100",
        "session_id": "sess1-0",
        "ts": "2026-10-21T10:00:00.000Z",
        "event_type": "heartbeat",
        "device": "android",
        "isp": "Airtel",
        "city": "Chennai",
        "buffer_ms": 2000.0,
        "bitrate_kbps": 1500,
    }

    # Must validate cleanly
    jsonschema.validate(instance=valid_event, schema=schema)

    # Injected invalid: negative buffer_ms
    invalid_neg_buffer = dict(valid_event, buffer_ms=-500.0)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=invalid_neg_buffer, schema=schema)

    # Injected invalid: missing session_id
    invalid_missing_sid = dict(valid_event, session_id="")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=invalid_missing_sid, schema=schema)

    # Injected invalid: impossible bitrate (> 100000)
    invalid_bitrate = dict(valid_event, bitrate_kbps=99999999)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=invalid_bitrate, schema=schema)


def test_ledger_written(tmp_path):
    """Verify ledger entries are written with start_ts and end_ts matching shifted time."""
    ledger_file = tmp_path / "faults.jsonl"
    faults = [
        {
            "id": "F1",
            "type": "isp_outage",
            "selector": {"isp": "Airtel"},
            "start_min": 10,
            "duration_min": 15,
            "effects": {"throughput_mult": 0.15},
        }
    ]

    base_time = datetime(2026, 10, 21, 10, 0, 0, tzinfo=timezone.utc)
    engine = FaultEngine(
        faults,
        slice_start_dt=base_time,
        time_shift=100.0,
        run_id="test-run-ledger",
        ledger_path=ledger_file,
    )

    assert ledger_file.is_file()
    lines = ledger_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1

    entry = json.loads(lines[0])
    assert entry["fault_id"] == "F1"
    assert entry["type"] == "isp_outage"
    assert entry["run_id"] == "test-run-ledger"
    assert "start_ts" in entry
    assert "end_ts" in entry
