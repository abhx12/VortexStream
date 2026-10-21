"""Unit tests for Phase 7: Stateful session tracker."""

from __future__ import annotations

import json
from pathlib import Path
import jsonschema
import pytest

from qoe.streaming.q1_session_tracker import (
    SessionState,
    check_session_timeouts,
    process_session_event,
    process_session_events,
    run_q1_session_tracker,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def fact_schema():
    schema_path = REPO_ROOT / "schemas" / "session_fact.schema.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def test_normal_session_lifecycle(fact_schema):
    """Test normal play_request -> first_frame -> heartbeats -> session_end sequence."""
    events = [
        {
            "event_id": "e1",
            "session_id": "sess-test-1",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "play_request",
            "device": "android",
            "isp": "Jio",
            "cdn": "Cloudflare",
            "city": "Mumbai",
        },
        {
            "event_id": "e2",
            "session_id": "sess-test-1",
            "ts": "2026-10-21T10:00:01.500Z",
            "event_type": "first_frame",
        },
        {
            "event_id": "e3",
            "session_id": "sess-test-1",
            "ts": "2026-10-21T10:00:06.500Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 2400,
        },
        {
            "event_id": "e4",
            "session_id": "sess-test-1",
            "ts": "2026-10-21T10:00:11.500Z",
            "event_type": "session_end",
        },
    ]

    state, facts, hrow = process_session_events(
        session_id="sess-test-1",
        events=events,
    )

    assert state.state == "ended"
    assert state.startup_ms == pytest.approx(1500.0, abs=1.0)
    assert state.watch_ms == 5000.0
    assert state.stall_count == 0

    # Validate emitted facts
    assert len(facts) == 2
    startup_fact, end_fact = facts[0], facts[1]

    assert startup_fact["kind"] == "startup"
    assert startup_fact["startup_ms"] == pytest.approx(1500.0, abs=1.0)
    assert startup_fact["vsf"] is False
    assert startup_fact["device"] == "android"

    assert end_fact["kind"] == "session_end"
    assert end_fact["watch_ms"] == 5000.0

    for f in facts:
        jsonschema.validate(instance=f, schema=fact_schema)

    # Check HBase row format
    assert hrow["session_id"] == "sess-test-1"
    assert hrow["state"] == "ended"
    assert hrow["device"] == "android"
    assert hrow["city"] == "Mumbai"


def test_session_with_stalls(fact_schema):
    """Test session with buffering events updating stall counts and duration."""
    events = [
        {
            "event_id": "e1",
            "session_id": "sess-test-2",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "play_request",
            "device": "smarttv",
        },
        {
            "event_id": "e2",
            "session_id": "sess-test-2",
            "ts": "2026-10-21T10:00:01.000Z",
            "event_type": "first_frame",
        },
        {
            "event_id": "e3",
            "session_id": "sess-test-2",
            "ts": "2026-10-21T10:00:05.000Z",
            "event_type": "buffer_start",
        },
        {
            "event_id": "e4",
            "session_id": "sess-test-2",
            "ts": "2026-10-21T10:00:07.500Z",
            "event_type": "buffer_end",
            "stall_ms": 2500.0,
        },
        {
            "event_id": "e5",
            "session_id": "sess-test-2",
            "ts": "2026-10-21T10:00:12.500Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 1200,
        },
    ]

    state, facts, hrow = process_session_events("sess-test-2", events)
    assert state.state == "playing"
    assert state.stall_count == 1
    assert state.stall_ms == 2500.0
    assert hrow["stall_count"] == 1
    assert hrow["stall_ms"] == 2500.0


def test_timeout_vsf(fact_schema):
    """Test video start failure (VSF) triggered by timeout when first_frame is missing."""
    events = [
        {
            "event_id": "e1",
            "session_id": "sess-test-vsf-1",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "play_request",
            "isp": "Airtel",
            "city": "Chennai",
        }
    ]

    # Process play_request, current time is 15 seconds later (vsf_timeout_s=10)
    from qoe.streaming.q1_session_tracker import _parse_iso_ts
    req_epoch = _parse_iso_ts("2026-10-21T10:00:00.000Z")

    state, facts, hrow = process_session_events(
        "sess-test-vsf-1",
        events,
        current_time_epoch_s=req_epoch + 15.0,
    )

    assert state.state == "failed_start"
    assert len(facts) == 1
    vsf_fact = facts[0]
    assert vsf_fact["kind"] == "vsf"
    assert vsf_fact["vsf"] is True
    assert vsf_fact["isp"] == "Airtel"

    jsonschema.validate(instance=vsf_fact, schema=fact_schema)


def test_early_features_emission(fact_schema):
    """Test that early_features is emitted exactly once when watch_ms reaches early_window_s (15s)."""
    events = [
        {
            "event_id": "e1",
            "session_id": "sess-test-early-1",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "play_request",
            "device": "web",
            "scheme": "bba",
        },
        {
            "event_id": "e2",
            "session_id": "sess-test-early-1",
            "ts": "2026-10-21T10:00:01.000Z",
            "event_type": "first_frame",
        },
        # 3 heartbeats x 5000ms = 15,000ms (15s)
        {
            "event_id": "e3",
            "session_id": "sess-test-early-1",
            "ts": "2026-10-21T10:00:06.000Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 3000,
        },
        {
            "event_id": "e4",
            "session_id": "sess-test-early-1",
            "ts": "2026-10-21T10:00:11.000Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 3000,
        },
        {
            "event_id": "e5",
            "session_id": "sess-test-early-1",
            "ts": "2026-10-21T10:00:16.000Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 3000,
        },
        # 4th heartbeat (should not emit early_features again)
        {
            "event_id": "e6",
            "session_id": "sess-test-early-1",
            "ts": "2026-10-21T10:00:21.000Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 3000,
        },
    ]

    state, facts, _ = process_session_events("sess-test-early-1", events)

    early_facts = [f for f in facts if f["kind"] == "early_features"]
    assert len(early_facts) == 1
    early = early_facts[0]
    assert early["watch_ms"] >= 15000.0
    assert early["startup_ms"] == pytest.approx(1000.0, abs=1.0)
    assert "first_N_s" in early
    assert early["first_N_s"]["scheme"] == "bba"

    jsonschema.validate(instance=early, schema=fact_schema)


def test_edge_cases_and_out_of_order(fact_schema):
    """Test edge cases: out-of-order first_frame, duplicate buffer_end, error flag."""
    # 1. Out-of-order first_frame before play_request
    state = SessionState(session_id="sess-edge-1")
    facts1 = process_session_event(state, {
        "event_id": "e1",
        "session_id": "sess-edge-1",
        "ts": "2026-10-21T10:00:01.000Z",
        "event_type": "first_frame",
    })
    assert state.state == "playing"
    assert state.startup_ms == 0.0
    assert len(facts1) == 1

    # 2. Duplicate buffer_end without buffer_start
    facts2 = process_session_event(state, {
        "event_id": "e2",
        "session_id": "sess-edge-1",
        "ts": "2026-10-21T10:00:03.000Z",
        "event_type": "buffer_end",
        "stall_ms": 100.0,
    })
    assert state.state == "playing"
    assert state.stall_count == 1

    # 3. Error event
    process_session_event(state, {
        "event_id": "e3",
        "session_id": "sess-edge-1",
        "ts": "2026-10-21T10:00:04.000Z",
        "event_type": "error",
    })
    assert state.error_flag is True



def test_batch_execution(tmp_path):
    """Test run_q1_session_tracker executing on sample event batch."""
    events = [
        {
            "event_id": "b1",
            "session_id": "batch-sess-1",
            "ts": "2026-10-21T10:00:00.000Z",
            "event_type": "play_request",
            "device": "ios",
        },
        {
            "event_id": "b2",
            "session_id": "batch-sess-1",
            "ts": "2026-10-21T10:00:01.200Z",
            "event_type": "first_frame",
        },
        {
            "event_id": "b3",
            "session_id": "batch-sess-1",
            "ts": "2026-10-21T10:00:05.000Z",
            "event_type": "session_end",
        },
    ]

    facts, hrows = run_q1_session_tracker(
        input_events=events,
        output_warehouse_dir=str(tmp_path / "warehouse" / "session_facts"),
    )

    assert len(facts) >= 2
    assert len(hrows) == 1
    assert hrows[0]["session_id"] == "batch-sess-1"
    assert hrows[0]["device"] == "ios"
