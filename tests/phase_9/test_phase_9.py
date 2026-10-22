"""Unit tests for Phase 9: Alerting rules, state machine, and evaluation."""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from qoe.streaming.alerting import AlertEvaluator, process_alerts
from qoe.streaming.calibrate import calibrate_thresholds
from qoe.verify.eval_alerts import evaluate_alerts


def test_r1_rebuffer_hysteresis():
    """Verify R1 alert fires only after consecutive breaches and resolves after consecutive healthy windows."""
    evaluator = AlertEvaluator()
    # Baseline window: healthy (sessions >= 20)
    w0 = {
        "grain": "city_isp",
        "dimkey": "Chennai|Airtel",
        "sessions": 50,
        "rebuffer_ratio": 0.01,
        "vsf_rate": 0.0,
        "error_rate": 0.0,
        "qoe_score": 90.0,
        "window_end": "2026-10-22T10:00:00.000Z",
    }
    assert evaluator.evaluate_window(w0) == []

    # Window 1: Breach 1 (threshold is 0.03, value 0.06) -> hysteresis requires 2 breaches
    w1 = dict(w0, rebuffer_ratio=0.06, window_end="2026-10-22T10:01:00.000Z")
    alerts1 = evaluator.evaluate_window(w1)
    assert len(alerts1) == 0

    # Window 2: Breach 2 -> fires OPEN alert
    w2 = dict(w0, rebuffer_ratio=0.08, window_end="2026-10-22T10:02:00.000Z")
    alerts2 = evaluator.evaluate_window(w2)
    assert len(alerts2) == 1
    a_open = alerts2[0]
    assert a_open["rule"] == "R1"
    assert a_open["status"] == "open"
    assert a_open["severity"] in ("warn", "critical")
    assert a_open["dimkey"] == "Chennai|Airtel"

    # Window 3: Healthy 1 (rebuffer_ratio = 0.01) -> still open
    w3 = dict(w0, rebuffer_ratio=0.01, window_end="2026-10-22T10:03:00.000Z")
    alerts3 = evaluator.evaluate_window(w3)
    assert len(alerts3) == 0

    # Window 4: Healthy 2 -> fires RESOLVED alert
    w4 = dict(w0, rebuffer_ratio=0.01, window_end="2026-10-22T10:04:00.000Z")
    alerts4 = evaluator.evaluate_window(w4)
    assert len(alerts4) == 1
    a_res = alerts4[0]
    assert a_res["rule"] == "R1"
    assert a_res["status"] == "resolved"
    assert a_res["alert_id"] == a_open["alert_id"]


def test_r2_qoe_drop():
    """Verify R2 fires when QoE drops >= 15 points vs the baseline window 5 minutes earlier."""
    evaluator = AlertEvaluator()

    # t = 0 min (10:00): QoE = 95.0
    w0 = {
        "grain": "cdn",
        "dimkey": "Akamai",
        "sessions": 100,
        "rebuffer_ratio": 0.005,
        "vsf_rate": 0.0,
        "error_rate": 0.0,
        "qoe_score": 95.0,
        "window_end": "2026-10-22T10:00:00.000Z",
    }
    evaluator.evaluate_window(w0)

    # Intermediate windows 10:01 - 10:04
    for m in range(1, 5):
        w = dict(w0, window_end=f"2026-10-22T10:0{m}:00.000Z")
        evaluator.evaluate_window(w)

    # 10:05 (5 min lag): QoE drops to 70.0 (drop of 25 >= 15 points) -> breach 1
    w5 = dict(w0, qoe_score=70.0, window_end="2026-10-22T10:05:00.000Z")
    evaluator.evaluate_window(w5)

    # 10:06: QoE drops to 68.0 -> breach 2 -> fires R2 open
    w6 = dict(w0, qoe_score=68.0, window_end="2026-10-22T10:06:00.000Z")
    alerts6 = evaluator.evaluate_window(w6)
    r2_alerts = [a for a in alerts6 if a["rule"] == "R2"]
    assert len(r2_alerts) == 1
    assert r2_alerts[0]["status"] == "open"
    assert r2_alerts[0]["baseline"] == 95.0


def test_min_sessions_guard():
    """Verify segments with sessions < min_sessions are ignored."""
    evaluator = AlertEvaluator()
    w_small = {
        "grain": "device",
        "dimkey": "android",
        "sessions": 5,  # < min_sessions (20)
        "rebuffer_ratio": 0.50,
        "vsf_rate": 0.50,
        "error_rate": 0.50,
        "qoe_score": 10.0,
        "window_end": "2026-10-22T10:00:00.000Z",
    }
    assert evaluator.evaluate_window(w_small) == []


def test_cooldown():
    """Verify alert does not immediately re-open within cooldown window."""
    evaluator = AlertEvaluator()
    # Sequence to open and resolve an R3 alert
    w0 = {"grain": "global", "dimkey": "ALL", "sessions": 50, "vsf_rate": 0.25, "window_end": "2026-10-22T10:00:00.000Z"}
    w1 = {"grain": "global", "dimkey": "ALL", "sessions": 50, "vsf_rate": 0.25, "window_end": "2026-10-22T10:01:00.000Z"}
    evaluator.evaluate_window(w0)
    alerts_open = evaluator.evaluate_window(w1)
    assert len(alerts_open) >= 1

    # Resolve it
    w2 = {"grain": "global", "dimkey": "ALL", "sessions": 50, "vsf_rate": 0.01, "window_end": "2026-10-22T10:02:00.000Z"}
    w3 = {"grain": "global", "dimkey": "ALL", "sessions": 50, "vsf_rate": 0.01, "window_end": "2026-10-22T10:03:00.000Z"}
    evaluator.evaluate_window(w2)
    alerts_res = evaluator.evaluate_window(w3)
    assert len(alerts_res) >= 1

    # Immediate new breach at 10:04 within 10 min cooldown
    w4 = {"grain": "global", "dimkey": "ALL", "sessions": 50, "vsf_rate": 0.25, "window_end": "2026-10-22T10:04:00.000Z"}
    w5 = {"grain": "global", "dimkey": "ALL", "sessions": 50, "vsf_rate": 0.25, "window_end": "2026-10-22T10:05:00.000Z"}
    evaluator.evaluate_window(w4)
    alerts_cooldown = evaluator.evaluate_window(w5)
    assert len(alerts_cooldown) == 0


def test_calibrate_and_evaluate(tmp_path):
    """Verify calibration script output and alert evaluation report."""
    doc_path = tmp_path / "calibration.md"
    thresh = calibrate_thresholds(output_doc=doc_path)
    assert doc_path.is_file()
    assert "rebuffer_ratio_threshold" in thresh

    # Test evaluate_alerts
    fake_faults = tmp_path / "faults.jsonl"
    with open(fake_faults, "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "fault_id": "F1",
            "selector": {"city": "Chennai", "isp": "Airtel"},
            "start_ts": "2026-10-22T10:00:00.000Z",
            "end_ts": "2026-10-22T10:15:00.000Z",
        }) + "\n")

    fake_alerts = [
        {
            "alert_id": "alt-test-1",
            "rule": "R1",
            "grain": "city_isp",
            "dimkey": "Chennai|Airtel",
            "status": "open",
            "window_end": "2026-10-22T10:04:00.000Z",
        }
    ]

    report = evaluate_alerts(ledger_path=fake_faults, alerts_list=fake_alerts)
    assert report["detected_faults"] == 1
    assert report["recall"] == 1.0
    assert report["per_fault"]["F1"]["detected"] is True
