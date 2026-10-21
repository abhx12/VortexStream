"""Unit tests for Phase 8: Windowed QoE and Network Aggregators."""

from __future__ import annotations

from pathlib import Path
import pytest

from qoe.common.hbase_keys import parse_qoe_key, qoe_key, rev_minute
from qoe.streaming.q2_qoe_aggregator import (
    compute_qoe_window_aggregates,
    run_q2_qoe_aggregator,
)
from qoe.streaming.q3_network_aggregator import (
    compute_network_window_aggregates,
    run_q3_network_aggregator,
)
from qoe.streaming.qoe_score import compute_qoe_score


def test_qoe_formula_bounds():
    """Verify QoE score is strictly bounded in [0, 100] across extreme cases."""
    # Perfect playback
    perfect = {
        "avg_startup_s": 0.5,
        "rebuffer_ratio": 0.0,
        "switches_per_min": 0.0,
        "hd_pct": 1.0,
        "vsf_rate": 0.0,
        "error_rate": 0.0,
    }
    res_perfect = compute_qoe_score(perfect)
    assert res_perfect["qoe_score"] == 100.0
    assert res_perfect["qoe_label"] == "Good"

    # Disaster playback
    disaster = {
        "avg_startup_s": 20.0,
        "rebuffer_ratio": 0.80,
        "switches_per_min": 15.0,
        "hd_pct": 0.0,
        "vsf_rate": 0.50,
        "error_rate": 0.50,
    }
    res_disaster = compute_qoe_score(disaster)
    assert res_disaster["qoe_score"] == 0.0
    assert res_disaster["qoe_label"] == "Poor"


def test_qoe_monotonic():
    """Verify QoE score decreases monotonically with worse conditions."""
    base = {
        "avg_startup_s": 1.0,
        "rebuffer_ratio": 0.01,
        "switches_per_min": 0.2,
        "hd_pct": 0.90,
        "vsf_rate": 0.0,
        "error_rate": 0.0,
    }
    base_score = compute_qoe_score(base)["qoe_score"]

    # 1. Higher rebuffering degrades score
    worse_rebuf = dict(base, rebuffer_ratio=0.08)
    assert compute_qoe_score(worse_rebuf)["qoe_score"] < base_score

    # 2. Longer startup delay degrades score
    worse_startup = dict(base, avg_startup_s=6.0)
    assert compute_qoe_score(worse_startup)["qoe_score"] < base_score

    # 3. More bitrate switches degrades score
    worse_switches = dict(base, switches_per_min=1.8)
    assert compute_qoe_score(worse_switches)["qoe_score"] < base_score

    # 4. Failures degrade score
    worse_failures = dict(base, vsf_rate=0.05)
    assert compute_qoe_score(worse_failures)["qoe_score"] < base_score

    # 5. Higher HD percentage improves score
    better_hd = dict(base, hd_pct=1.0)
    assert compute_qoe_score(better_hd)["qoe_score"] > base_score


def test_qoe_labels():
    """Verify Good / Fair / Poor label assignments."""
    assert compute_qoe_score({"hd_pct": 1.0, "rebuffer_ratio": 0.0})["qoe_label"] == "Good"
    # Modest rebuffer to fall into Fair (60-79)
    fair_metrics = {
        "avg_startup_s": 4.0,
        "rebuffer_ratio": 0.05,
        "hd_pct": 0.70,
    }
    fair_res = compute_qoe_score(fair_metrics)
    assert 60.0 <= fair_res["qoe_score"] < 80.0
    assert fair_res["qoe_label"] == "Fair"

    # Poor (< 60)
    poor_metrics = {
        "rebuffer_ratio": 0.15,
        "hd_pct": 0.20,
    }
    poor_res = compute_qoe_score(poor_metrics)
    assert poor_res["qoe_score"] < 60.0
    assert poor_res["qoe_label"] == "Poor"


def test_derived_metrics_zero_division():
    """Verify empty or zero metrics do not raise division by zero."""
    empty_res = compute_qoe_score({})
    assert empty_res["qoe_score"] == 100.0
    assert empty_res["qoe_partial"] is True

    # Zero watch time window
    empty_window = compute_qoe_window_aggregates([])
    assert empty_window == []


def test_multi_grain_window_aggregation():
    """Verify multi-grain explosion and windowed QoE aggregation."""
    events = [
        # Session 1 in Mumbai | Jio on Android
        {
            "session_id": "sess-agg-1",
            "ts": "2026-10-21T10:01:00.000Z",
            "event_type": "play_request",
            "city": "Mumbai",
            "isp": "Jio",
            "cdn": "Cloudflare",
            "device": "android",
            "video_id": "c1",
        },
        {
            "session_id": "sess-agg-1",
            "ts": "2026-10-21T10:01:01.200Z",
            "event_type": "first_frame",
            "startup_ms": 1200.0,
            "city": "Mumbai",
            "isp": "Jio",
            "cdn": "Cloudflare",
            "device": "android",
            "video_id": "c1",
        },
        {
            "session_id": "sess-agg-1",
            "ts": "2026-10-21T10:01:06.000Z",
            "event_type": "heartbeat",
            "interval_ms": 5000.0,
            "bitrate_kbps": 2500,
            "resolution": "1080p",
            "city": "Mumbai",
            "isp": "Jio",
            "cdn": "Cloudflare",
            "device": "android",
            "video_id": "c1",
        },
        {
            "session_id": "sess-agg-1",
            "ts": "2026-10-21T10:01:10.000Z",
            "event_type": "buffer_end",
            "stall_ms": 1000.0,
            "city": "Mumbai",
            "isp": "Jio",
            "cdn": "Cloudflare",
            "device": "android",
            "video_id": "c1",
        },
    ]

    rows = compute_qoe_window_aggregates(events, window_s=300, slide_s=60)
    assert len(rows) > 0

    # Collect grains represented
    grains = {r["grain"] for r in rows}
    assert "global" in grains
    assert "city_isp" in grains
    assert "cdn" in grains
    assert "device" in grains
    assert "channel" in grains

    # Verify global row values
    global_rows = [r for r in rows if r["grain"] == "global"]
    assert len(global_rows) > 0
    g0 = global_rows[0]
    assert g0["stall_count"] == 1
    assert g0["stall_ms_sum"] == 1000.0
    assert g0["watch_ms_sum"] == 5000.0
    assert g0["hd_pct"] == 1.0
    assert g0["qoe_score"] > 0.0

    # Verify city_isp row
    city_isp_rows = [r for r in rows if r["grain"] == "city_isp"]
    assert len(city_isp_rows) > 0
    assert city_isp_rows[0]["dimkey"] == "Mumbai|Jio"


def test_network_window_aggregation():
    """Verify network metrics window aggregation into column family n."""
    net_records = [
        {
            "session_id": "sess-net-1",
            "ts": "2026-10-21T10:02:00.000Z",
            "throughput_kbps": 5000.0,
            "rtt_ms": 40.0,
            "packet_loss_pct": 0.5,
            "city": "Delhi",
            "isp": "Airtel",
            "cdn": "Fastly",
            "device": "smarttv",
        },
        {
            "session_id": "sess-net-2",
            "ts": "2026-10-21T10:02:30.000Z",
            "throughput_kbps": 3000.0,
            "rtt_ms": 60.0,
            "packet_loss_pct": 1.5,
            "city": "Delhi",
            "isp": "Airtel",
            "cdn": "Fastly",
            "device": "smarttv",
        },
    ]

    rows = compute_network_window_aggregates(net_records, window_s=300, slide_s=60)
    assert len(rows) > 0

    delhi_rows = [r for r in rows if r["grain"] == "city_isp" and r["dimkey"] == "Delhi|Airtel"]
    assert len(delhi_rows) > 0
    d0 = delhi_rows[0]
    assert d0["avg_throughput_kbps"] == 4000.0
    assert d0["avg_rtt_ms"] == 50.0
    assert d0["avg_packet_loss_pct"] == 1.0
    assert d0["net_samples"] == 2


def test_hbase_qoe_keys():
    """Verify newest-first ordering and prefix isolation in HBase qoe_keys."""
    minute_100 = 100
    minute_105 = 105

    key_100 = qoe_key("city_isp", "Chennai|Airtel", minute_100)
    key_105 = qoe_key("city_isp", "Chennai|Airtel", minute_105)

    # In ASCII / byte sorting, key_105 must sort BEFORE key_100 (newest first)
    assert key_105 < key_100

    # Parsers correctly round-trip
    grain, dimkey, min_parsed = parse_qoe_key(key_105)
    assert grain == "city_isp"
    assert dimkey == "Chennai|Airtel"
    assert min_parsed == minute_105

    # Prefix isolation with delimiter '#'
    prefix = "city_isp#Chennai|Airtel#"
    assert key_105.startswith(prefix)

    neighbor_key = qoe_key("city_isp", "Chennai|Airtel2", minute_105)
    assert not neighbor_key.startswith(prefix)
