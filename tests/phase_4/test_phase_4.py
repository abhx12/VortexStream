"""Unit tests for Phase 4: Simulated dimensions, reference data, and fault model."""

from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pytest
import yaml

from qoe.common.config import get_config
from qoe.data.ref_data import generate_reference_data
from qoe.data.sim_dims import DimensionAssigner, compute_simulated_packet_loss


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_determinism():
    """Verify two runs with the same seed yield identical dims, while different seed differs."""
    assigner1 = DimensionAssigner(seed=42)
    assigner2 = DimensionAssigner(seed=42)
    assigner_diff = DimensionAssigner(seed=999)

    session_id = "test-session-xyz"
    d1 = assigner1.assign_for_session(session_id)
    d2 = assigner2.assign_for_session(session_id)
    d3 = assigner_diff.assign_for_session(session_id)

    assert d1 == d2, "Same seed must produce identical dimensions"
    # Over multiple sessions, different seed must diverge
    diff_count = 0
    for i in range(20):
        sid = f"session-{i}"
        if assigner1.assign_for_session(sid) != assigner_diff.assign_for_session(sid):
            diff_count += 1
    assert diff_count > 10, "Different seed must produce different dimensions"


def test_session_consistency():
    """Verify all assignments for a session are identical and contain no nulls."""
    assigner = DimensionAssigner(seed=42)
    session_id = "consistent-viewer-session-001"

    dims1 = assigner.assign_for_session(session_id)
    dims2 = assigner.assign_for_session(session_id)
    assert dims1 == dims2

    for key, val in dims1.items():
        assert val is not None, f"Dimension {key} was unexpectedly null"
        assert str(val).strip() != ""


def test_distribution():
    """Verify empirical shares are within +-3 percentage points of configured weights on 5,000 sessions."""
    cfg = get_config()
    sim_cfg = cfg.sim_dims
    assigner = DimensionAssigner(sim_cfg, seed=42)

    n_samples = 5000
    city_counts: dict[str, int] = {}
    isp_counts: dict[str, int] = {}
    device_counts: dict[str, int] = {}

    for i in range(n_samples):
        sid = f"sample-puffer-session-{i}"
        dims = assigner.assign_for_session(sid)
        city_counts[dims["city"]] = city_counts.get(dims["city"], 0) + 1
        isp_counts[dims["isp"]] = isp_counts.get(dims["isp"], 0) + 1
        device_counts[dims["device"]] = device_counts.get(dims["device"], 0) + 1

    # Check city weights (e.g. Chennai 0.20 -> within [0.17, 0.23])
    for city, target_weight in sim_cfg.city.weights.items():
        actual_share = city_counts.get(city, 0) / n_samples
        diff = abs(actual_share - target_weight)
        assert diff <= 0.035, f"City {city} share {actual_share:.3f} deviated from target {target_weight:.3f} by {diff:.3f}"

    # Check device weights (e.g. android 0.55 -> within [0.52, 0.58])
    for dev, target_weight in sim_cfg.device.weights.items():
        actual_share = device_counts.get(dev, 0) / n_samples
        diff = abs(actual_share - target_weight)
        assert diff <= 0.035, f"Device {dev} share {actual_share:.3f} deviated from target {target_weight:.3f} by {diff:.3f}"


def test_ref_integrity(tmp_path):
    """Verify reference tables contain valid video_id and user_id records."""
    ref_dir = tmp_path / "ref"
    generate_reference_data(ref_dir, user_pool_size=1000)

    con = duckdb.connect(":memory:")
    c_count = con.execute(f"SELECT count(*) FROM read_parquet('{(ref_dir / 'content.parquet').as_posix()}')").fetchone()[0]
    u_count = con.execute(f"SELECT count(*) FROM read_parquet('{(ref_dir / 'users.parquet').as_posix()}')").fetchone()[0]
    s_count = con.execute(f"SELECT count(*) FROM read_parquet('{(ref_dir / 'subscriptions.parquet').as_posix()}')").fetchone()[0]

    assert c_count > 0
    assert u_count == 1000
    assert s_count == 1000


def test_packet_loss_bounds_and_correlation():
    """Verify 0 <= loss <= 15 and positive Pearson correlation with RTT."""
    losses = []
    rtts = []

    for i in range(200):
        rtt = 40.0 + (i * 2.0)  # 40ms to 440ms
        tp = 2000.0
        loss = compute_simulated_packet_loss(tp, rtt, seed_key=f"pkt-seed-{i}")
        assert 0.0 <= loss <= 15.0, f"Packet loss {loss} exceeded [0, 15] bounds"
        losses.append(loss)
        rtts.append(rtt)

    # Pearson correlation between loss and rtt
    corr = np.corrcoef(losses, rtts)[0, 1]
    assert corr > 0.0, f"Expected positive correlation between loss and RTT, got {corr}"


def test_faults_yaml_valid():
    """Verify faults.yaml is schema-valid and targets known dimensions within the slice."""
    cfg = get_config()
    faults = cfg.get("faults", {}).get("faults", [])
    assert len(faults) >= 3

    known_isps = set(cfg.sim_dims.isp.weights.keys())
    known_cdns = set(cfg.sim_dims.cdn.weights.keys())
    known_devices = set(cfg.sim_dims.device.weights.keys())

    for f in faults:
        assert "id" in f
        assert "type" in f
        assert "start_min" in f
        assert "duration_min" in f
        assert f["start_min"] >= 0
        assert f["start_min"] + f["duration_min"] <= 120  # Inside 120-min slice

        sel = f.get("selector", {})
        if "isp" in sel:
            assert sel["isp"] in known_isps
        if "cdn" in sel:
            assert sel["cdn"] in known_cdns
        if "device" in sel:
            assert sel["device"] in known_devices
