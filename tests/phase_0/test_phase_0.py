"""Unit tests for Phase 0: Bootstrap & conventions."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest
import yaml

from qoe.common.config import get_config, load_config
from qoe.common.logging import JsonFormatter, setup_logging


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_config_loads():
    """Verify config loads all YAML files, weights sum to 100, and thresholds are in range."""
    cfg = load_config()

    # 1. Main sections exist
    assert "kafka" in cfg
    assert "replay" in cfg
    assert "streaming" in cfg
    assert "qoe" in cfg
    assert "alerts" in cfg
    assert "hbase" in cfg
    assert "ml" in cfg

    # 2. Additional configs loaded
    assert "faults" in cfg
    assert "chaos" in cfg
    assert "standardization" in cfg
    assert "sim_dims" in cfg

    # 3. QoE weights sum to 100
    weights = cfg.qoe.weights
    total_weight = sum(weights.values())
    assert abs(total_weight - 100.0) < 1e-4, f"Weights sum to {total_weight}, expected 100"

    # 4. Thresholds are in range
    alerts = cfg.alerts
    assert alerts.min_sessions > 0
    assert 0.0 <= alerts.rebuffer_ratio_threshold <= 1.0
    assert 0.0 <= alerts.vsf_rate_threshold <= 1.0
    assert 0.0 <= alerts.error_rate_threshold <= 1.0
    assert alerts.critical_multiplier > 1.0


def test_env_override(monkeypatch):
    """Verify QOE__<section>__<key> environment variable overrides take effect."""
    monkeypatch.setenv("QOE__replay__speed", "50")
    monkeypatch.setenv("QOE__alerts__min_sessions", "99")

    cfg = load_config()
    assert cfg.replay.speed == 50
    assert cfg.alerts.min_sessions == 99


def test_logging_json():
    """Verify log records format as JSON and contain run_id and timestamp."""
    stream = io.StringIO()
    test_run_id = "test-run-phase0-xyz"
    logger = setup_logging(run_id=test_run_id, stream=stream)

    logger.info("Bootstrap logging test message", extra={"custom_tag": "phase0"})

    output = stream.getvalue().strip()
    assert output, "Log output should not be empty"

    parsed = json.loads(output)
    assert parsed["run_id"] == test_run_id
    assert parsed["level"] == "INFO"
    assert parsed["message"] == "Bootstrap logging test message"
    assert "ts" in parsed
    assert parsed.get("custom_tag") == "phase0"


def test_makefile_targets():
    """Verify make -n succeeds for every target listed in targets.txt."""
    targets_file = REPO_ROOT / "tests" / "phase_0" / "targets.txt"
    assert targets_file.is_file(), f"targets.txt not found at {targets_file}"

    with open(targets_file, "r", encoding="utf-8") as f:
        targets = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    assert len(targets) >= 30, f"Expected at least 30 targets, found {len(targets)}"

    for target in targets:
        proc = subprocess.run(
            ["make", "-n", target],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, (
            f"make -n {target} failed with code {proc.returncode}:\n"
            f"stdout: {proc.stdout}\n"
            f"stderr: {proc.stderr}"
        )


def test_no_hardcoded_tunables():
    """Verify topic names, thresholds, and weights do not appear as literals in src/."""
    # Load canonical values from config
    with open(REPO_ROOT / "config" / "config.yaml", "r", encoding="utf-8") as f:
        raw_cfg = yaml.safe_load(f)

    # Topics to check
    topics = [
        "playback-events",
        "network-metrics",
        "playback-clean",
        "network-clean",
        "session-facts",
    ]

    # Threshold strings to check
    rebuf_thresh = str(raw_cfg["alerts"]["rebuffer_ratio_threshold"])
    vsf_thresh = str(raw_cfg["alerts"]["vsf_rate_threshold"])

    src_dir = REPO_ROOT / "src"

    for py_file in src_dir.rglob("*.py"):
        with open(py_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for idx, line in enumerate(lines, 1):
            stripped = line.strip()
            # Ignore comments and docstrings
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                continue

            for topic in topics:
                assert f'"{topic}"' not in line and f"'{topic}'" not in line, (
                    f"Found hardcoded topic '{topic}' in {py_file}:{idx}: {line.strip()}"
                )

            # Check threshold literals assignment (e.g. threshold = 0.03)
            assert f"= {rebuf_thresh}" not in line, (
                f"Found hardcoded threshold '{rebuf_thresh}' in {py_file}:{idx}: {line.strip()}"
            )
            assert f"= {vsf_thresh}" not in line, (
                f"Found hardcoded threshold '{vsf_thresh}' in {py_file}:{idx}: {line.strip()}"
            )


def test_doctor_runs_cleanly():
    """Verify scripts/doctor.py runs and exits with status 0."""
    doctor_script = REPO_ROOT / "scripts" / "doctor.py"
    proc = subprocess.run(
        [sys.executable, str(doctor_script)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"Doctor exited with code {proc.returncode}:\n{proc.stdout}\n{proc.stderr}"
    assert "OTT QoE Engine" in proc.stdout
