"""Unit tests for Phase 2: Puffer data acquisition & profiling."""

from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import patch
import pytest

from qoe.data.make_fake_puffer import (
    CLIENT_BUFFER_HEADER,
    DEFAULT_DAY,
    VIDEO_ACKED_HEADER,
    VIDEO_SENT_HEADER,
)
from qoe.data.profile import profile_dataset
from qoe.data.puffer_download import download_puffer_day, is_puffer_day_present


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "puffer_mini" / DEFAULT_DAY


def test_schema_columns():
    """Verify each generated/downloaded CSV has all documented columns."""
    cb_path = FIXTURE_DIR / f"client_buffer_{DEFAULT_DAY}.csv"
    vs_path = FIXTURE_DIR / f"video_sent_{DEFAULT_DAY}.csv"
    va_path = FIXTURE_DIR / f"video_acked_{DEFAULT_DAY}.csv"

    assert cb_path.is_file(), f"Missing {cb_path}"
    assert vs_path.is_file(), f"Missing {vs_path}"
    assert va_path.is_file(), f"Missing {va_path}"

    with open(cb_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == CLIENT_BUFFER_HEADER

    with open(vs_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == VIDEO_SENT_HEADER

    with open(va_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == VIDEO_ACKED_HEADER


def test_time_units():
    """Verify time values are within the day's window when read as nanoseconds."""
    cb_path = FIXTURE_DIR / f"client_buffer_{DEFAULT_DAY}.csv"

    with open(cb_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        times = [int(row["time"]) for row in reader]

    assert len(times) > 0
    min_t = min(times)
    max_t = max(times)

    # 1623495600 is 2021-06-12 11:00 UTC (1.623e18 ns)
    assert 1_600_000_000_000_000_000 < min_t < 1_700_000_000_000_000_000
    # Span must be <= 24 hours (8.64e13 ns)
    assert (max_t - min_t) <= 86400 * 1_000_000_000


def test_profile_report_generated(tmp_path):
    """Verify profile_dataset produces a markdown report with required sections."""
    out_md = tmp_path / "test_profile.md"
    profile_dataset(FIXTURE_DIR, out_md)

    assert out_md.is_file()
    content = out_md.read_text(encoding="utf-8")

    assert "Executive Summary" in content
    assert "Playback & QoE Metrics" in content
    assert "Video Format Distribution" in content
    assert "Experiment Settings Mapping" in content
    assert "client_buffer Rows" in content


def test_no_redownload():
    """Verify that if files exist, download_puffer_day makes zero network requests."""
    assert is_puffer_day_present(FIXTURE_DIR, DEFAULT_DAY)

    with patch("urllib.request.urlopen") as mock_urlopen:
        result_dir = download_puffer_day(
            day=DEFAULT_DAY,
            target_dir=str(FIXTURE_DIR.parent),
            allow_fake_fallback=True,
        )
        assert result_dir == FIXTURE_DIR
        mock_urlopen.assert_not_called()
