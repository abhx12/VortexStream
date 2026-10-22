"""Unit tests for Phase 13: ETL evidence, reconciliation, and schema mapping."""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from qoe.verify.lineage import generate_lineage_docs
from qoe.verify.reconcile import run_reconciliation

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_reconcile_runs_on_clean_run(tmp_path):
    """Verify reconciliation V1-V8 passes on clean run and fails when counts are corrupted."""
    clean_manifest = {
        "emitted_total": 500,
        "emitted_playback": 300,
        "emitted_network": 200,
        "injected_duplicates": 10,
        "injected_invalid": 5,
        "injected_late_dropped": 2,
    }

    # 483 silver valid + 5 quarantine + 10 dups + 2 late = 500 total
    clean_silver = [
        {
            "event_id": f"evt-{i}",
            "session_id": f"sess-{i//10}-0",
            "event_type": "heartbeat",
            "ts": "2026-10-22T10:00:00.000Z",
            "buffer_ms": 2000.0,
            "interval_ms": 5000.0,
            "bitrate_kbps": 2500.0,
            "device": "android",
            "isp": "Jio",
            "city": "Mumbai",
            "cdn": "Cloudflare",
        }
        for i in range(483)
    ]

    clean_quarantine = [{"event_id": f"rej-{i}", "reason": "BAD_TIMESTAMP"} for i in range(5)]

    report_clean = run_reconciliation(
        manifest_data=clean_manifest,
        silver_events=clean_silver,
        quarantine_events=clean_quarantine,
        output_report_path=tmp_path / "verification_report.md",
    )

    assert report_clean["overall"] == "PASS"
    for check_k, check_v in report_clean["checks"].items():
        assert check_v["passed"] is True, f"Check {check_k} failed unexpectedly: {check_v['details']}"

    # Corrupt emitted count in manifest -> conservation V2 must fail
    corrupt_manifest = dict(clean_manifest, emitted_total=999)
    report_corrupt = run_reconciliation(
        manifest_data=corrupt_manifest,
        silver_events=clean_silver,
        quarantine_events=clean_quarantine,
        output_report_path=tmp_path / "corrupt_report.md",
    )

    assert report_corrupt["overall"] == "FAIL"
    assert report_corrupt["checks"]["V2"]["passed"] is False


def test_mapping_doc_covers_all_fields():
    """Verify every field defined in schemas/*.schema.json appears in docs/etl_mapping.md."""
    mapping_path = REPO_ROOT / "docs" / "etl_mapping.md"
    assert mapping_path.is_file(), "etl_mapping.md does not exist"

    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping_text = f.read()

    schema_files = [
        REPO_ROOT / "schemas" / "playback_event.schema.json",
        REPO_ROOT / "schemas" / "network_metric.schema.json",
        REPO_ROOT / "schemas" / "session_fact.schema.json",
    ]

    missing_fields = []
    for sf in schema_files:
        with open(sf, "r", encoding="utf-8") as f:
            schema_data = json.load(f)
        properties = schema_data.get("properties", {})
        for prop in properties.keys():
            if prop not in mapping_text:
                missing_fields.append((sf.name, prop))

    assert not missing_fields, f"Missing fields in etl_mapping.md: {missing_fields}"


def test_lineage_docs_generated(tmp_path):
    """Verify lineage generation outputs valid Mermaid diagram."""
    out_file = tmp_path / "architecture.md"
    content = generate_lineage_docs(output_path=out_file)

    assert out_file.is_file()
    assert "```mermaid" in content
    assert "flowchart TD" in content
    assert "playback-events" in content
    assert "HBase: session_state" in content
