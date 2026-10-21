"""Unit tests for Phase 1 infrastructure specs and configurations."""

from __future__ import annotations

from pathlib import Path
import pytest
import yaml

from scripts.create_topics import REF_TOPIC_SPECS, TOPIC_SPECS
from scripts.hbase_init import get_salt_splits


REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def test_topic_specifications():
    """Verify topic specifications match §6.4 data contracts."""
    specs_dict = {name: (partitions, cfg) for name, partitions, cfg in TOPIC_SPECS}

    # Mandatory topics
    assert specs_dict["playback-events"][0] == 6
    assert specs_dict["network-metrics"][0] == 6
    assert specs_dict["errors"][0] == 3
    assert specs_dict["playback-clean"][0] == 6
    assert specs_dict["network-clean"][0] == 6
    assert specs_dict["session-facts"][0] == 6
    assert specs_dict["alerts"][0] == 1
    assert specs_dict["dlq"][0] == 1
    assert specs_dict["predictions"][0] == 3

    # Reference compacted topics
    ref_dict = {name: (partitions, cfg) for name, partitions, cfg in REF_TOPIC_SPECS}
    assert ref_dict["ref-content"][0] == 1
    assert ref_dict["ref-content"][1].get("cleanup.policy") == "compact"
    assert ref_dict["ref-users"][0] == 1
    assert ref_dict["ref-users"][1].get("cleanup.policy") == "compact"


def test_hbase_salt_splits():
    """Verify 16-bucket salt splits generation."""
    splits = get_salt_splits(16)
    assert len(splits) == 15
    assert splits[0] == b"01"
    assert splits[-1] == b"15"


def test_docker_compose_valid_yaml():
    """Verify docker-compose.yml defines required services."""
    compose_path = REPO_ROOT / "docker-compose.yml"
    assert compose_path.is_file()
    with open(compose_path, "r", encoding="utf-8") as f:
        dc = yaml.safe_load(f)

    services = dc.get("services", {})
    assert "kafka" in services
    assert "hbase" in services
    assert "app" in services
