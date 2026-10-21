"""Kafka producer and consumer factory utilities."""

from __future__ import annotations

from typing import Any, Dict, Optional


def get_producer_config(bootstrap_servers: str, extra_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return default recommended Kafka producer configuration."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    }
    if extra_config:
        conf.update(extra_config)
    return conf


def get_consumer_config(
    bootstrap_servers: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
    extra_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return default recommended Kafka consumer configuration."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
    }
    if extra_config:
        conf.update(extra_config)
    return conf
