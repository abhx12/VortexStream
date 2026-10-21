"""HBase row key generation and parsing logic."""

from __future__ import annotations

import binascii
from typing import Tuple

LONG_MAX = 9223372036854775807


def salted_key(session_id: str, salt_buckets: int = 16) -> str:
    """Calculate salted row key for session state: {salt:02d}-{session_id}."""
    salt = binascii.crc32(session_id.encode("utf-8")) % salt_buckets
    return f"{salt:02d}-{session_id}"


def parse_salted_key(key: str) -> Tuple[int, str]:
    """Parse salted row key back into (salt, session_id)."""
    parts = key.split("-", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid salted key format: {key}")
    return int(parts[0]), parts[1]


def rev_minute(epoch_minute: int) -> str:
    """Calculate 19-digit reversed minute string for chronological ordering (newest first)."""
    return str(LONG_MAX - epoch_minute).zfill(19)


def parse_rev_minute(rev_str: str) -> int:
    """Parse 19-digit reversed minute string back into epoch minute."""
    return LONG_MAX - int(rev_str)


def qoe_key(grain: str, dimkey: str, window_end_epoch_minute: int) -> str:
    """Generate row key for qoe_metrics: {grain}#{dimkey}#{rev_minute}."""
    return f"{grain}#{dimkey}#{rev_minute(window_end_epoch_minute)}"


def parse_qoe_key(key: str) -> Tuple[str, str, int]:
    """Parse qoe_metrics row key back into (grain, dimkey, window_end_epoch_minute)."""
    parts = key.split("#")
    if len(parts) != 3:
        raise ValueError(f"Invalid qoe_metrics row key format: {key}")
    grain, dimkey, rev_min_str = parts[0], parts[1], parts[2]
    return grain, dimkey, parse_rev_minute(rev_min_str)


def alert_key(window_end_epoch_minute: int, alert_id: str) -> str:
    """Generate row key for alerts: {rev_minute}#{alert_id}."""
    return f"{rev_minute(window_end_epoch_minute)}#{alert_id}"


def parse_alert_key(key: str) -> Tuple[int, str]:
    """Parse alert row key back into (window_end_epoch_minute, alert_id)."""
    parts = key.split("#", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid alert row key format: {key}")
    return parse_rev_minute(parts[0]), parts[1]
