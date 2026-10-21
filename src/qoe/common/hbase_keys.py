"""HBase row key generation and parsing logic."""

from __future__ import annotations

import binascii


def salted_key(session_id: str, salt_buckets: int = 16) -> str:
    """Calculate salted row key for session state: {salt:02d}-{session_id}."""
    salt = binascii.crc32(session_id.encode("utf-8")) % salt_buckets
    return f"{salt:02d}-{session_id}"


def rev_minute(epoch_minute: int) -> str:
    """Calculate 19-digit reversed minute string for chronological ordering (newest first)."""
    # 2^63 - 1 is Long.MAX_VALUE
    long_max = 9223372036854775807
    return str(long_max - epoch_minute).zfill(19)


def qoe_key(grain: str, dimkey: str, window_end_epoch_minute: int) -> str:
    """Generate row key for qoe_metrics: {grain}#{dimkey}#{rev_minute}."""
    return f"{grain}#{dimkey}#{rev_minute(window_end_epoch_minute)}"


def alert_key(window_end_epoch_minute: int, alert_id: str) -> str:
    """Generate row key for alerts: {rev_minute}#{alert_id}."""
    return f"{rev_minute(window_end_epoch_minute)}#{alert_id}"
