"""Time conversion and simulated clock utilities."""

from __future__ import annotations

from datetime import datetime, timezone


def iso_utc_now() -> str:
    """Return current UTC time formatted in ISO-8601 with Z suffix and millisecond precision."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def epoch_ms_to_iso(epoch_ms: int | float) -> str:
    """Convert epoch milliseconds to ISO-8601 UTC string."""
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
