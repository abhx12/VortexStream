"""Event parsing from raw Kafka payloads with explicit schemas and PERMISSIVE handling."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
import pandas as pd


PLAYBACK_SCHEMA_COLUMNS = [
    "schema_version",
    "event_id",
    "session_id",
    "user_id",
    "ts",
    "event_type",
    "video_id",
    "content_type",
    "device",
    "app_version",
    "country",
    "city",
    "isp",
    "cdn",
    "scheme",
    "bitrate_kbps",
    "resolution",
    "buffer_ms",
    "stall_ms",
    "interval_ms",
    "ssim_index",
    "throughput_kbps",
    "rtt_ms",
    "packet_loss_pct",
    "error_code",
]

NETWORK_SCHEMA_COLUMNS = [
    "schema_version",
    "event_id",
    "session_id",
    "ts",
    "isp",
    "cdn",
    "city",
    "device",
    "throughput_kbps",
    "rtt_ms",
    "min_rtt_ms",
    "cwnd",
    "in_flight",
    "packet_loss_pct",
]


def parse_json_record(raw_val: str | bytes) -> Dict[str, Any]:
    """Parse single JSON payload, capturing parsing error if malformed."""
    raw_str = raw_val.decode("utf-8") if isinstance(raw_val, bytes) else str(raw_val)
    try:
        data = json.loads(raw_str)
        if not isinstance(data, dict):
            return {"_corrupt_record": raw_str, "_raw": raw_str, "parse_error": "JSON not an object"}
        data["_raw"] = raw_str
        return data
    except Exception as e:
        return {"_corrupt_record": raw_str, "_raw": raw_str, "parse_error": str(e)}


def parse_batch_pandas(records: list[str | bytes]) -> pd.DataFrame:
    """Parse a list of raw strings into a pandas DataFrame."""
    parsed = [parse_json_record(r) for r in records]
    return pd.DataFrame(parsed)
