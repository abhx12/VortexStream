"""Data validation against canonical rules with detailed quarantine reasons."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd


VALID_EVENT_TYPES = {
    "play_request",
    "first_frame",
    "heartbeat",
    "buffer_start",
    "buffer_end",
    "bitrate_switch",
    "error",
    "session_end",
}


def check_record_validity(row: Dict[str, Any], event_category: str = "playback") -> Optional[str]:
    """
    Evaluate validation rules on a single record.
    Returns None if valid, or reason string if rejected.
    """
    # 1. Parse error
    if row.get("_corrupt_record") or row.get("parse_error"):
        return "PARSE_ERROR"

    # 2. Session ID checks
    sid = str(row.get("session_id", "") or "").strip()
    if not sid:
        return "MISSING_SESSION_ID"

    # 3. Timestamp checks
    ts_str = row.get("ts")
    if not ts_str:
        return "BAD_TIMESTAMP"
    try:
        # Normalize trailing Z
        cleaned_ts = str(ts_str).replace("Z", "+00:00")
        datetime.fromisoformat(cleaned_ts)
    except Exception:
        return "BAD_TIMESTAMP"

    # 4. Playback-specific event type rules
    if event_category == "playback":
        etype = row.get("event_type")
        if not etype or str(etype).strip() == "":
            return "MISSING_EVENT_TYPE"
        if etype not in VALID_EVENT_TYPES:
            return "UNKNOWN_EVENT_TYPE"

        # Buffer rules
        buf = row.get("buffer_ms")
        if buf is not None and not pd.isna(buf):
            try:
                if float(buf) < 0:
                    return "NEGATIVE_BUFFER"
            except (ValueError, TypeError):
                return "NEGATIVE_BUFFER"

        # Stall rules
        stall = row.get("stall_ms")
        if stall is not None and not pd.isna(stall):
            try:
                if float(stall) < 0:
                    return "NEGATIVE_STALL"
            except (ValueError, TypeError):
                return "NEGATIVE_STALL"

        # Bitrate rules
        br = row.get("bitrate_kbps")
        if br is not None and not pd.isna(br):
            try:
                f_br = float(br)
                if f_br < 0 or f_br > 100000:
                    return "IMPOSSIBLE_BITRATE"
            except (ValueError, TypeError):
                return "IMPOSSIBLE_BITRATE"

        # Presence matrix requirements
        if etype == "first_frame":
            if row.get("bitrate_kbps") is None or pd.isna(row.get("bitrate_kbps")):
                return "MISSING_REQUIRED_FIELD"
        elif etype == "heartbeat":
            if (
                row.get("buffer_ms") is None or pd.isna(row.get("buffer_ms"))
                or row.get("interval_ms") is None or pd.isna(row.get("interval_ms"))
                or row.get("bitrate_kbps") is None or pd.isna(row.get("bitrate_kbps"))
            ):
                return "MISSING_REQUIRED_FIELD"
        elif etype == "error":
            if not row.get("error_code") or pd.isna(row.get("error_code")):
                return "MISSING_REQUIRED_FIELD"

    # 5. Network-specific rules
    elif event_category == "network":
        tp = row.get("throughput_kbps")
        rtt = row.get("rtt_ms")
        loss = row.get("packet_loss_pct")

        if tp is not None and not pd.isna(tp) and float(tp) < 0:
            return "BAD_RTT/THROUGHPUT/LOSS"
        if rtt is not None and not pd.isna(rtt) and float(rtt) < 0:
            return "BAD_RTT/THROUGHPUT/LOSS"
        if loss is not None and not pd.isna(loss):
            f_loss = float(loss)
            if f_loss < 0 or f_loss > 100:
                return "BAD_RTT/THROUGHPUT/LOSS"

    return None


def validate_dataframe(df: pd.DataFrame, event_category: str = "playback") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split pandas DataFrame into (valid_df, rejected_df).
    Rejected rows include 'reason' and preserve '_raw'.
    """
    if df.empty:
        empty_rejected = df.copy()
        empty_rejected["reason"] = None
        return df.copy(), empty_rejected

    reasons = [check_record_validity(r, event_category) for r in df.to_dict(orient="records")]

    valid_mask = [r is None for r in reasons]
    valid_df = df[valid_mask].copy()

    rejected_df = df[[not m for m in valid_mask]].copy()
    rejected_df["reason"] = [r for r in reasons if r is not None]

    if "_raw" not in rejected_df.columns:
        import json
        rejected_df["_raw"] = [json.dumps(r, default=str) for r in rejected_df.to_dict(orient="records")]

    return valid_df, rejected_df
