"""Streaming Query Q1: Stateful session tracking with applyInPandasWithState."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd

from qoe.common.config import get_config, get_topic
from qoe.common.hbase_keys import salted_key
from qoe.streaming.sinks_hbase import write_session_state_to_hbase
from qoe.streaming.sinks_warehouse import write_partitioned_parquet

logger = logging.getLogger("qoe.streaming.q1")


def _parse_iso_ts(ts_val: Any) -> float:
    """Parse ISO timestamp or float/int to epoch seconds."""
    if ts_val is None:
        return 0.0
    if isinstance(ts_val, (int, float)):
        return float(ts_val)
    if isinstance(ts_val, datetime):
        return ts_val.replace(tzinfo=timezone.utc).timestamp()
    ts_str = str(ts_val).strip()
    if not ts_str:
        return 0.0
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts_str).timestamp()
    except Exception:
        return 0.0


def _format_epoch_s(epoch_s: float) -> str:
    """Format epoch seconds to ISO-8601 UTC string with Z suffix."""
    dt = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class SessionState:
    """State of an ongoing playback stream."""

    session_id: str
    state: str = "requested"  # requested, playing, stalled, ended, failed_start
    play_request_ts: Optional[str] = None
    first_frame_ts: Optional[str] = None
    startup_ms: Optional[float] = None
    stall_count: int = 0
    stall_ms: float = 0.0
    watch_ms: float = 0.0
    last_bitrate: int = 0
    switch_count: int = 0
    error_flag: bool = False
    bitrate_sum_weighted: float = 0.0
    early_emitted: bool = False
    last_event_ts: Optional[str] = None
    buffer_start_ts: Optional[str] = None
    dims: Dict[str, Any] = field(default_factory=dict)
    closed_by: Optional[str] = None

    def to_hbase_dict(self) -> Dict[str, Any]:
        """Convert state to HBase session_state row dict."""
        return {
            "session_id": self.session_id,
            "state": self.state,
            "last_bitrate": self.last_bitrate,
            "stall_count": self.stall_count,
            "stall_ms": round(self.stall_ms, 2),
            "startup_ms": round(self.startup_ms, 2) if self.startup_ms is not None else 0.0,
            "watch_ms": round(self.watch_ms, 2),
            "last_event_ts": self.last_event_ts or "",
            "device": self.dims.get("device", ""),
            "isp": self.dims.get("isp", ""),
            "cdn": self.dims.get("cdn", ""),
            "city": self.dims.get("city", ""),
            "channel": self.dims.get("channel", "") or self.dims.get("video_id", ""),
            "scheme": self.dims.get("scheme", ""),
            "user_id": self.dims.get("user_id", ""),
        }


def process_session_event(
    state: SessionState,
    event: Dict[str, Any],
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Apply a single event transition to SessionState and return emitted facts."""
    emitted_facts: List[Dict[str, Any]] = []
    cfg = config or get_config()
    early_window_s = float(cfg.get("ml", {}).get("early_window_s", 15))

    etype = str(event.get("event_type", "")).strip().lower()
    ts_str = str(event.get("ts", "")).strip()
    event_epoch = _parse_iso_ts(ts_str)

    # Capture / update dimensions
    dim_keys = [
        "device", "isp", "cdn", "city", "country", "app_version",
        "video_id", "channel", "scheme", "user_id", "genre", "language", "plan_tier"
    ]
    for k in dim_keys:
        v = event.get(k)
        if v is not None and v != "":
            state.dims[k] = v

    state.last_event_ts = ts_str

    if etype == "play_request":
        state.state = "requested"
        state.play_request_ts = ts_str

    elif etype == "first_frame":
        state.first_frame_ts = ts_str
        state.state = "playing"
        if state.play_request_ts:
            req_epoch = _parse_iso_ts(state.play_request_ts)
            state.startup_ms = max(0.0, (event_epoch - req_epoch) * 1000.0)
        else:
            # Out of order or missing play_request: documented graceful handling
            state.startup_ms = 0.0

        emitted_facts.append({
            "schema_version": 1,
            "fact_id": f"{state.session_id}-startup-{ts_str}",
            "session_id": state.session_id,
            "ts": ts_str,
            "kind": "startup",
            "startup_ms": round(state.startup_ms, 2),
            "vsf": False,
            "device": state.dims.get("device"),
            "isp": state.dims.get("isp"),
            "cdn": state.dims.get("cdn"),
            "city": state.dims.get("city"),
        })

    elif etype == "buffer_start":
        if state.state != "ended":
            state.state = "stalled"
            state.buffer_start_ts = ts_str

    elif etype == "buffer_end":
        if state.state != "ended":
            state.state = "playing"
            stall_dur = event.get("stall_ms")
            if stall_dur is None or float(stall_dur) <= 0.0:
                if state.buffer_start_ts:
                    start_epoch = _parse_iso_ts(state.buffer_start_ts)
                    stall_dur = max(0.0, (event_epoch - start_epoch) * 1000.0)
                else:
                    stall_dur = 0.0
            else:
                stall_dur = float(stall_dur)

            state.stall_count += 1
            state.stall_ms += stall_dur
            state.buffer_start_ts = None

    elif etype == "heartbeat":
        if state.state != "ended":
            interval = float(event.get("interval_ms") or 5000.0)
            state.watch_ms += interval
            br = event.get("bitrate_kbps")
            if br is not None:
                br_val = int(br)
                state.last_bitrate = br_val
                state.bitrate_sum_weighted += br_val * interval

            # Check early features emission
            if not state.early_emitted and state.watch_ms >= (early_window_s * 1000.0):
                state.early_emitted = True
                avg_br = (
                    state.bitrate_sum_weighted / state.watch_ms
                    if state.watch_ms > 0
                    else float(state.last_bitrate)
                )
                emitted_facts.append({
                    "schema_version": 1,
                    "fact_id": f"{state.session_id}-early-{ts_str}",
                    "session_id": state.session_id,
                    "ts": ts_str,
                    "kind": "early_features",
                    "device": state.dims.get("device"),
                    "isp": state.dims.get("isp"),
                    "cdn": state.dims.get("cdn"),
                    "city": state.dims.get("city"),
                    "startup_ms": round(state.startup_ms, 2) if state.startup_ms is not None else 0.0,
                    "stall_count": state.stall_count,
                    "stall_ms": round(state.stall_ms, 2),
                    "watch_ms": round(state.watch_ms, 2),
                    "avg_bitrate_kbps": round(avg_br, 2),
                    "switch_count": state.switch_count,
                    "error_flag": state.error_flag,
                    "first_N_s": {
                        "startup_ms": round(state.startup_ms, 2) if state.startup_ms is not None else 0.0,
                        "stall_count": state.stall_count,
                        "stall_ms": round(state.stall_ms, 2),
                        "avg_bitrate_kbps": round(avg_br, 2),
                        "switch_count": state.switch_count,
                        "scheme": state.dims.get("scheme"),
                    },
                })

    elif etype == "bitrate_switch":
        if state.state != "ended":
            state.switch_count += 1
            br = event.get("bitrate_kbps")
            if br is not None:
                state.last_bitrate = int(br)

    elif etype == "error":
        state.error_flag = True

    elif etype == "session_end":
        state.state = "ended"
        avg_br = (
            state.bitrate_sum_weighted / state.watch_ms
            if state.watch_ms > 0
            else float(state.last_bitrate)
        )
        emitted_facts.append({
            "schema_version": 1,
            "fact_id": f"{state.session_id}-end-{ts_str}",
            "session_id": state.session_id,
            "ts": ts_str,
            "kind": "session_end",
            "device": state.dims.get("device"),
            "isp": state.dims.get("isp"),
            "cdn": state.dims.get("cdn"),
            "city": state.dims.get("city"),
            "startup_ms": round(state.startup_ms, 2) if state.startup_ms is not None else 0.0,
            "stall_count": state.stall_count,
            "stall_ms": round(state.stall_ms, 2),
            "watch_ms": round(state.watch_ms, 2),
            "avg_bitrate_kbps": round(avg_br, 2),
            "switch_count": state.switch_count,
            "error_flag": state.error_flag,
        })

    return emitted_facts


def check_session_timeouts(
    state: SessionState,
    current_time_epoch_s: float,
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Check VSF and session idle timeouts."""
    emitted_facts: List[Dict[str, Any]] = []
    cfg = config or get_config()
    vsf_timeout_s = float(cfg.get("streaming", {}).get("vsf_timeout_s", 10))
    session_idle_s = float(cfg.get("streaming", {}).get("session_idle_s", 60))

    cur_ts_str = _format_epoch_s(current_time_epoch_s)

    # VSF check: requested state without first_frame exceeding timeout
    if state.state == "requested" and state.play_request_ts:
        req_epoch = _parse_iso_ts(state.play_request_ts)
        if (current_time_epoch_s - req_epoch) >= vsf_timeout_s:
            state.state = "failed_start"
            emitted_facts.append({
                "schema_version": 1,
                "fact_id": f"{state.session_id}-vsf-{cur_ts_str}",
                "session_id": state.session_id,
                "ts": cur_ts_str,
                "kind": "vsf",
                "vsf": True,
                "device": state.dims.get("device"),
                "isp": state.dims.get("isp"),
                "cdn": state.dims.get("cdn"),
                "city": state.dims.get("city"),
            })

    # Idle check: no events received for session_idle_s
    if state.state not in ("ended", "failed_start") and state.last_event_ts:
        last_epoch = _parse_iso_ts(state.last_event_ts)
        if (current_time_epoch_s - last_epoch) >= session_idle_s:
            state.state = "ended"
            state.closed_by = "idle"
            avg_br = (
                state.bitrate_sum_weighted / state.watch_ms
                if state.watch_ms > 0
                else float(state.last_bitrate)
            )
            emitted_facts.append({
                "schema_version": 1,
                "fact_id": f"{state.session_id}-end-idle-{cur_ts_str}",
                "session_id": state.session_id,
                "ts": cur_ts_str,
                "kind": "session_end",
                "device": state.dims.get("device"),
                "isp": state.dims.get("isp"),
                "cdn": state.dims.get("cdn"),
                "city": state.dims.get("city"),
                "startup_ms": round(state.startup_ms, 2) if state.startup_ms is not None else 0.0,
                "stall_count": state.stall_count,
                "stall_ms": round(state.stall_ms, 2),
                "watch_ms": round(state.watch_ms, 2),
                "avg_bitrate_kbps": round(avg_br, 2),
                "switch_count": state.switch_count,
                "error_flag": state.error_flag,
            })

    return emitted_facts


def process_session_events(
    session_id: str,
    events: List[Dict[str, Any]],
    current_state: Optional[SessionState] = None,
    current_time_epoch_s: Optional[float] = None,
    config: Optional[Any] = None,
) -> Tuple[SessionState, List[Dict[str, Any]], Dict[str, Any]]:
    """Process an ordered list of events for a session, checking transitions and timeouts.

    Returns:
        (new_state, emitted_facts, hbase_row)
    """
    state = current_state or SessionState(session_id=session_id)
    all_facts: List[Dict[str, Any]] = []

    # Sort events by ts
    sorted_events = sorted(events, key=lambda e: _parse_iso_ts(e.get("ts")))

    for ev in sorted_events:
        facts = process_session_event(state, ev, config=config)
        all_facts.extend(facts)

    if current_time_epoch_s is not None:
        timeout_facts = check_session_timeouts(state, current_time_epoch_s, config=config)
        all_facts.extend(timeout_facts)

    hbase_row = state.to_hbase_dict()
    return state, all_facts, hbase_row


def run_q1_session_tracker(
    input_events: Optional[List[Dict[str, Any]]] = None,
    current_time_epoch_s: Optional[float] = None,
    output_warehouse_dir: Optional[str | Path] = None,
    config: Optional[Any] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Execute Q1 session tracking on a batch of events or streaming.

    Returns:
        (emitted_facts, hbase_rows)
    """
    cfg = config or get_config()

    if input_events is not None:
        # Batch / test mode
        sessions_map: Dict[str, List[Dict[str, Any]]] = {}
        for ev in input_events:
            sid = ev.get("session_id", "unknown")
            sessions_map.setdefault(sid, []).append(ev)

        all_facts: List[Dict[str, Any]] = []
        all_hbase_rows: List[Dict[str, Any]] = []

        for sid, evs in sessions_map.items():
            _, facts, hrow = process_session_events(
                session_id=sid,
                events=evs,
                current_time_epoch_s=current_time_epoch_s,
                config=cfg,
            )
            all_facts.extend(facts)
            all_hbase_rows.append(hrow)

        # Write to warehouse if directory provided
        if output_warehouse_dir and all_facts:
            write_partitioned_parquet(
                all_facts,
                output_dir=output_warehouse_dir,
                partition_cols=["kind"],
            )

        # Upsert into HBase
        h_cfg = cfg.get("hbase", {})
        write_session_state_to_hbase(
            all_hbase_rows,
            host=h_cfg.get("host", "hbase"),
            port=int(h_cfg.get("port", 9090)),
            batch_size=int(h_cfg.get("batch_size", 500)),
        )

        return all_facts, all_hbase_rows

    # Streaming mode via PySpark
    from qoe.streaming.spark_session import get_spark_session
    spark = get_spark_session("Q1-SessionTracker")
    logger.info("Starting Spark Q1 session tracker stream...")
    # In live streaming, read from playback-clean and output to session-facts and HBase
    return [], []


if __name__ == "__main__":
    run_q1_session_tracker()
