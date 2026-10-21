"""Streaming Query Q2: Multi-grain QoE windowed aggregation and alerting."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from qoe.common.config import get_config
from qoe.common.hbase_keys import qoe_key
from qoe.streaming.qoe_score import compute_qoe_score
from qoe.streaming.sinks_hbase import write_qoe_metrics_to_hbase
from qoe.streaming.sinks_warehouse import write_gold_qoe_5m


def _parse_ts_to_epoch_s(ts_val: Any) -> float:
    """Parse ISO timestamp string or numeric timestamp to epoch seconds."""
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
    """Format epoch seconds to ISO-8601 UTC string."""
    dt = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _is_hd_resolution(res_str: Any, hd_min_height: int = 720) -> bool:
    """Check if resolution is HD (height >= hd_min_height)."""
    if not res_str:
        return False
    clean = str(res_str).strip().lower().rstrip("p")
    try:
        return int(clean) >= hd_min_height
    except ValueError:
        return False


def get_event_grains(event: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Explode event into multi-grain pairs: (grain, dimkey)."""
    grains: List[Tuple[str, str]] = [("global", "ALL")]

    city = event.get("city")
    isp = event.get("isp")
    if city or isp:
        c_str = str(city) if city else "unknown"
        i_str = str(isp) if isp else "unknown"
        grains.append(("city_isp", f"{c_str}|{i_str}"))

    cdn = event.get("cdn")
    if cdn:
        grains.append(("cdn", str(cdn)))

    device = event.get("device")
    if device:
        grains.append(("device", str(device)))

    channel = event.get("channel") or event.get("video_id")
    if channel:
        grains.append(("channel", str(channel)))

    return grains


def compute_qoe_window_aggregates(
    records: List[Dict[str, Any]],
    window_s: int = 300,
    slide_s: int = 60,
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Compute additive and derived multi-grain windowed QoE metrics."""
    if not records:
        return []

    cfg = config or get_config()
    qoe_cfg = cfg.get("qoe", {})
    hd_min_height = int(qoe_cfg.get("hd_min_height", 720))

    # Bucket accumulator: (window_end_epoch_s, grain, dimkey) -> stats
    buckets: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

    for r in records:
        ts_s = _parse_ts_to_epoch_s(r.get("ts"))
        if ts_s <= 0.0:
            continue

        sid = str(r.get("session_id", ""))
        etype = str(r.get("event_type", "")).strip().lower()
        kind = str(r.get("kind", "")).strip().lower()

        # Find all sliding windows covering ts_s:
        # Window covers [w_end - window_s, w_end)
        first_w_end = int(math.floor(ts_s / slide_s) + 1) * slide_s
        # Only iterate over valid sliding windows
        window_ends = [
            w for w in range(first_w_end, first_w_end + window_s, slide_s)
            if (w - window_s) <= ts_s < w
        ]

        grains = get_event_grains(r)

        for w_end in window_ends:
            for grain, dimkey in grains:
                bkey = (w_end, grain, dimkey)
                if bkey not in buckets:
                    buckets[bkey] = {
                        "window_end_s": w_end,
                        "grain": grain,
                        "dimkey": dimkey,
                        "stall_ms_sum": 0.0,
                        "stall_count": 0,
                        "watch_ms_sum": 0.0,
                        "bitrate_weighted_sum": 0.0,
                        "hd_ms_sum": 0.0,
                        "ssim_weighted_sum": 0.0,
                        "switch_count": 0,
                        "startup_ms_sum": 0.0,
                        "startup_n": 0,
                        "play_requests": 0,
                        "vsf_count": 0,
                        "error_sessions": set(),
                        "sessions": set(),
                    }

                b = buckets[bkey]
                if sid:
                    b["sessions"].add(sid)

                # Additive aggregates based on event type / kind
                if etype == "play_request":
                    b["play_requests"] += 1

                elif etype == "first_frame" or kind == "startup":
                    su_ms = r.get("startup_ms")
                    if su_ms is not None:
                        b["startup_ms_sum"] += float(su_ms)
                        b["startup_n"] += 1

                elif kind == "vsf":
                    b["vsf_count"] += 1

                elif etype == "buffer_end":
                    st_ms = float(r.get("stall_ms") or 0.0)
                    b["stall_ms_sum"] += st_ms
                    b["stall_count"] += 1

                elif etype == "heartbeat":
                    interval = float(r.get("interval_ms") or 5000.0)
                    br = float(r.get("bitrate_kbps") or 0.0)
                    ssim = float(r.get("ssim_index") or 0.0)
                    res = r.get("resolution")

                    b["watch_ms_sum"] += interval
                    b["bitrate_weighted_sum"] += (br * interval)
                    b["ssim_weighted_sum"] += (ssim * interval)

                    if _is_hd_resolution(res, hd_min_height):
                        b["hd_ms_sum"] += interval

                elif etype == "bitrate_switch":
                    b["switch_count"] += 1

                elif etype == "error":
                    if sid:
                        b["error_sessions"].add(sid)

    # Derive final metrics and composite QoE score
    out_rows: List[Dict[str, Any]] = []
    now_iso = _format_epoch_s(datetime.now(timezone.utc).timestamp())

    for (w_end, grain, dimkey), b in sorted(buckets.items()):
        watch_ms = b["watch_ms_sum"]
        stall_ms = b["stall_ms_sum"]
        total_time = watch_ms + stall_ms

        rebuf_ratio = (stall_ms / total_time) if total_time > 0 else 0.0
        rebuf_freq = (b["stall_count"] / (watch_ms / 60000.0)) if watch_ms > 0 else 0.0
        avg_bitrate = (b["bitrate_weighted_sum"] / watch_ms) if watch_ms > 0 else 0.0
        hd_pct = (b["hd_ms_sum"] / watch_ms) if watch_ms > 0 else 0.0
        switches_min = (b["switch_count"] / (watch_ms / 60000.0)) if watch_ms > 0 else 0.0

        avg_startup_ms = (b["startup_ms_sum"] / b["startup_n"]) if b["startup_n"] > 0 else None
        avg_startup_s = (avg_startup_ms / 1000.0) if avg_startup_ms is not None else None

        play_reqs = b["play_requests"]
        vsf_rate = (b["vsf_count"] / play_reqs) if play_reqs > 0 else 0.0

        session_count = len(b["sessions"])
        err_sessions = len(b["error_sessions"])
        err_rate = (err_sessions / session_count) if session_count > 0 else 0.0

        derived_metrics = {
            "rebuffer_ratio": rebuf_ratio,
            "rebuffer_freq_per_min": rebuf_freq,
            "avg_bitrate_kbps": avg_bitrate,
            "hd_pct": hd_pct,
            "switches_per_min": switches_min,
            "avg_startup_ms": avg_startup_ms,
            "avg_startup_s": avg_startup_s,
            "vsf_rate": vsf_rate,
            "error_rate": err_rate,
        }

        qoe_res = compute_qoe_score(derived_metrics, config=cfg)

        w_end_iso = _format_epoch_s(float(w_end))
        w_start_iso = _format_epoch_s(float(w_end - window_s))
        rev_key = qoe_key(grain, dimkey, int(w_end // 60))

        row: Dict[str, Any] = {
            "row_key": rev_key,
            "grain": grain,
            "dimkey": dimkey,
            "window_start": w_start_iso,
            "window_end": w_end_iso,
            "updated_at": now_iso,
            "stall_ms_sum": round(stall_ms, 2),
            "stall_count": b["stall_count"],
            "watch_ms_sum": round(watch_ms, 2),
            "bitrate_weighted_sum": round(b["bitrate_weighted_sum"], 2),
            "hd_ms_sum": round(b["hd_ms_sum"], 2),
            "switch_count": b["switch_count"],
            "startup_ms_sum": round(b["startup_ms_sum"], 2),
            "startup_n": b["startup_n"],
            "play_requests": play_reqs,
            "vsf_count": b["vsf_count"],
            "error_sessions": err_sessions,
            "sessions": session_count,
            "rebuffer_ratio": round(rebuf_ratio, 4),
            "rebuffer_freq_per_min": round(rebuf_freq, 3),
            "avg_bitrate_kbps": round(avg_bitrate, 1),
            "hd_pct": round(hd_pct, 4),
            "switches_per_min": round(switches_min, 3),
            "avg_startup_ms": round(avg_startup_ms, 2) if avg_startup_ms is not None else None,
            "vsf_rate": round(vsf_rate, 4),
            "error_rate": round(err_rate, 4),
            "qoe_score": qoe_res["qoe_score"],
            "qoe_label": qoe_res["qoe_label"],
            "qoe_partial": qoe_res["qoe_partial"],
            "startup_pen": qoe_res["startup_pen"],
            "rebuffer_pen": qoe_res["rebuffer_pen"],
            "switch_pen": qoe_res["switch_pen"],
            "bitrate_pen": qoe_res["bitrate_pen"],
            "failure_pen": qoe_res["failure_pen"],
        }
        out_rows.append(row)

    return out_rows


def run_q2_qoe_aggregator(
    input_records: Optional[List[Dict[str, Any]]] = None,
    window_s: int = 300,
    slide_s: int = 60,
    warehouse_dir: Optional[str | Path] = None,
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Run Q2 window aggregation query in batch or streaming mode."""
    cfg = config or get_config()

    if input_records is not None:
        rows = compute_qoe_window_aggregates(
            input_records,
            window_s=window_s,
            slide_s=slide_s,
            config=cfg,
        )

        # Upsert into HBase
        h_cfg = cfg.get("hbase", {})
        write_qoe_metrics_to_hbase(
            rows,
            host=h_cfg.get("host", "hbase"),
            port=int(h_cfg.get("port", 9090)),
            batch_size=int(h_cfg.get("batch_size", 500)),
        )

        # Append to warehouse gold
        if warehouse_dir and rows:
            write_gold_qoe_5m(rows, warehouse_dir=warehouse_dir)

        return rows

    # Streaming mode via PySpark
    return []


if __name__ == "__main__":
    run_q2_qoe_aggregator()
