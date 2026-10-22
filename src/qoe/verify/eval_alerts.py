"""Alert accuracy evaluation against ground truth fault ledger."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb

from qoe.common.config import get_config


def _parse_ts(ts_val: Any) -> float:
    if not ts_val:
        return 0.0
    ts_str = str(ts_val).strip()
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts_str).timestamp()
    except Exception:
        return 0.0


def evaluate_alerts(
    ledger_path: str | Path = "data/ledger/faults.jsonl",
    alerts_path: str | Path = "data/warehouse/alerts",
    alerts_list: Optional[List[Dict[str, Any]]] = None,
    config: Optional[Any] = None,
) -> Dict[str, Any]:
    """Compute precision, recall, and detection latency for alerts vs faults."""
    faults: List[Dict[str, Any]] = []
    l_file = Path(ledger_path)
    if l_file.is_file():
        with open(l_file, "r", encoding="utf-8") as f:
            for line in f:
                line_s = line.strip()
                if line_s:
                    faults.append(json.loads(line_s))

    alerts: List[Dict[str, Any]] = []
    if alerts_list is not None:
        alerts = alerts_list
    else:
        a_dir = Path(alerts_path)
        if a_dir.is_dir() and any(a_dir.glob("**/*.parquet")):
            con = duckdb.connect(":memory:")
            df = con.execute(f"SELECT * FROM read_parquet('{a_dir.as_posix()}/**/*.parquet')").df()
            alerts = df.to_dict(orient="records")

    per_fault_metrics: Dict[str, Any] = {}
    detected_count = 0
    false_positives = 0

    open_alerts = [a for a in alerts if a.get("status") == "open"]

    for fault in faults:
        fid = fault.get("fault_id", "unknown")
        selector = fault.get("selector", {})
        f_start = _parse_ts(fault.get("start_ts"))
        f_end = _parse_ts(fault.get("end_ts"))

        target_grain = "city_isp" if ("city" in selector and "isp" in selector) else "cdn" if "cdn" in selector else "device"
        target_dimkey = (
            f"{selector.get('city')}|{selector.get('isp')}"
            if target_grain == "city_isp"
            else selector.get("cdn") or selector.get("device") or "ALL"
        )

        matching_alerts = []
        for a in open_alerts:
            a_ts = _parse_ts(a.get("window_end"))
            if a.get("grain") == target_grain and a.get("dimkey") == target_dimkey:
                if (f_start - 300) <= a_ts <= (f_end + 600):
                    matching_alerts.append((a_ts, a))

        if matching_alerts:
            detected_count += 1
            matching_alerts.sort(key=lambda x: x[0])
            first_alert_ts = matching_alerts[0][0]
            lat_min = max(0.0, (first_alert_ts - f_start) / 60.0)
            per_fault_metrics[fid] = {
                "detected": True,
                "latency_minutes": round(lat_min, 1),
                "matched_alerts": len(matching_alerts),
            }
        else:
            per_fault_metrics[fid] = {
                "detected": False,
                "latency_minutes": None,
                "matched_alerts": 0,
            }

    # Count false positives
    for a in open_alerts:
        a_ts = _parse_ts(a.get("window_end"))
        in_any_fault = False
        for fault in faults:
            f_start = _parse_ts(fault.get("start_ts"))
            f_end = _parse_ts(fault.get("end_ts"))
            if (f_start - 300) <= a_ts <= (f_end + 600):
                in_any_fault = True
                break
        if not in_any_fault:
            false_positives += 1

    total_faults = len(faults)
    recall = (detected_count / total_faults) if total_faults > 0 else 1.0
    precision = (
        (detected_count / (detected_count + false_positives))
        if (detected_count + false_positives) > 0
        else 1.0
    )

    summary = {
        "total_faults": total_faults,
        "detected_faults": detected_count,
        "recall": round(recall, 3),
        "precision": round(precision, 3),
        "false_positives": false_positives,
        "per_fault": per_fault_metrics,
    }
    return summary


if __name__ == "__main__":
    res = evaluate_alerts()
    print(json.dumps(res, indent=2))
