"""Real-time alerting rules evaluation and state machine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from qoe.common.config import get_config
from qoe.common.hbase_keys import alert_key
from qoe.streaming.sinks_hbase import write_alerts_to_hbase
from qoe.streaming.sinks_warehouse import write_alerts


def _parse_ts_to_epoch_s(ts_val: Any) -> float:
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
class SegmentAlertState:
    """Alert state tracker for a specific (rule, grain, dimkey) segment."""

    rule: str
    grain: str
    dimkey: str
    consecutive_breaches: int = 0
    consecutive_healthy: int = 0
    active_alert: Optional[Dict[str, Any]] = None
    last_resolved_epoch_s: Optional[float] = None


class AlertEvaluator:
    """Evaluates rules R1-R4 against aggregate metrics and manages hysteresis."""

    def __init__(self, config: Optional[Any] = None):
        cfg = config or get_config()
        self.config = cfg
        self.alerts_cfg = cfg.get("alerts", {})

        self.min_sessions = int(self.alerts_cfg.get("min_sessions", 20))
        self.rebuf_thresh = float(self.alerts_cfg.get("rebuffer_ratio_threshold", 0.03))
        self.vsf_thresh = float(self.alerts_cfg.get("vsf_rate_threshold", 0.15))
        self.error_thresh = float(self.alerts_cfg.get("error_rate_threshold", 0.05))
        self.crit_mult = float(self.alerts_cfg.get("critical_multiplier", 2.0))
        self.qoe_drop_points = float(self.alerts_cfg.get("qoe_drop_points", 15))
        self.baseline_lag_min = int(self.alerts_cfg.get("baseline_lag_minutes", 5))
        self.consecutive_win = int(self.alerts_cfg.get("consecutive_windows", 2))
        self.cooldown_min = float(self.alerts_cfg.get("cooldown_minutes", 10))

        # State storage
        self.segment_states: Dict[Tuple[str, str, str], SegmentAlertState] = {}
        # History map: (grain, dimkey, epoch_minute) -> qoe_score
        self.qoe_history: Dict[Tuple[str, str, int], float] = {}

    def _get_state(self, rule: str, grain: str, dimkey: str) -> SegmentAlertState:
        key = (rule, grain, dimkey)
        if key not in self.segment_states:
            self.segment_states[key] = SegmentAlertState(rule=rule, grain=grain, dimkey=dimkey)
        return self.segment_states[key]

    def evaluate_window(
        self,
        window_row: Dict[str, Any],
        run_id: str = "run-default",
    ) -> List[Dict[str, Any]]:
        """Evaluate rules against a single window row."""
        emitted: List[Dict[str, Any]] = []

        grain = str(window_row.get("grain", "global"))
        dimkey = str(window_row.get("dimkey", "ALL"))
        sessions = int(window_row.get("sessions") or 0)
        window_end_iso = str(window_row.get("window_end", ""))
        window_end_s = _parse_ts_to_epoch_s(window_end_iso)
        window_end_min = int(window_end_s // 60) if window_end_s > 0 else 0

        qoe_score = float(window_row.get("qoe_score") or 0.0)
        self.qoe_history[(grain, dimkey, window_end_min)] = qoe_score

        # Ignore segments below minimum sessions threshold
        if sessions < self.min_sessions:
            return emitted

        now_iso = _format_epoch_s(datetime.now(timezone.utc).timestamp())

        # Rule evaluation specs: (rule, metric_name, value, threshold, is_breach, baseline)
        specs = []

        # R1: Rebuffer ratio
        rebuf = float(window_row.get("rebuffer_ratio") or 0.0)
        specs.append((
            "R1",
            "rebuffer_ratio",
            rebuf,
            self.rebuf_thresh,
            rebuf > self.rebuf_thresh,
            None,
        ))

        # R2: QoE score drop vs baseline lag
        lag_min = window_end_min - self.baseline_lag_min
        baseline_qoe = self.qoe_history.get((grain, dimkey, lag_min))
        if baseline_qoe is not None:
            drop = baseline_qoe - qoe_score
            specs.append((
                "R2",
                "qoe_score_drop",
                drop,
                self.qoe_drop_points,
                drop >= self.qoe_drop_points,
                baseline_qoe,
            ))

        # R3: VSF rate
        vsf = float(window_row.get("vsf_rate") or 0.0)
        specs.append((
            "R3",
            "vsf_rate",
            vsf,
            self.vsf_thresh,
            vsf > self.vsf_thresh,
            None,
        ))

        # R4: Error rate
        err_rate = float(window_row.get("error_rate") or 0.0)
        specs.append((
            "R4",
            "error_rate",
            err_rate,
            self.error_thresh,
            err_rate > self.error_thresh,
            None,
        ))

        for rule, metric_name, val, thresh, breached, base_val in specs:
            state = self._get_state(rule, grain, dimkey)

            if breached:
                state.consecutive_breaches += 1
                state.consecutive_healthy = 0

                # Check if we should open a new alert
                if state.active_alert is None and state.consecutive_breaches >= self.consecutive_win:
                    in_cooldown = False
                    if state.last_resolved_epoch_s and (window_end_s - state.last_resolved_epoch_s) < (self.cooldown_min * 60):
                        in_cooldown = True

                    if not in_cooldown:
                        # Open alert
                        hasher = hashlib.sha1(f"{rule}:{grain}:{dimkey}:{window_end_iso}".encode())
                        aid = f"alt-{hasher.hexdigest()[:12]}"
                        severity = "critical" if val > (thresh * self.crit_mult) else "warn"

                        alert_rec = {
                            "alert_id": aid,
                            "rule": rule,
                            "severity": severity,
                            "grain": grain,
                            "dimkey": dimkey,
                            "metric": metric_name,
                            "value": round(val, 4),
                            "threshold": round(thresh, 4),
                            "baseline": round(base_val, 2) if base_val is not None else None,
                            "status": "open",
                            "opened_at": now_iso,
                            "resolved_at": None,
                            "window_end": window_end_iso,
                            "run_id": run_id,
                            "row_key": alert_key(window_end_min, aid),
                        }
                        state.active_alert = alert_rec
                        emitted.append(alert_rec)
            else:
                state.consecutive_healthy += 1
                state.consecutive_breaches = 0

                # Check if we should resolve an open alert
                if state.active_alert is not None and state.consecutive_healthy >= self.consecutive_win:
                    resolved_alert = dict(state.active_alert)
                    resolved_alert["status"] = "resolved"
                    resolved_alert["resolved_at"] = now_iso
                    resolved_alert["value"] = round(val, 4)
                    resolved_alert["window_end"] = window_end_iso
                    resolved_alert["row_key"] = alert_key(window_end_min, resolved_alert["alert_id"])

                    state.active_alert = None
                    state.last_resolved_epoch_s = window_end_s
                    emitted.append(resolved_alert)

        return emitted


def process_alerts(
    window_rows: List[Dict[str, Any]],
    evaluator: Optional[AlertEvaluator] = None,
    warehouse_dir: Optional[str | Path] = None,
    config: Optional[Any] = None,
    run_id: str = "run-default",
) -> List[Dict[str, Any]]:
    """Evaluate alerts on a collection of window rows and sink to HBase & warehouse."""
    ev = evaluator or AlertEvaluator(config=config)
    all_alerts: List[Dict[str, Any]] = []

    for row in window_rows:
        alerts = ev.evaluate_window(row, run_id=run_id)
        all_alerts.extend(alerts)

    if all_alerts:
        cfg = config or get_config()
        h_cfg = cfg.get("hbase", {})
        write_alerts_to_hbase(
            all_alerts,
            host=h_cfg.get("host", "hbase"),
            port=int(h_cfg.get("port", 9090)),
        )
        if warehouse_dir:
            write_alerts(all_alerts, warehouse_dir=warehouse_dir)

    return all_alerts
