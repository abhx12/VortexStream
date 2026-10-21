"""Fault injection engine and ground-truth fault ledger tracker."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Tuple


class FaultEngine:
    """Evaluates and applies fault effects to telemetry and network events."""

    def __init__(
        self,
        faults_config: List[Dict[str, Any]],
        slice_start_dt: datetime,
        time_shift: float,
        run_id: str,
        ledger_path: Optional[Path] = None,
        seed: int = 42,
    ):
        self.faults = faults_config
        self.slice_start_dt = slice_start_dt
        self.time_shift = time_shift
        self.run_id = run_id
        self.ledger_path = ledger_path
        self.rng = random.Random(seed)
        self.suppressed_sessions: set[str] = set()
        self.abandoned_sessions: set[str] = set()

        if self.ledger_path:
            self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_ledger()

    def _write_ledger(self) -> None:
        """Write ground-truth fault ledger to faults.jsonl."""
        if not self.ledger_path:
            return

        with open(self.ledger_path, "a", encoding="utf-8") as f:
            for fault in self.faults:
                start_min = fault.get("start_min", 0)
                dur_min = fault.get("duration_min", 0)

                # Time in simulated wall clock
                start_epoch = self.slice_start_dt.timestamp() + (start_min * 60) + self.time_shift
                end_epoch = start_epoch + (dur_min * 60)

                start_iso = datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat()
                end_iso = datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat()

                ledger_entry = {
                    "fault_id": fault.get("id"),
                    "type": fault.get("type"),
                    "selector": fault.get("selector", {}),
                    "start_ts": start_iso,
                    "end_ts": end_iso,
                    "params": fault.get("effects", {}),
                    "run_id": self.run_id,
                }
                f.write(json.dumps(ledger_entry) + "\n")

    def _matches_selector(self, event: Dict[str, Any], selector: Dict[str, Any]) -> bool:
        """Check if event dimensions match selector dictionary."""
        for k, v in selector.items():
            if event.get(k) != v:
                return False
        return True

    def _is_in_fault_window(self, event_dt: datetime, fault: Dict[str, Any]) -> bool:
        """Check if event timestamp is within fault [start, end) data window."""
        start_min = fault.get("start_min", 0)
        dur_min = fault.get("duration_min", 0)

        data_offset_s = (event_dt.timestamp() - self.slice_start_dt.timestamp())
        start_s = start_min * 60.0
        end_s = start_s + (dur_min * 60.0)

        return start_s <= data_offset_s < end_s

    def process_network_event(self, event: Dict[str, Any], event_dt: datetime) -> Dict[str, Any]:
        """Apply network impairments during active faults."""
        ev = dict(event)
        for fault in self.faults:
            if not self._is_in_fault_window(event_dt, fault):
                continue
            if not self._matches_selector(ev, fault.get("selector", {})):
                continue

            effects = fault.get("effects", {})
            if "throughput_mult" in effects and ev.get("throughput_kbps") is not None:
                ev["throughput_kbps"] = max(10.0, ev["throughput_kbps"] * effects["throughput_mult"])
            if "rtt_mult" in effects and ev.get("rtt_ms") is not None:
                ev["rtt_ms"] = ev["rtt_ms"] * effects["rtt_mult"]
            if "loss_add_pct" in effects:
                current_loss = ev.get("packet_loss_pct", 0.0) or 0.0
                ev["packet_loss_pct"] = min(100.0, current_loss + effects["loss_add_pct"])

        return ev

    def process_playback_event(
        self, event: Dict[str, Any], event_dt: datetime
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """Apply playback impairments: VSF, stalls, errors, early abandonment."""
        ev = dict(event)
        extra_events: List[Dict[str, Any]] = []
        etype = ev.get("event_type")
        sid = ev.get("session_id", "")

        # If session was suppressed (VSF), drop subsequent first_frame
        if etype == "first_frame" and sid in self.suppressed_sessions:
            return None, []

        # If session already ended by early abandonment, drop subsequent events
        if sid in self.abandoned_sessions and etype != "session_end":
            return None, []

        for fault in self.faults:
            if not self._is_in_fault_window(event_dt, fault):
                continue
            if not self._matches_selector(ev, fault.get("selector", {})):
                continue

            effects = fault.get("effects", {})

            # 1. Video Start Failure (suppress first_frame)
            if etype == "play_request" and "vsf_prob" in effects:
                if self.rng.random() < effects["vsf_prob"]:
                    self.suppressed_sessions.add(sid)

            # 2. Injected Stalls & Bitrate downshift on heartbeat
            if etype == "heartbeat":
                if "stall_prob_per_heartbeat" in effects:
                    if self.rng.random() < effects["stall_prob_per_heartbeat"]:
                        stall_range = effects.get("stall_ms_range", [1000, 5000])
                        stall_duration = self.rng.uniform(stall_range[0], stall_range[1])

                        # Create synthetic buffer_start + buffer_end
                        b_start = dict(ev)
                        b_start["event_id"] = f"{ev['event_id']}-fs"
                        b_start["event_type"] = "buffer_start"
                        b_start["buffer_ms"] = 50.0

                        b_end = dict(ev)
                        b_end["event_id"] = f"{ev['event_id']}-fe"
                        b_end["event_type"] = "buffer_end"
                        b_end["buffer_ms"] = 1200.0
                        b_end["stall_ms"] = stall_duration

                        extra_events.extend([b_start, b_end])

                        # Check for early abandonment after stall
                        if "early_abandon_prob" in effects:
                            if self.rng.random() < effects["early_abandon_prob"]:
                                self.abandoned_sessions.add(sid)
                                s_end = dict(ev)
                                s_end["event_id"] = f"{ev['event_id']}-fa"
                                s_end["event_type"] = "session_end"
                                extra_events.append(s_end)

                if "bitrate_downshift_steps" in effects and ev.get("bitrate_kbps"):
                    ev["bitrate_kbps"] = max(300, int(ev["bitrate_kbps"] * 0.5))

            # 3. Injected Errors
            if "error_prob_per_heartbeat" in effects and etype == "heartbeat":
                if self.rng.random() < effects["error_prob_per_heartbeat"]:
                    err_codes = effects.get("error_codes", ["NET_TIMEOUT", "CDN_5XX", "DECODE_ERR"])
                    code = self.rng.choice(err_codes)
                    err_ev = dict(ev)
                    err_ev["event_id"] = f"{ev['event_id']}-err"
                    err_ev["event_type"] = "error"
                    err_ev["error_code"] = code
                    extra_events.append(err_ev)

        return ev, extra_events
