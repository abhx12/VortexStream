"""Data-quality chaos injector: duplicate, late, invalid, and dirty dimensions."""

from __future__ import annotations

import copy
import random
from typing import Any, Dict, List, Optional, Tuple


class ChaosEngine:
    """Injects synthetic data-quality issues into live stream events."""

    def __init__(self, chaos_config: Optional[Dict[str, Any]] = None, seed: int = 42):
        if chaos_config is None:
            chaos_config = {}

        self.duplicate_rate = float(chaos_config.get("duplicate_rate", 0.005))
        self.late_rate = float(chaos_config.get("late_rate", 0.005))
        self.late_delay_range = chaos_config.get("late_delay_s", [20, 90])
        self.invalid_rate = float(chaos_config.get("invalid_rate", 0.003))
        self.dirty_dims_rate = float(chaos_config.get("dirty_dims_rate", 0.02))

        self.invalid_kinds = chaos_config.get("invalid_kinds", {
            "negative_buffer": 0.25,
            "missing_session_id": 0.2,
            "impossible_bitrate": 0.2,
            "bad_ts": 0.2,
            "missing_event_type": 0.15,
        })

        self.rng = random.Random(seed)

    def process_event(self, event: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], bool, bool, bool]:
        """
        Process event through chaos engine.
        Returns:
            (main_event, optional_duplicate_or_late_event, is_late, is_invalid, is_dirty)
        """
        ev = dict(event)
        extra_event: Optional[Dict[str, Any]] = None
        is_late = False
        is_invalid = False
        is_dirty = False

        # 1. Invalid payload injection
        if self.rng.random() < self.invalid_rate:
            is_invalid = True
            kind = self.rng.choice(list(self.invalid_kinds.keys()))
            if kind == "negative_buffer":
                ev["buffer_ms"] = -500.0
            elif kind == "missing_session_id":
                ev["session_id"] = ""
            elif kind == "impossible_bitrate":
                ev["bitrate_kbps"] = 99999999
            elif kind == "bad_ts":
                ev["ts"] = "invalid-bad-timestamp"
            elif kind == "missing_event_type":
                ev["event_type"] = ""

        # 2. Dirty dimensions injection
        if not is_invalid and self.rng.random() < self.dirty_dims_rate:
            is_dirty = True
            dirty_choice = self.rng.choice(["device", "isp", "city"])
            if dirty_choice == "device" and ev.get("device"):
                ev["device"] = ev["device"].upper() + " "
            elif dirty_choice == "isp" and ev.get("isp") == "Airtel":
                ev["isp"] = "bharti airtel"
            elif dirty_choice == "city" and ev.get("city"):
                ev["city"] = ev["city"].lower() + " "

        # 3. Duplicate injection
        if self.rng.random() < self.duplicate_rate:
            # Emit exact same event_id
            extra_event = copy.deepcopy(ev)

        # 4. Late arrival injection
        if self.rng.random() < self.late_rate:
            is_late = True

        return ev, extra_event, is_late, is_invalid, is_dirty
