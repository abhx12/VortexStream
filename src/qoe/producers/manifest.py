"""Run manifest serializer for stream accounting and reconciliation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


from qoe.common.config import get_topic


class RunManifest:
    """Tracks counts per topic of emitted events, faults, and chaos injections."""

    def __init__(self, run_id: str, speed: float = 10.0, seed: int = 42):
        self.run_id = run_id
        self.speed = speed
        self.seed = seed
        self.counts: Dict[str, Dict[str, int]] = {
            get_topic("playback_events"): {"emitted": 0, "duplicates": 0, "late": 0, "invalid": 0, "dirty": 0},
            get_topic("network_metrics"): {"emitted": 0, "duplicates": 0, "late": 0, "invalid": 0, "dirty": 0},
            get_topic("errors"): {"emitted": 0, "duplicates": 0, "late": 0, "invalid": 0, "dirty": 0},
        }
        self.first_ts: Optional[str] = None
        self.last_ts: Optional[str] = None

    def record_event(
        self,
        topic: str,
        ts: str,
        is_duplicate: bool = False,
        is_late: bool = False,
        is_invalid: bool = False,
        is_dirty: bool = False,
    ) -> None:
        """Record an emitted event and any injected data quality tags."""
        if topic not in self.counts:
            self.counts[topic] = {"emitted": 0, "duplicates": 0, "late": 0, "invalid": 0, "dirty": 0}

        self.counts[topic]["emitted"] += 1
        if is_duplicate:
            self.counts[topic]["duplicates"] += 1
        if is_late:
            self.counts[topic]["late"] += 1
        if is_invalid:
            self.counts[topic]["invalid"] += 1
        if is_dirty:
            self.counts[topic]["dirty"] += 1

        if self.first_ts is None:
            self.first_ts = ts
        self.last_ts = ts

    def save(self, manifests_dir: str | Path = "data/manifests") -> Path:
        """Write manifest file run-<id>.json."""
        out_dir = Path(manifests_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"run-{self.run_id}.json"

        data = {
            "run_id": self.run_id,
            "speed": self.speed,
            "seed": self.seed,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "topics": self.counts,
        }

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        return out_file
