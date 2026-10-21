"""Run manifest tracker and emitter counts serializer."""

from __future__ import annotations


class RunManifest:
    """Records event emission counts, injected faults, and metadata."""

    def __init__(self, run_id: str):
        self.run_id = run_id
